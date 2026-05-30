import ast
import asyncio
import logging
import os
from pathlib import Path
from typing import Any

import asyncpg
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.server import Settings as FastMCPSettings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("archlens-mcp")

ENV_FILE = Path(".env")


def _detect_env_encoding(path: Path) -> str:
    """Detect BOM-based .env encoding and default to UTF-8 when uncertain."""
    if not path.exists():
        return "utf-8"

    with path.open("rb") as env_file:
        bom = env_file.read(4)

    if bom.startswith((b"\xff\xfe", b"\xfe\xff")):
        return "utf-16"
    if bom.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    return "utf-8"


def _load_environment() -> None:
    """Load DATABASE_URL via python-dotenv and align FastMCP .env decoding."""
    env_encoding = _detect_env_encoding(ENV_FILE)
    FastMCPSettings.model_config["env_file_encoding"] = env_encoding

    try:
        load_dotenv(dotenv_path=ENV_FILE, encoding=env_encoding)
    except UnicodeDecodeError:
        fallback_encoding = "utf-16" if env_encoding != "utf-16" else "utf-8"
        logger.warning(
            "Failed to decode .env as %s, retrying with %s.",
            env_encoding,
            fallback_encoding,
        )
        FastMCPSettings.model_config["env_file_encoding"] = fallback_encoding
        load_dotenv(dotenv_path=ENV_FILE, encoding=fallback_encoding)


_load_environment()
DATABASE_URL = os.getenv("DATABASE_URL")

mcp = FastMCP("ArchLens-MCP")


@mcp.tool()
async def ping() -> str:
    """Simple health-check tool to confirm the MCP server is running."""
    logger.info("Ping tool called.")
    return "Pong! ArchLens-MCP server is active and ready."


@mcp.tool()
async def get_schema_info() -> dict[str, Any]:
    """Fetch table/column/type metadata and explicitly flag PostGIS spatial columns."""
    if not DATABASE_URL:
        message = "DATABASE_URL is not set. Add it to your .env file."
        logger.error(message)
        return {"error": message, "tables": []}

    query = """
        SELECT
            c.table_schema,
            c.table_name,
            c.column_name,
            c.data_type,
            c.udt_name,
            CASE
                WHEN c.udt_name IN ('geometry', 'geography') THEN TRUE
                WHEN c.udt_name IN ('_geometry', '_geography') THEN TRUE
                ELSE FALSE
            END AS is_spatial,
            CASE
                WHEN c.udt_name IN ('geometry', 'geography') THEN c.udt_name
                WHEN c.udt_name IN ('_geometry', '_geography') THEN substring(c.udt_name from 2)
                ELSE NULL
            END AS spatial_type
        FROM information_schema.columns c
        JOIN information_schema.tables t
          ON c.table_schema = t.table_schema
         AND c.table_name = t.table_name
        WHERE t.table_type = 'BASE TABLE'
          AND c.table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY c.table_schema, c.table_name, c.ordinal_position
    """

    conn: asyncpg.Connection | None = None
    try:
        conn = await asyncpg.connect(DATABASE_URL)
    except Exception as exc:
        logger.exception("Database connection failed.")
        return {"error": f"Database connection failed: {exc}", "tables": []}

    try:
        rows = await conn.fetch(query)
        schema_rows = [
            {
                "table_schema": row["table_schema"],
                "table_name": row["table_name"],
                "column_name": row["column_name"],
                "data_type": row["data_type"],
                "udt_name": row["udt_name"],
                "is_spatial": row["is_spatial"],
                "spatial_type": row["spatial_type"],
            }
            for row in rows
        ]
        return {"tables": schema_rows}
    except Exception as exc:
        logger.exception("Schema query failed.")
        return {"error": f"Schema query failed: {exc}", "tables": []}
    finally:
        await conn.close()


def _is_celery_task_decorator(decorator: ast.AST) -> bool:
    """Return True when decorator matches @app.task or @shared_task."""
    target = decorator.func if isinstance(decorator, ast.Call) else decorator

    if isinstance(target, ast.Attribute):
        return (
            target.attr == "task"
            and isinstance(target.value, ast.Name)
            and target.value.id == "app"
        )

    if isinstance(target, ast.Name):
        return target.id == "shared_task"

    return False


def _analyze_celery_jobs_sync(target_directory: str) -> dict[str, Any]:
    target_path = Path(target_directory).expanduser().resolve()

    if not target_path.exists():
        return {
            "error": f"Target directory does not exist: {target_path}",
            "target_directory": str(target_path),
            "tasks": [],
            "skipped_files": [],
        }

    if not target_path.is_dir():
        return {
            "error": f"Target path is not a directory: {target_path}",
            "target_directory": str(target_path),
            "tasks": [],
            "skipped_files": [],
        }

    tasks: list[dict[str, Any]] = []
    skipped_files: list[dict[str, str]] = []

    def _on_walk_error(error: OSError) -> None:
        skipped_files.append(
            {
                "file_path": str(error.filename) if error.filename else "",
                "reason": f"walk_error: {error}",
            }
        )
        logger.warning("Directory walk error for %s: %s", error.filename, error)

    for root, _, files in os.walk(target_path, onerror=_on_walk_error):
        for filename in files:
            if not filename.endswith(".py"):
                continue

            file_path = Path(root) / filename

            try:
                source = file_path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                try:
                    source = file_path.read_text(encoding="utf-8-sig")
                except (OSError, UnicodeDecodeError) as exc:
                    skipped_files.append(
                        {"file_path": str(file_path), "reason": f"read_error: {exc}"}
                    )
                    logger.warning("Skipping file %s due to read error: %s", file_path, exc)
                    continue
            except OSError as exc:
                skipped_files.append(
                    {"file_path": str(file_path), "reason": f"read_error: {exc}"}
                )
                logger.warning("Skipping file %s due to read error: %s", file_path, exc)
                continue

            try:
                tree = ast.parse(source, filename=str(file_path))
            except SyntaxError as exc:
                skipped_files.append(
                    {
                        "file_path": str(file_path),
                        "reason": f"syntax_error: line {exc.lineno}: {exc.msg}",
                    }
                )
                logger.warning("Skipping file %s due to syntax error: %s", file_path, exc)
                continue

            for node in ast.walk(tree):
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue

                if not any(_is_celery_task_decorator(d) for d in node.decorator_list):
                    continue

                tasks.append(
                    {
                        "file_path": str(file_path.resolve()),
                        "function_name": node.name,
                        "docstring": ast.get_docstring(node),
                    }
                )

    return {
        "target_directory": str(target_path),
        "tasks": tasks,
        "task_count": len(tasks),
        "skipped_files": skipped_files,
    }


@mcp.tool()
async def analyze_celery_jobs(target_directory: str) -> dict[str, Any]:
    """Analyze Python files in a directory and list Celery tasks defined with @app.task or @shared_task."""
    return await asyncio.to_thread(_analyze_celery_jobs_sync, target_directory)


if __name__ == "__main__":
    logger.info("Starting ArchLens-MCP...")
    mcp.run(transport="stdio")
