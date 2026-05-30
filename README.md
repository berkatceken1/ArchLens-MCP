# ArchLens-MCP 🔍🤖

A Model Context Protocol (MCP) server designed to give AI agents (like OpenAI Codex, Cursor, and Claude) deep architectural context into your Python projects.

AI agents are great at writing code, but they often lack the "big picture"—understanding database schemas, spatial data types, and asynchronous job topologies. ArchLens-MCP bridges this gap by securely analyzing your project architecture and feeding it directly to the LLM.

## 🚀 Features

- 🗺️ **PostGIS-Aware Schema Mapping:** Automatically extracts PostgreSQL database schemas, explicitly highlighting spatial data types (`geometry`, `geography`) so AI doesn't make migration mistakes.
- ⚙️ **AST-Based Celery Analysis:** Uses Python's built-in Abstract Syntax Tree (`ast`) to recursively scan directories and discover background jobs (`@app.task`, `@shared_task`) without executing any code.
- 🤖 **Auto AGENTS.md Generation:** Compiles the database and background job insights into a structured `AGENTS.md` file, providing instant context to any AI coding assistant.

## 🛠️ Quick Start

### Installation

1. Clone the repository and navigate to the directory:
   ```bash
   git clone [https://github.com/berkatceken1/ArchLens-MCP.git](https://github.com/berkatceken1/ArchLens-MCP.git)
   cd ArchLens-MCP
   ```
2. Set up a virtual environment and install dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: .\venv\Scripts\activate
   pip install mcp asyncpg python-dotenv
   ```
3. Create a `.env` file for your target database:
   ```env
   DATABASE_URL=postgresql://user:password@localhost:5432/your_db
   ```

### Running the MCP Server

```bash
python server.py
```

The server will start listening on `stdio`, ready to be attached to your favorite MCP-compatible AI agent workspace.

## 🤝 Contributing

Contributions, issues, and feature requests are welcome! ArchLens-MCP aims to be the standard context-provider for open-source AI development.
