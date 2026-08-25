

# LangGraph Threads Export Tool

A Python tool to export threads, checkpoints, and conversation history from LangGraph Cloud deployments. Save your data to JSON files, PostgreSQL databases, or migrate directly to another deployment.

## Why This Tool?

LangGraph Cloud stores your conversation threads and checkpoints, but there's no built-in way to:
- **Backup your data** before deleting a deployment
- **Migrate conversations** between environments (prod → dev)
- **Store threads in your own database** for analytics or compliance
- **Download conversation history** as JSON for processing

This tool solves all of these problems.

## Features

- **Export to JSON** - Download all threads and checkpoints as a backup file
- **Export to PostgreSQL** - Store threads in your own database with proper schema
- **Migrate between deployments** - Transfer threads from one LangGraph Cloud deployment to another
- **Preserve everything** - Thread IDs, metadata (including `owner` for multi-tenancy), checkpoints, and conversation history
- **Test mode** - Export/migrate a single thread first to verify everything works
- **Dry-run mode** - Preview changes without making any modifications
- **Progress tracking** - Real-time progress bars and detailed summaries

## Use Cases

| Scenario | Command |
|----------|---------|
| Backup before deleting deployment | `--export-json backup.json` |
| Cost optimization (expensive → cheaper deployment) | `--full` |
| Store in your own PostgreSQL | `--export-postgres` |
| Environment migration (staging → prod) | `--migrate` |
| Disaster recovery | `--import-json backup.json` |

## Installation

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/langgraph-threads-migration.git
cd langgraph-threads-migration

# Using uv (recommended)
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt

# Or using pip
pip install -r requirements.txt
```

## Configuration

Create a `.env` file:

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```bash
# Required: LangSmith API key
LANGSMITH_API_KEY=lsv2_sk_your_api_key_here

# Optional: PostgreSQL connection URL (for --export-postgres)
DATABASE_URL=postgresql://user:password@localhost:5432/dbname
```

> **Note**: Use a Service Key (`lsv2_sk_...`) from [smith.langchain.com](https://smith.langchain.com/) → Settings → API Keys

## Usage

### Export to JSON file

Download all threads and checkpoints as a backup:

```bash
python migrate_threads.py \
  --source-url https://my-deployment.langgraph.app \
  --export-json threads_backup.json
```

### Export to PostgreSQL

Store threads in your own database:

```bash
python migrate_threads.py \
  --source-url https://my-deployment.langgraph.app \
  --export-postgres \
  --database-url postgresql://user:password@localhost:5432/dbname
```

This creates two tables:
- `langgraph_threads` - Thread metadata and current state
- `langgraph_checkpoints` - Full checkpoint history

### Migrate between deployments

Transfer all threads from one deployment to another:

```bash
python migrate_threads.py \
  --source-url https://my-prod.langgraph.app \
  --target-url https://my-dev.langgraph.app \
  --full
```

### Import from JSON

Restore threads from a backup file:

```bash
python migrate_threads.py \
  --target-url https://my-deployment.langgraph.app \
  --import-json threads_backup.json
```

### Test with a single thread first

Always recommended before a full operation:

```bash
python migrate_threads.py \
  --source-url https://my-prod.langgraph.app \
  --export-json test.json \
  --test-single
```

## Command Reference

| Argument | Description |
|----------|-------------|
| `--source-url` | Source LangGraph Cloud deployment URL |
| `--target-url` | Target LangGraph Cloud deployment URL |
| `--api-key` | LangSmith API key (or set in `.env`) |
| `--database-url` | PostgreSQL URL (or set in `.env`) |
| `--export-json FILE` | Export threads to JSON file |
| `--export-postgres` | Export threads to PostgreSQL database |
| `--import-json FILE` | Import threads from JSON file |
| `--migrate` | Migrate threads (export + import) |
| `--full` | Full migration (export + import + validate) |
| `--validate` | Compare source vs target thread counts |
| `--dry-run` | Simulation mode (no changes made) |
| `--test-single` | Process only one thread (for testing) |

## PostgreSQL Schema

When using `--export-postgres`, the tool creates:

```sql
-- Threads table
CREATE TABLE langgraph_threads (
    id SERIAL PRIMARY KEY,
    thread_id VARCHAR(255) UNIQUE NOT NULL,
    metadata JSONB DEFAULT '{}',
    values JSONB DEFAULT '{}',
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    source_url TEXT,
    exported_at TIMESTAMP DEFAULT NOW()
);

-- Checkpoints table
CREATE TABLE langgraph_checkpoints (
    id SERIAL PRIMARY KEY,
    thread_id VARCHAR(255) REFERENCES langgraph_threads(thread_id),
    checkpoint_id VARCHAR(255),
    parent_checkpoint_id VARCHAR(255),
    checkpoint_data JSONB DEFAULT '{}',
    values JSONB DEFAULT '{}',
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP
);
```

## Example Output

```
╭────────────────────────────────────────╮
│ 🔄 LangGraph Threads Export Tool       │
╰────────────────────────────────────────╯

╭─────────────────────────────────────────╮
│ Phase 1: Export threads from source     │
╰─────────────────────────────────────────╯
✓ 66 threads found
✓ JSON backup saved: threads_backup.json
✓ Size: 29.30 MB
✓ Total checkpoints exported: 842
✓ PostgreSQL: 66 threads, 842 checkpoints
```

## Important Notes

### Authentication

If your LangGraph deployment uses custom authentication (e.g., Auth0), you may need to temporarily disable it during export:

```json
// langgraph.json - temporarily set auth to null
{
  "auth": null
}
```

Remember to re-enable authentication after!

### Multi-tenancy

The tool preserves `metadata.owner`, so each user will only see their own threads after migration.

### Rate Limiting

Built-in delays (0.2-0.3s) prevent API overload. For large exports (1000+ threads), consider running during off-peak hours.

## Troubleshooting

| Error | Solution |
|-------|----------|
| `PermissionDeniedError` | Use Service Key (`lsv2_sk_...`), not Personal Token |
| `ConflictError (409)` | Thread already exists (automatically skipped) |
| `asyncpg not installed` | Run `pip install asyncpg` for PostgreSQL support |

## Project Structure

```
langgraph-threads-migration/
├── migrate_threads.py          # CLI entry point
├── langgraph_export/           # Main package
│   ├── __init__.py
│   ├── client.py               # LangGraph SDK wrapper
│   ├── migrator.py             # Thread migration orchestrator
│   ├── models.py               # SQLAlchemy models for PostgreSQL
│   └── exporters/              # Export backends
│       ├── base.py             # Abstract base exporter
│       ├── json_exporter.py    # JSON file export
│       └── postgres_exporter.py # PostgreSQL export
├── requirements.txt
├── .env.example
└── LICENSE
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Acknowledgments

Built for the [LangGraph](https://github.com/langchain-ai/langgraph) community.
