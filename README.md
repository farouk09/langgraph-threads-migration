# LangGraph Threads Migration Tool

A Python tool to export, backup, and migrate threads with full checkpoint history between LangGraph Cloud deployments.

## Why This Tool?

LangGraph Cloud stores your conversation threads and checkpoints, but there's no built-in way to:
- **Backup your data** before deleting a deployment
- **Migrate conversations** between environments (prod → staging, or across orgs)
- **Store threads in your own database** for analytics or compliance
- **Download conversation history** as JSON for processing

This tool solves all of these problems.

## Features

- **Export to JSON** — Streaming writes, memory-efficient even for multi-GB exports
- **Export to PostgreSQL** — Store threads in your own database with proper schema
- **Migrate between deployments** — Full migration with supersteps (preserves checkpoint chains) and automatic legacy fallback
- **Concurrent fetching** — Configurable parallelism (default: 5 threads) for fast exports
- **Per-page retry** — Automatic retries with exponential backoff on paginated API calls
- **History pagination** — Correct cursor format (`{"configurable": {"checkpoint_id": ...}}`) for complete checkpoint retrieval
- **Legacy import fix** — `--legacy-terminal-node` sets `next=[]` on threads imported via fallback mode
- **Rich progress bars** — Real-time progress with thread counts, elapsed time, and per-thread details
- **Metadata filtering** — Export only threads matching specific metadata (e.g., by workspace)
- **Dry-run & test modes** — Preview changes or test with a single thread before full operations
- **Cross-org support** — Separate API keys for source and target deployments

## Installation

```bash
git clone https://github.com/farouk09/langgraph-threads-migration.git
cd langgraph-threads-migration

# Using uv (recommended)
uv venv && source .venv/bin/activate
uv pip install -r requirements.txt

# Or using pip
pip install -r requirements.txt
```

## Configuration

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```bash
# Required: LangSmith API key (shared fallback for source and target)
LANGSMITH_API_KEY=lsv2_sk_your_api_key_here

# For cross-org migration: separate keys override the shared key
# LANGSMITH_SOURCE_API_KEY=lsv2_sk_source_org_key
# LANGSMITH_TARGET_API_KEY=lsv2_sk_target_org_key

# Optional: PostgreSQL connection URL (for --export-postgres)
DATABASE_URL=postgresql://user:password@localhost:5432/dbname
```

> **Note**: Use a Service Key (`lsv2_sk_...`) from [smith.langchain.com](https://smith.langchain.com/) → Settings → API Keys

## Usage

### Export to JSON

```bash
python migrate_threads.py \
  --source-url https://my-deployment.langgraph.app \
  --export-json threads_backup.json
```

### Export to PostgreSQL

```bash
python migrate_threads.py \
  --source-url https://my-deployment.langgraph.app \
  --export-postgres
```

### Full migration (export + import + validate)

```bash
# Same org (shared API key)
python migrate_threads.py \
  --source-url https://old-deploy.langgraph.app \
  --target-url https://new-deploy.langgraph.app \
  --full

# Cross-org (separate API keys)
python migrate_threads.py \
  --source-url https://org1.langgraph.app \
  --target-url https://org2.langgraph.app \
  --source-api-key lsv2_sk_source... \
  --target-api-key lsv2_sk_target... \
  --full
```

### Import from JSON

```bash
python migrate_threads.py \
  --target-url https://my-deployment.langgraph.app \
  --import-json threads_backup.json
```

### Import with legacy terminal node fix

When importing threads that fall back to legacy mode (no supersteps), the thread's `next` field may point to the graph's entry node instead of being empty. Use `--legacy-terminal-node` to specify your graph's terminal node, which sets `next=[]` so threads are continuable:

```bash
python migrate_threads.py \
  --target-url https://my-deployment.langgraph.app \
  --import-json backup.json \
  --legacy-terminal-node "MyLastNode.after_handler"
```

### Filter by metadata

```bash
# Export threads for a specific workspace
python migrate_threads.py \
  --source-url https://my-deployment.langgraph.app \
  --export-json workspace_4.json \
  --metadata-filter '{"workspace_id": 4}'
```

### Test with a single thread first

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
| `--api-key` | Shared API key fallback (or `LANGSMITH_API_KEY` env var) |
| `--source-api-key` | Source API key for cross-org (or `LANGSMITH_SOURCE_API_KEY`) |
| `--target-api-key` | Target API key for cross-org (or `LANGSMITH_TARGET_API_KEY`) |
| `--database-url` | PostgreSQL URL (or `DATABASE_URL` env var) |
| `--export-json FILE` | Export threads to JSON file |
| `--export-postgres` | Export threads to PostgreSQL database |
| `--import-json FILE` | Import threads from JSON file |
| `--full` | Full migration (export + import + validate) |
| `--validate` | Compare source vs target thread counts |
| `--dry-run` | Simulation mode (no changes made) |
| `--test-single` | Process only one thread (for testing) |
| `--metadata-filter JSON` | Filter threads by metadata (JSON object) |
| `--history-limit N` | Max checkpoints per thread (default: all) |
| `--concurrency N` | Parallel thread fetches (default: 5) |
| `--legacy-terminal-node NODE` | Graph terminal node name for legacy imports (sets `next=[]`) |
| `--backup-file FILE` | Backup file path (default: `threads_backup.json`) |

## Import Strategies

The tool uses two import strategies, with automatic fallback:

### 1. Supersteps (preferred)
Replays state changes via `threads.create(supersteps=...)`, preserving the full checkpoint chain. This enables time-travel operations on the target deployment.

### 2. Legacy fallback
When supersteps fail (e.g., due to incompatible serialized objects in old checkpoints), the tool falls back to `create_thread()` + `update_thread_state()`. This preserves the final state but creates only a single checkpoint.

**Known issue with legacy import**: Without `--legacy-terminal-node`, threads imported in legacy mode may have `next=['SomeMiddleware.before_handler']` instead of `next=[]`, making them non-continuable. The flag fixes this by telling LangGraph which node "last ran".

## Key Bug Fixes (vs upstream)

### History pagination cursor format
The LangGraph Cloud API expects the `before` cursor in the format `{"configurable": {"checkpoint_id": "..."}}`, not `{"checkpoint_id": "..."}`. The incorrect format caused silent 500 errors on every page after the first, resulting in incomplete exports. This fix is critical for any thread with more than 100 checkpoints.

### JSON parsing of agent messages
Agent messages may contain unescaped control characters (e.g., `\n` inside JSON strings). The exporter now uses `strict=False` when loading JSON backups to handle these correctly.

## PostgreSQL Schema

When using `--export-postgres`, the tool creates:

```sql
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

## Project Structure

```
langgraph-threads-migration/
├── migrate_threads.py          # CLI entry point (Rich progress bars)
├── langgraph_export/
│   ├── __init__.py
│   ├── client.py               # LangGraph SDK wrapper (per-page retry, cursor fix)
│   ├── migrator.py             # Migration orchestrator (concurrent fetch, supersteps, legacy)
│   ├── models.py               # SQLAlchemy models for PostgreSQL
│   └── exporters/
│       ├── base.py             # Abstract base exporter
│       ├── json_exporter.py    # Streaming JSON export
│       └── postgres_exporter.py # PostgreSQL export
├── requirements.txt
├── .env.example
└── LICENSE
```

## Troubleshooting

| Error | Solution |
|-------|----------|
| `PermissionDeniedError` | Use Service Key (`lsv2_sk_...`), not Personal Token |
| `ConflictError (409)` | Thread already exists on target (automatically skipped) |
| `asyncpg not installed` | Run `pip install asyncpg` for PostgreSQL support |
| `KeyError: 'configurable'` | Server-side error from wrong pagination cursor — fixed in this fork |
| `next != []` after import | Use `--legacy-terminal-node` with your graph's terminal node |

## Important Notes

### Authentication
If your LangGraph deployment uses custom authentication, you may need to temporarily disable it during export.

### Multi-tenancy
The tool preserves thread metadata including `owner`, so each user will only see their own threads after migration.

### Rate Limiting
Built-in delays (0.1s) between API calls prevent overload. Failed calls are retried up to 3 times with exponential backoff + jitter.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - see [LICENSE](LICENSE) file for details.
