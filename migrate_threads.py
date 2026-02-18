#!/usr/bin/env python3
"""
LangGraph Threads Migration Tool

Export/import threads with full checkpoint history between LangGraph Cloud deployments.
Features: concurrent fetching, streaming JSON export, per-page retry, rich progress.
"""

import argparse
import asyncio
import json
import os
import sys
from typing import Optional

from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    Progress, SpinnerColumn, TextColumn, BarColumn,
    TimeElapsedColumn, MofNCompleteColumn,
)
from rich.table import Table

from langgraph_export import ThreadMigrator
from langgraph_export.exporters import JSONExporter

load_dotenv()
console = Console()


def make_progress(**kwargs) -> Progress:
    """Create a rich Progress bar with consistent columns."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=30),
        MofNCompleteColumn(),
        TextColumn("•"),
        TimeElapsedColumn(),
        TextColumn("•"),
        TextColumn("{task.fields[detail]}"),
        console=console,
        **kwargs,
    )


# ── Export to JSON ────────────────────────────────────────────────

async def run_export_json(
    source_url: str,
    source_api_key: str,
    output_file: str,
    test_single: bool = False,
    metadata_filter: Optional[dict] = None,
    history_limit: Optional[int] = None,
) -> None:
    console.print(Panel.fit(
        "[bold cyan]Export threads to JSON[/bold cyan]",
        border_style="cyan",
    ))
    if metadata_filter:
        console.print(f"  [dim]Filter:[/dim] {metadata_filter}")
    console.print(f"  [dim]Output:[/dim] {output_file}")
    console.print(f"  [dim]Concurrency:[/dim] 5 parallel fetches")
    console.print()

    async with ThreadMigrator(source_url=source_url, source_api_key=source_api_key) as migrator:
        json_exporter = migrator.add_json_exporter(output_file)

        with make_progress() as progress:
            # Phase 1: discover
            discover_task = progress.add_task(
                "[cyan]Discovering threads...", total=None, detail="searching..."
            )

            limit = 1 if test_single else None
            summaries = await migrator._discover_thread_ids(limit, metadata_filter)
            total = len(summaries)
            progress.update(discover_task, completed=total, total=total, detail=f"{total} found")

            # Phase 2: fetch + export (streaming)
            export_task = progress.add_task(
                "[green]Fetching & writing", total=total, detail="starting..."
            )

            exported = 0

            def on_progress(count: int, message: str) -> None:
                nonlocal exported
                exported = count
                progress.update(export_task, completed=count, detail=message)

            stats = await migrator.export_threads(
                limit=limit,
                metadata_filter=metadata_filter,
                history_limit=history_limit,
                progress_callback=on_progress,
            )

        # Summary
        console.print()
        table = Table(title="Export Complete", show_header=False, border_style="green")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", justify="right", style="bold")
        table.add_row("Threads", str(stats.threads_exported))
        table.add_row("Checkpoints", str(stats.checkpoints_exported))
        table.add_row("File", output_file)
        table.add_row("Size", f"{json_exporter.get_file_size_mb():.2f} MB")
        console.print(table)


# ── Export to PostgreSQL ──────────────────────────────────────────

async def run_export_postgres(
    source_url: str,
    source_api_key: str,
    database_url: str,
    output_file: str,
    test_single: bool = False,
    metadata_filter: Optional[dict] = None,
    history_limit: Optional[int] = None,
) -> None:
    console.print(Panel.fit(
        "[bold cyan]Export threads to PostgreSQL + JSON[/bold cyan]",
        border_style="cyan",
    ))

    async with ThreadMigrator(source_url=source_url, source_api_key=source_api_key) as migrator:
        json_exporter = migrator.add_json_exporter(output_file)
        pg_exporter = migrator.add_postgres_exporter(database_url)

        with make_progress() as progress:
            task = progress.add_task("[cyan]Exporting...", total=None, detail="starting...")

            def on_progress(count: int, message: str) -> None:
                progress.update(task, completed=count, detail=message)

            limit = 1 if test_single else None
            stats = await migrator.export_threads(
                limit=limit,
                metadata_filter=metadata_filter,
                history_limit=history_limit,
                progress_callback=on_progress,
            )

        db_stats = await pg_exporter.get_database_stats()
        console.print(f"\n[green]✓[/green] Threads: {stats.threads_exported}")
        console.print(f"[green]✓[/green] JSON: {output_file} ({json_exporter.get_file_size_mb():.2f} MB)")
        console.print(f"[green]✓[/green] PostgreSQL: {db_stats['threads']} threads, {db_stats['checkpoints']} checkpoints")


# ── Import from JSON ──────────────────────────────────────────────

async def run_import_json(
    target_url: str,
    target_api_key: str,
    input_file: str,
    dry_run: bool = False,
    legacy_terminal_node: Optional[str] = None,
) -> None:
    console.print(Panel.fit(
        "[bold green]Import threads from JSON[/bold green]",
        border_style="green",
    ))

    threads = JSONExporter.load_threads(input_file)
    info = JSONExporter.get_export_info(input_file)
    total = len(threads)

    console.print(f"  [dim]Source:[/dim] {info['source']}")
    console.print(f"  [dim]Date:[/dim] {info['export_date']}")
    console.print(f"  [dim]Threads:[/dim] {total}")
    if dry_run:
        console.print("  [yellow]DRY-RUN — no changes[/yellow]")
    console.print()

    async with ThreadMigrator(target_url=target_url, target_api_key=target_api_key) as migrator:
        with make_progress() as progress:
            task = progress.add_task("[green]Importing...", total=total, detail="starting...")

            def on_progress(count: int, message: str) -> None:
                progress.update(task, completed=count, detail=message)

            results = await migrator.import_threads(
                threads, dry_run=dry_run,
                legacy_terminal_node=legacy_terminal_node,
                progress_callback=on_progress,
            )

    console.print()
    table = Table(title="Import Summary", border_style="green")
    table.add_column("Status", style="cyan")
    table.add_column("Count", justify="right", style="bold")
    table.add_row("Created", str(results["created"]))
    table.add_row("Skipped (exists)", str(results["skipped"]))
    table.add_row("Failed", str(results["failed"]))
    table.add_row("Checkpoints (supersteps)", str(results.get("checkpoints", 0)))
    console.print(table)


# ── Full migration ────────────────────────────────────────────────

async def run_full_migration(
    source_url: str,
    target_url: str,
    source_api_key: str,
    target_api_key: str,
    backup_file: str,
    dry_run: bool = False,
    test_single: bool = False,
    metadata_filter: Optional[dict] = None,
    history_limit: Optional[int] = None,
    legacy_terminal_node: Optional[str] = None,
) -> None:
    # Phase 1: Export
    console.print(Panel.fit(
        "[bold cyan]Phase 1 — Export from source[/bold cyan]",
        border_style="cyan",
    ))
    if metadata_filter:
        console.print(f"  [dim]Filter:[/dim] {metadata_filter}")

    async with ThreadMigrator(
        source_url=source_url,
        target_url=target_url,
        source_api_key=source_api_key,
        target_api_key=target_api_key,
    ) as migrator:
        json_exporter = migrator.add_json_exporter(backup_file)

        with make_progress() as progress:
            task = progress.add_task("[cyan]Exporting...", total=None, detail="discovering...")

            limit = 1 if test_single else None
            threads = await migrator.fetch_all_threads(
                limit=limit,
                metadata_filter=metadata_filter,
                history_limit=history_limit,
                progress_callback=lambda c, m: progress.update(task, completed=c, detail=m),
            )

        await json_exporter.connect()
        for thread in threads:
            await json_exporter.export_thread(thread)
        await json_exporter.finalize()

        console.print(f"  [green]✓[/green] {len(threads)} threads → {backup_file} ({json_exporter.get_file_size_mb():.2f} MB)")

        # Phase 2: Import
        console.print(Panel.fit(
            f"[bold green]Phase 2 — Import {len(threads)} threads to target[/bold green]",
            border_style="green",
        ))
        if dry_run:
            console.print("  [yellow]DRY-RUN — no changes[/yellow]")

        with make_progress() as progress:
            task = progress.add_task("[green]Importing...", total=len(threads), detail="starting...")

            results = await migrator.import_threads(
                threads,
                dry_run=dry_run,
                legacy_terminal_node=legacy_terminal_node,
                progress_callback=lambda c, m: progress.update(task, completed=c, detail=m),
            )

        table = Table(title="Import Summary", border_style="green")
        table.add_column("Status", style="cyan")
        table.add_column("Count", justify="right", style="bold")
        table.add_row("Created", str(results["created"]))
        table.add_row("Skipped", str(results["skipped"]))
        table.add_row("Failed", str(results["failed"]))
        table.add_row("Checkpoints", str(results.get("checkpoints", 0)))
        console.print(table)

        # Phase 3: Validate
        console.print(Panel.fit(
            "[bold blue]Phase 3 — Validate[/bold blue]",
            border_style="blue",
        ))
        sample_id = threads[0].thread_id if test_single and threads else None
        validation = await migrator.validate_migration(
            check_history=bool(sample_id),
            sample_thread_id=sample_id,
        )

        table = Table(title="Validation", border_style="blue")
        table.add_column("Metric", style="cyan")
        table.add_column("Source", justify="right")
        table.add_column("Target", justify="right")
        table.add_row("Threads", str(validation["source_count"]), str(validation["target_count"]))
        if "history_source" in validation:
            table.add_row(
                f"History ({sample_id[:8]}...)",
                str(validation["history_source"]),
                str(validation["history_target"]),
            )
        console.print(table)

        if validation["difference"] <= 0:
            console.print("[green]✓ Thread count validated[/green]")
        else:
            console.print(f"[yellow]⚠ {validation['difference']} threads missing on target[/yellow]")


# ── Validate only ─────────────────────────────────────────────────

async def run_validate(
    source_url: str,
    target_url: str,
    source_api_key: str,
    target_api_key: str,
) -> None:
    console.print(Panel.fit("[bold blue]Validate migration[/bold blue]", border_style="blue"))

    async with ThreadMigrator(
        source_url=source_url,
        target_url=target_url,
        source_api_key=source_api_key,
        target_api_key=target_api_key,
    ) as migrator:
        validation = await migrator.validate_migration()

    table = Table(title="Source vs Target", border_style="blue")
    table.add_column("Deployment", style="cyan")
    table.add_column("Threads", justify="right", style="bold")
    table.add_row("Source", str(validation["source_count"]))
    table.add_row("Target", str(validation["target_count"]))
    console.print(table)

    if validation["difference"] <= 0:
        console.print("[green]✓ Migration validated[/green]")
    else:
        console.print(f"[yellow]⚠ {validation['difference']} threads missing on target[/yellow]")


# ── CLI ───────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="LangGraph Threads Migration Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Export to JSON
  python migrate_threads.py --source-url https://my.langgraph.app --export-json backup.json

  # Full migration (same org)
  python migrate_threads.py --source-url https://src.langgraph.app --target-url https://tgt.langgraph.app --full

  # Full migration (cross-org)
  python migrate_threads.py --source-url https://org1.langgraph.app --target-url https://org2.langgraph.app \\
      --source-api-key lsv2_sk_... --target-api-key lsv2_sk_... --full

  # Import from JSON
  python migrate_threads.py --target-url https://dev.langgraph.app --import-json backup.json
        """
    )

    parser.add_argument("--source-url", help="Source LangGraph deployment URL")
    parser.add_argument("--target-url", help="Target LangGraph deployment URL")
    parser.add_argument("--api-key", help="Shared API key (or LANGSMITH_API_KEY)")
    parser.add_argument("--source-api-key", help="Source API key (or LANGSMITH_SOURCE_API_KEY)")
    parser.add_argument("--target-api-key", help="Target API key (or LANGSMITH_TARGET_API_KEY)")
    parser.add_argument("--database-url", help="PostgreSQL URL (or DATABASE_URL)")

    parser.add_argument("--export-json", metavar="FILE", help="Export to JSON file")
    parser.add_argument("--export-postgres", action="store_true", help="Export to PostgreSQL")
    parser.add_argument("--import-json", metavar="FILE", help="Import from JSON file")
    parser.add_argument("--full", action="store_true", help="Full migration (export + import + validate)")
    parser.add_argument("--validate", action="store_true", help="Validate migration")

    parser.add_argument("--backup-file", default="threads_backup.json", help="Backup file path")
    parser.add_argument("--dry-run", action="store_true", help="Simulation mode")
    parser.add_argument("--test-single", action="store_true", help="Test with single thread")
    parser.add_argument("--metadata-filter", metavar="JSON",
                        help="Filter threads by metadata (JSON, e.g. '{\"workspace_id\": 4}')")
    parser.add_argument("--history-limit", type=int, default=None,
                        help="Max checkpoints per thread (default: all)")
    parser.add_argument("--concurrency", type=int, default=5,
                        help="Parallel thread fetches (default: 5)")
    parser.add_argument("--legacy-terminal-node", metavar="NODE",
                        help="Graph terminal node name for legacy imports (sets next=[] via as_node)")

    args = parser.parse_args()

    # Resolve API keys
    shared_key = args.api_key or os.getenv("LANGSMITH_API_KEY")
    source_api_key = args.source_api_key or os.getenv("LANGSMITH_SOURCE_API_KEY") or shared_key
    target_api_key = args.target_api_key or os.getenv("LANGSMITH_TARGET_API_KEY") or shared_key
    database_url = args.database_url or os.getenv("DATABASE_URL")

    # Parse metadata filter
    metadata_filter = None
    if args.metadata_filter:
        try:
            metadata_filter = json.loads(args.metadata_filter)
            if not isinstance(metadata_filter, dict):
                console.print("[red]✗ --metadata-filter must be a JSON object[/red]")
                sys.exit(1)
        except json.JSONDecodeError as e:
            console.print(f"[red]✗ Invalid JSON: {e}[/red]")
            sys.exit(1)

    # Validate keys
    needs_source = bool(args.export_json or args.export_postgres or args.full or args.validate)
    needs_target = bool(args.import_json or args.full or args.validate)

    if needs_source and not source_api_key:
        console.print("[red]✗ Source API key required[/red]")
        sys.exit(1)
    if needs_target and not target_api_key:
        console.print("[red]✗ Target API key required[/red]")
        sys.exit(1)

    # Header
    console.print(Panel.fit(
        "[bold magenta]LangGraph Threads Migration Tool[/bold magenta]",
        border_style="magenta",
    ))
    if needs_source and needs_target and source_api_key != target_api_key:
        console.print("[cyan]ℹ Cross-org: separate API keys[/cyan]")
    if args.test_single:
        console.print("[yellow]⚠ TEST MODE: single thread[/yellow]")
    console.print()

    # Dispatch
    try:
        if args.export_json:
            if not args.source_url:
                console.print("[red]✗ --source-url required[/red]"); sys.exit(1)
            asyncio.run(run_export_json(
                source_url=args.source_url, source_api_key=source_api_key,
                output_file=args.export_json, test_single=args.test_single,
                metadata_filter=metadata_filter, history_limit=args.history_limit,
            ))
        elif args.export_postgres:
            if not args.source_url:
                console.print("[red]✗ --source-url required[/red]"); sys.exit(1)
            if not database_url:
                console.print("[red]✗ --database-url required[/red]"); sys.exit(1)
            asyncio.run(run_export_postgres(
                source_url=args.source_url, source_api_key=source_api_key,
                database_url=database_url, output_file=args.backup_file,
                test_single=args.test_single, metadata_filter=metadata_filter,
                history_limit=args.history_limit,
            ))
        elif args.import_json:
            if not args.target_url:
                console.print("[red]✗ --target-url required[/red]"); sys.exit(1)
            asyncio.run(run_import_json(
                target_url=args.target_url, target_api_key=target_api_key,
                input_file=args.import_json, dry_run=args.dry_run,
                legacy_terminal_node=args.legacy_terminal_node,
            ))
        elif args.full:
            if not args.source_url or not args.target_url:
                console.print("[red]✗ Both --source-url and --target-url required[/red]"); sys.exit(1)
            asyncio.run(run_full_migration(
                source_url=args.source_url, target_url=args.target_url,
                source_api_key=source_api_key, target_api_key=target_api_key,
                backup_file=args.backup_file, dry_run=args.dry_run,
                test_single=args.test_single, metadata_filter=metadata_filter,
                history_limit=args.history_limit,
                legacy_terminal_node=args.legacy_terminal_node,
            ))
        elif args.validate:
            if not args.source_url or not args.target_url:
                console.print("[red]✗ Both URLs required[/red]"); sys.exit(1)
            asyncio.run(run_validate(
                source_url=args.source_url, target_url=args.target_url,
                source_api_key=source_api_key, target_api_key=target_api_key,
            ))
        else:
            parser.print_help()
            sys.exit(1)

    except KeyboardInterrupt:
        console.print("\n[yellow]⚠ Interrupted[/yellow]")
        sys.exit(1)
    except Exception as e:
        console.print(f"\n[red]✗ {e}[/red]")
        import traceback
        console.print(f"[dim]{traceback.format_exc()}[/dim]")
        sys.exit(1)


if __name__ == "__main__":
    main()
