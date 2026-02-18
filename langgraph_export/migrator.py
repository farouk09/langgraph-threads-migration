"""
Thread Migrator — concurrent fetch, streaming export, per-page retry.
"""

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from langgraph_sdk.errors import ConflictError, APIStatusError

from langgraph_export.client import LangGraphClient
from langgraph_export.exporters.base import BaseExporter, ExportStats, ThreadData
from langgraph_export.exporters.json_exporter import JSONExporter
from langgraph_export.exporters.postgres_exporter import PostgresExporter

logger = logging.getLogger(__name__)

CONCURRENCY = 5  # max parallel thread fetches


@dataclass
class MigrationProgress:
    """Mutable progress state shared across concurrent tasks."""
    exported: int = 0
    skipped: int = 0
    failed: int = 0
    total_checkpoints: int = 0
    total_threads: int = 0  # set after discovery
    start_time: float = field(default_factory=time.time)
    errors: List[str] = field(default_factory=list)

    @property
    def elapsed(self) -> float:
        return time.time() - self.start_time

    @property
    def rate(self) -> float:
        done = self.exported + self.skipped + self.failed
        return done / self.elapsed if self.elapsed > 0 else 0


class ThreadMigrator:
    """Thread migration and export manager with concurrent fetching."""

    def __init__(
        self,
        source_url: Optional[str] = None,
        target_url: Optional[str] = None,
        api_key: str = "",
        source_api_key: Optional[str] = None,
        target_api_key: Optional[str] = None,
        rate_limit_delay: float = 0.1,
        concurrency: int = CONCURRENCY,
    ):
        self.source_url = source_url
        self.target_url = target_url
        self.source_api_key = source_api_key or api_key
        self.target_api_key = target_api_key or api_key
        self.rate_limit_delay = rate_limit_delay
        self.concurrency = concurrency

        self._source_client: Optional[LangGraphClient] = None
        self._target_client: Optional[LangGraphClient] = None
        self._exporters: List[BaseExporter] = []

    async def __aenter__(self) -> "ThreadMigrator":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def connect(self) -> None:
        if self.source_url:
            self._source_client = LangGraphClient(self.source_url, self.source_api_key)
        if self.target_url:
            self._target_client = LangGraphClient(self.target_url, self.target_api_key)

    async def close(self) -> None:
        if self._source_client:
            await self._source_client.close()
        if self._target_client:
            await self._target_client.close()
        for exporter in self._exporters:
            await exporter.close()

    @staticmethod
    async def _retry(coro_factory, max_attempts=3, base_delay=0.8, label=""):
        last_error = None
        for attempt in range(max_attempts):
            try:
                return await coro_factory()
            except Exception as e:
                last_error = e
                if attempt < max_attempts - 1:
                    delay = base_delay * (2 ** attempt) * (0.7 + random.random() * 0.6)
                    logger.warning(f"Retry {attempt+1}/{max_attempts} {label}: {e} ({delay:.1f}s)")
                    await asyncio.sleep(delay)
        raise last_error

    def add_json_exporter(self, output_file: str = "threads_backup.json") -> JSONExporter:
        exporter = JSONExporter(self.source_url or "", output_file)
        self._exporters.append(exporter)
        return exporter

    def add_postgres_exporter(self, database_url: str) -> PostgresExporter:
        exporter = PostgresExporter(self.source_url or "", database_url)
        self._exporters.append(exporter)
        return exporter

    # ── Discovery: list all thread IDs ────────────────────────────

    async def _discover_thread_ids(
        self,
        limit: Optional[int] = None,
        metadata_filter: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch all thread summaries (ID + metadata only, no history)."""
        if not self._source_client:
            raise RuntimeError("Source URL not configured")

        all_summaries: List[Dict[str, Any]] = []
        offset = 0
        batch_size = 100 if limit is None else min(limit, 100)

        while True:
            batch = await self._retry(
                lambda o=offset: self._source_client.search_threads(
                    limit=batch_size, offset=o, metadata=metadata_filter,
                ),
                label="search_threads",
            )
            if not batch:
                break

            all_summaries.extend(batch)

            if limit and len(all_summaries) >= limit:
                all_summaries = all_summaries[:limit]
                break
            if len(batch) < batch_size:
                break

            offset += len(batch)
            await asyncio.sleep(self.rate_limit_delay)

        return all_summaries

    # ── Fetch single thread (details + history) ───────────────────

    async def _fetch_thread(
        self,
        thread_id: str,
        history_limit: Optional[int] = None,
    ) -> ThreadData:
        """Fetch one thread's details + full history."""
        details = await self._retry(
            lambda: self._source_client.get_thread(thread_id),
            label=f"get({thread_id[:8]})",
        )
        history = await self._source_client.get_all_history(
            thread_id, limit=history_limit,
        )
        return ThreadData(
            thread_id=thread_id,
            metadata=details.get("metadata", {}),
            values=details.get("values", {}),
            history=history,
            created_at=details.get("created_at"),
            updated_at=details.get("updated_at"),
        )

    # ── Fetch all (in-memory, for --full mode) ────────────────────

    async def fetch_all_threads(
        self,
        limit: Optional[int] = None,
        metadata_filter: Optional[Dict[str, Any]] = None,
        history_limit: Optional[int] = None,
        progress_callback: Optional[Callable[[int, str], None]] = None,
    ) -> List[ThreadData]:
        """Fetch all threads concurrently."""
        summaries = await self._discover_thread_ids(limit, metadata_filter)

        if progress_callback:
            progress_callback(0, f"Discovered {len(summaries)} threads, fetching...")

        sem = asyncio.Semaphore(self.concurrency)
        results: List[Optional[ThreadData]] = [None] * len(summaries)
        done_count = 0

        async def fetch_one(idx: int, thread_id: str):
            nonlocal done_count
            async with sem:
                try:
                    results[idx] = await self._fetch_thread(thread_id, history_limit)
                    done_count += 1
                    if progress_callback:
                        cp = len(results[idx].history) if results[idx] else 0
                        progress_callback(done_count, f"Fetched {thread_id[:8]}... ({cp} cp)")
                except Exception as e:
                    done_count += 1
                    logger.warning(f"Skip {thread_id[:8]}: {e}")
                    if progress_callback:
                        progress_callback(done_count, f"Skip {thread_id[:8]}: {e}")

        tasks = [
            fetch_one(i, s.get("thread_id"))
            for i, s in enumerate(summaries)
            if s.get("thread_id")
        ]
        await asyncio.gather(*tasks)

        return [t for t in results if t is not None]

    # ── Streaming export (fetch + write one at a time) ────────────

    async def export_threads(
        self,
        threads: Optional[List[ThreadData]] = None,
        limit: Optional[int] = None,
        metadata_filter: Optional[Dict[str, Any]] = None,
        history_limit: Optional[int] = None,
        progress_callback: Optional[Callable[[int, str], None]] = None,
    ) -> ExportStats:
        """
        Export threads to all configured exporters.
        Streams fetch+write concurrently to minimize memory.
        """
        for exporter in self._exporters:
            await exporter.connect()

        if threads is not None:
            return await self._export_prefetched(threads, progress_callback)

        # Streaming: discover → concurrent fetch → sequential write
        if not self._source_client:
            raise RuntimeError("Source URL not configured")

        summaries = await self._discover_thread_ids(limit, metadata_filter)
        total = len(summaries)

        if progress_callback:
            progress_callback(0, f"Found {total} threads — exporting...")

        sem = asyncio.Semaphore(self.concurrency)
        # Use a queue: fetchers produce, writer consumes
        queue: asyncio.Queue[Optional[ThreadData]] = asyncio.Queue(maxsize=self.concurrency * 2)
        progress = MigrationProgress(total_threads=total)

        async def fetch_worker(thread_id: str):
            async with sem:
                try:
                    td = await self._fetch_thread(thread_id, history_limit)
                    await queue.put(td)
                except Exception as e:
                    progress.failed += 1
                    progress.errors.append(f"{thread_id[:8]}: {e}")
                    logger.warning(f"Fetch failed {thread_id[:8]}: {e}")

        async def writer():
            while True:
                td = await queue.get()
                if td is None:  # sentinel
                    break
                for exporter in self._exporters:
                    await exporter.export_thread(td)
                progress.exported += 1
                progress.total_checkpoints += len(td.history)
                if progress_callback:
                    progress_callback(
                        progress.exported,
                        f"{td.thread_id[:8]} ({len(td.history)} cp) "
                        f"[{progress.exported}/{total}]"
                    )

        # Start writer task
        writer_task = asyncio.create_task(writer())

        # Launch all fetchers
        thread_ids = [s.get("thread_id") for s in summaries if s.get("thread_id")]
        fetch_tasks = [asyncio.create_task(fetch_worker(tid)) for tid in thread_ids]
        await asyncio.gather(*fetch_tasks)

        # Signal writer to stop
        await queue.put(None)
        await writer_task

        for exporter in self._exporters:
            await exporter.finalize()

        return ExportStats(
            threads_exported=progress.exported,
            checkpoints_exported=progress.total_checkpoints,
        )

    async def _export_prefetched(
        self,
        threads: List[ThreadData],
        progress_callback: Optional[Callable[[int, str], None]] = None,
    ) -> ExportStats:
        total_checkpoints = 0
        for i, thread in enumerate(threads):
            for exporter in self._exporters:
                await exporter.export_thread(thread)
            total_checkpoints += len(thread.history)
            if progress_callback:
                progress_callback(i + 1, f"Exported {thread.thread_id[:8]}...")

        for exporter in self._exporters:
            await exporter.finalize()

        return ExportStats(
            threads_exported=len(threads),
            checkpoints_exported=total_checkpoints,
        )

    # ── Supersteps computation ────────────────────────────────────

    @staticmethod
    def _compute_values_delta(
        prev_values: Dict[str, Any],
        curr_values: Dict[str, Any],
    ) -> Dict[str, Any]:
        delta: Dict[str, Any] = {}
        for key, curr_val in curr_values.items():
            prev_val = prev_values.get(key)
            if curr_val == prev_val:
                continue

            if key == "messages":
                prev_ids = {m.get("id") for m in (prev_val or []) if isinstance(m, dict)}
                new_msgs = [m for m in (curr_val or []) if isinstance(m, dict) and m.get("id") not in prev_ids]
                if new_msgs:
                    delta["messages"] = new_msgs
            elif (
                isinstance(curr_val, list)
                and curr_val
                and isinstance(curr_val[0], dict)
                and "id" in curr_val[0]
            ):
                prev_ids = {item.get("id") for item in (prev_val or []) if isinstance(item, dict)}
                new_items = [item for item in curr_val if isinstance(item, dict) and item.get("id") not in prev_ids]
                if new_items:
                    delta[key] = new_items
            else:
                delta[key] = curr_val
        return delta

    @staticmethod
    def _history_to_supersteps(history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not history:
            return []

        chronological = list(reversed(history))
        supersteps: List[Dict[str, Any]] = []

        has_writes = any(
            isinstance((h.get("metadata") or {}).get("writes"), dict) for h in history
        )

        if has_writes:
            for state in chronological:
                metadata = state.get("metadata") or {}
                writes = metadata.get("writes")
                if not writes or not isinstance(writes, dict):
                    continue
                updates = []
                for node_name, node_values in writes.items():
                    if node_values is None:
                        continue
                    updates.append({"values": node_values, "as_node": node_name})
                if updates:
                    supersteps.append({"updates": updates})
        else:
            prev_values: Dict[str, Any] = {}
            for i, state in enumerate(chronological):
                curr_values = state.get("values") or {}
                if i == 0:
                    if curr_values:
                        supersteps.append({
                            "updates": [{"values": curr_values, "as_node": "__start__"}],
                        })
                    prev_values = curr_values
                    continue

                prev_next = chronological[i - 1].get("next", [])
                as_node = prev_next[0] if prev_next else "__unknown__"
                delta = ThreadMigrator._compute_values_delta(prev_values, curr_values)
                if delta:
                    supersteps.append({
                        "updates": [{"values": delta, "as_node": as_node}],
                    })
                prev_values = curr_values

        return supersteps

    # ── Import ────────────────────────────────────────────────────

    async def _import_thread_with_history(self, thread: ThreadData) -> int:
        supersteps = self._history_to_supersteps(thread.history)
        await self._target_client.create_thread_with_history(
            thread_id=thread.thread_id,
            metadata=thread.metadata,
            supersteps=supersteps if supersteps else None,
        )
        return len(supersteps)

    async def _import_thread_legacy(
        self, thread: ThreadData, as_node: Optional[str] = None,
    ) -> None:
        await self._target_client.create_thread(
            thread_id=thread.thread_id,
            metadata=thread.metadata,
        )
        if thread.values:
            await self._target_client.update_thread_state(
                thread_id=thread.thread_id,
                values=thread.values,
                as_node=as_node,
            )

    async def import_threads(
        self,
        threads: List[ThreadData],
        dry_run: bool = False,
        legacy_terminal_node: Optional[str] = None,
        progress_callback: Optional[Callable[[int, str], None]] = None,
    ) -> Dict[str, int]:
        if not self._target_client:
            raise RuntimeError("Target URL not configured")

        results = {"created": 0, "skipped": 0, "failed": 0, "checkpoints": 0}
        use_legacy = False

        for i, thread in enumerate(threads):
            try:
                if dry_run:
                    supersteps = self._history_to_supersteps(thread.history)
                    results["skipped"] += 1
                    if progress_callback:
                        progress_callback(
                            i + 1,
                            f"[DRY-RUN] {thread.thread_id[:8]}... ({len(supersteps)} supersteps)"
                        )
                    continue

                if not use_legacy:
                    try:
                        checkpoints = await self._import_thread_with_history(thread)
                        results["checkpoints"] += checkpoints
                    except APIStatusError as e:
                        if e.status_code in (400, 422):
                            use_legacy = True
                            if progress_callback:
                                progress_callback(i + 1, "supersteps unsupported → legacy mode")
                            await self._import_thread_legacy(thread, as_node=legacy_terminal_node)
                        else:
                            raise
                else:
                    await self._import_thread_legacy(thread, as_node=legacy_terminal_node)

                results["created"] += 1
                if progress_callback:
                    cp_count = len(thread.history)
                    mode = "legacy" if use_legacy else f"{cp_count} cp"
                    progress_callback(i + 1, f"Created {thread.thread_id[:8]}... ({mode})")

            except ConflictError:
                results["skipped"] += 1
                if progress_callback:
                    progress_callback(i + 1, f"Skip (exists) {thread.thread_id[:8]}...")
            except APIStatusError as e:
                results["failed"] += 1
                if progress_callback:
                    progress_callback(i + 1, f"FAIL {thread.thread_id[:8]}: {e}")
            except Exception as e:
                results["failed"] += 1
                if progress_callback:
                    progress_callback(i + 1, f"ERROR {thread.thread_id[:8]}: {e}")

            await asyncio.sleep(self.rate_limit_delay)

        return results

    # ── Validation ────────────────────────────────────────────────

    async def validate_migration(
        self,
        check_history: bool = False,
        sample_thread_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        source_count = 0
        target_count = 0

        if self._source_client:
            threads = await self._source_client.search_threads(limit=10000)
            source_count = len(threads)
        if self._target_client:
            threads = await self._target_client.search_threads(limit=10000)
            target_count = len(threads)

        result: Dict[str, Any] = {
            "source_count": source_count,
            "target_count": target_count,
            "difference": source_count - target_count,
        }

        if check_history and sample_thread_id:
            if self._source_client:
                src_history = await self._source_client.get_all_history(sample_thread_id)
                result["history_source"] = len(src_history)
            if self._target_client:
                tgt_history = await self._target_client.get_all_history(sample_thread_id)
                result["history_target"] = len(tgt_history)

        return result
