"""
LangGraph Cloud API Client — with per-page retry and concurrency support.
"""

import asyncio
import logging
import random
from typing import Any, Dict, List, Optional

from langgraph_sdk import get_client

logger = logging.getLogger(__name__)


class LangGraphClient:
    """Client for interacting with LangGraph Cloud API via the official SDK."""

    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self._client = get_client(url=base_url, api_key=api_key)

    async def close(self) -> None:
        pass

    # ── Retry helper ──────────────────────────────────────────────

    @staticmethod
    async def _retry(coro_factory, max_attempts=3, base_delay=0.8, label=""):
        """Retry with exponential backoff + jitter. Returns result or raises."""
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

    # ── Thread operations ─────────────────────────────────────────

    async def search_threads(
        self,
        limit: int = 100,
        offset: int = 0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        kwargs = {"limit": limit, "offset": offset}
        if metadata:
            kwargs["metadata"] = metadata
        threads = await self._client.threads.search(**kwargs)
        return list(threads) if threads else []

    async def get_thread(self, thread_id: str) -> Dict[str, Any]:
        thread = await self._client.threads.get(thread_id)
        return dict(thread) if thread else {}

    async def get_thread_history(
        self,
        thread_id: str,
        limit: int = 100,
        before: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        kwargs: Dict[str, Any] = {"limit": limit}
        if before:
            kwargs["before"] = before
        history = await self._client.threads.get_history(thread_id, **kwargs)
        return list(history) if history else []

    async def get_all_history(
        self,
        thread_id: str,
        limit: Optional[int] = None,
        page_size: int = 100,
    ) -> List[Dict[str, Any]]:
        """Get complete history with per-page retry."""
        all_history: List[Dict[str, Any]] = []
        before = None
        tid_short = thread_id[:8]

        while True:
            batch_size = page_size
            if limit:
                remaining = limit - len(all_history)
                batch_size = min(page_size, remaining)

            page_num = len(all_history) // page_size + 1

            batch = await self._retry(
                lambda bs=batch_size, b=before: self.get_thread_history(
                    thread_id, limit=bs, before=b,
                ),
                label=f"history({tid_short} p{page_num})",
            )

            if not batch:
                break

            all_history.extend(batch)

            if limit and len(all_history) >= limit:
                break
            if len(batch) < batch_size:
                break

            # Build cursor for next page — server expects {"configurable": {"checkpoint_id": ...}}
            last = batch[-1]
            cp_obj = last.get("checkpoint", {})
            cp_id = cp_obj.get("checkpoint_id") or last.get("checkpoint_id")
            if not cp_id:
                break
            before = {"configurable": {"checkpoint_id": cp_id}}

        return all_history

    # ── Write operations ──────────────────────────────────────────

    async def create_thread(
        self,
        thread_id: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        thread = await self._client.threads.create(
            thread_id=thread_id,
            metadata=metadata or {},
        )
        return dict(thread) if thread else {}

    async def create_thread_with_history(
        self,
        thread_id: str,
        metadata: Optional[Dict[str, Any]] = None,
        supersteps: Optional[List[Dict[str, Any]]] = None,
        if_exists: Optional[str] = None,
    ) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {
            "thread_id": thread_id,
            "metadata": metadata or {},
        }
        if supersteps:
            kwargs["supersteps"] = supersteps
        if if_exists:
            kwargs["if_exists"] = if_exists
        thread = await self._client.threads.create(**kwargs)
        return dict(thread) if thread else {}

    async def update_thread_state(
        self,
        thread_id: str,
        values: Dict[str, Any],
        as_node: Optional[str] = None,
    ) -> Dict[str, Any]:
        result = await self._client.threads.update_state(
            thread_id, values=values, as_node=as_node,
        )
        return dict(result) if result else {}

    async def get_thread_state(self, thread_id: str) -> Dict[str, Any]:
        state = await self._client.threads.get_state(thread_id)
        return dict(state) if state else {}
