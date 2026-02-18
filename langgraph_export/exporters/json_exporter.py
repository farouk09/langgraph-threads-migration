"""
JSON Exporter

Export threads to a JSON file with streaming writes.
Threads are written incrementally to avoid holding all data in memory.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, IO, List, Optional

from langgraph_export.exporters.base import BaseExporter, ExportStats, ThreadData

_JSON_INDENT = 2


class JSONExporter(BaseExporter):
    """
    Export threads to JSON file with streaming writes.

    Writes each thread to disk as it's exported, rather than
    buffering everything in memory. The output format is
    backward-compatible with the original buffered exporter.
    """

    def __init__(
        self,
        source_url: str,
        output_file: str = "threads_backup.json",
    ):
        super().__init__(source_url)
        self.output_file = Path(output_file)
        self._file: Optional[IO[str]] = None
        self._thread_count = 0

    async def connect(self) -> None:
        """Open the file and write the JSON header."""
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self.output_file, "w", encoding="utf-8")
        self._thread_count = 0
        # Start the JSON object with threads array
        self._file.write("{\n")
        self._file.write(f'  "threads": [\n')

    async def export_thread(self, thread: ThreadData) -> None:
        """Write a single thread to the file immediately."""
        if not self._file:
            raise RuntimeError("Exporter not connected. Call connect() first.")
        if self._thread_count > 0:
            self._file.write(",\n")

        thread_json = json.dumps(
            thread.to_dict(), indent=_JSON_INDENT, ensure_ascii=False, default=str,
        )
        # Indent each line of the thread JSON by 4 spaces (inside the array)
        indented = "\n".join(f"    {line}" for line in thread_json.splitlines())
        self._file.write(indented)

        self._thread_count += 1
        self.stats.threads_exported += 1
        self.stats.checkpoints_exported += len(thread.history)

    async def finalize(self) -> None:
        """Close the threads array and write metadata footer."""
        if not self._file:
            return

        # Close the threads array
        if self._thread_count > 0:
            self._file.write("\n")
        self._file.write("  ],\n")

        # Write metadata at the end (known only after all threads are exported)
        self._file.write(f'  "export_date": {json.dumps(datetime.now().isoformat())},\n')
        self._file.write(f'  "source": {json.dumps(self.source_url)},\n')
        self._file.write(f'  "total_threads": {self.stats.threads_exported},\n')
        self._file.write(f'  "total_checkpoints": {self.stats.checkpoints_exported}\n')
        self._file.write("}\n")

        self._file.flush()

    async def close(self) -> None:
        """Close the file handle."""
        if self._file:
            self._file.close()
            self._file = None

    def get_file_size_mb(self) -> float:
        """Get output file size in MB."""
        if self.output_file.exists():
            return self.output_file.stat().st_size / 1024 / 1024
        return 0.0

    @staticmethod
    def load_threads(file_path: str) -> List[ThreadData]:
        """Load threads from JSON file."""
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        data = json.loads(path.read_text(encoding="utf-8"), strict=False)
        threads = data.get("threads", [])
        return [ThreadData.from_dict(t) for t in threads]

    @staticmethod
    def get_export_info(file_path: str) -> Dict[str, Any]:
        """Get export metadata from JSON file."""
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        data = json.loads(path.read_text(encoding="utf-8"), strict=False)
        return {
            "export_date": data.get("export_date"),
            "source": data.get("source"),
            "total_threads": data.get("total_threads", 0),
            "total_checkpoints": data.get("total_checkpoints", 0),
            "file_size_mb": path.stat().st_size / 1024 / 1024,
        }
