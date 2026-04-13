# -*- coding: utf-8 -*-

import threading
import time
from typing import Callable, Dict, Optional

from optimizer_app.engine import ProcessingEngine
from optimizer_app.logging_utils import logger


class WorkerManager:
    def __init__(
        self,
        engine: ProcessingEngine,
        engine_factory: Optional[Callable[[], ProcessingEngine]] = None,
    ) -> None:
        self.engine = engine
        self._engine_factory = engine_factory
        self._thread = None
        self._stop_event = threading.Event()
        self._lock = threading.RLock()
        self._running = False

    def start(self) -> bool:
        with self._lock:
            if self._running:
                return False
            if self._engine_factory is not None:
                self.engine = self._engine_factory()
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._loop, name="optimizer-worker", daemon=True)
            self._running = True
            self._thread.start()
            logger.info("Background worker started")
            return True

    def stop(self) -> bool:
        with self._lock:
            if not self._running:
                return False
            self._stop_event.set()
            thread = self._thread
        if thread:
            thread.join(timeout=5)
        with self._lock:
            self._running = False
            self._thread = None
        logger.info("Background worker stopped")
        return True

    def status(self) -> Dict[str, bool]:
        with self._lock:
            return {"running": bool(self._running)}

    def restart(self) -> bool:
        was_running = self.status().get("running", False)
        if was_running:
            self.stop()
            self.start()
            return True
        return False

    def run_single_cycle(self) -> None:
        self.engine.run_once()
        self.engine.state.save()

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.run_single_cycle()
            except Exception as exc:
                logger.exception("Worker loop error: %s", exc)
            wait_seconds = max(1, int(self.engine.config.poll_seconds))
            for _ in range(wait_seconds):
                if self._stop_event.is_set():
                    break
                time.sleep(1)
