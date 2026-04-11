# -*- coding: utf-8 -*-

from optimizer_app.bazarr_client import BazarrClient
from optimizer_app.config_service import ConfigService
from optimizer_app.db import SQLiteStore
from optimizer_app.engine import ProcessingEngine
from optimizer_app.http_client import HttpClient
from optimizer_app.logging_utils import logger, setup_logging
from optimizer_app.radarr_client import RadarrClient
from optimizer_app.state_manager import StateManager
from optimizer_app.web import create_web_app
from optimizer_app.worker import WorkerManager


def run_app() -> None:
    # phase 1: env defaults only for resolving DB path + initial bootstrap
    from optimizer_app.config import load_config

    env_cfg = load_config()
    setup_logging(env_cfg.log_level)

    store = SQLiteStore(env_cfg.db_path)
    config_service = ConfigService(store)
    cfg = config_service.get_runtime_config()

    # refresh logger level from DB-backed config
    setup_logging(cfg.log_level)

    http = HttpClient(
        timeout=cfg.http_timeout,
        retries=cfg.http_retries,
        backoff_seconds=cfg.http_backoff_seconds,
        verify_ssl=cfg.verify_ssl,
        user_agent=cfg.user_agent,
    )
    state = StateManager(cfg.db_path, cfg, store)
    radarr = RadarrClient(cfg, http)
    bazarr = BazarrClient(cfg, http)
    engine = ProcessingEngine(cfg, state, bazarr, radarr)
    worker = WorkerManager(engine)

    app = create_web_app(config_service, worker, engine, radarr)
    logger.info("Web console starting on %s:%s", cfg.web_host, cfg.web_port)
    ready_for_worker = bool(cfg.radarr_url and cfg.radarr_api_key and cfg.bazarr_url)
    if cfg.worker_auto_start and config_service.onboarding_completed() and ready_for_worker:
        worker.start()
    app.run(host=cfg.web_host, port=int(cfg.web_port), debug=False, use_reloader=False)
