# -*- coding: utf-8 -*-

import logging


def setup_logging(log_level) -> None:
    normalized = str(log_level or "INFO").strip().upper()
    level = getattr(logging, normalized, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )


logger = logging.getLogger("radarr_bazarr_optimizer_oop")
