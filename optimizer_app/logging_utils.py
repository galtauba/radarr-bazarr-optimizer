# -*- coding: utf-8 -*-

import logging


def setup_logging(log_level: str) -> None:
    level = getattr(logging, log_level, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


logger = logging.getLogger("radarr_bazarr_optimizer_oop")

