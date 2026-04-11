# -*- coding: utf-8 -*-

import os
import ast
import json
from dataclasses import dataclass, field
from typing import Any, Dict, List

from dotenv import load_dotenv


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _normalize_list_items(items: List[Any]) -> List[str]:
    out: List[str] = []
    for item in items:
        text = str(item).strip()
        text = text.strip().strip("[]").strip().strip("\"'")
        if text:
            out.append(text)
    return out


def parse_list_value(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return _normalize_list_items(raw)
    if isinstance(raw, tuple):
        return _normalize_list_items(list(raw))
    if not isinstance(raw, str):
        return _normalize_list_items([raw])

    text = raw.strip()
    if not text:
        return []

    # JSON list
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return _normalize_list_items(parsed)
        except Exception:
            pass
        # Python repr list
        try:
            parsed = ast.literal_eval(text)
            if isinstance(parsed, (list, tuple)):
                return _normalize_list_items(list(parsed))
        except Exception:
            pass

    # Fallback CSV parsing
    parts = [x.strip() for x in text.split(",") if x.strip()]
    cleaned: List[str] = []
    for part in parts:
        p = part.strip().strip("[]").strip().strip("\"'")
        if p:
            cleaned.append(p)
    return cleaned


@dataclass
class AppConfig:
    db_path: str
    radarr_url: str
    radarr_api_key: str
    bazarr_url: str
    bazarr_api_key: str
    bazarr_api_key_header: str
    poll_seconds: int
    bazarr_grace_seconds: int
    retry_cooldown_seconds: int
    max_followup_attempts: int
    state_file: str
    log_level: str
    http_timeout: int
    http_retries: int
    http_backoff_seconds: int
    verify_ssl: bool
    user_agent: str
    preferred_languages: List[str] = field(default_factory=list)
    good_subtitle_min_score: float = 80.0
    match_similarity_threshold: float = 45.0
    exact_match_only: bool = True
    strict_profile_guard: bool = True
    use_bazarr_score_when_release_missing: bool = True
    treat_file_reference_as_good_when_score_missing: bool = True

    bazarr_mode: str = "manual_api"
    bazarr_movie_lookup_endpoint: str = "/api/movies"
    bazarr_movie_lookup_fallback_endpoints: List[str] = field(default_factory=list)
    bazarr_enable_history_lookup: bool = True
    bazarr_lookup_style: str = "query_param_radarrid"

    bazarr_search_endpoint: str = "/api/movies/subtitles"
    enable_bazarr_search_trigger: bool = False
    enable_bazarr_manual_search_on_poor: bool = True
    bazarr_manual_search_endpoints: List[str] = field(default_factory=list)
    bazarr_manual_search_method: str = "AUTO"
    bazarr_manual_search_wait_seconds: int = 8
    bazarr_manual_search_max_attempts: int = 1
    bazarr_manual_search_retry_cooldown_seconds: int = 1800

    enable_bazarr_providers_release_hint: bool = True
    bazarr_providers_movies_endpoint: str = "/api/providers/movies"

    enable_radarr_release_inspection: bool = True
    enable_radarr_delete_existing_file_on_poor: bool = True
    enable_radarr_movies_search_fallback: bool = False

    radarr_grab_verify_enabled: bool = True
    radarr_grab_verify_timeout_seconds: int = 45
    radarr_grab_verify_poll_seconds: int = 5
    radarr_grab_verify_use_history: bool = False
    web_host: str = "0.0.0.0"
    web_port: int = 8686
    web_secret_key: str = "change-me"
    auth_mode: str = "none"  # none|basic
    auth_username: str = ""
    auth_password_hash: str = ""
    worker_auto_start: bool = True


def load_config() -> AppConfig:
    load_dotenv(dotenv_path=".env", override=False)

    preferred_languages = parse_list_value(os.getenv("PREFERRED_LANGUAGES", "he,heb,en,eng"))
    lookup_fallbacks = parse_list_value(
        os.getenv("BAZARR_MOVIE_LOOKUP_FALLBACK_ENDPOINTS", "/api/movies/wanted,/api/movies/history")
    )
    manual_search_endpoints = parse_list_value(
        os.getenv("BAZARR_MANUAL_SEARCH_ENDPOINTS", "/api/movies/subtitles,/api/movies/manual")
    )

    return AppConfig(
        db_path=os.getenv("DB_PATH", "data/optimizer.db"),
        radarr_url=os.getenv("RADARR_URL", "http://localhost:7878").rstrip("/"),
        radarr_api_key=os.getenv("RADARR_API_KEY", "").strip(),
        bazarr_url=os.getenv("BAZARR_URL", "http://localhost:6767").rstrip("/"),
        bazarr_api_key=os.getenv("BAZARR_API_KEY", "").strip(),
        bazarr_api_key_header=os.getenv("BAZARR_API_KEY_HEADER", "X-Api-Key").strip(),
        poll_seconds=env_int("POLL_SECONDS", 300),
        bazarr_grace_seconds=env_int("BAZARR_GRACE_SECONDS", 900),
        retry_cooldown_seconds=env_int("RETRY_COOLDOWN_SECONDS", 21600),
        max_followup_attempts=env_int("MAX_FOLLOWUP_ATTEMPTS", 1),
        state_file=os.getenv("STATE_FILE", "state.json"),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        http_timeout=env_int("HTTP_TIMEOUT", 20),
        http_retries=env_int("HTTP_RETRIES", 3),
        http_backoff_seconds=env_int("HTTP_BACKOFF_SECONDS", 2),
        verify_ssl=env_bool("VERIFY_SSL", True),
        user_agent=os.getenv("USER_AGENT", "radarr-bazarr-optimizer-oop/1.0"),
        preferred_languages=preferred_languages,
        good_subtitle_min_score=env_float("GOOD_SUBTITLE_MIN_SCORE", 80.0),
        match_similarity_threshold=env_float("MATCH_SIMILARITY_THRESHOLD", 45.0),
        exact_match_only=env_bool("EXACT_MATCH_ONLY", True),
        strict_profile_guard=env_bool("STRICT_PROFILE_GUARD", True),
        use_bazarr_score_when_release_missing=env_bool("USE_BAZARR_SCORE_WHEN_RELEASE_MISSING", True),
        treat_file_reference_as_good_when_score_missing=env_bool(
            "TREAT_FILE_REFERENCE_AS_GOOD_WHEN_SCORE_MISSING", True
        ),
        bazarr_mode=os.getenv("BAZARR_MODE", "manual_api").strip().lower(),
        bazarr_movie_lookup_endpoint=os.getenv("BAZARR_MOVIE_LOOKUP_ENDPOINT", "/api/movies").strip(),
        bazarr_movie_lookup_fallback_endpoints=lookup_fallbacks,
        bazarr_enable_history_lookup=env_bool("BAZARR_ENABLE_HISTORY_LOOKUP", True),
        bazarr_lookup_style=os.getenv("BAZARR_LOOKUP_STYLE", "query_param_radarrid").strip().lower(),
        bazarr_search_endpoint=os.getenv("BAZARR_SEARCH_ENDPOINT", "/api/movies/subtitles").strip(),
        enable_bazarr_search_trigger=env_bool("ENABLE_BAZARR_SEARCH_TRIGGER", False),
        enable_bazarr_manual_search_on_poor=env_bool("ENABLE_BAZARR_MANUAL_SEARCH_ON_POOR", True),
        bazarr_manual_search_endpoints=manual_search_endpoints,
        bazarr_manual_search_method=os.getenv("BAZARR_MANUAL_SEARCH_METHOD", "AUTO").strip().upper(),
        bazarr_manual_search_wait_seconds=env_int("BAZARR_MANUAL_SEARCH_WAIT_SECONDS", 8),
        bazarr_manual_search_max_attempts=env_int("BAZARR_MANUAL_SEARCH_MAX_ATTEMPTS", 1),
        bazarr_manual_search_retry_cooldown_seconds=env_int(
            "BAZARR_MANUAL_SEARCH_RETRY_COOLDOWN_SECONDS", 1800
        ),
        enable_bazarr_providers_release_hint=env_bool("ENABLE_BAZARR_PROVIDERS_RELEASE_HINT", True),
        bazarr_providers_movies_endpoint=os.getenv(
            "BAZARR_PROVIDERS_MOVIES_ENDPOINT", "/api/providers/movies"
        ).strip(),
        enable_radarr_release_inspection=env_bool("ENABLE_RADARR_RELEASE_INSPECTION", True),
        enable_radarr_delete_existing_file_on_poor=env_bool(
            "ENABLE_RADARR_DELETE_EXISTING_FILE_ON_POOR", True
        ),
        enable_radarr_movies_search_fallback=env_bool("ENABLE_RADARR_MOVIES_SEARCH_FALLBACK", False),
        radarr_grab_verify_enabled=env_bool("RADARR_GRAB_VERIFY_ENABLED", True),
        radarr_grab_verify_timeout_seconds=env_int("RADARR_GRAB_VERIFY_TIMEOUT_SECONDS", 45),
        radarr_grab_verify_poll_seconds=env_int("RADARR_GRAB_VERIFY_POLL_SECONDS", 5),
        radarr_grab_verify_use_history=env_bool("RADARR_GRAB_VERIFY_USE_HISTORY", False),
        web_host=os.getenv("WEB_HOST", "0.0.0.0").strip(),
        web_port=env_int("WEB_PORT", 8686),
        web_secret_key=os.getenv("WEB_SECRET_KEY", "change-me").strip(),
        auth_mode=os.getenv("AUTH_MODE", "none").strip().lower(),
        auth_username=os.getenv("AUTH_USERNAME", "").strip(),
        auth_password_hash=os.getenv("AUTH_PASSWORD_HASH", "").strip(),
        worker_auto_start=env_bool("WORKER_AUTO_START", True),
    )


def config_to_settings_map(config: AppConfig) -> Dict[str, Any]:
    return {
        k: v
        for k, v in config.__dict__.items()
    }


def app_config_from_settings(settings: Dict[str, Any], defaults: AppConfig) -> AppConfig:
    data: Dict[str, Any] = dict(defaults.__dict__)
    for key, value in settings.items():
        if key not in data:
            continue
        default_value = data[key]
        if isinstance(default_value, bool):
            if isinstance(value, bool):
                data[key] = value
            elif isinstance(value, str):
                data[key] = value.strip().lower() in ("1", "true", "yes", "on")
            else:
                data[key] = bool(value)
        elif isinstance(default_value, int):
            try:
                data[key] = int(value)
            except Exception:
                data[key] = default_value
        elif isinstance(default_value, float):
            try:
                data[key] = float(value)
            except Exception:
                data[key] = default_value
        elif isinstance(default_value, list):
            data[key] = parse_list_value(value)
        else:
            data[key] = value
    return AppConfig(**data)
