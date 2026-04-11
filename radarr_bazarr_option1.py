#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
radarr_bazarr_option1.py

Flow implemented (Option 1):
1. Detect newly added movies in Radarr
2. Wait until the movie actually has a file (hasFile / movieFile)
3. Wait a grace period so Bazarr can do its normal automatic subtitle work
4. Query Bazarr (adapter, instance-specific) for subtitle status / subtitle candidates
5. Evaluate subtitle/file release exact-match status
6. If not exact:
   - inspect Radarr release candidates
   - exact-grab only when a strict release match exists and is profile-allowed
   - otherwise mark manual_required and stop
7. Persist everything in local JSON state to avoid loops

IMPORTANT:
- Radarr API usage here is based on official API docs and standard command flow. Adjust only if your instance differs. 
- Bazarr API usage here is intentionally isolated because the API shape can vary between versions / deployments.
"""

import json
import logging
import os
import random
import re
import sys
import time
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv


load_dotenv(dotenv_path=".env", override=False)


# =============================================================================
# CONFIG
# =============================================================================

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


class Config:
    # Required
    RADARR_URL = os.getenv("RADARR_URL", "http://localhost:7878").rstrip("/")
    RADARR_API_KEY = os.getenv("RADARR_API_KEY", "").strip()

    BAZARR_URL = os.getenv("BAZARR_URL", "http://localhost:6767").rstrip("/")
    BAZARR_API_KEY = os.getenv("BAZARR_API_KEY", "").strip()

    # Polling / delays
    POLL_SECONDS = env_int("POLL_SECONDS", 300)
    BAZARR_GRACE_SECONDS = env_int("BAZARR_GRACE_SECONDS", 900)  # 15 minutes by default
    RETRY_COOLDOWN_SECONDS = env_int("RETRY_COOLDOWN_SECONDS", 21600)  # 6 hours
    MAX_FOLLOWUP_ATTEMPTS = env_int("MAX_FOLLOWUP_ATTEMPTS", 1)

    # Files
    STATE_FILE = os.getenv("STATE_FILE", "state.json")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

    # HTTP
    HTTP_TIMEOUT = env_int("HTTP_TIMEOUT", 20)
    HTTP_RETRIES = env_int("HTTP_RETRIES", 3)
    HTTP_BACKOFF_SECONDS = env_int("HTTP_BACKOFF_SECONDS", 2)
    VERIFY_SSL = env_bool("VERIFY_SSL", True)
    USER_AGENT = os.getenv("USER_AGENT", "radarr-bazarr-option1/1.0")

    # Subtitle preferences
    PREFERRED_LANGUAGES = [
        x.strip()
        for x in os.getenv("PREFERRED_LANGUAGES", "he,heb,en,eng").split(",")
        if x.strip()
    ]
    GOOD_SUBTITLE_MIN_SCORE = float(os.getenv("GOOD_SUBTITLE_MIN_SCORE", "80"))
    MATCH_SIMILARITY_THRESHOLD = float(os.getenv("MATCH_SIMILARITY_THRESHOLD", "45"))
    EXACT_MATCH_ONLY = env_bool("EXACT_MATCH_ONLY", True)
    STRICT_PROFILE_GUARD = env_bool("STRICT_PROFILE_GUARD", True)
    USE_BAZARR_SCORE_WHEN_RELEASE_MISSING = env_bool("USE_BAZARR_SCORE_WHEN_RELEASE_MISSING", True)
    TREAT_FILE_REFERENCE_AS_GOOD_WHEN_SCORE_MISSING = env_bool(
        "TREAT_FILE_REFERENCE_AS_GOOD_WHEN_SCORE_MISSING", True
    )

    # Bazarr adapter mode
    # "manual_api" = try Bazarr API endpoints you configure
    # "disabled"   = skip Bazarr querying, only rely on fallback logic
    BAZARR_MODE = os.getenv("BAZARR_MODE", "manual_api").strip().lower()

    # IMPORTANT: these are placeholders and may need adaptation for your instance
    BAZARR_API_KEY_HEADER = os.getenv("BAZARR_API_KEY_HEADER", "X-Api-Key").strip()

    # Endpoint to fetch movie details / subtitles state by Radarr movie ID
    # Example placeholders only:
    #   /api/movies
    #   /api/movies/{radarrId}
    #   /api/movies/wanted
    BAZARR_MOVIE_LOOKUP_ENDPOINT = os.getenv("BAZARR_MOVIE_LOOKUP_ENDPOINT", "/api/movies").strip()
    BAZARR_MOVIE_LOOKUP_FALLBACK_ENDPOINTS = [
        x.strip()
        for x in os.getenv("BAZARR_MOVIE_LOOKUP_FALLBACK_ENDPOINTS", "/api/movies/wanted,/api/movies/history").split(",")
        if x.strip()
    ]
    BAZARR_ENABLE_HISTORY_LOOKUP = env_bool("BAZARR_ENABLE_HISTORY_LOOKUP", True)

    # Endpoint to trigger/ask Bazarr to search subtitles for a movie that already exists
    # This may differ in your instance or may not be exposed the way this script expects.
    BAZARR_SEARCH_ENDPOINT = os.getenv("BAZARR_SEARCH_ENDPOINT", "/api/movies/subtitles").strip()
    ENABLE_BAZARR_SEARCH_TRIGGER = env_bool("ENABLE_BAZARR_SEARCH_TRIGGER", False)
    ENABLE_BAZARR_MANUAL_SEARCH_ON_POOR = env_bool("ENABLE_BAZARR_MANUAL_SEARCH_ON_POOR", True)
    BAZARR_MANUAL_SEARCH_ENDPOINTS = [
        x.strip()
        for x in os.getenv(
            "BAZARR_MANUAL_SEARCH_ENDPOINTS",
            "/api/movies/subtitles,/api/movies/manual",
        ).split(",")
        if x.strip()
    ]
    # Supported: POST / GET / AUTO
    BAZARR_MANUAL_SEARCH_METHOD = os.getenv("BAZARR_MANUAL_SEARCH_METHOD", "AUTO").strip().upper()
    BAZARR_MANUAL_SEARCH_WAIT_SECONDS = env_int("BAZARR_MANUAL_SEARCH_WAIT_SECONDS", 8)
    BAZARR_MANUAL_SEARCH_MAX_ATTEMPTS = env_int("BAZARR_MANUAL_SEARCH_MAX_ATTEMPTS", 1)
    BAZARR_MANUAL_SEARCH_RETRY_COOLDOWN_SECONDS = env_int("BAZARR_MANUAL_SEARCH_RETRY_COOLDOWN_SECONDS", 1800)
    ENABLE_BAZARR_PROVIDERS_RELEASE_HINT = env_bool("ENABLE_BAZARR_PROVIDERS_RELEASE_HINT", True)
    BAZARR_PROVIDERS_MOVIES_ENDPOINT = os.getenv("BAZARR_PROVIDERS_MOVIES_ENDPOINT", "/api/providers/movies").strip()

    # Strategy for Bazarr movie lookup:
    # "query_param_radarrid" => GET endpoint?radarrid=123
    # "path_radarrid"        => GET endpoint/123
    BAZARR_LOOKUP_STYLE = os.getenv("BAZARR_LOOKUP_STYLE", "query_param_radarrid").strip().lower()

    # Radarr follow-up behavior
    ENABLE_RADARR_RELEASE_INSPECTION = env_bool("ENABLE_RADARR_RELEASE_INSPECTION", True)
    ENABLE_RADARR_MOVIES_SEARCH_FALLBACK = env_bool("ENABLE_RADARR_MOVIES_SEARCH_FALLBACK", False)
    ENABLE_RADARR_DELETE_EXISTING_FILE_ON_POOR = env_bool("ENABLE_RADARR_DELETE_EXISTING_FILE_ON_POOR", True)
    RADARR_GRAB_VERIFY_ENABLED = env_bool("RADARR_GRAB_VERIFY_ENABLED", True)
    RADARR_GRAB_VERIFY_TIMEOUT_SECONDS = env_int("RADARR_GRAB_VERIFY_TIMEOUT_SECONDS", 45)
    RADARR_GRAB_VERIFY_POLL_SECONDS = env_int("RADARR_GRAB_VERIFY_POLL_SECONDS", 5)
    RADARR_GRAB_VERIFY_USE_HISTORY = env_bool("RADARR_GRAB_VERIFY_USE_HISTORY", False)


# =============================================================================
# LOGGING
# =============================================================================

def setup_logging() -> None:
    level = getattr(logging, Config.LOG_LEVEL, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


logger = logging.getLogger(__name__)


# =============================================================================
# HELPERS
# =============================================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def seconds_since(iso_value: Optional[str]) -> Optional[float]:
    dt = parse_iso(iso_value)
    if not dt:
        return None
    return (utc_now() - dt).total_seconds()


def safe_json_load(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return deepcopy(default)
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        logger.warning("Failed loading JSON from %s: %s", path, exc)
        return deepcopy(default)


def safe_json_dump(path: str, data: Any) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
    os.replace(tmp, path)


def normalize_string(value: str) -> str:
    if not value:
        return ""
    value = value.strip().lower()
    value = value.replace("_", " ")
    value = re.sub(r"\s+", " ", value)
    return value


def normalize_releaseish(value: str) -> str:
    if not value:
        return ""
    value = value.strip()
    value = value.replace("_", ".")
    value = re.sub(r"\s+", ".", value)
    value = re.sub(r"\.+", ".", value)
    return value.strip(".")


def normalize_release_for_exact(value: str) -> str:
    if not value:
        return ""
    value = normalize_releaseish(value).lower()
    value = re.sub(r"[^a-z0-9\.\- ]+", ".", value)
    value = value.replace("-", ".")
    value = re.sub(r"[.\s]+", ".", value)
    return value.strip(".")


def _looks_like_language_label(value: str) -> bool:
    normalized = normalize_string(value)
    if not normalized:
        return False

    language_labels = {
        "he", "heb", "hebrew", "iw",
        "en", "eng", "english",
        "ar", "ara", "arabic",
        "ru", "rus", "russian",
        "es", "spa", "spanish",
        "fr", "fra", "fre", "french",
        "de", "ger", "deu", "german",
        "it", "ita", "italian",
    }
    if normalized in language_labels:
        return True

    if re.fullmatch(r"[a-z ]{2,20}", normalized):
        words = [w for w in normalized.split(" ") if w]
        if 1 <= len(words) <= 2:
            return True

    return False


def _looks_like_release_name(value: str) -> bool:
    if not value:
        return False
    stripped = str(value).strip()
    if len(stripped) < 8:
        return False
    if _looks_like_language_label(stripped):
        return False

    normalized = normalize_release_for_exact(stripped)
    if not normalized:
        return False

    if longest_nontrivial_releaseish_fragment(stripped):
        return True

    has_digits = bool(re.search(r"\d", stripped))
    token_count = len([t for t in re.split(r"[.\-_ ]+", normalized) if t])
    has_separators = any(ch in stripped for ch in (".", "-", "_"))
    return (has_digits and token_count >= 3) or (has_separators and token_count >= 4)


def _clean_subtitle_release_candidate(value: str) -> str:
    if not value:
        return ""

    text = str(value).strip().strip("'\"")
    if not text:
        return ""

    # If Bazarr gives a full path, match using the actual subtitle file name.
    text = text.replace("\\", "/")
    if "/" in text:
        text = os.path.basename(text)
    text = text.strip()

    # Remove common subtitle file extensions.
    while True:
        base, ext = os.path.splitext(text)
        if not ext:
            break
        if ext.lower() in {".srt", ".ass", ".ssa", ".sub", ".vtt", ".txt"}:
            text = base
            continue
        break

    # Remove trailing language suffixes like ".he", "-eng", " hebrew".
    lang_tail = r"(he|heb|hebrew|iw|en|eng|english|ar|ara|arabic|ru|rus|russian|es|spa|spanish|fr|fra|fre|french|de|ger|deu|german|it|ita|italian)"
    for _ in range(2):
        updated = re.sub(rf"(?i)[\.\-_\s]+{lang_tail}$", "", text).strip()
        if updated == text:
            break
        text = updated

    return text.strip(" .-_")


def subtitle_release_name(subtitle_entry: Optional[Dict[str, Any]]) -> str:
    if not subtitle_entry:
        return ""

    # Bazarr may rename subtitle files to match movie filename after download.
    # Prefer metadata fields that describe the source release.
    candidates = [
        subtitle_entry.get("release_name"),
        subtitle_entry.get("releaseName"),
        subtitle_entry.get("scene_name"),
        subtitle_entry.get("sceneName"),
        # Fallbacks when Bazarr does not expose explicit release metadata.
        subtitle_entry.get("subtitles_path"),
        subtitle_entry.get("subtitle_path"),
        subtitle_entry.get("path"),
        subtitle_entry.get("file_path"),
        subtitle_entry.get("filename"),
        subtitle_entry.get("name"),
        subtitle_entry.get("title"),
    ]

    for candidate in candidates:
        if candidate is None:
            continue
        text = _clean_subtitle_release_candidate(str(candidate))
        if _looks_like_release_name(text):
            return text

    return ""


def radarr_release_name(candidate: Optional[Dict[str, Any]]) -> str:
    if not candidate:
        return ""
    value = (
        candidate.get("title")
        or candidate.get("releaseTitle")
        or candidate.get("guid")
        or ""
    )
    return str(value).strip()


def candidate_state_snapshot(candidate: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not candidate:
        return None
    return {
        "title": radarr_release_name(candidate),
        "guid": candidate.get("guid"),
        "indexerId": candidate.get("indexerId"),
        "rejected": candidate.get("rejected"),
        "rejections": candidate.get("rejections"),
    }


def title_year_string(movie: Dict[str, Any]) -> str:
    title = movie.get("title") or "<unknown>"
    year = movie.get("year") or "?"
    return f"{title} ({year})"


def longest_nontrivial_releaseish_fragment(text: str) -> Optional[str]:
    if not text:
        return None

    patterns = [
        r"([A-Za-z0-9\.\-\[\]\(\) ]{10,}?(?:2160p|1080p|720p|WEB[-\. ]DL|WEBRip|BluRay|BRRip|DVDRip|HDRip|REMUX|UHD|x264|x265|H\.264|H\.265|HEVC|DDP5\.1|AAC|DTS|TRUEHD|ATMOS)[A-Za-z0-9\.\-\[\]\(\) ]{0,120})",
        r"([A-Za-z0-9\.\-\[\]\(\) ]{10,}-[A-Za-z0-9]{2,20})",
    ]

    best = None
    best_len = 0

    for pattern in patterns:
        for m in re.finditer(pattern, text, flags=re.IGNORECASE):
            candidate = normalize_releaseish(m.group(1))
            if len(candidate) > best_len:
                best = candidate
                best_len = len(candidate)

    return best


def extract_metadata_tokens(text: str) -> List[str]:
    if not text:
        return []

    known = [
        "2160p", "1080p", "720p",
        "web-dl", "webrip", "bluray", "brrip", "dvdrip", "hdrip", "remux", "uhd",
        "x264", "x265", "h264", "h265", "hevc",
        "ddp5.1", "aac", "dts", "truehd", "atmos",
        "extended", "unrated", "proper", "repack", "imax", "criterion",
    ]

    normalized = normalize_releaseish(text).lower()
    flat = normalized.replace(".", "").replace("-", "")
    found = []

    for token in known:
        probe = token.replace(".", "").replace("-", "").replace("'", "")
        if probe in flat:
            found.append(token)

    out = []
    seen = set()
    for item in found:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def subtitle_language_rank(sub: Dict[str, Any], preferred_languages: List[str]) -> int:
    possible_values: List[str] = []

    def add_value(raw: Any) -> None:
        if raw is None:
            return
        normalized = normalize_string(str(raw))
        if normalized:
            possible_values.append(normalized)

    def add_language_obj(obj: Any) -> None:
        if not isinstance(obj, dict):
            return
        for key in ("code2", "code3", "name", "language", "lang"):
            add_value(obj.get(key))

    add_value(sub.get("language"))
    add_value(sub.get("lang"))
    add_value(sub.get("language_code"))
    add_value(sub.get("languageCode"))
    add_value(sub.get("code2"))
    add_value(sub.get("code3"))

    language_obj = sub.get("language")
    add_language_obj(language_obj)

    for idx, wanted in enumerate(preferred_languages):
        wanted_norm = normalize_string(wanted)
        if wanted_norm in possible_values:
            return idx

    return len(preferred_languages) + 10


def subtitle_score_value(sub: Dict[str, Any]) -> float:
    def parse_numeric(raw: Any) -> Optional[float]:
        if raw is None:
            return None
        if isinstance(raw, (int, float)):
            return float(raw)

        text = str(raw).strip()
        if not text:
            return None
        text = text.replace(",", ".")
        text = text.replace("%", "")
        text = re.sub(r"[^0-9.\-]+", "", text)
        if not text:
            return None
        try:
            return float(text)
        except Exception:
            return None

    for key in ("score", "matches", "percent", "match_score"):
        raw = sub.get(key)
        if raw is None:
            continue

        # Direct primitive value (e.g. 85.83 or "85.83%")
        direct = parse_numeric(raw)
        if direct is not None:
            return direct

        # Nested score structures from some Bazarr providers/adapters
        if isinstance(raw, dict):
            for nested_key in ("score", "value", "percent", "matches", "match_score"):
                nested = parse_numeric(raw.get(nested_key))
                if nested is not None:
                    return nested

        if isinstance(raw, list):
            for item in raw:
                if isinstance(item, dict):
                    for nested_key in ("score", "value", "percent", "matches", "match_score"):
                        nested = parse_numeric(item.get(nested_key))
                        if nested is not None:
                            return nested
                else:
                    nested = parse_numeric(item)
                    if nested is not None:
                        return nested
    return 0.0


def subtitle_has_score(sub: Dict[str, Any]) -> bool:
    for key in ("score", "matches", "percent", "match_score"):
        if sub.get(key) is not None:
            return True
    return False


def subtitle_has_file_reference(sub: Dict[str, Any]) -> bool:
    possible = [
        sub.get("path"),
        sub.get("file"),
        sub.get("file_path"),
        sub.get("filename"),
        sub.get("name"),
        sub.get("title"),
    ]
    for value in possible:
        if value is None:
            continue
        text = str(value).strip().lower()
        if text.endswith((".srt", ".ass", ".ssa", ".sub", ".vtt", ".txt")):
            return True
    return False


def subtitle_metadata_richness(sub: Dict[str, Any]) -> int:
    text = (
        sub.get("release_name")
        or sub.get("releaseName")
        or sub.get("name")
        or sub.get("title")
        or sub.get("filename")
        or ""
    )
    richness = len(extract_metadata_tokens(text))
    if longest_nontrivial_releaseish_fragment(text):
        richness += 3
    if "-" in text:
        richness += 1
    return richness


def choose_best_subtitle(subtitles: List[Dict[str, Any]], preferred_languages: List[str]) -> Optional[Dict[str, Any]]:
    if not subtitles:
        return None

    def sort_key(sub: Dict[str, Any]) -> Tuple[int, float, int, int]:
        lang_rank = subtitle_language_rank(sub, preferred_languages)
        score = subtitle_score_value(sub)
        richness = subtitle_metadata_richness(sub)
        title_len = len(
            str(
                sub.get("release_name")
                or sub.get("releaseName")
                or sub.get("name")
                or sub.get("title")
                or sub.get("filename")
                or ""
            )
        )
        return (lang_rank, -score, -richness, -title_len)

    return sorted(subtitles, key=sort_key)[0]


def build_release_hint(movie: Dict[str, Any], subtitle_entry: Dict[str, Any]) -> str:
    title = movie.get("title") or ""
    year = str(movie.get("year") or "").strip()

    subtitle_title = subtitle_release_name(subtitle_entry)

    releaseish = longest_nontrivial_releaseish_fragment(subtitle_title)
    if releaseish:
        return releaseish

    tokens = extract_metadata_tokens(subtitle_title)
    parts = [normalize_releaseish(title)]
    if year:
        parts.append(year)
    parts.extend(tokens)

    result = ".".join([p for p in parts if p])
    result = re.sub(r"\.+", ".", result).strip(".")
    return result or subtitle_title or f"{title} {year}".strip()


def extract_file_basename_from_radarr(movie: Dict[str, Any]) -> str:
    movie_file = movie.get("movieFile") or {}
    path = movie_file.get("path") or ""
    if not path:
        return ""
    base = os.path.basename(path)
    base_no_ext = re.sub(r"\.[A-Za-z0-9]{2,5}$", "", base)
    return normalize_releaseish(base_no_ext)


def match_quality_between_release_strings(a_value: str, b_value: str) -> float:
    if not a_value or not b_value:
        return 0.0

    a = normalize_releaseish(a_value).lower()
    b = normalize_releaseish(b_value).lower()

    score = 0.0

    if a in b or b in a:
        score += 50.0

    tokens_a = set(re.split(r"[.\-_ ]+", a))
    tokens_b = set(re.split(r"[.\-_ ]+", b))
    tokens_a = {t for t in tokens_a if t}
    tokens_b = {t for t in tokens_b if t}

    overlap = tokens_a & tokens_b
    score += len(overlap) * 2.5

    important = ["2160p", "1080p", "720p", "web", "webrip", "bluray", "remux", "x264", "x265", "h264", "h265", "hevc"]
    for token in important:
        if token in tokens_a and token in tokens_b:
            score += 4.0

    if "-" in a and "-" in b:
        group_a = a.rsplit("-", 1)[-1]
        group_b = b.rsplit("-", 1)[-1]
        if group_a == group_b and group_a:
            score += 15.0

    return score


# =============================================================================
# STATE
# =============================================================================

DEFAULT_STATE = {
    "version": 3,
    "last_run": None,
    "movies": {},
}


class StateManager:
    def __init__(self, path: str) -> None:
        self.path = path
        self.state = safe_json_load(path, DEFAULT_STATE)
        if "movies" not in self.state:
            self.state["movies"] = {}
        if "last_run" not in self.state:
            self.state["last_run"] = None

    def save(self) -> None:
        self.state["last_run"] = utc_now_iso()
        safe_json_dump(self.path, self.state)

    def get_movie_state(self, movie_id: int) -> Dict[str, Any]:
        key = str(movie_id)
        if key not in self.state["movies"]:
            self.state["movies"][key] = {
                "movie_id": movie_id,
                "title": None,
                "year": None,
                "status": "new",
                "detected": False,
                "first_seen_at": None,
                "radarr_date_added": None,
                "file_detected_at": None,
                "bazarr_checked_at": None,
                "bazarr_grace_started_at": None,
                "subtitle_evaluation": None,
                "selected_subtitle": None,
                "release_hint": None,
                "exact_match_required": True,
                "radarr_followup_attempts": 0,
                "radarr_followup_last_at": None,
                "radarr_followup_success": False,
                "bazarr_manual_search_attempts": 0,
                "bazarr_manual_search_last_at": None,
                "bazarr_manual_search_last_success": False,
                "manual_required_reason": None,
                "selected_release_candidate": None,
                "last_error": None,
                "done": False,
            }
        ms = self.state["movies"][key]
        ms.setdefault("exact_match_required", Config.EXACT_MATCH_ONLY)
        ms.setdefault("manual_required_reason", None)
        ms.setdefault("selected_release_candidate", None)
        ms.setdefault("bazarr_manual_search_attempts", 0)
        ms.setdefault("bazarr_manual_search_last_at", None)
        ms.setdefault("bazarr_manual_search_last_success", False)
        return ms

    def set_error(self, movie_id: int, error: str) -> None:
        ms = self.get_movie_state(movie_id)
        ms["last_error"] = error
        ms["status"] = "error"

    def record_detected(self, movie: Dict[str, Any]) -> None:
        movie_id = int(movie["id"])
        ms = self.get_movie_state(movie_id)
        if not ms["detected"]:
            ms["detected"] = True
            ms["title"] = movie.get("title")
            ms["year"] = movie.get("year")
            ms["first_seen_at"] = utc_now_iso()
            ms["radarr_date_added"] = movie.get("added") or movie.get("dateAdded")
            ms["status"] = "waiting_for_file"

    def record_file_detected(self, movie_id: int) -> None:
        ms = self.get_movie_state(movie_id)
        if not ms["file_detected_at"]:
            ms["file_detected_at"] = utc_now_iso()
        if not ms["bazarr_grace_started_at"]:
            ms["bazarr_grace_started_at"] = utc_now_iso()
        ms["status"] = "bazarr_waiting"

    def record_bazarr_checked(
        self,
        movie_id: int,
        evaluation: str,
        selected_subtitle: Optional[Dict[str, Any]],
        release_hint: Optional[str],
    ) -> None:
        ms = self.get_movie_state(movie_id)
        ms["bazarr_checked_at"] = utc_now_iso()
        ms["subtitle_evaluation"] = evaluation
        ms["selected_subtitle"] = selected_subtitle
        ms["release_hint"] = release_hint
        ms["exact_match_required"] = Config.EXACT_MATCH_ONLY
        ms["manual_required_reason"] = None
        ms["selected_release_candidate"] = None

        if evaluation == "good":
            ms["status"] = "done"
            ms["done"] = True
        elif evaluation == "poor":
            ms["status"] = "subtitle_matched_poor"
        elif evaluation == "none":
            ms["status"] = "subtitle_missing"
        else:
            ms["status"] = "bazarr_checked"

    def record_followup_attempt(
        self,
        movie_id: int,
        success: bool,
        *,
        manual_required_reason: Optional[str] = None,
        selected_release_candidate: Optional[Dict[str, Any]] = None,
    ) -> None:
        ms = self.get_movie_state(movie_id)
        ms["radarr_followup_attempts"] += 1
        ms["radarr_followup_last_at"] = utc_now_iso()
        ms["radarr_followup_success"] = success
        ms["selected_release_candidate"] = candidate_state_snapshot(selected_release_candidate)
        if success:
            ms["status"] = "radarr_followup_done"
            ms["manual_required_reason"] = None
        else:
            ms["status"] = "manual_required"
            ms["manual_required_reason"] = manual_required_reason or "Exact release match was not found."
        ms["done"] = True

    def record_bazarr_manual_search_attempt(self, movie_id: int, success: bool) -> None:
        ms = self.get_movie_state(movie_id)
        ms["bazarr_manual_search_attempts"] = int(ms.get("bazarr_manual_search_attempts") or 0) + 1
        ms["bazarr_manual_search_last_at"] = utc_now_iso()
        ms["bazarr_manual_search_last_success"] = bool(success)

    def is_done(self, movie_id: int) -> bool:
        return bool(self.get_movie_state(movie_id).get("done"))


# =============================================================================
# HTTP CLIENT
# =============================================================================

class HttpClient:
    def __init__(self, timeout: int, retries: int, backoff_seconds: int, verify_ssl: bool, user_agent: str) -> None:
        self.timeout = timeout
        self.retries = retries
        self.backoff_seconds = backoff_seconds
        self.verify_ssl = verify_ssl
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": user_agent,
            "Accept": "application/json",
        })

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Any] = None,
        allow_statuses: Optional[List[int]] = None,
    ) -> requests.Response:
        allow_statuses = allow_statuses or []
        last_exc = None

        for attempt in range(1, self.retries + 1):
            try:
                resp = self.session.request(
                    method=method.upper(),
                    url=url,
                    headers=headers,
                    params=params,
                    json=json_body,
                    timeout=self.timeout,
                    verify=self.verify_ssl,
                )

                if resp.status_code in allow_statuses:
                    return resp

                if 200 <= resp.status_code < 300:
                    return resp

                if resp.status_code in (408, 409, 425, 429, 500, 502, 503, 504):
                    sleep_for = self.backoff_seconds * attempt + random.uniform(0, 0.4)
                    logger.warning(
                        "HTTP %s %s returned %s (attempt %s/%s), retrying in %.1fs",
                        method.upper(), url, resp.status_code, attempt, self.retries, sleep_for
                    )
                    time.sleep(sleep_for)
                    continue

                raise requests.HTTPError(
                    f"{method.upper()} {url} failed with {resp.status_code}: {resp.text[:500]}",
                    response=resp,
                )

            except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:
                last_exc = exc
                if attempt >= self.retries:
                    break
                sleep_for = self.backoff_seconds * attempt + random.uniform(0, 0.4)
                logger.warning(
                    "HTTP %s %s failed on attempt %s/%s: %s; retrying in %.1fs",
                    method.upper(), url, attempt, self.retries, exc, sleep_for
                )
                time.sleep(sleep_for)

        raise RuntimeError(f"HTTP request failed after {self.retries} attempts: {method.upper()} {url} | {last_exc}")


# =============================================================================
# RADARR CLIENT
# =============================================================================

class RadarrClient:
    def __init__(self, base_url: str, api_key: str, http: HttpClient) -> None:
        if not api_key:
            raise ValueError("RADARR_API_KEY is required")
        self.base_url = base_url.rstrip("/")
        self.headers = {"X-Api-Key": api_key}
        self.http = http

    def get_movies(self) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/api/v3/movie"
        resp = self.http.request("GET", url, headers=self.headers)
        data = resp.json()
        if not isinstance(data, list):
            raise RuntimeError("Unexpected Radarr /movie response format")
        return data

    def get_release_candidates(self, movie_id: int) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/api/v3/release"
        resp = self.http.request("GET", url, headers=self.headers, params={"movieId": movie_id})
        data = resp.json()
        if isinstance(data, list):
            return data
        return []

    def get_movie_files(self, movie_id: int) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/api/v3/moviefile"
        resp = self.http.request("GET", url, headers=self.headers, params={"movieId": movie_id})
        data = resp.json()
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        return []

    def delete_movie_file(self, movie_file_id: int) -> bool:
        url = f"{self.base_url}/api/v3/moviefile/{int(movie_file_id)}"
        resp = self.http.request("DELETE", url, headers=self.headers, allow_statuses=[200, 202, 204, 404])
        return resp.status_code in (200, 202, 204, 404)

    def _candidate_rejection_reason(self, candidate: Dict[str, Any]) -> str:
        reasons = candidate.get("rejections")
        if not reasons:
            return "candidate marked as rejected"
        parts: List[str] = []
        if isinstance(reasons, list):
            for item in reasons:
                if isinstance(item, str):
                    parts.append(item)
                elif isinstance(item, dict):
                    parts.append(str(item.get("reason") or item.get("message") or item))
                else:
                    parts.append(str(item))
        else:
            parts.append(str(reasons))
        return "; ".join([p for p in parts if p]) or "candidate marked as rejected"

    def _candidate_allowed_by_profile(self, candidate: Dict[str, Any]) -> Tuple[bool, str]:
        if not Config.STRICT_PROFILE_GUARD:
            return True, "profile guard disabled"

        if bool(candidate.get("rejected")):
            return False, self._candidate_rejection_reason(candidate)

        rejections = candidate.get("rejections")
        if isinstance(rejections, list) and len(rejections) > 0:
            return False, self._candidate_rejection_reason(candidate)

        return True, ""

    def find_exact_release_candidate(
        self,
        movie_id: int,
        target_release: str,
    ) -> Tuple[Optional[Dict[str, Any]], List[str]]:
        target_norm = normalize_release_for_exact(target_release)
        if not target_norm:
            return None, ["target subtitle release is empty after normalization"]

        candidates = self.get_release_candidates(movie_id)
        if not candidates:
            return None, ["no Radarr release candidates available"]

        mismatch_count = 0
        rejected_count = 0
        missing_title_count = 0

        for candidate in candidates:
            title = radarr_release_name(candidate)
            title_norm = normalize_release_for_exact(title)
            if not title_norm:
                missing_title_count += 1
                continue
            if title_norm != target_norm:
                mismatch_count += 1
                continue

            allowed, reason = self._candidate_allowed_by_profile(candidate)
            if not allowed:
                rejected_count += 1
                logger.info("Rejecting exact-match candidate due to profile guard: %s", reason)
                continue

            candidate["_normalized_release"] = title_norm
            return candidate, []

        return None, [
            f"exact target normalized release={target_norm}",
            f"mismatch_count={mismatch_count}",
            f"profile_rejected_count={rejected_count}",
            f"missing_title_count={missing_title_count}",
        ]

    def list_candidates_from_release_pool(
        self,
        movie_id: int,
        release_pool: List[str],
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        candidates = self.get_release_candidates(movie_id)
        if not candidates:
            return [], ["no Radarr release candidates available"]

        normalized_pool: List[str] = []
        for item in release_pool:
            norm = normalize_release_for_exact(item)
            if norm and norm not in normalized_pool:
                normalized_pool.append(norm)

        if not normalized_pool:
            return [], ["release pool is empty after normalization"]

        rank_map = {name: idx for idx, name in enumerate(normalized_pool)}
        matched_allowed: List[Tuple[int, Dict[str, Any]]] = []
        matched_fallback: List[Tuple[int, Dict[str, Any], str]] = []
        blocked_count = 0
        mismatch_count = 0

        for candidate in candidates:
            title = radarr_release_name(candidate)
            title_norm = normalize_release_for_exact(title)
            if not title_norm or title_norm not in rank_map:
                mismatch_count += 1
                continue

            if not bool(candidate.get("downloadAllowed")):
                blocked_count += 1
                continue

            rejections = candidate.get("rejections")
            has_rejections = isinstance(rejections, list) and len(rejections) > 0
            rank = rank_map[title_norm]
            if has_rejections:
                matched_fallback.append((rank, candidate, self._candidate_rejection_reason(candidate)))
            else:
                matched_allowed.append((rank, candidate))

        matched_allowed.sort(key=lambda x: x[0])
        matched_fallback.sort(key=lambda x: x[0])

        ordered: List[Dict[str, Any]] = [x[1] for x in matched_allowed] + [x[1] for x in matched_fallback]
        if ordered:
            notes: List[str] = []
            if matched_fallback:
                notes.append(
                    "Some fallback candidates include rejections; they are attempted only after clean candidates."
                )
            return ordered, notes

        return [], [
            f"release_pool_size={len(normalized_pool)}",
            f"mismatch_count={mismatch_count}",
            f"download_blocked_count={blocked_count}",
            "No downloadAllowed candidate matched Bazarr release_info pool.",
        ]

    def find_candidate_from_release_pool(
        self,
        movie_id: int,
        release_pool: List[str],
    ) -> Tuple[Optional[Dict[str, Any]], List[str]]:
        ordered, notes = self.list_candidates_from_release_pool(movie_id, release_pool)
        if not ordered:
            return None, notes
        return ordered[0], notes

    def grab_release_candidate(self, movie_id: int, candidate: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/api/v3/release"
        guid = candidate.get("guid")
        indexer_id = candidate.get("indexerId")

        if not guid:
            raise RuntimeError("Cannot grab release candidate: missing guid")
        if indexer_id is None:
            raise RuntimeError("Cannot grab release candidate: missing indexerId")

        payload = {
            "guid": guid,
            "indexerId": indexer_id,
            "movieId": movie_id,
        }
        resp = self.http.request(
            "POST",
            url,
            headers=self.headers,
            json_body=payload,
            allow_statuses=[400, 404, 409, 422],
        )
        try:
            data = resp.json()
            if isinstance(data, dict):
                data.setdefault("status_code", resp.status_code)
                return data
            return {
                "status_code": resp.status_code,
                "data": data,
            }
        except Exception:
            return {
                "status_code": resp.status_code,
                "text": resp.text[:500],
            }

    def interpret_grab_response(self, result: Dict[str, Any]) -> Tuple[bool, str]:
        status_code = result.get("status_code")
        if isinstance(status_code, int) and status_code >= 400:
            return False, f"HTTP {status_code}: {result.get('message') or result.get('text') or 'grab failed'}"

        message = normalize_string(str(result.get("message") or ""))
        description = normalize_string(str(result.get("description") or ""))
        combined = f"{message} {description}".strip()
        if "failed" in combined or "exception" in combined or "error" in combined:
            return False, str(result.get("message") or result.get("description") or "grab failed")

        return True, "grab request accepted"

    def get_queue_records(self, movie_id: int) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/api/v3/queue"
        resp = self.http.request(
            "GET",
            url,
            headers=self.headers,
            params={"movieId": movie_id, "page": 1, "pageSize": 100},
        )
        data = resp.json()
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict):
            records = data.get("records")
            if isinstance(records, list):
                return [x for x in records if isinstance(x, dict)]
        return []

    def get_history_records(self, movie_id: int) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/api/v3/history/movie"
        resp = self.http.request(
            "GET",
            url,
            headers=self.headers,
            params={"movieId": movie_id, "page": 1, "pageSize": 100},
            allow_statuses=[404],
        )
        if resp.status_code == 404:
            return []
        data = resp.json()
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict):
            records = data.get("records")
            if isinstance(records, list):
                return [x for x in records if isinstance(x, dict)]
        return []

    def _history_record_is_grab_event(self, record: Dict[str, Any]) -> bool:
        event_values = [
            record.get("eventType"),
            record.get("eventTypeName"),
            record.get("event"),
        ]
        for value in event_values:
            text = normalize_string(str(value or ""))
            if "grab" in text:
                return True
        return False

    def wait_for_grab_confirmation(
        self,
        movie_id: int,
        candidate: Dict[str, Any],
        *,
        timeout_seconds: int,
        poll_seconds: int,
    ) -> Tuple[bool, str]:
        start = utc_now()
        candidate_title_norm = normalize_release_for_exact(radarr_release_name(candidate))
        deadline = time.time() + max(1, timeout_seconds)
        poll = max(1, poll_seconds)

        while time.time() <= deadline:
            queue_records = self.get_queue_records(movie_id)
            for rec in queue_records:
                rec_movie_id = rec.get("movieId")
                try:
                    if int(rec_movie_id) != int(movie_id):
                        continue
                except Exception:
                    continue
                title_norm = normalize_release_for_exact(
                    str(rec.get("title") or rec.get("sourceTitle") or "")
                )
                if candidate_title_norm and title_norm and (
                    candidate_title_norm in title_norm or title_norm in candidate_title_norm
                ):
                    return True, "confirmed via queue title match"
                if not candidate_title_norm:
                    return True, "confirmed via queue movieId"

            if Config.RADARR_GRAB_VERIFY_USE_HISTORY:
                history_records = self.get_history_records(movie_id)
                for rec in history_records:
                    if not self._history_record_is_grab_event(rec):
                        continue
                    event_dt = parse_iso(rec.get("date") or rec.get("parsedDate") or rec.get("eventDate"))
                    if event_dt is not None and event_dt < start:
                        continue
                    source_title_norm = normalize_release_for_exact(str(rec.get("sourceTitle") or rec.get("title") or ""))
                    if candidate_title_norm and source_title_norm and (
                        candidate_title_norm in source_title_norm or source_title_norm in candidate_title_norm
                    ):
                        return True, "confirmed via history grab event"
                    if not candidate_title_norm:
                        return True, "confirmed via history grab event"

            time.sleep(poll)

        if Config.RADARR_GRAB_VERIFY_USE_HISTORY:
            return False, "grab not confirmed in queue/history within timeout"
        return False, "grab not confirmed in queue within timeout"


# =============================================================================
# BAZARR CLIENT (INSTANCE-SPECIFIC ADAPTER)
# =============================================================================

class BazarrClient:
    def __init__(self, base_url: str, api_key: str, http: HttpClient) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.http = http
        self._search_endpoint_disabled = False
        self._manual_search_endpoints_disabled: set = set()

    def _headers(self) -> Dict[str, str]:
        headers = {}
        if self.api_key:
            headers[Config.BAZARR_API_KEY_HEADER] = self.api_key
        return headers

    def _movie_lookup_url_and_params(self, radarr_movie_id: int) -> Tuple[str, Dict[str, Any]]:
        endpoint = Config.BAZARR_MOVIE_LOOKUP_ENDPOINT
        if Config.BAZARR_LOOKUP_STYLE == "path_radarrid":
            return f"{self.base_url}{endpoint.rstrip('/')}/{radarr_movie_id}", {}
        return f"{self.base_url}{endpoint}", {"radarrid": radarr_movie_id, "radarrId": radarr_movie_id}

    def _fetch_lookup_payload(
        self,
        movie: Dict[str, Any],
        endpoint: str,
        *,
        include_lookup_style: bool,
    ) -> Optional[Any]:
        radarr_movie_id = int(movie["id"])
        if include_lookup_style:
            if Config.BAZARR_LOOKUP_STYLE == "path_radarrid":
                url = f"{self.base_url}{endpoint.rstrip('/')}/{radarr_movie_id}"
                params: Dict[str, Any] = {}
            else:
                url = f"{self.base_url}{endpoint}"
                params = {"radarrid": radarr_movie_id, "radarrId": radarr_movie_id}
        else:
            url = f"{self.base_url}{endpoint}"
            params = {}

        try:
            resp = self.http.request(
                "GET",
                url,
                headers=self._headers(),
                params=params,
                allow_statuses=[400, 404, 405],
            )
        except Exception as exc:
            logger.error("Bazarr lookup failed for %s via %s: %s", title_year_string(movie), endpoint, exc)
            return None

        if resp.status_code in (400, 404, 405):
            logger.warning(
                "Bazarr lookup endpoint seems incompatible (%s %s returned %s).",
                "GET", url, resp.status_code
            )
            return None

        try:
            return resp.json()
        except Exception:
            logger.warning("Bazarr lookup response is not valid JSON for %s via %s", title_year_string(movie), endpoint)
            return None

    def lookup_movie_subtitle_state(self, movie: Dict[str, Any]) -> Dict[str, Any]:
        """
        IMPORTANT:
        This is the main Bazarr adapter point you will probably need to adjust.

        Expected normalized return:
        {
          "movie_found": bool,
          "subtitles_present": bool,
          "subtitles": [ {...}, {...} ],
          "raw": <original JSON>,
        }

        This function tries several response-shape interpretations.
        """
        if Config.BAZARR_MODE == "disabled":
            return {
                "movie_found": False,
                "subtitles_present": False,
                "subtitles": [],
                "raw": None,
            }
        radarr_movie_id = int(movie["id"])
        endpoints_to_try: List[Tuple[str, bool]] = [(Config.BAZARR_MOVIE_LOOKUP_ENDPOINT, True)]
        for endpoint in Config.BAZARR_MOVIE_LOOKUP_FALLBACK_ENDPOINTS:
            endpoints_to_try.append((endpoint, False))

        best_result = {
            "movie_found": False,
            "subtitles_present": False,
            "subtitles": [],
            "raw": None,
        }
        aggregated_subtitles: List[Dict[str, Any]] = []

        for endpoint, with_lookup_style in endpoints_to_try:
            payload = self._fetch_lookup_payload(movie, endpoint, include_lookup_style=with_lookup_style)
            if payload is None:
                continue
            normalized = self._normalize_bazarr_movie_lookup(payload, radarr_movie_id)
            if Config.BAZARR_ENABLE_HISTORY_LOOKUP:
                history_subtitles = self._extract_subtitles_from_history_payload(payload, movie)
                if history_subtitles:
                    existing = normalized.get("subtitles") or []
                    merged = self._merge_subtitle_candidates(existing, history_subtitles)
                    normalized["subtitles"] = merged
                    normalized["subtitles_present"] = len(merged) > 0
                    normalized["movie_found"] = True
            if normalized.get("subtitles"):
                aggregated_subtitles = self._merge_subtitle_candidates(
                    aggregated_subtitles,
                    normalized.get("subtitles") or [],
                )
            if normalized.get("movie_found") and not best_result.get("movie_found"):
                best_result = normalized

        if aggregated_subtitles:
            best_result["subtitles"] = aggregated_subtitles
            best_result["subtitles_present"] = True
            best_result["movie_found"] = True

        return best_result

    def _merge_subtitle_candidates(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        merged: List[Dict[str, Any]] = []
        seen: set = set()

        for candidate in (left or []) + (right or []):
            release_name = subtitle_release_name(candidate)
            normalized_release = normalize_release_for_exact(release_name) if release_name else ""
            signature = (
                normalized_release,
                str(candidate.get("path") or candidate.get("file") or candidate.get("file_path") or ""),
                str(candidate.get("score") or candidate.get("percent") or candidate.get("matches") or candidate.get("match_score") or ""),
                normalize_string(str(candidate.get("language") or candidate.get("lang") or candidate.get("code2") or candidate.get("code3") or "")),
            )
            if signature in seen:
                continue
            seen.add(signature)
            merged.append(candidate)

        return merged

    def trigger_subtitle_search(self, movie: Dict[str, Any]) -> bool:
        """
        Optional Bazarr-triggered search after the file exists.
        IMPORTANT: may need endpoint/method/payload adjustment for your instance.
        """
        if Config.BAZARR_MODE == "disabled" or not Config.ENABLE_BAZARR_SEARCH_TRIGGER:
            return False

        if self._search_endpoint_disabled:
            return False

        url = f"{self.base_url}{Config.BAZARR_SEARCH_ENDPOINT}"
        movie_id = int(movie["id"])
        payload_variants = [
            {"radarrid": movie_id},
            {"radarrId": movie_id},
            {"movieId": movie_id},
            {
                "radarrid": movie_id,
                "title": movie.get("title"),
                "year": movie.get("year"),
                "path": (movie.get("movieFile") or {}).get("path"),
                "languages": Config.PREFERRED_LANGUAGES,
            },
        ]

        for payload in payload_variants:
            for send_mode in ("json", "query"):
                try:
                    if send_mode == "json":
                        resp = self.http.request(
                            "POST",
                            url,
                            headers=self._headers(),
                            json_body=payload,
                            allow_statuses=[400, 404, 405, 422],
                        )
                    else:
                        resp = self.http.request(
                            "POST",
                            url,
                            headers=self._headers(),
                            params=payload,
                            allow_statuses=[400, 404, 405, 422],
                        )
                except Exception as exc:
                    logger.error("Bazarr subtitle search trigger failed for %s: %s", title_year_string(movie), exc)
                    continue

                if 200 <= resp.status_code < 300:
                    return True

        logger.warning(
            "Bazarr search trigger endpoint seems incompatible (%s). "
            "Disabling trigger for this run. Adjust BAZARR_SEARCH_ENDPOINT / method / payload for your instance.",
            url,
        )
        self._search_endpoint_disabled = True
        return False

    def trigger_manual_subtitle_search(self, movie: Dict[str, Any]) -> bool:
        """
        Try to trigger Bazarr manual subtitle search (equivalent to UI Manual search flow).
        Endpoint/payload can vary per Bazarr version, so we try several payload shapes.
        """
        if Config.BAZARR_MODE == "disabled" or not Config.ENABLE_BAZARR_MANUAL_SEARCH_ON_POOR:
            return False

        method = (Config.BAZARR_MANUAL_SEARCH_METHOD or "AUTO").upper()
        allow_statuses = [400, 404, 405, 409, 422]
        movie_id = int(movie["id"])

        payload_variants = [
            {"radarrid": movie_id},
            {"radarrId": movie_id},
            {"movieId": movie_id},
            {"id": movie_id, "type": "movie"},
            {
                "radarrid": movie_id,
                "title": movie.get("title"),
                "year": movie.get("year"),
                "path": (movie.get("movieFile") or {}).get("path"),
                "languages": Config.PREFERRED_LANGUAGES,
            },
        ]

        for endpoint in Config.BAZARR_MANUAL_SEARCH_ENDPOINTS:
            if endpoint in self._manual_search_endpoints_disabled:
                continue

            url = f"{self.base_url}{endpoint}"
            endpoint_had_accepted = False
            for payload in payload_variants:
                try:
                    request_variants = []
                    if method == "GET":
                        request_variants.append(("GET", payload, None))
                    elif method == "POST":
                        request_variants.append(("POST", None, payload))   # JSON body
                        request_variants.append(("POST", payload, None))   # Query string
                    else:
                        # AUTO mode: try broad compatibility set.
                        request_variants.append(("POST", None, payload))   # JSON body
                        request_variants.append(("POST", payload, None))   # Query string
                        request_variants.append(("GET", payload, None))    # Query string

                    resp = None
                    for req_method, req_params, req_json in request_variants:
                        resp = self.http.request(
                            req_method,
                            url,
                            headers=self._headers(),
                            params=req_params,
                            json_body=req_json,
                            allow_statuses=allow_statuses,
                        )
                        if 200 <= resp.status_code < 300:
                            endpoint_had_accepted = True
                            break
                        logger.info(
                            "Bazarr manual search attempt got HTTP %s for %s via %s (payload keys: %s)",
                            resp.status_code,
                            title_year_string(movie),
                            endpoint,
                            ", ".join(sorted((req_json or req_params or {}).keys())),
                        )
                except Exception as exc:
                    logger.warning(
                        "Bazarr manual search trigger failed for %s via %s (%s): %s",
                        title_year_string(movie),
                        endpoint,
                        method,
                        exc,
                    )
                    continue

                if resp is not None and 200 <= resp.status_code < 300:
                    logger.info(
                        "Bazarr manual search trigger accepted for %s via %s (%s).",
                        title_year_string(movie),
                        endpoint,
                        method,
                    )
                    return True

                if resp is not None and resp.status_code in (400, 404, 405, 422):
                    continue

            if not endpoint_had_accepted:
                logger.warning(
                    "Bazarr manual search endpoint may be incompatible for %s: %s",
                    title_year_string(movie),
                    endpoint,
                )
                self._manual_search_endpoints_disabled.add(endpoint)

        return False

    def fetch_provider_movie_candidates(self, movie: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Query Bazarr providers endpoint for manual subtitle candidates.
        Expected endpoint shape (observed):
          GET /api/providers/movies?radarrid=<id>
          {"data":[{"release_info":[...], "score":85, "language":"he", ...}, ...]}
        """
        if Config.BAZARR_MODE == "disabled" or not Config.ENABLE_BAZARR_PROVIDERS_RELEASE_HINT:
            return []

        url = f"{self.base_url}{Config.BAZARR_PROVIDERS_MOVIES_ENDPOINT}"
        movie_id = int(movie["id"])

        param_variants = [
            {"radarrid": movie_id},
            {"radarrId": movie_id},
            {"movieId": movie_id},
        ]

        rows: List[Dict[str, Any]] = []
        for params in param_variants:
            try:
                resp = self.http.request(
                    "GET",
                    url,
                    headers=self._headers(),
                    params=params,
                    allow_statuses=[400, 404, 405, 422],
                )
            except Exception as exc:
                logger.warning(
                    "Bazarr providers lookup failed for %s via %s: %s",
                    title_year_string(movie),
                    Config.BAZARR_PROVIDERS_MOVIES_ENDPOINT,
                    exc,
                )
                continue

            if not (200 <= resp.status_code < 300):
                continue

            try:
                payload = resp.json()
            except Exception:
                continue

            data = payload.get("data") if isinstance(payload, dict) else None
            if isinstance(data, list):
                rows = [x for x in data if isinstance(x, dict)]
            elif isinstance(payload, list):
                rows = [x for x in payload if isinstance(x, dict)]
            else:
                rows = []

            if rows:
                break

        if not rows:
            return []

        subtitles: List[Dict[str, Any]] = []
        seen: set = set()
        for row in rows:
            score = row.get("score") or row.get("score_without_hash") or row.get("orig_score")
            language = row.get("language")
            provider = row.get("provider")
            release_infos = row.get("release_info")

            release_candidates: List[str] = []
            if isinstance(release_infos, list):
                release_candidates = [str(x).strip() for x in release_infos if str(x).strip()]
            elif release_infos:
                release_candidates = [str(release_infos).strip()]

            for release_text in release_candidates:
                normalized_release = normalize_release_for_exact(_clean_subtitle_release_candidate(release_text))
                if not normalized_release:
                    continue
                signature = (normalized_release, str(score), normalize_string(str(language)), normalize_string(str(provider)))
                if signature in seen:
                    continue
                seen.add(signature)
                subtitles.append(
                    {
                        "release_name": release_text,
                        "score": score,
                        "language": language,
                        "provider": provider,
                        "url": row.get("url"),
                        "subtitle": row.get("subtitle"),
                        "matches": row.get("matches"),
                        "dont_matches": row.get("dont_matches"),
                    }
                )

        return subtitles

    def _normalize_bazarr_movie_lookup(self, data: Any, radarr_movie_id: int) -> Dict[str, Any]:
        result = {
            "movie_found": False,
            "subtitles_present": False,
            "subtitles": [],
            "raw": data,
        }

        candidate_movie_obj = None

        # Case 1: direct object for one movie
        if isinstance(data, dict):
            if self._looks_like_movie_record(data, radarr_movie_id):
                candidate_movie_obj = data
            else:
                for key in ("data", "result", "item", "movie"):
                    value = data.get(key)
                    if isinstance(value, dict) and self._looks_like_movie_record(value, radarr_movie_id):
                        candidate_movie_obj = value
                        break
                if candidate_movie_obj is None:
                    for key in ("data", "results", "items", "movies"):
                        value = data.get(key)
                        if isinstance(value, list):
                            for item in value:
                                if isinstance(item, dict) and self._looks_like_movie_record(item, radarr_movie_id):
                                    candidate_movie_obj = item
                                    break
                            if candidate_movie_obj is not None:
                                break

        # Case 2: list of movie objects
        if candidate_movie_obj is None and isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and self._looks_like_movie_record(item, radarr_movie_id):
                    candidate_movie_obj = item
                    break

        # Case 3: endpoint returns one movie object but without an obvious radarrId marker
        if candidate_movie_obj is None and isinstance(data, dict):
            if any(k in data for k in ("subtitles", "missing_subtitles", "external_subtitles", "downloaded_subtitles")):
                candidate_movie_obj = data

        if candidate_movie_obj is None:
            return result

        result["movie_found"] = True

        subtitles = self._extract_subtitles_from_movie_record(candidate_movie_obj)
        result["subtitles"] = subtitles
        result["subtitles_present"] = len(subtitles) > 0
        return result

    def _looks_like_movie_record(self, obj: Dict[str, Any], radarr_movie_id: int) -> bool:
        candidates = [
            obj.get("radarrId"),
            obj.get("radarrid"),
            obj.get("movieId"),
            obj.get("id"),
        ]
        for value in candidates:
            try:
                if int(value) == radarr_movie_id:
                    return True
            except Exception:
                continue
        return False

    def _extract_subtitles_from_movie_record(self, movie_record: Dict[str, Any]) -> List[Dict[str, Any]]:
        possible_keys = [
            "subtitles",
            "missing_subtitles",
            "external_subtitles",
            "downloaded_subtitles",
            "audio_subtitles",
        ]

        subtitles: List[Dict[str, Any]] = []
        seen_releases: set = set()
        seen_score_only: set = set()

        def iter_candidate_dicts(value: Any):
            if isinstance(value, dict):
                yield value
                for nested_value in value.values():
                    yield from iter_candidate_dicts(nested_value)
            elif isinstance(value, list):
                for item in value:
                    yield from iter_candidate_dicts(item)
            elif isinstance(value, (str, int, float)):
                # Some Bazarr responses expose subtitle rows as scalar strings (paths/names).
                yield {"name": str(value)}

        for key in possible_keys:
            value = movie_record.get(key)
            if not isinstance(value, (list, dict)):
                continue

            for candidate in iter_candidate_dicts(value):
                release_name = subtitle_release_name(candidate)
                has_score = subtitle_has_score(candidate)
                has_file_ref = subtitle_has_file_reference(candidate)
                if not release_name and not has_score and not has_file_ref:
                    continue

                if release_name:
                    normalized_release = normalize_release_for_exact(release_name)
                    if not normalized_release or normalized_release in seen_releases:
                        continue
                    seen_releases.add(normalized_release)
                else:
                    score_only_key = (
                        normalize_string(str(candidate.get("language") or candidate.get("lang") or "")),
                        normalize_string(str(candidate.get("provider") or candidate.get("source") or "")),
                        str(candidate.get("score") or candidate.get("percent") or candidate.get("matches") or candidate.get("match_score") or ""),
                    )
                    if score_only_key in seen_score_only:
                        continue
                    seen_score_only.add(score_only_key)

                normalized_candidate = dict(candidate)
                if release_name and "release_name" not in normalized_candidate:
                    normalized_candidate["release_name"] = release_name
                if has_file_ref:
                    normalized_candidate["_has_file_reference"] = True
                subtitles.append(normalized_candidate)
        return subtitles

    def _extract_subtitles_from_history_payload(
        self,
        payload: Any,
        movie: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        rows = self._history_rows(payload)
        if not rows:
            return []

        subtitles: List[Dict[str, Any]] = []
        seen_keys: set = set()

        for row in rows:
            if not self._history_row_matches_movie(row, movie):
                continue

            row_candidates = list(self._history_row_candidate_dicts(row))
            for candidate in row_candidates:
                normalized_candidate = dict(candidate)

                # Bazarr history often keeps score fields on the history row itself.
                for key in ("score", "percent", "matches", "match_score"):
                    if normalized_candidate.get(key) is None and row.get(key) is not None:
                        normalized_candidate[key] = row.get(key)

                # Copy common path keys from row when candidate does not carry them.
                row_path = (
                    row.get("path")
                    or row.get("subtitle_path")
                    or row.get("subtitles_path")
                    or row.get("file")
                    or row.get("filename")
                    or row.get("name")
                )
                if not any(normalized_candidate.get(k) for k in ("path", "file", "file_path", "filename", "name", "title")) and row_path:
                    normalized_candidate["path"] = row_path

                if normalized_candidate.get("language") is None and row.get("language") is not None:
                    normalized_candidate["language"] = row.get("language")
                if isinstance(row.get("language"), dict):
                    lang_obj = row.get("language") or {}
                    if normalized_candidate.get("code2") is None and lang_obj.get("code2") is not None:
                        normalized_candidate["code2"] = lang_obj.get("code2")
                    if normalized_candidate.get("code3") is None and lang_obj.get("code3") is not None:
                        normalized_candidate["code3"] = lang_obj.get("code3")
                    if normalized_candidate.get("lang") is None and lang_obj.get("code2") is not None:
                        normalized_candidate["lang"] = lang_obj.get("code2")
                    if normalized_candidate.get("language_code") is None and lang_obj.get("code2") is not None:
                        normalized_candidate["language_code"] = lang_obj.get("code2")
                if normalized_candidate.get("provider") is None and row.get("provider") is not None:
                    normalized_candidate["provider"] = row.get("provider")

                release_name = subtitle_release_name(normalized_candidate)
                has_score = subtitle_has_score(normalized_candidate)
                has_file_ref = subtitle_has_file_reference(normalized_candidate)
                if not release_name and not has_score and not has_file_ref:
                    continue

                signature = (
                    normalize_release_for_exact(release_name) if release_name else "",
                    str(normalized_candidate.get("path") or normalized_candidate.get("file") or normalized_candidate.get("file_path") or ""),
                    str(normalized_candidate.get("score") or normalized_candidate.get("percent") or normalized_candidate.get("matches") or normalized_candidate.get("match_score") or ""),
                    normalize_string(str(normalized_candidate.get("language") or normalized_candidate.get("lang") or normalized_candidate.get("code2") or normalized_candidate.get("code3") or "")),
                )
                if signature in seen_keys:
                    continue
                seen_keys.add(signature)

                if release_name and "release_name" not in normalized_candidate:
                    normalized_candidate["release_name"] = release_name
                if has_file_ref:
                    normalized_candidate["_has_file_reference"] = True

                subtitles.append(normalized_candidate)

        return subtitles

    def _history_rows(self, payload: Any) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []

        def collect(value: Any, depth: int = 0) -> None:
            if depth > 4:
                return
            if isinstance(value, list):
                for item in value:
                    collect(item, depth + 1)
                return
            if not isinstance(value, dict):
                return

            list_keys = ("data", "results", "items", "history", "records")
            found_list = False
            for key in list_keys:
                nested = value.get(key)
                if isinstance(nested, list):
                    found_list = True
                    for item in nested:
                        if isinstance(item, dict):
                            rows.append(item)
                        collect(item, depth + 1)

            if not found_list and self._looks_like_history_row(value):
                rows.append(value)

            for nested in value.values():
                if isinstance(nested, (list, dict)):
                    collect(nested, depth + 1)

        collect(payload, 0)
        return rows

    def _looks_like_history_row(self, row: Dict[str, Any]) -> bool:
        marker_keys = (
            "action",
            "event",
            "provider",
            "score",
            "percent",
            "matches",
            "match_score",
            "language",
            "path",
            "subtitle_path",
        )
        return any(key in row for key in marker_keys)

    def _history_row_matches_movie(self, row: Dict[str, Any], movie: Dict[str, Any]) -> bool:
        movie_id = int(movie["id"])

        id_keys = (
            "radarrId",
            "radarrid",
            "radarr_id",
            "movieId",
            "movie_id",
            "id",
        )
        for key in id_keys:
            value = row.get(key)
            try:
                if int(value) == movie_id:
                    return True
            except Exception:
                pass

        nested_obj_keys = ("movie", "radarr", "item", "data")
        for key in nested_obj_keys:
            nested = row.get(key)
            if isinstance(nested, dict):
                if self._looks_like_movie_record(nested, movie_id):
                    return True
                for nested_id_key in id_keys:
                    value = nested.get(nested_id_key)
                    try:
                        if int(value) == movie_id:
                            return True
                    except Exception:
                        pass

        return False

    def _history_row_candidate_dicts(self, row: Dict[str, Any]):
        yield row
        for key in (
            "subtitle",
            "subtitles",
            "external_subtitle",
            "external_subtitles",
            "downloaded_subtitles",
            "item",
            "data",
        ):
            nested = row.get(key)
            if isinstance(nested, dict):
                yield nested
            elif isinstance(nested, list):
                for item in nested:
                    if isinstance(item, dict):
                        yield item


# =============================================================================
# DECISION LOGIC
# =============================================================================

def is_new_movie(movie: Dict[str, Any], state: StateManager) -> bool:
    ms = state.get_movie_state(int(movie["id"]))
    return not ms["detected"]


def has_movie_file(movie: Dict[str, Any]) -> bool:
    return bool(movie.get("hasFile") or movie.get("movieFile"))


def should_wait_for_bazarr_grace(movie_state: Dict[str, Any]) -> bool:
    started = movie_state.get("bazarr_grace_started_at")
    if not started:
        return True
    elapsed = seconds_since(started)
    if elapsed is None:
        return True
    return elapsed < Config.BAZARR_GRACE_SECONDS


def should_retry_followup(movie_state: Dict[str, Any]) -> bool:
    attempts = int(movie_state.get("radarr_followup_attempts") or 0)
    if attempts >= Config.MAX_FOLLOWUP_ATTEMPTS:
        return False

    last_at = movie_state.get("radarr_followup_last_at")
    if not last_at:
        return True

    elapsed = seconds_since(last_at)
    if elapsed is None:
        return True
    return elapsed >= Config.RETRY_COOLDOWN_SECONDS


def should_retry_bazarr_manual_search(movie_state: Dict[str, Any]) -> bool:
    attempts = int(movie_state.get("bazarr_manual_search_attempts") or 0)
    if attempts >= Config.BAZARR_MANUAL_SEARCH_MAX_ATTEMPTS:
        return False

    last_at = movie_state.get("bazarr_manual_search_last_at")
    if not last_at:
        return True

    elapsed = seconds_since(last_at)
    if elapsed is None:
        return True
    return elapsed >= Config.BAZARR_MANUAL_SEARCH_RETRY_COOLDOWN_SECONDS


def evaluate_subtitle_quality(movie: Dict[str, Any], subtitles: List[Dict[str, Any]]) -> Tuple[str, Optional[Dict[str, Any]], Optional[str], float]:
    """
    Returns:
    - evaluation: "good" | "poor" | "none"
    - best subtitle
    - release hint
    - similarity score
    """
    if not subtitles:
        return "none", None, None, 0.0

    best = choose_best_subtitle(subtitles, Config.PREFERRED_LANGUAGES)
    if not best:
        return "none", None, None, 0.0

    release_hint = subtitle_release_name(best)
    bazarr_score = subtitle_score_value(best)
    if bazarr_score > 0:
        good_by_score = bazarr_score >= Config.GOOD_SUBTITLE_MIN_SCORE
        return ("good" if good_by_score else "poor"), best, release_hint or None, bazarr_score

    if not release_hint:
        release_hint = build_release_hint(movie, best)

    # No Bazarr score exposed for this subtitle candidate.
    # Keep behavior configurable for instances that only expose downloaded subtitle path.
    if (
        Config.TREAT_FILE_REFERENCE_AS_GOOD_WHEN_SCORE_MISSING
        and subtitle_has_file_reference(best)
    ):
        return "good", best, release_hint or None, -1.0

    return "none", best, release_hint or None, 0.0


def process_movie(
    movie: Dict[str, Any],
    state: StateManager,
    bazarr: BazarrClient,
    radarr: RadarrClient,
) -> None:
    movie_id = int(movie["id"])
    label = title_year_string(movie)
    ms = state.get_movie_state(movie_id)

    if state.is_done(movie_id):
        return

    # Detect new movie
    if not ms["detected"]:
        logger.info("Found new movie in Radarr: %s | id=%s", label, movie_id)
        state.record_detected(movie)
        state.save()
        ms = state.get_movie_state(movie_id)

    # Wait for actual file
    if not has_movie_file(movie):
        logger.info("Movie still has no file, waiting: %s", label)
        ms["status"] = "waiting_for_file"
        state.save()
        return

    # File detected
    if not ms.get("file_detected_at"):
        logger.info("Movie file is now available: %s", label)
        state.record_file_detected(movie_id)
        state.save()
        ms = state.get_movie_state(movie_id)

    # Grace period for Bazarr automatic behavior
    if should_wait_for_bazarr_grace(ms):
        logger.info(
            "Waiting for Bazarr grace period before evaluation: %s | grace=%ss",
            label, Config.BAZARR_GRACE_SECONDS
        )
        ms["status"] = "bazarr_waiting"
        state.save()
        return

    # Optional: trigger Bazarr search once, just before we inspect.
    if Config.ENABLE_BAZARR_SEARCH_TRIGGER:
        logger.info("Attempting Bazarr subtitle search trigger for: %s", label)
        bazarr.trigger_subtitle_search(movie)

    # Query Bazarr
    logger.info("Checking Bazarr subtitle state for: %s", label)
    lookup = bazarr.lookup_movie_subtitle_state(movie)

    subtitles = lookup.get("subtitles") or []
    logger.info("Bazarr reported %s subtitle item(s) for %s", len(subtitles), label)

    if Config.EXACT_MATCH_ONLY and not subtitles:
        raw = lookup.get("raw")
        if isinstance(raw, dict):
            logger.info("Bazarr raw top-level keys for %s: %s", label, ", ".join(sorted(raw.keys())[:25]))
        logger.warning(
            "No subtitle entries were extracted from Bazarr for %s; keeping movie pending for next cycle.",
            label
        )
        state.record_bazarr_checked(movie_id, "none", None, None)
        state.save()
        return

    evaluation, best_subtitle, release_hint, quality_score = evaluate_subtitle_quality(movie, subtitles)
    subtitle_release = subtitle_release_name(best_subtitle)
    file_release = extract_file_basename_from_radarr(movie)
    normalized_subtitle_release = normalize_release_for_exact(subtitle_release or release_hint or "")
    normalized_file_release = normalize_release_for_exact(file_release)

    if best_subtitle and subtitle_has_score(best_subtitle):
        logger.info(
            "Best subtitle score source for %s: score=%.2f provider=%s",
            label,
            subtitle_score_value(best_subtitle),
            best_subtitle.get("provider") or "<unknown>",
        )

    if best_subtitle and not subtitle_release and quality_score <= 0:
        logger.info(
            "Best subtitle keys for %s (no release/score recognized): %s",
            label,
            ", ".join(sorted(best_subtitle.keys())[:30]),
        )

    if evaluation == "good":
        chosen_name = subtitle_release_name(best_subtitle) or "<unknown>"
        logger.info("Subtitle match looks good for %s", label)
        logger.info("Selected subtitle: %s", chosen_name)
        logger.info("File release (normalized): %s", normalized_file_release or "<empty>")
        logger.info("Subtitle release (normalized): %s", normalized_subtitle_release or "<empty>")
        if quality_score < 0:
            logger.info("Quality score: N/A (Bazarr did not expose score; subtitle file exists)")
        else:
            logger.info("Quality score: %.2f", quality_score)
        state.record_bazarr_checked(movie_id, "good", best_subtitle, release_hint)
        state.save()
        return

    if evaluation == "none":
        logger.warning("No subtitles found in Bazarr for %s", label)
        state.record_bazarr_checked(movie_id, "none", None, None)
    else:
        chosen_name = subtitle_release_name(best_subtitle) or "<unknown>"
        logger.warning("Subtitle match is poor for %s", label)
        logger.info("Selected subtitle candidate: %s", chosen_name)
        logger.info("File release (normalized): %s", normalized_file_release or "<empty>")
        logger.info("Subtitle release (normalized): %s", normalized_subtitle_release or "<empty>")
        if quality_score < 0:
            logger.info("Quality score: N/A (Bazarr did not expose score; subtitle file exists)")
        else:
            logger.info("Quality score: %.2f", quality_score)
        state.record_bazarr_checked(movie_id, "poor", best_subtitle, release_hint)

    state.save()
    ms = state.get_movie_state(movie_id)
    provider_release_pool: List[str] = []

    if evaluation == "poor" and Config.ENABLE_BAZARR_MANUAL_SEARCH_ON_POOR:
        if should_retry_bazarr_manual_search(ms):
            logger.info("Trying Bazarr manual subtitle search for %s", label)
            manual_triggered = bazarr.trigger_manual_subtitle_search(movie)
            state.record_bazarr_manual_search_attempt(movie_id, manual_triggered)
            state.save()
            ms = state.get_movie_state(movie_id)

            if manual_triggered:
                wait_seconds = max(0, int(Config.BAZARR_MANUAL_SEARCH_WAIT_SECONDS))
                if wait_seconds > 0:
                    logger.info(
                        "Waiting %ss for Bazarr manual search results: %s",
                        wait_seconds,
                        label,
                    )
                    time.sleep(wait_seconds)

                logger.info("Re-checking Bazarr subtitle state after manual search: %s", label)
                lookup_after_manual = bazarr.lookup_movie_subtitle_state(movie)
                subtitles_after_manual = lookup_after_manual.get("subtitles") or []
                logger.info(
                    "Bazarr reported %s subtitle item(s) after manual search for %s",
                    len(subtitles_after_manual),
                    label,
                )

                evaluation2, best2, release_hint2, quality_score2 = evaluate_subtitle_quality(movie, subtitles_after_manual)
                if evaluation2 == "good":
                    chosen_name2 = subtitle_release_name(best2) or "<unknown>"
                    logger.info("Subtitle match looks good after Bazarr manual search for %s", label)
                    logger.info("Selected subtitle: %s", chosen_name2)
                    if quality_score2 < 0:
                        logger.info("Quality score: N/A (Bazarr did not expose score; subtitle file exists)")
                    else:
                        logger.info("Quality score: %.2f", quality_score2)
                    state.record_bazarr_checked(movie_id, "good", best2, release_hint2)
                    state.save()
                    return

                # Keep best known values for next follow-up phase.
                best_subtitle = best2
                release_hint = release_hint2
                quality_score = quality_score2
                state.record_bazarr_checked(movie_id, evaluation2, best2, release_hint2)
                state.save()
                ms = state.get_movie_state(movie_id)
        else:
            logger.info("Skipping Bazarr manual search due to retry policy: %s", label)

    if (
        evaluation == "poor"
        and best_subtitle is not None
        and subtitle_has_score(best_subtitle)
        and Config.ENABLE_BAZARR_PROVIDERS_RELEASE_HINT
    ):
        logger.info(
            "Subtitle is poor with Bazarr score for %s; querying providers endpoint.",
            label,
        )
        provider_subtitles = bazarr.fetch_provider_movie_candidates(movie)
        if provider_subtitles:
            provider_release_pool = [
                str(item.get("release_name")).strip()
                for item in provider_subtitles
                if item.get("release_name")
            ]
            logger.info(
                "Bazarr providers endpoint returned %s candidate subtitle item(s) for %s (release_info extracted: %s)",
                len(provider_subtitles),
                label,
                len(provider_release_pool),
            )
            evaluation_p, best_p, release_hint_p, quality_score_p = evaluate_subtitle_quality(movie, provider_subtitles)
            if best_p and subtitle_has_score(best_p):
                logger.info(
                    "Best providers candidate score for %s: score=%.2f provider=%s",
                    label,
                    subtitle_score_value(best_p),
                    best_p.get("provider") or "<unknown>",
                )
            if release_hint_p and not release_hint:
                release_hint = release_hint_p
            best_subtitle = best_p or best_subtitle
            quality_score = quality_score_p if quality_score_p != 0 else quality_score
        else:
            logger.info("Bazarr providers endpoint returned no usable release_info for %s", label)

    if not should_retry_followup(ms):
        logger.info("Skipping Radarr follow-up due to retry policy: %s", label)
        return

    # Follow-up on Radarr
    logger.info("Starting Radarr follow-up for: %s", label)

    success = False
    manual_required_reason: Optional[str] = None
    selected_candidate: Optional[Dict[str, Any]] = None

    if Config.ENABLE_RADARR_RELEASE_INSPECTION:
        try:
            # Preferred path for poor subtitles: use provider release_info pool exactly as user requested.
            if evaluation == "poor" and provider_release_pool:
                logger.info("Running Radarr manual search flow from Bazarr provider release_info for %s", label)

                # Step 0/0.1 - check and optionally delete existing movie file(s)
                movie_files = radarr.get_movie_files(movie_id)
                logger.info("Radarr moviefile check for %s returned %s file(s)", label, len(movie_files))
                if movie_files and Config.ENABLE_RADARR_DELETE_EXISTING_FILE_ON_POOR:
                    for mf in movie_files:
                        mf_id = mf.get("id")
                        if mf_id is None:
                            continue
                        deleted = radarr.delete_movie_file(int(mf_id))
                        logger.info("Radarr moviefile delete for %s fileId=%s result=%s", label, mf_id, deleted)

                # Step 1/2/3 - manual release search + selection from release_info pool + grab
                ordered_candidates, notes = radarr.list_candidates_from_release_pool(movie_id, provider_release_pool)
                if ordered_candidates:
                    for note in notes:
                        logger.info("Candidate selection note for %s: %s", label, note)

                    total = len(ordered_candidates)
                    for idx, candidate in enumerate(ordered_candidates, start=1):
                        selected_candidate = candidate
                        candidate_title = radarr_release_name(candidate) or "<unknown>"
                        logger.info(
                            "Trying Radarr candidate %s/%s from provider release_info pool for %s: %s",
                            idx, total, label, candidate_title
                        )
                        try:
                            result = radarr.grab_release_candidate(movie_id, candidate)
                            logger.info("Radarr release grab request sent for %s: %s", label, result)
                            accepted, accept_note = radarr.interpret_grab_response(result)
                            if not accepted:
                                manual_required_reason = f"Grab rejected for '{candidate_title}': {accept_note}"
                                logger.warning("Radarr grab rejected for %s candidate '%s': %s", label, candidate_title, accept_note)
                                continue
                            if Config.RADARR_GRAB_VERIFY_ENABLED:
                                confirmed, confirm_note = radarr.wait_for_grab_confirmation(
                                    movie_id,
                                    candidate,
                                    timeout_seconds=Config.RADARR_GRAB_VERIFY_TIMEOUT_SECONDS,
                                    poll_seconds=Config.RADARR_GRAB_VERIFY_POLL_SECONDS,
                                )
                                logger.info("Radarr grab verification for %s: %s", label, confirm_note)
                                if confirmed:
                                    success = True
                                    break
                                manual_required_reason = (
                                    f"Candidate '{candidate_title}' grab not confirmed; trying next candidate."
                                )
                            else:
                                success = True
                                break
                        except Exception as exc:
                            manual_required_reason = f"Release grab failed for '{candidate_title}': {exc}"
                            logger.error("Release grab failed for %s candidate '%s': %s", label, candidate_title, exc)

                    if not success and total > 0:
                        manual_required_reason = (
                            "Tried all pool-matched Radarr candidates but none started downloading."
                        )
                else:
                    manual_required_reason = "No downloadAllowed Radarr candidate matched Bazarr release_info pool."
                    logger.warning("No pool-matched candidate found for %s", label)
                    for note in notes:
                        logger.info("Candidate selection note for %s: %s", label, note)
            else:
                # Legacy exact-release path
                if not release_hint:
                    manual_required_reason = "Missing subtitle release name, exact match cannot be evaluated."
                    logger.warning("Moving to manual_required for %s: %s", label, manual_required_reason)
                else:
                    logger.info("Inspecting Radarr release candidates for %s", label)
                    selected_candidate, notes = radarr.find_exact_release_candidate(movie_id, release_hint)
                    if selected_candidate:
                        candidate_title = radarr_release_name(selected_candidate) or "<unknown>"
                        logger.info("Selected exact Radarr candidate for %s: %s", label, candidate_title)
                        logger.info(
                            "Selected candidate normalized release: %s",
                            normalize_release_for_exact(candidate_title) or "<empty>"
                        )
                        try:
                            result = radarr.grab_release_candidate(movie_id, selected_candidate)
                            logger.info("Radarr exact release grab started for %s: %s", label, result)
                            accepted, accept_note = radarr.interpret_grab_response(result)
                            if not accepted:
                                manual_required_reason = f"Exact release grab rejected: {accept_note}"
                                logger.warning("Exact release grab rejected for %s: %s", label, accept_note)
                                success = False
                            elif Config.RADARR_GRAB_VERIFY_ENABLED:
                                confirmed, confirm_note = radarr.wait_for_grab_confirmation(
                                    movie_id,
                                    selected_candidate,
                                    timeout_seconds=Config.RADARR_GRAB_VERIFY_TIMEOUT_SECONDS,
                                    poll_seconds=Config.RADARR_GRAB_VERIFY_POLL_SECONDS,
                                )
                                logger.info("Radarr grab verification for %s: %s", label, confirm_note)
                                success = confirmed
                                if not confirmed:
                                    manual_required_reason = (
                                        "Radarr grab request sent but not confirmed in queue/history within timeout."
                                    )
                            else:
                                success = True
                        except Exception as exc:
                            manual_required_reason = f"Exact release grab failed: {exc}"
                            logger.error("Exact release grab failed for %s: %s", label, exc)
                    else:
                        manual_required_reason = (
                            "No exact profile-allowed Radarr release candidate matched subtitle release."
                        )
                        logger.warning("No exact candidate found for %s", label)
                        for note in notes:
                            logger.info("Candidate selection note for %s: %s", label, note)
        except Exception as exc:
            manual_required_reason = f"Radarr release inspection failed: {exc}"
            logger.error("Radarr release inspection failed for %s: %s", label, exc)
    else:
        manual_required_reason = "Radarr release inspection is disabled."

    if not success and Config.ENABLE_RADARR_MOVIES_SEARCH_FALLBACK:
        logger.warning(
            "MoviesSearch fallback is enabled but strict exact mode requested; fallback is intentionally skipped."
        )

    state.record_followup_attempt(
        movie_id,
        success,
        manual_required_reason=manual_required_reason,
        selected_release_candidate=selected_candidate,
    )
    state.save()

    if success:
        logger.info("Radarr follow-up completed successfully for %s", label)
    else:
        logger.warning(
            "Radarr follow-up moved to manual_required for %s: %s",
            label,
            manual_required_reason or "exact match not available",
        )


# =============================================================================
# MAIN LOOP
# =============================================================================

def validate_config() -> None:
    problems = []

    if not Config.RADARR_API_KEY:
        problems.append("RADARR_API_KEY is missing")

    if Config.POLL_SECONDS < 30:
        problems.append("POLL_SECONDS should usually be >= 30")

    if Config.BAZARR_GRACE_SECONDS < 0:
        problems.append("BAZARR_GRACE_SECONDS must be >= 0")

    if problems:
        for problem in problems:
            logger.error("Config error: %s", problem)
        sys.exit(1)

    logger.info("Configuration loaded")
    logger.info("Radarr URL: %s", Config.RADARR_URL)
    logger.info("Bazarr URL: %s", Config.BAZARR_URL)
    logger.info("Polling interval: %ss", Config.POLL_SECONDS)
    logger.info("Bazarr grace period: %ss", Config.BAZARR_GRACE_SECONDS)
    logger.info("State file: %s", Config.STATE_FILE)
    logger.info("Preferred languages: %s", ", ".join(Config.PREFERRED_LANGUAGES))
    logger.info("Exact match only: %s", Config.EXACT_MATCH_ONLY)
    logger.info("Strict profile guard: %s", Config.STRICT_PROFILE_GUARD)
    logger.info("Use Bazarr score when release missing: %s", Config.USE_BAZARR_SCORE_WHEN_RELEASE_MISSING)
    logger.info(
        "Treat subtitle file reference as good when score missing: %s",
        Config.TREAT_FILE_REFERENCE_AS_GOOD_WHEN_SCORE_MISSING,
    )
    logger.info("Bazarr mode: %s", Config.BAZARR_MODE)
    logger.info("Bazarr lookup endpoint: %s", Config.BAZARR_MOVIE_LOOKUP_ENDPOINT)
    logger.info("Bazarr lookup fallback endpoints: %s", ", ".join(Config.BAZARR_MOVIE_LOOKUP_FALLBACK_ENDPOINTS))
    logger.info("Bazarr history lookup enabled: %s", Config.BAZARR_ENABLE_HISTORY_LOOKUP)
    logger.info("Bazarr search endpoint: %s", Config.BAZARR_SEARCH_ENDPOINT)
    logger.info("Bazarr search trigger enabled: %s", Config.ENABLE_BAZARR_SEARCH_TRIGGER)
    logger.info("Bazarr manual search on poor enabled: %s", Config.ENABLE_BAZARR_MANUAL_SEARCH_ON_POOR)
    logger.info("Bazarr manual search endpoints: %s", ", ".join(Config.BAZARR_MANUAL_SEARCH_ENDPOINTS))
    logger.info("Bazarr manual search method: %s", Config.BAZARR_MANUAL_SEARCH_METHOD)
    logger.info("Bazarr manual search wait seconds: %s", Config.BAZARR_MANUAL_SEARCH_WAIT_SECONDS)
    logger.info("Bazarr manual search max attempts: %s", Config.BAZARR_MANUAL_SEARCH_MAX_ATTEMPTS)
    logger.info("Bazarr providers release hint enabled: %s", Config.ENABLE_BAZARR_PROVIDERS_RELEASE_HINT)
    logger.info("Bazarr providers movies endpoint: %s", Config.BAZARR_PROVIDERS_MOVIES_ENDPOINT)
    logger.info("MoviesSearch fallback enabled: %s", Config.ENABLE_RADARR_MOVIES_SEARCH_FALLBACK)
    logger.info("Radarr delete existing file on poor enabled: %s", Config.ENABLE_RADARR_DELETE_EXISTING_FILE_ON_POOR)
    logger.info("Radarr grab verify enabled: %s", Config.RADARR_GRAB_VERIFY_ENABLED)
    logger.info("Radarr grab verify timeout seconds: %s", Config.RADARR_GRAB_VERIFY_TIMEOUT_SECONDS)
    logger.info("Radarr grab verify poll seconds: %s", Config.RADARR_GRAB_VERIFY_POLL_SECONDS)
    logger.info("Radarr grab verify use history: %s", Config.RADARR_GRAB_VERIFY_USE_HISTORY)


def run_once(state: StateManager, bazarr: BazarrClient, radarr: RadarrClient) -> None:
    logger.info("Starting polling cycle")

    try:
        movies = radarr.get_movies()
    except Exception as exc:
        logger.error("Failed to fetch movie list from Radarr: %s", exc)
        return

    logger.info("Received %s movie(s) from Radarr", len(movies))

    # Process all movies that are either new or already tracked and not done
    tracked_ids = set(state.state.get("movies", {}).keys())

    candidates = []
    for movie in movies:
        movie_id = str(movie["id"])
        if movie_id in tracked_ids or is_new_movie(movie, state):
            candidates.append(movie)

    logger.info("Processing %s candidate movie(s)", len(candidates))

    # Sort by date added when available
    def sort_key(m: Dict[str, Any]) -> Tuple[int, str]:
        dt = m.get("added") or m.get("dateAdded") or ""
        return (0 if dt else 1, dt)

    candidates.sort(key=sort_key)

    for movie in candidates:
        movie_id = int(movie["id"])
        if state.is_done(movie_id):
            continue
        try:
            process_movie(movie, state, bazarr, radarr)
        except Exception as exc:
            logger.exception("Unhandled error while processing %s: %s", title_year_string(movie), exc)
            state.set_error(movie_id, str(exc))
            state.save()


def main() -> None:
    setup_logging()
    validate_config()

    http = HttpClient(
        timeout=Config.HTTP_TIMEOUT,
        retries=Config.HTTP_RETRIES,
        backoff_seconds=Config.HTTP_BACKOFF_SECONDS,
        verify_ssl=Config.VERIFY_SSL,
        user_agent=Config.USER_AGENT,
    )

    state = StateManager(Config.STATE_FILE)
    radarr = RadarrClient(Config.RADARR_URL, Config.RADARR_API_KEY, http)
    bazarr = BazarrClient(Config.BAZARR_URL, Config.BAZARR_API_KEY, http)

    logger.info("Script started")

    while True:
        try:
            run_once(state, bazarr, radarr)
            state.save()
        except KeyboardInterrupt:
            logger.info("Interrupted by user, exiting.")
            break
        except Exception as exc:
            logger.exception("Top-level loop error: %s", exc)

        logger.info("Polling cycle complete. Sleeping for %s seconds.", Config.POLL_SECONDS)
        time.sleep(Config.POLL_SECONDS)


if __name__ == "__main__":
    main()
