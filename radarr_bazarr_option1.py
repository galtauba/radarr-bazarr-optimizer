#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
radarr_bazarr_option1.py

Flow implemented (Option 1):
1. Detect newly added movies in Radarr
2. Wait until the movie actually has a file (hasFile / movieFile)
3. Wait a grace period so Bazarr can do its normal automatic subtitle work
4. Query Bazarr (adapter, instance-specific) for subtitle status / subtitle candidates
5. Evaluate whether the subtitle match is good enough
6. If not good enough:
   - extract a release hint from subtitle data
   - inspect Radarr release candidates
   - fallback to MoviesSearch command
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

    # Endpoint to trigger/ask Bazarr to search subtitles for a movie that already exists
    # This may differ in your instance or may not be exposed the way this script expects.
    BAZARR_SEARCH_ENDPOINT = os.getenv("BAZARR_SEARCH_ENDPOINT", "/api/movies/subtitles").strip()

    # Strategy for Bazarr movie lookup:
    # "query_param_radarrid" => GET endpoint?radarrid=123
    # "path_radarrid"        => GET endpoint/123
    BAZARR_LOOKUP_STYLE = os.getenv("BAZARR_LOOKUP_STYLE", "query_param_radarrid").strip().lower()

    # Enable/disable exact candidate grab TODO
    ENABLE_RADARR_RELEASE_INSPECTION = env_bool("ENABLE_RADARR_RELEASE_INSPECTION", True)
    ENABLE_RADARR_MOVIES_SEARCH_FALLBACK = env_bool("ENABLE_RADARR_MOVIES_SEARCH_FALLBACK", True)
    ENABLE_EXACT_RELEASE_GRAB_TODO = env_bool("ENABLE_EXACT_RELEASE_GRAB_TODO", False)


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
    possible_values = [
        sub.get("language"),
        sub.get("lang"),
        sub.get("language_code"),
        sub.get("languageCode"),
        sub.get("code2"),
        sub.get("code3"),
    ]
    possible_values = [normalize_string(str(x)) for x in possible_values if x is not None]

    for idx, wanted in enumerate(preferred_languages):
        wanted_norm = normalize_string(wanted)
        if wanted_norm in possible_values:
            return idx

    return len(preferred_languages) + 10


def subtitle_score_value(sub: Dict[str, Any]) -> float:
    for key in ("score", "matches", "percent", "match_score"):
        raw = sub.get(key)
        if raw is None:
            continue
        try:
            return float(raw)
        except Exception:
            continue
    return 0.0


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

    subtitle_title = (
        subtitle_entry.get("release_name")
        or subtitle_entry.get("releaseName")
        or subtitle_entry.get("name")
        or subtitle_entry.get("title")
        or subtitle_entry.get("filename")
        or ""
    ).strip()

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
    "version": 2,
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
                "radarr_followup_attempts": 0,
                "radarr_followup_last_at": None,
                "radarr_followup_success": False,
                "last_error": None,
                "done": False,
            }
        return self.state["movies"][key]

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

        if evaluation == "good":
            ms["status"] = "done"
            ms["done"] = True
        elif evaluation == "poor":
            ms["status"] = "subtitle_matched_poor"
        elif evaluation == "none":
            ms["status"] = "subtitle_missing"
        else:
            ms["status"] = "bazarr_checked"

    def record_followup_attempt(self, movie_id: int, success: bool) -> None:
        ms = self.get_movie_state(movie_id)
        ms["radarr_followup_attempts"] += 1
        ms["radarr_followup_last_at"] = utc_now_iso()
        ms["radarr_followup_success"] = success
        ms["status"] = "radarr_followup_done" if success else "radarr_followup_failed"
        ms["done"] = True

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

    def trigger_movies_search(self, movie_id: int) -> Dict[str, Any]:
        url = f"{self.base_url}/api/v3/command"
        payload = {
            "name": "MoviesSearch",
            "movieIds": [movie_id],
        }
        resp = self.http.request("POST", url, headers=self.headers, json_body=payload)
        return resp.json()

    def inspect_release_candidates(self, movie_id: int, release_hint: str) -> Optional[Dict[str, Any]]:
        candidates = self.get_release_candidates(movie_id)
        if not candidates:
            return None

        best = None
        best_score = -1.0

        for item in candidates:
            title = (
                item.get("title")
                or item.get("releaseTitle")
                or item.get("guid")
                or ""
            )
            score = match_quality_between_release_strings(title, release_hint)
            if score > best_score:
                best = item
                best_score = score

        if best:
            best["_local_match_score"] = best_score
        return best

    def exact_release_grab_todo(self, candidate: Dict[str, Any]) -> bool:
        logger.warning(
            "TODO: exact release grab is not implemented. "
            "Confirm the safe endpoint and payload against your Radarr API docs first."
        )
        logger.info("Best candidate snapshot: %s", json.dumps(candidate, ensure_ascii=False)[:1200])
        return False


# =============================================================================
# BAZARR CLIENT (INSTANCE-SPECIFIC ADAPTER)
# =============================================================================

class BazarrClient:
    def __init__(self, base_url: str, api_key: str, http: HttpClient) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.http = http

    def _headers(self) -> Dict[str, str]:
        headers = {}
        if self.api_key:
            headers[Config.BAZARR_API_KEY_HEADER] = self.api_key
        return headers

    def _movie_lookup_url_and_params(self, radarr_movie_id: int) -> Tuple[str, Dict[str, Any]]:
        endpoint = Config.BAZARR_MOVIE_LOOKUP_ENDPOINT
        if Config.BAZARR_LOOKUP_STYLE == "path_radarrid":
            return f"{self.base_url}{endpoint.rstrip('/')}/{radarr_movie_id}", {}
        return f"{self.base_url}{endpoint}", {"radarrid": radarr_movie_id}

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
        url, params = self._movie_lookup_url_and_params(radarr_movie_id)

        try:
            resp = self.http.request(
                "GET",
                url,
                headers=self._headers(),
                params=params,
                allow_statuses=[400, 404, 405],
            )
        except Exception as exc:
            logger.error("Bazarr lookup failed for %s: %s", title_year_string(movie), exc)
            return {
                "movie_found": False,
                "subtitles_present": False,
                "subtitles": [],
                "raw": None,
            }

        if resp.status_code in (400, 404, 405):
            logger.warning(
                "Bazarr movie lookup endpoint seems incompatible (%s %s returned %s). "
                "Adjust BAZARR_MOVIE_LOOKUP_ENDPOINT / BAZARR_LOOKUP_STYLE.",
                "GET", url, resp.status_code
            )
            return {
                "movie_found": False,
                "subtitles_present": False,
                "subtitles": [],
                "raw": None,
            }

        try:
            data = resp.json()
        except Exception:
            logger.warning("Bazarr lookup response is not valid JSON for %s", title_year_string(movie))
            return {
                "movie_found": False,
                "subtitles_present": False,
                "subtitles": [],
                "raw": None,
            }

        normalized = self._normalize_bazarr_movie_lookup(data, radarr_movie_id)
        return normalized

    def trigger_subtitle_search(self, movie: Dict[str, Any]) -> bool:
        """
        Optional Bazarr-triggered search after the file exists.
        IMPORTANT: may need endpoint/method/payload adjustment for your instance.
        """
        if Config.BAZARR_MODE == "disabled":
            return False

        url = f"{self.base_url}{Config.BAZARR_SEARCH_ENDPOINT}"
        payload = {
            "radarrId": int(movie["id"]),
            "title": movie.get("title"),
            "year": movie.get("year"),
            "path": (movie.get("movieFile") or {}).get("path"),
            "languages": Config.PREFERRED_LANGUAGES,
        }

        try:
            resp = self.http.request(
                "POST",
                url,
                headers=self._headers(),
                json_body=payload,
                allow_statuses=[400, 404, 405],
            )
        except Exception as exc:
            logger.error("Bazarr subtitle search trigger failed for %s: %s", title_year_string(movie), exc)
            return False

        if resp.status_code in (400, 404, 405):
            logger.warning(
                "Bazarr search trigger endpoint seems incompatible (%s returned %s). "
                "Adjust BAZARR_SEARCH_ENDPOINT / method / payload for your instance.",
                url, resp.status_code
            )
            return False

        return True

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

        for key in possible_keys:
            value = movie_record.get(key)
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        subtitles.append(item)
                    else:
                        subtitles.append({"name": str(item)})
        return subtitles


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

    release_hint = build_release_hint(movie, best)
    file_basename = extract_file_basename_from_radarr(movie)
    similarity = match_quality_between_release_strings(file_basename, release_hint)

    subtitle_score = subtitle_score_value(best)
    richness = subtitle_metadata_richness(best)

    # Good if:
    # - similarity is high enough, OR
    # - score is high enough and similarity is at least decent
    good = False
    if similarity >= Config.MATCH_SIMILARITY_THRESHOLD:
        good = True
    elif subtitle_score >= Config.GOOD_SUBTITLE_MIN_SCORE and similarity >= 25:
        good = True
    elif richness >= 5 and similarity >= 30:
        good = True

    return ("good" if good else "poor"), best, release_hint, similarity


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

    # Optional: trigger Bazarr search once, just before we inspect, if you want a gentle nudge
    # This does not assume certainty of endpoint shape.
    logger.info("Attempting Bazarr subtitle search trigger for: %s", label)
    bazarr.trigger_subtitle_search(movie)

    # Query Bazarr
    logger.info("Checking Bazarr subtitle state for: %s", label)
    lookup = bazarr.lookup_movie_subtitle_state(movie)

    subtitles = lookup.get("subtitles") or []
    logger.info("Bazarr reported %s subtitle item(s) for %s", len(subtitles), label)

    evaluation, best_subtitle, release_hint, similarity = evaluate_subtitle_quality(movie, subtitles)

    if evaluation == "good":
        chosen_name = (
            best_subtitle.get("release_name")
            or best_subtitle.get("releaseName")
            or best_subtitle.get("name")
            or best_subtitle.get("title")
            or best_subtitle.get("filename")
            or "<unknown>"
        )
        logger.info("Subtitle match looks good for %s", label)
        logger.info("Selected subtitle: %s", chosen_name)
        logger.info("Release hint: %s", release_hint)
        logger.info("Similarity against current file: %.2f", similarity)
        state.record_bazarr_checked(movie_id, "good", best_subtitle, release_hint)
        state.save()
        return

    if evaluation == "none":
        logger.warning("No subtitles found in Bazarr for %s", label)
        state.record_bazarr_checked(movie_id, "none", None, None)
    else:
        chosen_name = (
            best_subtitle.get("release_name")
            or best_subtitle.get("releaseName")
            or best_subtitle.get("name")
            or best_subtitle.get("title")
            or best_subtitle.get("filename")
            or "<unknown>"
        )
        logger.warning("Subtitle match is poor for %s", label)
        logger.info("Selected subtitle candidate: %s", chosen_name)
        logger.info("Release hint: %s", release_hint)
        logger.info("Similarity against current file: %.2f", similarity)
        state.record_bazarr_checked(movie_id, "poor", best_subtitle, release_hint)

    state.save()
    ms = state.get_movie_state(movie_id)

    if not should_retry_followup(ms):
        logger.info("Skipping Radarr follow-up due to retry policy: %s", label)
        return

    # Follow-up on Radarr
    logger.info("Starting Radarr follow-up for: %s", label)

    success = False

    if Config.ENABLE_RADARR_RELEASE_INSPECTION and release_hint:
        try:
            logger.info("Inspecting Radarr release candidates for %s", label)
            best_candidate = radarr.inspect_release_candidates(movie_id, release_hint)
            if best_candidate:
                candidate_title = (
                    best_candidate.get("title")
                    or best_candidate.get("releaseTitle")
                    or best_candidate.get("guid")
                    or "<unknown>"
                )
                candidate_score = best_candidate.get("_local_match_score")
                logger.info(
                    "Best Radarr release candidate for %s: %s | local_match_score=%.2f",
                    label, candidate_title, candidate_score if candidate_score is not None else -1.0
                )

                if Config.ENABLE_EXACT_RELEASE_GRAB_TODO:
                    success = radarr.exact_release_grab_todo(best_candidate)
                else:
                    logger.info(
                        "Exact release grab is intentionally disabled/TODO. "
                        "Using fallback MoviesSearch instead for %s",
                        label
                    )
            else:
                logger.info("No Radarr release candidates found for %s", label)
        except Exception as exc:
            logger.error("Radarr release inspection failed for %s: %s", label, exc)

    if not success and Config.ENABLE_RADARR_MOVIES_SEARCH_FALLBACK:
        try:
            logger.info("Sending MoviesSearch command to Radarr for %s", label)
            result = radarr.trigger_movies_search(movie_id)
            logger.info("Radarr MoviesSearch started for %s: %s", label, result)
            success = True
        except Exception as exc:
            logger.error("Radarr MoviesSearch failed for %s: %s", label, exc)
            state.set_error(movie_id, str(exc))

    state.record_followup_attempt(movie_id, success)
    state.save()

    if success:
        logger.info("Radarr follow-up completed successfully for %s", label)
    else:
        logger.warning("Radarr follow-up completed with failure for %s", label)


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
    logger.info("Bazarr mode: %s", Config.BAZARR_MODE)
    logger.info("Bazarr lookup endpoint: %s", Config.BAZARR_MOVIE_LOOKUP_ENDPOINT)
    logger.info("Bazarr search endpoint: %s", Config.BAZARR_SEARCH_ENDPOINT)


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