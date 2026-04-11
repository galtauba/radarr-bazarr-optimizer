# -*- coding: utf-8 -*-

from typing import Any, Dict, List, Optional, Tuple

from optimizer_app.config import AppConfig
from optimizer_app.http_client import HttpClient
from optimizer_app.logging_utils import logger
from optimizer_app.utils import (
    _clean_subtitle_release_candidate,
    normalize_release_for_exact,
    normalize_string,
    subtitle_has_file_reference,
    subtitle_has_score,
    subtitle_release_name,
    title_year_string,
)


class BazarrClient:
    def __init__(self, config: AppConfig, http: HttpClient) -> None:
        self.config = config
        self.base_url = config.bazarr_url.rstrip("/")
        self.api_key = config.bazarr_api_key
        self.http = http
        self._search_endpoint_disabled = False
        self._manual_search_endpoints_disabled: set = set()

    def _headers(self) -> Dict[str, str]:
        headers = {}
        if self.api_key:
            headers[self.config.bazarr_api_key_header] = self.api_key
        return headers

    def _sanitize_endpoint(self, endpoint: str, fallback: str = "/api/movies") -> str:
        text = str(endpoint or "").strip()
        text = text.strip().strip("[]").strip().strip("\"'")
        if not text:
            return fallback
        # if a malformed list fragment was saved, keep only the first endpoint-like token
        if "," in text and not text.startswith("/"):
            text = text.split(",", 1)[0].strip()
        if not text.startswith("/"):
            text = "/" + text.lstrip("/")
        return text

    def _fetch_lookup_payload(
        self,
        movie: Dict[str, Any],
        endpoint: str,
        *,
        include_lookup_style: bool,
    ) -> Optional[Any]:
        radarr_movie_id = int(movie["id"])
        endpoint = self._sanitize_endpoint(endpoint)
        if include_lookup_style:
            if self.config.bazarr_lookup_style == "path_radarrid":
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
                "GET",
                url,
                resp.status_code,
            )
            return None

        try:
            return resp.json()
        except Exception:
            logger.warning("Bazarr lookup response is not valid JSON for %s via %s", title_year_string(movie), endpoint)
            return None

    def lookup_movie_subtitle_state(self, movie: Dict[str, Any]) -> Dict[str, Any]:
        if self.config.bazarr_mode == "disabled":
            return {"movie_found": False, "subtitles_present": False, "subtitles": [], "raw": None}

        radarr_movie_id = int(movie["id"])
        endpoints_to_try: List[Tuple[str, bool]] = [
            (self._sanitize_endpoint(self.config.bazarr_movie_lookup_endpoint), True)
        ]
        for endpoint in self.config.bazarr_movie_lookup_fallback_endpoints:
            endpoints_to_try.append((self._sanitize_endpoint(endpoint), False))

        best_result = {"movie_found": False, "subtitles_present": False, "subtitles": [], "raw": None}
        aggregated_subtitles: List[Dict[str, Any]] = []

        for endpoint, with_lookup_style in endpoints_to_try:
            payload = self._fetch_lookup_payload(movie, endpoint, include_lookup_style=with_lookup_style)
            if payload is None:
                continue
            normalized = self._normalize_bazarr_movie_lookup(payload, radarr_movie_id)
            if self.config.bazarr_enable_history_lookup:
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
                str(
                    candidate.get("score")
                    or candidate.get("percent")
                    or candidate.get("matches")
                    or candidate.get("match_score")
                    or ""
                ),
                normalize_string(
                    str(candidate.get("language") or candidate.get("lang") or candidate.get("code2") or candidate.get("code3") or "")
                ),
            )
            if signature in seen:
                continue
            seen.add(signature)
            merged.append(candidate)
        return merged

    def trigger_subtitle_search(self, movie: Dict[str, Any]) -> bool:
        if self.config.bazarr_mode == "disabled" or not self.config.enable_bazarr_search_trigger:
            return False
        if self._search_endpoint_disabled:
            return False

        url = f"{self.base_url}{self._sanitize_endpoint(self.config.bazarr_search_endpoint, '/api/movies/subtitles')}"
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
                "languages": self.config.preferred_languages,
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
            "Bazarr search trigger endpoint seems incompatible (%s). Disabling trigger for this run.",
            url,
        )
        self._search_endpoint_disabled = True
        return False

    def trigger_manual_subtitle_search(self, movie: Dict[str, Any]) -> bool:
        if self.config.bazarr_mode == "disabled" or not self.config.enable_bazarr_manual_search_on_poor:
            return False

        method = (self.config.bazarr_manual_search_method or "AUTO").upper()
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
                "languages": self.config.preferred_languages,
            },
        ]

        for endpoint in self.config.bazarr_manual_search_endpoints:
            endpoint = self._sanitize_endpoint(endpoint, "/api/movies/subtitles")
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
                        request_variants.append(("POST", None, payload))
                        request_variants.append(("POST", payload, None))
                    else:
                        request_variants.append(("POST", None, payload))
                        request_variants.append(("POST", payload, None))
                        request_variants.append(("GET", payload, None))

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

            if not endpoint_had_accepted:
                logger.warning(
                    "Bazarr manual search endpoint may be incompatible for %s: %s",
                    title_year_string(movie),
                    endpoint,
                )
                self._manual_search_endpoints_disabled.add(endpoint)
        return False

    def fetch_provider_movie_candidates(self, movie: Dict[str, Any]) -> List[Dict[str, Any]]:
        if self.config.bazarr_mode == "disabled" or not self.config.enable_bazarr_providers_release_hint:
            return []

        url = f"{self.base_url}{self._sanitize_endpoint(self.config.bazarr_providers_movies_endpoint, '/api/providers/movies')}"
        movie_id = int(movie["id"])
        param_variants = [{"radarrid": movie_id}, {"radarrId": movie_id}, {"movieId": movie_id}]
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
                    self.config.bazarr_providers_movies_endpoint,
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
                signature = (
                    normalized_release,
                    str(score),
                    normalize_string(str(language)),
                    normalize_string(str(provider)),
                )
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
        result = {"movie_found": False, "subtitles_present": False, "subtitles": [], "raw": data}
        candidate_movie_obj = None

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

        if candidate_movie_obj is None and isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and self._looks_like_movie_record(item, radarr_movie_id):
                    candidate_movie_obj = item
                    break

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
        candidates = [obj.get("radarrId"), obj.get("radarrid"), obj.get("movieId"), obj.get("id")]
        for value in candidates:
            try:
                if int(value) == radarr_movie_id:
                    return True
            except Exception:
                continue
        return False

    def _extract_subtitles_from_movie_record(self, movie_record: Dict[str, Any]) -> List[Dict[str, Any]]:
        possible_keys = ["subtitles", "missing_subtitles", "external_subtitles", "downloaded_subtitles", "audio_subtitles"]
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

    def _extract_subtitles_from_history_payload(self, payload: Any, movie: Dict[str, Any]) -> List[Dict[str, Any]]:
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
                for key in ("score", "percent", "matches", "match_score"):
                    if normalized_candidate.get(key) is None and row.get(key) is not None:
                        normalized_candidate[key] = row.get(key)

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
        marker_keys = ("action", "event", "provider", "score", "percent", "matches", "match_score", "language", "path", "subtitle_path")
        return any(key in row for key in marker_keys)

    def _history_row_matches_movie(self, row: Dict[str, Any], movie: Dict[str, Any]) -> bool:
        movie_id = int(movie["id"])
        id_keys = ("radarrId", "radarrid", "radarr_id", "movieId", "movie_id", "id")
        for key in id_keys:
            value = row.get(key)
            try:
                if int(value) == movie_id:
                    return True
            except Exception:
                pass

        for key in ("movie", "radarr", "item", "data"):
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
        for key in ("subtitle", "subtitles", "external_subtitle", "external_subtitles", "downloaded_subtitles", "item", "data"):
            nested = row.get(key)
            if isinstance(nested, dict):
                yield nested
            elif isinstance(nested, list):
                for item in nested:
                    if isinstance(item, dict):
                        yield item
