# -*- coding: utf-8 -*-

import time
from typing import Any, Dict, List, Optional, Tuple

from optimizer_app.config import AppConfig
from optimizer_app.http_client import HttpClient
from optimizer_app.logging_utils import logger
from optimizer_app.utils import normalize_release_for_exact, normalize_string, parse_iso, utc_now


def radarr_release_name(candidate: Optional[Dict[str, Any]]) -> str:
    if not candidate:
        return ""
    value = candidate.get("title") or candidate.get("releaseTitle") or candidate.get("guid") or ""
    return str(value).strip()


class RadarrClient:
    def __init__(self, config: AppConfig, http: HttpClient) -> None:
        self.config = config
        self.base_url = config.radarr_url.rstrip("/")
        self.headers = {}
        if config.radarr_api_key:
            self.headers["X-Api-Key"] = config.radarr_api_key
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
        if not self.config.strict_profile_guard:
            return True, "profile guard disabled"
        if bool(candidate.get("rejected")):
            return False, self._candidate_rejection_reason(candidate)
        rejections = candidate.get("rejections")
        if isinstance(rejections, list) and len(rejections) > 0:
            return False, self._candidate_rejection_reason(candidate)
        return True, ""

    def find_exact_release_candidate(self, movie_id: int, target_release: str) -> Tuple[Optional[Dict[str, Any]], List[str]]:
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
            title_norm = normalize_release_for_exact(radarr_release_name(candidate))
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

    def list_candidates_from_release_pool(self, movie_id: int, release_pool: List[str]) -> Tuple[List[Dict[str, Any]], List[str]]:
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
        blocked_count = 0
        mismatch_count = 0
        rejected_count = 0

        for candidate in candidates:
            title_norm = normalize_release_for_exact(radarr_release_name(candidate))
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
                rejected_count += 1
                continue
            matched_allowed.append((rank, candidate))

        matched_allowed.sort(key=lambda x: x[0])
        ordered: List[Dict[str, Any]] = [x[1] for x in matched_allowed]
        if ordered:
            return ordered, []

        return [], [
            f"release_pool_size={len(normalized_pool)}",
            f"mismatch_count={mismatch_count}",
            f"download_blocked_count={blocked_count}",
            f"rejected_count={rejected_count}",
            "No strict Radarr candidate matched Bazarr release_info pool (requires downloadAllowed=true and rejections=[]).",
        ]

    def grab_release_candidate(self, movie_id: int, candidate: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/api/v3/release"
        guid = candidate.get("guid")
        indexer_id = candidate.get("indexerId")
        if not guid:
            raise RuntimeError("Cannot grab release candidate: missing guid")
        if indexer_id is None:
            raise RuntimeError("Cannot grab release candidate: missing indexerId")

        resp = self.http.request(
            "POST",
            url,
            headers=self.headers,
            json_body={"guid": guid, "indexerId": indexer_id, "movieId": movie_id},
            allow_statuses=[400, 404, 409, 422],
        )
        try:
            data = resp.json()
            if isinstance(data, dict):
                data.setdefault("status_code", resp.status_code)
                return data
            return {"status_code": resp.status_code, "data": data}
        except Exception:
            return {"status_code": resp.status_code, "text": resp.text[:500]}

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

    def has_active_queue_download(self, movie_id: int) -> Tuple[bool, str]:
        records = self.get_queue_records(movie_id)
        if not records:
            return False, "no queue records for movie"

        inactive_statuses = {
            "completed",
            "imported",
            "failed",
            "warning",
            "queued_for_import",
        }
        for rec in records:
            status = normalize_string(str(rec.get("status") or ""))
            tracked_state = normalize_string(str(rec.get("trackedDownloadState") or ""))
            tracked_status = normalize_string(str(rec.get("trackedDownloadStatus") or ""))
            title = str(rec.get("title") or rec.get("sourceTitle") or "<unknown>")
            download_id = str(rec.get("downloadId") or "")

            is_active = True
            if status in inactive_statuses or tracked_state in inactive_statuses:
                is_active = False
            if "failed" in tracked_status:
                is_active = False
            if "downloading" in status or "queued" in status or "downloading" in tracked_state or "queued" in tracked_state:
                is_active = True

            if is_active:
                note = f"active queue item exists (title={title}, status={status or tracked_state or 'unknown'})"
                if download_id:
                    note = f"{note}, downloadId={download_id}"
                return True, note
        return False, "queue records exist but none are active"

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
        event_values = [record.get("eventType"), record.get("eventTypeName"), record.get("event")]
        for value in event_values:
            if "grab" in normalize_string(str(value or "")):
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
            for rec in self.get_queue_records(movie_id):
                rec_movie_id = rec.get("movieId")
                try:
                    if int(rec_movie_id) != int(movie_id):
                        continue
                except Exception:
                    continue
                title_norm = normalize_release_for_exact(str(rec.get("title") or rec.get("sourceTitle") or ""))
                if candidate_title_norm and title_norm and (
                    candidate_title_norm in title_norm or title_norm in candidate_title_norm
                ):
                    return True, "confirmed via queue title match"
                if not candidate_title_norm:
                    return True, "confirmed via queue movieId"

            if self.config.radarr_grab_verify_use_history:
                for rec in self.get_history_records(movie_id):
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

        if self.config.radarr_grab_verify_use_history:
            return False, "grab not confirmed in queue/history within timeout"
        return False, "grab not confirmed in queue within timeout"

    def trigger_movies_search(self, movie_id: int) -> Tuple[bool, str, Dict[str, Any]]:
        url = f"{self.base_url}/api/v3/command"
        payload = {
            "name": "MoviesSearch",
            "movieIds": [int(movie_id)],
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
            if not isinstance(data, dict):
                data = {"data": data}
        except Exception:
            data = {"text": resp.text[:500]}

        data.setdefault("status_code", resp.status_code)

        if resp.status_code >= 400:
            message = str(data.get("message") or data.get("error") or data.get("text") or "MoviesSearch command failed")
            return False, f"HTTP {resp.status_code}: {message}", data

        command_name = str(data.get("name") or "")
        command_id = data.get("id")
        command_status = str(data.get("status") or "")
        note = f"MoviesSearch command accepted (id={command_id}, status={command_status or 'queued'})"
        if command_name:
            note = f"{command_name} accepted (id={command_id}, status={command_status or 'queued'})"
        return True, note, data
