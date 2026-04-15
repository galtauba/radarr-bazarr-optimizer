# -*- coding: utf-8 -*-

from typing import Any, Dict, Optional

from optimizer_app.config import AppConfig
from optimizer_app.db import SQLiteStore


def candidate_state_snapshot(candidate: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not candidate:
        return None
    return {
        "title": str(candidate.get("title") or candidate.get("releaseTitle") or candidate.get("guid") or "").strip(),
        "guid": candidate.get("guid"),
        "indexerId": candidate.get("indexerId"),
        "rejected": candidate.get("rejected"),
        "rejections": candidate.get("rejections"),
    }


class StateManager:
    def __init__(self, path: str, config: AppConfig, store: SQLiteStore) -> None:
        self.path = path
        self.config = config
        self.store = store
        self.state = {"movies": {}, "last_run": None}

    def save(self) -> None:
        self.store.set_last_run()
        self.state["last_run"] = self.store.get_meta("last_run")

    def get_movie_state(self, movie_id: int) -> Dict[str, Any]:
        return self.store.get_active_movie_state(movie_id)

    def is_done(self, movie_id: int) -> bool:
        return self.store.is_done(movie_id)

    def set_error(self, movie_id: int, error: str) -> None:
        def mutator(ms: Dict[str, Any]) -> None:
            ms["last_error"] = error
            ms["status"] = "error"
        updated = self.store.update_movie_state(movie_id, mutator)
        self.store.add_event(
            int(updated["_db_id"]),
            movie_id,
            int(updated["_cycle"]),
            "error",
            "Unhandled processing error",
            {"error": error},
        )

    def record_detected(self, movie: Dict[str, Any]) -> None:
        movie_id = int(movie["id"])
        from optimizer_app.utils import utc_now_iso

        def mutator(ms: Dict[str, Any]) -> None:
            if not ms.get("detected"):
                ms["detected"] = True
                ms["title"] = movie.get("title")
                ms["year"] = movie.get("year")
                ms["imdb_id"] = movie.get("imdbId")
                ms["first_seen_at"] = utc_now_iso()
                ms["radarr_date_added"] = movie.get("added") or movie.get("dateAdded")
                ms["status"] = "waiting_for_file"

        updated = self.store.update_movie_state(movie_id, mutator)
        self.store.add_event(
            int(updated["_db_id"]),
            movie_id,
            int(updated["_cycle"]),
            "detected",
            "Detected in Radarr",
            {"title": movie.get("title"), "year": movie.get("year")},
        )

    def record_file_detected(self, movie_id: int) -> None:
        from optimizer_app.utils import utc_now_iso

        def mutator(ms: Dict[str, Any]) -> None:
            if not ms.get("file_detected_at"):
                ms["file_detected_at"] = utc_now_iso()
            if not ms.get("bazarr_grace_started_at"):
                ms["bazarr_grace_started_at"] = utc_now_iso()
            ms["status"] = "bazarr_waiting"

        updated = self.store.update_movie_state(movie_id, mutator)
        self.store.add_event(
            int(updated["_db_id"]),
            movie_id,
            int(updated["_cycle"]),
            "file_detected",
            "Movie file detected",
        )

    def record_bazarr_checked(
        self,
        movie_id: int,
        evaluation: str,
        selected_subtitle: Optional[Dict[str, Any]],
        release_hint: Optional[str],
    ) -> None:
        from optimizer_app.utils import utc_now_iso

        def mutator(ms: Dict[str, Any]) -> None:
            ms["bazarr_checked_at"] = utc_now_iso()
            ms["subtitle_evaluation"] = evaluation
            ms["selected_subtitle"] = selected_subtitle
            ms["release_hint"] = release_hint
            ms["exact_match_required"] = self.config.exact_match_only
            ms["manual_required_reason"] = None
            if evaluation == "good":
                ms["status"] = "done"
                ms["done"] = True
            elif evaluation == "poor":
                ms["status"] = "subtitle_matched_poor"
            elif evaluation == "none":
                ms["status"] = "subtitle_missing"
            else:
                ms["status"] = "bazarr_checked"

        updated = self.store.update_movie_state(movie_id, mutator)
        self.store.add_event(
            int(updated["_db_id"]),
            movie_id,
            int(updated["_cycle"]),
            "bazarr_checked",
            f"Bazarr evaluated subtitle as {evaluation}",
            {"evaluation": evaluation, "release_hint": release_hint, "selected_subtitle": selected_subtitle},
        )

    def record_bazarr_manual_search_attempt(self, movie_id: int, success: bool) -> None:
        from optimizer_app.utils import utc_now_iso

        def mutator(ms: Dict[str, Any]) -> None:
            ms["bazarr_manual_search_attempts"] = int(ms.get("bazarr_manual_search_attempts") or 0) + 1
            ms["bazarr_manual_search_last_at"] = utc_now_iso()
            ms["bazarr_manual_search_last_success"] = bool(success)

        updated = self.store.update_movie_state(movie_id, mutator)
        self.store.add_event(
            int(updated["_db_id"]),
            movie_id,
            int(updated["_cycle"]),
            "bazarr_manual_search",
            "Bazarr manual search attempted",
            {"success": bool(success)},
        )

    def record_followup_attempt(
        self,
        movie_id: int,
        success: bool,
        *,
        manual_required_reason: Optional[str] = None,
        selected_release_candidate: Optional[Dict[str, Any]] = None,
    ) -> None:
        from optimizer_app.utils import utc_now_iso

        def mutator(ms: Dict[str, Any]) -> None:
            ms["radarr_followup_attempts"] = int(ms.get("radarr_followup_attempts") or 0) + 1
            ms["radarr_followup_last_at"] = utc_now_iso()
            ms["radarr_followup_success"] = bool(success)
            ms["selected_release_candidate"] = candidate_state_snapshot(selected_release_candidate)
            if success:
                ms["status"] = "radarr_followup_done"
                ms["manual_required_reason"] = None
            else:
                ms["status"] = "manual_required"
                ms["manual_required_reason"] = manual_required_reason or "Follow-up failed"
            ms["done"] = True

        updated = self.store.update_movie_state(movie_id, mutator)
        self.store.add_event(
            int(updated["_db_id"]),
            movie_id,
            int(updated["_cycle"]),
            "radarr_followup",
            "Radarr follow-up completed" if success else "Radarr follow-up failed",
            {
                "success": bool(success),
                "manual_required_reason": manual_required_reason,
                "selected_release_candidate": candidate_state_snapshot(selected_release_candidate),
            },
        )

    def record_moviessearch_fallback_triggered(
        self,
        movie_id: int,
        reason: str,
        command_result: Optional[Dict[str, Any]] = None,
    ) -> None:
        from optimizer_app.utils import utc_now_iso

        def mutator(ms: Dict[str, Any]) -> None:
            ms["radarr_followup_attempts"] = int(ms.get("radarr_followup_attempts") or 0) + 1
            ms["radarr_followup_last_at"] = utc_now_iso()
            ms["radarr_followup_success"] = True
            ms["selected_release_candidate"] = None
            ms["manual_required_reason"] = reason
            ms["status"] = "radarr_moviessearch_fallback_triggered"
            ms["done"] = True

        updated = self.store.update_movie_state(movie_id, mutator)
        self.store.add_event(
            int(updated["_db_id"]),
            movie_id,
            int(updated["_cycle"]),
            "radarr_moviessearch_fallback",
            "Strict match not found; triggered Radarr MoviesSearch fallback and closed cycle.",
            {
                "reason": reason,
                "command_result": command_result or {},
            },
        )

    def list_tracked_movie_ids(self):
        return self.store.list_tracked_radarr_ids()
