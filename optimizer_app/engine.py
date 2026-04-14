# -*- coding: utf-8 -*-

import time
from typing import Any, Dict, List, Optional, Tuple

from optimizer_app.bazarr_client import BazarrClient
from optimizer_app.config import AppConfig
from optimizer_app.logging_utils import logger
from optimizer_app.radarr_client import RadarrClient, radarr_release_name
from optimizer_app.state_manager import StateManager
from optimizer_app.utils import (
    build_release_hint,
    choose_best_subtitle,
    normalize_release_for_exact,
    seconds_since,
    subtitle_has_file_reference,
    subtitle_has_score,
    subtitle_release_name,
    subtitle_score_value,
    title_year_string,
    utc_now_iso,
)


class ProcessingEngine:
    def __init__(self, config: AppConfig, state: StateManager, bazarr: BazarrClient, radarr: RadarrClient) -> None:
        self.config = config
        self.state = state
        self.bazarr = bazarr
        self.radarr = radarr
        self._initial_catalog_seed_completed: Optional[bool] = None

    def _seed_existing_movies_as_done_once(self, movies: List[Dict[str, Any]]) -> None:
        if self._initial_catalog_seed_completed is None:
            self._initial_catalog_seed_completed = bool(
                self.state.store.get_meta("initial_radarr_seed_completed", False)
            )

        if self._initial_catalog_seed_completed:
            return

        seeded = 0
        for movie in movies:
            movie_id = int(movie["id"])
            now_iso = utc_now_iso()
            did_seed = {"value": False}

            def mutator(ms: Dict[str, Any]) -> None:
                # First-run baseline: existing Radarr movies should appear in UI as Done.
                if ms.get("detected") or ms.get("done"):
                    return
                ms["detected"] = True
                ms["title"] = movie.get("title")
                ms["year"] = movie.get("year")
                ms["imdb_id"] = movie.get("imdbId")
                ms["first_seen_at"] = ms.get("first_seen_at") or now_iso
                ms["radarr_date_added"] = movie.get("added") or movie.get("dateAdded")
                if self.has_movie_file(movie):
                    ms["file_detected_at"] = ms.get("file_detected_at") or now_iso
                    ms["bazarr_grace_started_at"] = ms.get("bazarr_grace_started_at") or now_iso
                ms["status"] = "done"
                ms["done"] = True
                did_seed["value"] = True

            updated = self.state.store.update_movie_state(movie_id, mutator)
            if did_seed["value"]:
                self.state.store.add_event(
                    int(updated["_db_id"]),
                    movie_id,
                    int(updated["_cycle"]),
                    "initial_baseline_done",
                    "Movie existed before first startup; baseline status set to done.",
                    {"title": movie.get("title"), "year": movie.get("year")},
                )
                seeded += 1

        self.state.store.set_meta("initial_radarr_seed_completed", True)
        self._initial_catalog_seed_completed = True
        logger.info(
            "Initial Radarr baseline completed: %s existing movie(s) were seeded as done.",
            seeded,
        )

    def has_movie_file(self, movie: Dict[str, Any]) -> bool:
        return bool(movie.get("hasFile") or movie.get("movieFile"))

    def should_wait_for_bazarr_grace(self, movie_state: Dict[str, Any]) -> bool:
        started = movie_state.get("bazarr_grace_started_at")
        if not started:
            return True
        elapsed = seconds_since(started)
        if elapsed is None:
            return True
        return elapsed < self.config.bazarr_grace_seconds

    def should_retry_followup(self, movie_state: Dict[str, Any]) -> bool:
        attempts = int(movie_state.get("radarr_followup_attempts") or 0)
        if attempts >= self.config.max_followup_attempts:
            return False
        last_at = movie_state.get("radarr_followup_last_at")
        if not last_at:
            return True
        elapsed = seconds_since(last_at)
        if elapsed is None:
            return True
        return elapsed >= self.config.retry_cooldown_seconds

    def should_retry_bazarr_manual_search(self, movie_state: Dict[str, Any]) -> bool:
        attempts = int(movie_state.get("bazarr_manual_search_attempts") or 0)
        if attempts >= self.config.bazarr_manual_search_max_attempts:
            return False
        last_at = movie_state.get("bazarr_manual_search_last_at")
        if not last_at:
            return True
        elapsed = seconds_since(last_at)
        if elapsed is None:
            return True
        return elapsed >= self.config.bazarr_manual_search_retry_cooldown_seconds

    def evaluate_subtitle_quality(
        self, movie: Dict[str, Any], subtitles: List[Dict[str, Any]]
    ) -> Tuple[str, Optional[Dict[str, Any]], Optional[str], float]:
        if not subtitles:
            return "none", None, None, 0.0
        best = choose_best_subtitle(subtitles, self.config.preferred_languages)
        if not best:
            return "none", None, None, 0.0

        release_hint = subtitle_release_name(best)
        bazarr_score = subtitle_score_value(best)
        if bazarr_score > 0:
            good_by_score = bazarr_score >= self.config.good_subtitle_min_score
            return ("good" if good_by_score else "poor"), best, release_hint or None, bazarr_score

        if not release_hint:
            release_hint = build_release_hint(movie, best)
        if self.config.treat_file_reference_as_good_when_score_missing and subtitle_has_file_reference(best):
            return "good", best, release_hint or None, -1.0
        return "none", best, release_hint or None, 0.0

    def process_movie(self, movie: Dict[str, Any]) -> None:
        movie_id = int(movie["id"])
        label = title_year_string(movie)
        ms = self.state.get_movie_state(movie_id)
        if self.state.is_done(movie_id):
            return

        if not ms["detected"]:
            logger.info("Found new movie in Radarr: %s | id=%s", label, movie_id)
            self.state.record_detected(movie)
            self.state.save()
            ms = self.state.get_movie_state(movie_id)

        if not self.has_movie_file(movie):
            logger.info("Movie still has no file, waiting: %s", label)
            ms["status"] = "waiting_for_file"
            self.state.save()
            return

        if not ms.get("file_detected_at"):
            logger.info("Movie file is now available: %s", label)
            self.state.record_file_detected(movie_id)
            self.state.save()
            ms = self.state.get_movie_state(movie_id)

        if self.should_wait_for_bazarr_grace(ms):
            logger.info(
                "Waiting for Bazarr grace period before evaluation: %s | grace=%ss",
                label,
                self.config.bazarr_grace_seconds,
            )
            ms["status"] = "bazarr_waiting"
            self.state.save()
            return

        if self.config.enable_bazarr_search_trigger:
            logger.info("Attempting Bazarr subtitle search trigger for: %s", label)
            self.bazarr.trigger_subtitle_search(movie)

        logger.info("Checking Bazarr subtitle state for: %s", label)
        lookup = self.bazarr.lookup_movie_subtitle_state(movie)
        subtitles = lookup.get("subtitles") or []
        logger.info("Bazarr reported %s subtitle item(s) for %s", len(subtitles), label)

        if self.config.exact_match_only and not subtitles:
            raw = lookup.get("raw")
            if isinstance(raw, dict):
                logger.info("Bazarr raw top-level keys for %s: %s", label, ", ".join(sorted(raw.keys())[:25]))
            logger.warning("No subtitle entries were extracted from Bazarr for %s; keeping movie pending for next cycle.", label)
            self.state.record_bazarr_checked(movie_id, "none", None, None)
            self.state.save()
            return

        evaluation, best_subtitle, release_hint, quality_score = self.evaluate_subtitle_quality(movie, subtitles)
        subtitle_release = subtitle_release_name(best_subtitle)
        file_release = self._extract_file_basename_from_radarr(movie)
        normalized_subtitle_release = normalize_release_for_exact(subtitle_release or release_hint or "")
        normalized_file_release = normalize_release_for_exact(file_release)

        if best_subtitle and subtitle_has_score(best_subtitle):
            logger.info(
                "Best subtitle score source for %s: score=%.2f provider=%s",
                label,
                subtitle_score_value(best_subtitle),
                best_subtitle.get("provider") or "<unknown>",
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
            self.state.record_bazarr_checked(movie_id, "good", best_subtitle, release_hint)
            self.state.save()
            return

        if evaluation == "none":
            logger.warning("No subtitles found in Bazarr for %s", label)
            self.state.record_bazarr_checked(movie_id, "none", None, None)
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
            self.state.record_bazarr_checked(movie_id, "poor", best_subtitle, release_hint)

        self.state.save()
        ms = self.state.get_movie_state(movie_id)
        provider_release_pool: List[str] = []

        if evaluation == "poor" and self.config.enable_bazarr_manual_search_on_poor:
            if self.should_retry_bazarr_manual_search(ms):
                logger.info("Trying Bazarr manual subtitle search for %s", label)
                manual_triggered = self.bazarr.trigger_manual_subtitle_search(movie)
                self.state.record_bazarr_manual_search_attempt(movie_id, manual_triggered)
                self.state.save()
                ms = self.state.get_movie_state(movie_id)

                if manual_triggered:
                    wait_seconds = max(0, int(self.config.bazarr_manual_search_wait_seconds))
                    if wait_seconds > 0:
                        logger.info("Waiting %ss for Bazarr manual search results: %s", wait_seconds, label)
                        time.sleep(wait_seconds)
                    logger.info("Re-checking Bazarr subtitle state after manual search: %s", label)
                    lookup_after_manual = self.bazarr.lookup_movie_subtitle_state(movie)
                    subtitles_after_manual = lookup_after_manual.get("subtitles") or []
                    logger.info("Bazarr reported %s subtitle item(s) after manual search for %s", len(subtitles_after_manual), label)

                    evaluation2, best2, release_hint2, quality_score2 = self.evaluate_subtitle_quality(movie, subtitles_after_manual)
                    if evaluation2 == "good":
                        chosen_name2 = subtitle_release_name(best2) or "<unknown>"
                        logger.info("Subtitle match looks good after Bazarr manual search for %s", label)
                        logger.info("Selected subtitle: %s", chosen_name2)
                        if quality_score2 < 0:
                            logger.info("Quality score: N/A (Bazarr did not expose score; subtitle file exists)")
                        else:
                            logger.info("Quality score: %.2f", quality_score2)
                        self.state.record_bazarr_checked(movie_id, "good", best2, release_hint2)
                        self.state.save()
                        return

                    best_subtitle = best2
                    release_hint = release_hint2
                    quality_score = quality_score2
                    self.state.record_bazarr_checked(movie_id, evaluation2, best2, release_hint2)
                    self.state.save()
                    ms = self.state.get_movie_state(movie_id)
            else:
                logger.info("Skipping Bazarr manual search due to retry policy: %s", label)

        if (
            evaluation == "poor"
            and best_subtitle is not None
            and subtitle_has_score(best_subtitle)
            and self.config.enable_bazarr_providers_release_hint
        ):
            logger.info("Subtitle is poor with Bazarr score for %s; querying providers endpoint.", label)
            provider_subtitles = self.bazarr.fetch_provider_movie_candidates(movie)
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
                evaluation_p, best_p, release_hint_p, quality_score_p = self.evaluate_subtitle_quality(movie, provider_subtitles)
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

        if not self.should_retry_followup(ms):
            logger.info("Skipping Radarr follow-up due to retry policy: %s", label)
            return

        logger.info("Starting Radarr follow-up for: %s", label)
        success = False
        manual_required_reason: Optional[str] = None
        selected_candidate: Optional[Dict[str, Any]] = None

        if self.config.enable_radarr_release_inspection:
            try:
                if evaluation == "poor" and provider_release_pool:
                    logger.info("Running Radarr manual search flow from Bazarr provider release_info for %s", label)

                    movie_files = self.radarr.get_movie_files(movie_id)
                    logger.info("Radarr moviefile check for %s returned %s file(s)", label, len(movie_files))
                    if movie_files and self.config.enable_radarr_delete_existing_file_on_poor:
                        for mf in movie_files:
                            mf_id = mf.get("id")
                            if mf_id is None:
                                continue
                            deleted = self.radarr.delete_movie_file(int(mf_id))
                            logger.info("Radarr moviefile delete for %s fileId=%s result=%s", label, mf_id, deleted)

                    ordered_candidates, notes = self.radarr.list_candidates_from_release_pool(movie_id, provider_release_pool)
                    if ordered_candidates:
                        for note in notes:
                            logger.info("Candidate selection note for %s: %s", label, note)

                        total = len(ordered_candidates)
                        for idx, candidate in enumerate(ordered_candidates, start=1):
                            queue_active, queue_note = self.radarr.has_active_queue_download(movie_id)
                            if queue_active:
                                logger.info(
                                    "Radarr queue already has active download for %s; stopping additional release attempts: %s",
                                    label,
                                    queue_note,
                                )
                                success = True
                                manual_required_reason = None
                                break

                            selected_candidate = candidate
                            candidate_title = radarr_release_name(candidate) or "<unknown>"
                            logger.info(
                                "Trying Radarr candidate %s/%s from provider release_info pool for %s: %s",
                                idx,
                                total,
                                label,
                                candidate_title,
                            )
                            try:
                                result = self.radarr.grab_release_candidate(movie_id, candidate)
                                logger.info("Radarr release grab request sent for %s: %s", label, result)
                                accepted, accept_note = self.radarr.interpret_grab_response(result)
                                if not accepted:
                                    manual_required_reason = f"Grab rejected for '{candidate_title}': {accept_note}"
                                    logger.warning("Radarr grab rejected for %s candidate '%s': %s", label, candidate_title, accept_note)
                                    continue
                                if self.config.radarr_grab_verify_enabled:
                                    confirmed, confirm_note = self.radarr.wait_for_grab_confirmation(
                                        movie_id,
                                        candidate,
                                        timeout_seconds=self.config.radarr_grab_verify_timeout_seconds,
                                        poll_seconds=self.config.radarr_grab_verify_poll_seconds,
                                    )
                                    logger.info("Radarr grab verification for %s: %s", label, confirm_note)
                                    if confirmed:
                                        success = True
                                        break
                                    queue_active_after_verify, queue_note_after_verify = self.radarr.has_active_queue_download(movie_id)
                                    if queue_active_after_verify:
                                        logger.info(
                                            "Radarr queue became active for %s after candidate %s; stopping additional attempts: %s",
                                            label,
                                            idx,
                                            queue_note_after_verify,
                                        )
                                        success = True
                                        manual_required_reason = None
                                        break
                                    manual_required_reason = f"Candidate '{candidate_title}' grab not confirmed; trying next candidate."
                                else:
                                    success = True
                                    break
                            except Exception as exc:
                                manual_required_reason = f"Release grab failed for '{candidate_title}': {exc}"
                                logger.error("Release grab failed for %s candidate '%s': %s", label, candidate_title, exc)
                                queue_active_on_error, queue_note_on_error = self.radarr.has_active_queue_download(movie_id)
                                if queue_active_on_error:
                                    logger.info(
                                        "Radarr queue has active download for %s after error on candidate %s; stopping additional attempts: %s",
                                        label,
                                        idx,
                                        queue_note_on_error,
                                    )
                                    success = True
                                    manual_required_reason = None
                                    break

                        if not success and total > 0:
                            manual_required_reason = "Tried all pool-matched Radarr candidates but none started downloading."
                    else:
                        manual_required_reason = "No downloadAllowed Radarr candidate matched Bazarr release_info pool."
                        logger.warning("No pool-matched candidate found for %s", label)
                        for note in notes:
                            logger.info("Candidate selection note for %s: %s", label, note)
                else:
                    if not release_hint:
                        manual_required_reason = "Missing subtitle release name, exact match cannot be evaluated."
                        logger.warning("Moving to manual_required for %s: %s", label, manual_required_reason)
                    else:
                        logger.info("Inspecting Radarr release candidates for %s", label)
                        selected_candidate, notes = self.radarr.find_exact_release_candidate(movie_id, release_hint)
                        if selected_candidate:
                            candidate_title = radarr_release_name(selected_candidate) or "<unknown>"
                            logger.info("Selected exact Radarr candidate for %s: %s", label, candidate_title)
                            logger.info("Selected candidate normalized release: %s", normalize_release_for_exact(candidate_title) or "<empty>")
                            try:
                                result = self.radarr.grab_release_candidate(movie_id, selected_candidate)
                                logger.info("Radarr exact release grab started for %s: %s", label, result)
                                accepted, accept_note = self.radarr.interpret_grab_response(result)
                                if not accepted:
                                    manual_required_reason = f"Exact release grab rejected: {accept_note}"
                                    logger.warning("Exact release grab rejected for %s: %s", label, accept_note)
                                    success = False
                                elif self.config.radarr_grab_verify_enabled:
                                    confirmed, confirm_note = self.radarr.wait_for_grab_confirmation(
                                        movie_id,
                                        selected_candidate,
                                        timeout_seconds=self.config.radarr_grab_verify_timeout_seconds,
                                        poll_seconds=self.config.radarr_grab_verify_poll_seconds,
                                    )
                                    logger.info("Radarr grab verification for %s: %s", label, confirm_note)
                                    success = confirmed
                                    if not confirmed:
                                        manual_required_reason = "Radarr grab request sent but not confirmed in queue/history within timeout."
                                else:
                                    success = True
                            except Exception as exc:
                                manual_required_reason = f"Exact release grab failed: {exc}"
                                logger.error("Exact release grab failed for %s: %s", label, exc)
                        else:
                            manual_required_reason = "No exact profile-allowed Radarr release candidate matched subtitle release."
                            logger.warning("No exact candidate found for %s", label)
                            for note in notes:
                                logger.info("Candidate selection note for %s: %s", label, note)
            except Exception as exc:
                manual_required_reason = f"Radarr release inspection failed: {exc}"
                logger.error("Radarr release inspection failed for %s: %s", label, exc)
        else:
            manual_required_reason = "Radarr release inspection is disabled."

        if not success and self.config.enable_radarr_movies_search_fallback:
            logger.warning("MoviesSearch fallback is enabled but strict exact mode requested; fallback is intentionally skipped.")

        self.state.record_followup_attempt(
            movie_id,
            success,
            manual_required_reason=manual_required_reason,
            selected_release_candidate=selected_candidate,
        )
        self.state.save()

        if success:
            logger.info("Radarr follow-up completed successfully for %s", label)
        else:
            logger.warning("Radarr follow-up moved to manual_required for %s: %s", label, manual_required_reason or "exact match not available")

    def _extract_file_basename_from_radarr(self, movie: Dict[str, Any]) -> str:
        movie_file = movie.get("movieFile") or {}
        path = movie_file.get("path") or ""
        if not path:
            return ""
        base = path.replace("\\", "/").split("/")[-1]
        if "." in base:
            base = ".".join(base.split(".")[:-1])
        return base

    def run_once(self) -> None:
        logger.info("Starting polling cycle")
        try:
            movies = self.radarr.get_movies()
        except Exception as exc:
            logger.error("Failed to fetch movie list from Radarr: %s", exc)
            return

        logger.info("Received %s movie(s) from Radarr", len(movies))
        self.state.store.relink_removed_movies_by_imdb(movies)
        current_ids = [int(m["id"]) for m in movies if isinstance(m, dict) and m.get("id") is not None]
        self.state.store.reconcile_radarr_presence(current_ids)
        self._seed_existing_movies_as_done_once(movies)
        candidates: List[Dict[str, Any]] = list(movies)

        logger.info("Processing %s candidate movie(s)", len(candidates))

        def sort_key(m: Dict[str, Any]) -> Tuple[int, str]:
            dt = m.get("added") or m.get("dateAdded") or ""
            return (0 if dt else 1, dt)

        candidates.sort(key=sort_key)
        for movie in candidates:
            movie_id = int(movie["id"])
            if self.state.is_done(movie_id):
                continue
            try:
                self.process_movie(movie)
            except Exception as exc:
                logger.exception("Unhandled error while processing %s: %s", title_year_string(movie), exc)
                self.state.set_error(movie_id, str(exc))
                self.state.save()
