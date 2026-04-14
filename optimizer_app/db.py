# -*- coding: utf-8 -*-

import json
import os
import sqlite3
import threading
from typing import Any, Dict, List, Optional

from optimizer_app.logging_utils import logger
from optimizer_app.utils import utc_now_iso


MOVIE_STATE_DEFAULTS: Dict[str, Any] = {
    "status": "new",
    "detected": False,
    "imdb_id": None,
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


def _json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _json_load(value: Optional[str], default: Any = None) -> Any:
    if value is None:
        return default
    try:
        return json.loads(value)
    except Exception:
        return default


def _normalize_imdb_id(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.lower() in {"none", "null", "tt0"}:
        return None
    if not text.startswith("tt"):
        return None
    digits = text[2:]
    if not digits.isdigit():
        return None
    return f"tt{digits}"


class SQLiteStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        db_dir = os.path.dirname(os.path.abspath(db_path))
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        self._init_schema()

    def close(self) -> None:
        with self.lock:
            self.conn.close()

    def _init_schema(self) -> None:
        with self.lock:
            cur = self.conn.cursor()
            cur.executescript(
                """
                PRAGMA journal_mode=WAL;

                CREATE TABLE IF NOT EXISTS settings (
                  key TEXT PRIMARY KEY,
                  value TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS app_meta (
                  key TEXT PRIMARY KEY,
                  value TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS movies (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  radarr_movie_id INTEGER NOT NULL,
                  cycle INTEGER NOT NULL DEFAULT 1,
                  title TEXT,
                  year INTEGER,
                  is_removed INTEGER NOT NULL DEFAULT 0,
                  removed_at TEXT,
                  state_json TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  UNIQUE(radarr_movie_id, cycle)
                );

                CREATE INDEX IF NOT EXISTS idx_movies_radarr ON movies(radarr_movie_id);
                CREATE INDEX IF NOT EXISTS idx_movies_active ON movies(radarr_movie_id, is_removed);

                CREATE TABLE IF NOT EXISTS movie_events (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  movie_db_id INTEGER NOT NULL,
                  radarr_movie_id INTEGER NOT NULL,
                  cycle INTEGER NOT NULL,
                  event_type TEXT NOT NULL,
                  message TEXT,
                  data_json TEXT,
                  created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_events_movie ON movie_events(movie_db_id, id DESC);
                CREATE INDEX IF NOT EXISTS idx_events_radarr ON movie_events(radarr_movie_id, id DESC);
                """
            )
            self.conn.commit()

    # Settings / meta
    def set_setting(self, key: str, value: Any) -> None:
        now = utc_now_iso()
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO settings(key, value, updated_at) VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                (key, _json_dump(value), now),
            )
            self.conn.commit()

    def get_setting(self, key: str, default: Any = None) -> Any:
        with self.lock:
            row = self.conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        if not row:
            return default
        return _json_load(row["value"], default)

    def list_settings(self) -> Dict[str, Any]:
        with self.lock:
            rows = self.conn.execute("SELECT key, value FROM settings").fetchall()
        return {r["key"]: _json_load(r["value"]) for r in rows}

    def set_meta(self, key: str, value: Any) -> None:
        now = utc_now_iso()
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO app_meta(key, value, updated_at) VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                (key, _json_dump(value), now),
            )
            self.conn.commit()

    def get_meta(self, key: str, default: Any = None) -> Any:
        with self.lock:
            row = self.conn.execute("SELECT value FROM app_meta WHERE key = ?", (key,)).fetchone()
        if not row:
            return default
        return _json_load(row["value"], default)

    # Movies + state
    def _create_cycle_row(self, movie_id: int, title: Optional[str], year: Optional[int], cycle: int, *, readded: bool = False) -> int:
        now = utc_now_iso()
        with self.lock:
            cur = self.conn.execute(
                """
                INSERT INTO movies(radarr_movie_id, cycle, title, year, is_removed, removed_at, state_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, 0, NULL, ?, ?, ?)
                """,
                (movie_id, cycle, title, year, _json_dump(dict(MOVIE_STATE_DEFAULTS)), now, now),
            )
            self.conn.commit()
            db_id = int(cur.lastrowid)
        self.add_event(
            db_id,
            movie_id,
            cycle,
            "movie_readded" if readded else "movie_cycle_created",
            "Movie re-added after removal" if readded else "Movie cycle row created",
            {"cycle": cycle},
        )
        return db_id

    def _row_to_state(self, row: sqlite3.Row) -> Dict[str, Any]:
        state = _json_load(row["state_json"], {}) or {}
        state.setdefault("movie_id", int(row["radarr_movie_id"]))
        state.setdefault("title", row["title"])
        state.setdefault("year", row["year"])
        state["_db_id"] = int(row["id"])
        state["_cycle"] = int(row["cycle"])
        state["_is_removed"] = bool(row["is_removed"])
        state["_removed_at"] = row["removed_at"]
        return state

    def _save_state(self, db_id: int, state: Dict[str, Any]) -> None:
        payload = dict(state)
        for key in ("_db_id", "_cycle", "_is_removed", "_removed_at"):
            payload.pop(key, None)
        now = utc_now_iso()
        with self.lock:
            self.conn.execute(
                "UPDATE movies SET title=?, year=?, state_json=?, updated_at=? WHERE id=?",
                (payload.get("title"), payload.get("year"), _json_dump(payload), now, db_id),
            )
            self.conn.commit()

    def get_active_movie_state(self, movie_id: int, *, movie_title: Optional[str] = None, movie_year: Optional[int] = None) -> Dict[str, Any]:
        with self.lock:
            row = self.conn.execute(
                """
                SELECT * FROM movies
                WHERE radarr_movie_id = ? AND is_removed = 0
                ORDER BY cycle DESC
                LIMIT 1
                """,
                (movie_id,),
            ).fetchone()
            if row:
                return self._row_to_state(row)

            last_row = self.conn.execute(
                "SELECT MAX(cycle) AS max_cycle FROM movies WHERE radarr_movie_id = ?",
                (movie_id,),
            ).fetchone()
            next_cycle = (int(last_row["max_cycle"]) + 1) if last_row and last_row["max_cycle"] else 1
            had_previous = bool(last_row and last_row["max_cycle"])

        db_id = self._create_cycle_row(movie_id, movie_title, movie_year, next_cycle, readded=had_previous)
        with self.lock:
            row = self.conn.execute("SELECT * FROM movies WHERE id=?", (db_id,)).fetchone()
        return self._row_to_state(row)

    def update_movie_state(self, movie_id: int, mutator) -> Dict[str, Any]:
        state = self.get_active_movie_state(movie_id)
        mutator(state)
        self._save_state(int(state["_db_id"]), state)
        return state

    def is_done(self, movie_id: int) -> bool:
        return bool(self.get_active_movie_state(movie_id).get("done"))

    def set_removed(self, movie_id: int) -> None:
        with self.lock:
            row = self.conn.execute(
                """
                SELECT * FROM movies
                WHERE radarr_movie_id=? AND is_removed=0
                ORDER BY cycle DESC LIMIT 1
                """,
                (movie_id,),
            ).fetchone()
            if not row:
                return
            now = utc_now_iso()
            state = _json_load(row["state_json"], {}) or {}
            state["status"] = "removed_from_radarr"
            state["done"] = True
            self.conn.execute(
                "UPDATE movies SET is_removed=1, removed_at=?, state_json=?, updated_at=? WHERE id=?",
                (now, _json_dump(state), now, row["id"]),
            )
            self.conn.commit()
        self.add_event(int(row["id"]), movie_id, int(row["cycle"]), "radarr_removed", "Movie removed from Radarr; soft-deleted locally.")

    def list_active_radarr_ids(self) -> List[int]:
        with self.lock:
            rows = self.conn.execute("SELECT radarr_movie_id FROM movies WHERE is_removed=0").fetchall()
        return [int(r["radarr_movie_id"]) for r in rows]

    def list_tracked_radarr_ids(self) -> List[int]:
        with self.lock:
            rows = self.conn.execute("SELECT DISTINCT radarr_movie_id FROM movies").fetchall()
        return [int(r["radarr_movie_id"]) for r in rows]

    def add_event(
        self,
        movie_db_id: int,
        radarr_movie_id: int,
        cycle: int,
        event_type: str,
        message: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO movie_events(movie_db_id, radarr_movie_id, cycle, event_type, message, data_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (movie_db_id, radarr_movie_id, cycle, event_type, message, _json_dump(data or {}), utc_now_iso()),
            )
            self.conn.commit()

    def list_movies(self, view: str = "active") -> List[Dict[str, Any]]:
        where = ""
        if view == "active":
            where = "WHERE is_removed=0"
        elif view == "removed":
            where = "WHERE is_removed=1"
        with self.lock:
            rows = self.conn.execute(
                f"""
                SELECT * FROM movies
                {where}
                ORDER BY updated_at DESC
                """
            ).fetchall()
        out: List[Dict[str, Any]] = []
        for row in rows:
            state = self._row_to_state(row)
            out.append(
                {
                    "db_id": int(row["id"]),
                    "radarr_movie_id": int(row["radarr_movie_id"]),
                    "cycle": int(row["cycle"]),
                    "title": row["title"],
                    "year": row["year"],
                    "status": state.get("status"),
                    "done": bool(state.get("done")),
                    "subtitle_evaluation": state.get("subtitle_evaluation"),
                    "updated_at": row["updated_at"],
                    "removed_at": row["removed_at"],
                    "is_removed": bool(row["is_removed"]),
                }
            )
        return out

    def get_movie_detail(self, radarr_movie_id: int) -> Optional[Dict[str, Any]]:
        with self.lock:
            active = self.conn.execute(
                """
                SELECT * FROM movies WHERE radarr_movie_id=? ORDER BY is_removed ASC, cycle DESC LIMIT 1
                """,
                (radarr_movie_id,),
            ).fetchone()
        if not active:
            return None
        active_state = self._row_to_state(active)
        with self.lock:
            events_rows = self.conn.execute(
                """
                SELECT * FROM movie_events WHERE radarr_movie_id=? ORDER BY id DESC LIMIT 300
                """,
                (radarr_movie_id,),
            ).fetchall()
            cycle_rows = self.conn.execute(
                "SELECT id, cycle, is_removed, removed_at, updated_at FROM movies WHERE radarr_movie_id=? ORDER BY cycle DESC",
                (radarr_movie_id,),
            ).fetchall()
        events = []
        for row in events_rows:
            events.append(
                {
                    "id": int(row["id"]),
                    "cycle": int(row["cycle"]),
                    "event_type": row["event_type"],
                    "message": row["message"],
                    "data": _json_load(row["data_json"], {}),
                    "created_at": row["created_at"],
                }
            )
        cycles = [
            {
                "db_id": int(r["id"]),
                "cycle": int(r["cycle"]),
                "is_removed": bool(r["is_removed"]),
                "removed_at": r["removed_at"],
                "updated_at": r["updated_at"],
            }
            for r in cycle_rows
        ]
        return {
            "radarr_movie_id": radarr_movie_id,
            "title": active["title"],
            "year": active["year"],
            "active_cycle": int(active["cycle"]),
            "state": active_state,
            "events": events,
            "cycles": cycles,
        }

    def update_movie_action(self, radarr_movie_id: int, action: str) -> bool:
        state = self.get_active_movie_state(radarr_movie_id)
        if action == "retry":
            state["radarr_followup_attempts"] = 0
            state["radarr_followup_last_at"] = None
            state["bazarr_manual_search_attempts"] = 0
            state["bazarr_manual_search_last_at"] = None
            state["done"] = False
            state["status"] = "subtitle_matched_poor"
            self._save_state(int(state["_db_id"]), state)
            self.add_event(int(state["_db_id"]), radarr_movie_id, int(state["_cycle"]), "manual_retry", "Retry follow-up requested from UI.")
            return True
        if action == "mark_done":
            state["done"] = True
            state["status"] = "done"
            self._save_state(int(state["_db_id"]), state)
            self.add_event(int(state["_db_id"]), radarr_movie_id, int(state["_cycle"]), "manual_mark_done", "Movie marked done from UI.")
            return True
        if action == "reopen":
            state["done"] = False
            state["status"] = "reopened"
            self._save_state(int(state["_db_id"]), state)
            self.add_event(int(state["_db_id"]), radarr_movie_id, int(state["_cycle"]), "manual_reopen", "Movie reopened from UI.")
            return True
        return False

    def set_last_run(self) -> None:
        self.set_meta("last_run", utc_now_iso())

    def get_dashboard_counts(self) -> Dict[str, int]:
        with self.lock:
            active = self.conn.execute("SELECT COUNT(*) AS c FROM movies WHERE is_removed=0").fetchone()["c"]
            removed = self.conn.execute("SELECT COUNT(*) AS c FROM movies WHERE is_removed=1").fetchone()["c"]
            manual_required = self.conn.execute(
                "SELECT COUNT(*) AS c FROM movies WHERE is_removed=0 AND json_extract(state_json, '$.status')='manual_required'"
            ).fetchone()["c"]
            done = self.conn.execute(
                "SELECT COUNT(*) AS c FROM movies WHERE is_removed=0 AND json_extract(state_json, '$.done')=1"
            ).fetchone()["c"]
        return {
            "active": int(active),
            "removed": int(removed),
            "manual_required": int(manual_required),
            "done": int(done),
        }

    def reconcile_radarr_presence(self, current_radarr_ids: List[int]) -> None:
        current = set(int(x) for x in current_radarr_ids)
        active = set(self.list_active_radarr_ids())
        to_remove = sorted(active - current)
        for movie_id in to_remove:
            logger.info("Movie %s disappeared from Radarr. Marking as soft-removed in local DB.", movie_id)
            self.set_removed(movie_id)

    def relink_removed_movies_by_imdb(self, movies: List[Dict[str, Any]]) -> None:
        for movie in movies:
            try:
                self._relink_removed_movie_by_imdb(movie)
            except Exception as exc:
                movie_id = movie.get("id")
                logger.warning("Failed IMDb relink check for movie id=%s: %s", movie_id, exc)

    def _relink_removed_movie_by_imdb(self, movie: Dict[str, Any]) -> None:
        new_movie_id = int(movie.get("id"))
        imdb_id = _normalize_imdb_id(movie.get("imdbId"))
        if not imdb_id:
            return

        new_title = movie.get("title")
        new_year = movie.get("year")

        with self.lock:
            existing_new = self.conn.execute(
                "SELECT COUNT(*) AS c FROM movies WHERE radarr_movie_id=?",
                (new_movie_id,),
            ).fetchone()
            if existing_new and int(existing_new["c"] or 0) > 0:
                return

            match_rows = self.conn.execute(
                """
                SELECT radarr_movie_id, MAX(COALESCE(removed_at, updated_at)) AS ts
                FROM movies
                WHERE is_removed=1
                  AND json_extract(state_json, '$.imdb_id') = ?
                GROUP BY radarr_movie_id
                ORDER BY ts DESC
                """,
                (imdb_id,),
            ).fetchall()

            if not match_rows:
                return

            if len(match_rows) > 1:
                logger.info(
                    "Skipping IMDb relink for Radarr movie id=%s imdb=%s due to ambiguity (%s removed matches).",
                    new_movie_id,
                    imdb_id,
                    len(match_rows),
                )
                return

            old_movie_id = int(match_rows[0]["radarr_movie_id"])
            if old_movie_id == new_movie_id:
                return

            old_rows = self.conn.execute(
                """
                SELECT id, state_json FROM movies
                WHERE radarr_movie_id=?
                ORDER BY cycle ASC
                """,
                (old_movie_id,),
            ).fetchall()
            if not old_rows:
                return

            now = utc_now_iso()

            for old_row in old_rows:
                state = _json_load(old_row["state_json"], {}) or {}
                state["movie_id"] = new_movie_id
                if new_title is not None:
                    state["title"] = new_title
                if new_year is not None:
                    state["year"] = new_year
                state["imdb_id"] = imdb_id
                self.conn.execute(
                    """
                    UPDATE movies
                    SET radarr_movie_id=?, title=?, year=?, state_json=?, updated_at=?
                    WHERE id=?
                    """,
                    (new_movie_id, new_title, new_year, _json_dump(state), now, int(old_row["id"])),
                )

            self.conn.execute(
                "UPDATE movie_events SET radarr_movie_id=? WHERE radarr_movie_id=?",
                (new_movie_id, old_movie_id),
            )
            self.conn.commit()

        with self.lock:
            row = self.conn.execute(
                """
                SELECT id, cycle FROM movies
                WHERE radarr_movie_id=?
                ORDER BY cycle DESC
                LIMIT 1
                """,
                (new_movie_id,),
            ).fetchone()

        if row:
            self.add_event(
                int(row["id"]),
                new_movie_id,
                int(row["cycle"]),
                "imdb_relinked",
                "Movie relinked by IMDb after Radarr re-add with new movie id.",
                {"old_radarr_movie_id": old_movie_id, "new_radarr_movie_id": new_movie_id, "imdb_id": imdb_id},
            )

        logger.info(
            "Relinked movie by IMDb %s: old Radarr id=%s -> new Radarr id=%s",
            imdb_id,
            old_movie_id,
            new_movie_id,
        )
