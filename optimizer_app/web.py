# -*- coding: utf-8 -*-

import hashlib
import json
import time
from functools import wraps
from typing import Any, Dict, Optional

from flask import Flask, Response, flash, jsonify, redirect, render_template, request, session, stream_with_context, url_for

from optimizer_app.config_service import ConfigService
from optimizer_app.logging_utils import logger
from optimizer_app.worker import WorkerManager


def hash_password(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def create_web_app(
    config_service: ConfigService,
    worker: WorkerManager,
) -> Flask:
    runtime_config = config_service.get_runtime_config()
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.secret_key = runtime_config.web_secret_key

    @app.route("/favicon.ico")
    def favicon():
        return redirect(url_for("static", filename="logo.svg"), code=302)

    def _engine():
        return worker.engine

    def _radarr():
        return worker.engine.radarr

    def _has_worker_related_changes(before: Dict[str, Any], after: Dict[str, Any]) -> bool:
        non_worker_keys = {
            "auth_mode",
            "auth_username",
            "auth_password_hash",
            "web_secret_key",
            "web_host",
            "web_port",
        }
        all_keys = set(before.keys()) | set(after.keys())
        for key in all_keys:
            if key in non_worker_keys:
                continue
            if before.get(key) != after.get(key):
                return True
        return False

    def _state_version() -> str:
        rows = _engine().state.store.list_movies(view="all")
        max_updated = ""
        for row in rows:
            updated_at = str(row.get("updated_at") or "")
            if updated_at > max_updated:
                max_updated = updated_at
        last_run = str(_engine().state.store.get_meta("last_run") or "")
        return f"{len(rows)}|{max_updated}|{last_run}"

    def _filter_movie_rows(rows, query: str):
        q = str(query or "").strip().lower()
        if not q:
            return rows
        filtered = []
        for row in rows:
            haystack = " ".join(
                [
                    str(row.get("title") or ""),
                    str(row.get("year") or ""),
                    str(row.get("status") or ""),
                    str(row.get("subtitle_evaluation") or ""),
                    "done" if row.get("done") else "open",
                    str(row.get("radarr_movie_id") or ""),
                ]
            ).lower()
            if q in haystack:
                filtered.append(row)
        return filtered

    def _auth_enabled() -> bool:
        cfg = config_service.get_runtime_config()
        return cfg.auth_mode == "basic" and bool(cfg.auth_username and cfg.auth_password_hash)

    def _require_login(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if request.endpoint in ("login", "static"):
                return fn(*args, **kwargs)
            if not config_service.onboarding_completed() and request.endpoint != "onboarding":
                return redirect(url_for("onboarding"))
            if _auth_enabled():
                if not session.get("authenticated"):
                    return redirect(url_for("login"))
            return fn(*args, **kwargs)

        return wrapper

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if not _auth_enabled():
            return redirect(url_for("dashboard"))
        cfg = config_service.get_runtime_config()
        if request.method == "POST":
            username = str(request.form.get("username", "")).strip()
            password = str(request.form.get("password", ""))
            if username == cfg.auth_username and hash_password(password) == cfg.auth_password_hash:
                session["authenticated"] = True
                return redirect(url_for("dashboard"))
            flash("Invalid credentials", "error")
        return render_template("login.html")

    @app.route("/logout", methods=["POST"])
    def logout():
        session.clear()
        return redirect(url_for("login"))

    @app.route("/onboarding", methods=["GET", "POST"])
    def onboarding():
        defaults = config_service.get_defaults_map()
        # On first run, show direct .env-derived values (and empty fields for missing env vars)
        # instead of DB bootstrap/runtime defaults.
        merged = config_service.get_onboarding_seed_map()
        if request.method == "POST":
            incoming = _extract_settings_from_form(defaults, request.form.to_dict())
            _handle_auth_form(incoming, request.form)
            config_service.save_settings(incoming, onboarding_complete=True)
            flash("Onboarding completed", "success")
            return redirect(url_for("dashboard"))
        return render_template(
            "onboarding.html",
            settings=merged,
            all_keys=_ordered_settings_keys(defaults),
        )

    @app.route("/")
    @_require_login
    def dashboard():
        cfg = config_service.get_runtime_config()
        counts = _engine().state.store.get_dashboard_counts()
        return render_template(
            "dashboard.html",
            worker=worker.status(),
            counts=counts,
            config=cfg,
            last_run=_engine().state.store.get_meta("last_run"),
        )

    @app.route("/settings", methods=["GET", "POST"])
    @_require_login
    def settings_page():
        defaults = config_service.get_defaults_map()
        current = config_service.get_settings_map()
        before_save = dict(current)
        merged = dict(defaults)
        merged.update(current)
        if request.method == "POST":
            incoming = _extract_settings_from_form(defaults, request.form.to_dict())
            _handle_auth_form(incoming, request.form)
            config_service.save_settings(incoming, onboarding_complete=False)
            after_save = config_service.get_settings_map()
            if _has_worker_related_changes(before_save, after_save):
                cfg_after = config_service.get_runtime_config()
                ready_for_worker = bool(cfg_after.radarr_url and cfg_after.radarr_api_key and cfg_after.bazarr_url)
                if ready_for_worker:
                    try:
                        if worker.status().get("running"):
                            worker.restart()
                        else:
                            worker.start()
                        flash("Settings saved. Worker restarted automatically with latest DB configuration.", "success")
                    except Exception as exc:
                        logger.exception("Automatic worker restart failed after settings save: %s", exc)
                        flash(f"Settings saved, but worker auto-restart failed: {exc}", "error")
                else:
                    flash("Settings saved. Worker restart skipped: missing required Radarr/Bazarr configuration.", "error")
            else:
                flash("Settings saved", "success")
            return redirect(url_for("settings_page"))
        return render_template(
            "settings.html",
            settings=merged,
            all_keys=_ordered_settings_keys(defaults),
        )

    @app.route("/movies")
    @_require_login
    def movies():
        view = str(request.args.get("view", "active"))
        query = str(request.args.get("q", "")).strip()
        all_rows = _engine().state.store.list_movies(view=view)
        rows = _filter_movie_rows(all_rows, query)
        return render_template(
            "movies.html",
            rows=rows,
            view=view,
            search_query=query,
            total_rows=len(all_rows),
        )

    @app.route("/movies/suggest")
    @_require_login
    def movies_suggest():
        query = str(request.args.get("q", "")).strip().lower()
        if len(query) < 2:
            return jsonify({"ok": True, "items": []})

        rows = _engine().state.store.list_movies(view="all")
        items = []
        seen = set()

        for row in rows:
            title = str(row.get("title") or "").strip()
            year = row.get("year")
            status = str(row.get("status") or "").strip()
            movie_id = int(row.get("radarr_movie_id") or 0)
            label = f"{title} ({year})" if year else title
            haystack = " ".join([title, str(year or ""), status, str(movie_id)]).lower()
            if query not in haystack:
                continue
            if movie_id in seen:
                continue
            seen.add(movie_id)
            items.append(
                {
                    "movie_id": movie_id,
                    "label": label,
                    "status": status,
                    "url": url_for("movie_detail", movie_id=movie_id),
                }
            )
            if len(items) >= 8:
                break

        return jsonify({"ok": True, "items": items})

    @app.route("/events/stream")
    @_require_login
    def events_stream():
        def event_generator():
            last_version = None
            last_keepalive = 0.0
            while True:
                version = _state_version()
                if version != last_version:
                    payload = json.dumps({"version": version})
                    yield f"event: state\ndata: {payload}\n\n"
                    last_version = version

                now = time.time()
                if now - last_keepalive >= 15:
                    yield ": keepalive\n\n"
                    last_keepalive = now

                time.sleep(2)

        headers = {
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
        return Response(stream_with_context(event_generator()), mimetype="text/event-stream", headers=headers)

    @app.route("/events/version")
    @_require_login
    def events_version():
        return jsonify({"ok": True, "version": _state_version()})

    @app.route("/movies/<int:movie_id>")
    @_require_login
    def movie_detail(movie_id: int):
        detail = _engine().state.store.get_movie_detail(movie_id)
        if not detail:
            flash("Movie not found", "error")
            return redirect(url_for("movies"))
        return render_template("movie_detail.html", detail=detail)

    @app.route("/movies/<int:movie_id>/recheck", methods=["POST"])
    @_require_login
    def movie_recheck(movie_id: int):
        try:
            all_movies = _radarr().get_movies()
            target = next((m for m in all_movies if int(m.get("id", -1)) == int(movie_id)), None)
            if not target:
                flash("Movie not found in Radarr", "error")
                return redirect(url_for("movie_detail", movie_id=movie_id))
            _engine().process_movie(target)
            _engine().state.save()
            flash("Recheck completed", "success")
        except Exception as exc:
            logger.exception("Recheck failed for movie %s: %s", movie_id, exc)
            flash(f"Recheck failed: {exc}", "error")
        return redirect(url_for("movie_detail", movie_id=movie_id))

    @app.route("/movies/<int:movie_id>/retry", methods=["POST"])
    @_require_login
    def movie_retry(movie_id: int):
        ok = _engine().state.store.update_movie_action(movie_id, "retry")
        flash("Retry flags reset" if ok else "Retry action failed", "success" if ok else "error")
        return redirect(url_for("movie_detail", movie_id=movie_id))

    @app.route("/movies/<int:movie_id>/state", methods=["POST"])
    @_require_login
    def movie_state_action(movie_id: int):
        action = str(request.form.get("action", "")).strip()
        ok = _engine().state.store.update_movie_action(movie_id, action)
        flash("State updated" if ok else "Invalid state action", "success" if ok else "error")
        return redirect(url_for("movie_detail", movie_id=movie_id))

    @app.route("/worker/start", methods=["POST"])
    @_require_login
    def worker_start():
        cfg = config_service.get_runtime_config()
        if not cfg.radarr_api_key:
            if "application/json" in (request.headers.get("Accept") or ""):
                return jsonify({"ok": False, "error": "RADARR_API_KEY is missing in settings"}), 400
            flash("Cannot start worker: RADARR_API_KEY is missing", "error")
            return redirect(url_for("dashboard"))
        started = worker.start()
        if "application/json" in (request.headers.get("Accept") or ""):
            return jsonify({"ok": True, "started": started, "status": worker.status()})
        flash("Worker started" if started else "Worker already running", "success")
        return redirect(url_for("dashboard"))

    @app.route("/worker/stop", methods=["POST"])
    @_require_login
    def worker_stop():
        stopped = worker.stop()
        if "application/json" in (request.headers.get("Accept") or ""):
            return jsonify({"ok": True, "stopped": stopped, "status": worker.status()})
        flash("Worker stopped" if stopped else "Worker already stopped", "success")
        return redirect(url_for("dashboard"))

    @app.route("/worker/status")
    @_require_login
    def worker_status():
        return jsonify({"ok": True, "status": worker.status()})

    @app.context_processor
    def inject_live_context():
        return {
            "live_state_version": _state_version(),
            "auth_enabled": _auth_enabled(),
        }

    return app


def _ordered_settings_keys(defaults: Dict[str, Any]):
    # hides internal secrets in generic listing; dedicated inputs are used in templates
    hidden = {"auth_password_hash", "web_secret_key", "auth_mode", "auth_username"}
    return [k for k in defaults.keys() if k not in hidden]


def _extract_settings_from_form(defaults: Dict[str, Any], form_data: Dict[str, str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, default in defaults.items():
        if key in ("auth_password_hash", "web_secret_key"):
            continue
        if key not in form_data and isinstance(default, bool):
            out[key] = False
            continue
        raw = form_data.get(key, default)
        if isinstance(default, bool):
            out[key] = str(raw).strip().lower() in ("1", "true", "yes", "on")
        elif isinstance(default, int):
            try:
                out[key] = int(raw)
            except Exception:
                out[key] = default
        elif isinstance(default, float):
            try:
                out[key] = float(raw)
            except Exception:
                out[key] = default
        elif isinstance(default, list):
            out[key] = [x.strip() for x in str(raw).split(",") if x.strip()]
        else:
            out[key] = str(raw).strip()

    # optional secret key update
    if "web_secret_key" in form_data and str(form_data.get("web_secret_key", "")).strip():
        out["web_secret_key"] = str(form_data.get("web_secret_key", "")).strip()
    return out


def _handle_auth_form(incoming: Dict[str, Any], form) -> None:
    mode_values = form.getlist("auth_mode") if hasattr(form, "getlist") else [incoming.get("auth_mode", "none")]
    mode = str(mode_values[-1] if mode_values else incoming.get("auth_mode", "none")).strip().lower()
    incoming["auth_mode"] = mode if mode in ("none", "basic") else "none"

    username_values = form.getlist("auth_username") if hasattr(form, "getlist") else [incoming.get("auth_username", "")]
    incoming["auth_username"] = str(username_values[-1] if username_values else incoming.get("auth_username", "")).strip()

    password_values = form.getlist("auth_password") if hasattr(form, "getlist") else [""]
    password = str(password_values[-1] if password_values else "").strip()

    hash_values = form.getlist("auth_password_hash") if hasattr(form, "getlist") else [incoming.get("auth_password_hash", "")]
    existing_hash = str(hash_values[-1] if hash_values else "").strip()
    if password:
        incoming["auth_password_hash"] = hash_password(password)
    elif existing_hash:
        incoming["auth_password_hash"] = existing_hash
    else:
        incoming["auth_password_hash"] = incoming.get("auth_password_hash", "")
