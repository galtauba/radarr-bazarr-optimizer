# -*- coding: utf-8 -*-

import os
from typing import Any, Dict

from optimizer_app.config import AppConfig, app_config_from_settings, config_to_settings_map, load_config
from optimizer_app.db import SQLiteStore
from optimizer_app.config import parse_list_value


class ConfigService:
    def __init__(self, store: SQLiteStore) -> None:
        self.store = store
        self.env_defaults = load_config()
        self._bootstrap_defaults()

    def _bootstrap_defaults(self) -> None:
        if self.store.get_meta("settings_initialized", False):
            return
        defaults_map = config_to_settings_map(self.env_defaults)
        for key, value in defaults_map.items():
            self.store.set_setting(key, value)
        self.store.set_meta("settings_initialized", True)
        self.store.set_meta("onboarding_completed", False)

    def get_runtime_config(self) -> AppConfig:
        settings = self.store.list_settings()
        # Self-heal malformed endpoint lists saved by older builds/UI forms.
        self._normalize_endpoint_settings(settings)
        return app_config_from_settings(settings, self.env_defaults)

    def get_settings_map(self) -> Dict[str, Any]:
        return self.store.list_settings()

    def get_defaults_map(self) -> Dict[str, Any]:
        return config_to_settings_map(self.env_defaults)

    def get_onboarding_seed_map(self) -> Dict[str, Any]:
        """
        Build onboarding prefill directly from process environment.
        If a variable is missing in .env/environment, return an empty field value
        (or [] for list fields) for display purposes.
        """
        defaults = self.get_defaults_map()
        out: Dict[str, Any] = {}
        for key, default in defaults.items():
            env_name = key.upper()
            raw = os.getenv(env_name)

            if isinstance(default, list):
                out[key] = parse_list_value(raw) if raw is not None else []
                continue

            if raw is None:
                if isinstance(default, bool):
                    out[key] = False
                else:
                    out[key] = ""
                continue

            if isinstance(default, bool):
                out[key] = str(raw).strip().lower() in ("1", "true", "yes", "on", "y")
            elif isinstance(default, int):
                try:
                    out[key] = int(raw)
                except Exception:
                    out[key] = ""
            elif isinstance(default, float):
                try:
                    out[key] = float(raw)
                except Exception:
                    out[key] = ""
            else:
                out[key] = str(raw).strip()

        return out

    def save_settings(self, values: Dict[str, Any], *, onboarding_complete: bool = False) -> None:
        # auth fallback: if basic selected without full credentials -> disable auth
        auth_mode = str(values.get("auth_mode", "none")).strip().lower()
        auth_username = str(values.get("auth_username", "")).strip()
        auth_password_hash = str(values.get("auth_password_hash", "")).strip()
        if auth_mode == "basic" and (not auth_username or not auth_password_hash):
            values["auth_mode"] = "none"

        for key, value in values.items():
            self.store.set_setting(key, value)
        if onboarding_complete:
            self.store.set_meta("onboarding_completed", True)

    def coerce_from_form(self, raw_values: Dict[str, Any]) -> Dict[str, Any]:
        defaults = self.get_defaults_map()
        out: Dict[str, Any] = {}
        for key, default in defaults.items():
            if key not in raw_values:
                continue
            raw = raw_values.get(key)
            if isinstance(default, bool):
                out[key] = str(raw).strip().lower() in ("1", "true", "yes", "on", "y")
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
                if isinstance(raw, str):
                    out[key] = [x.strip() for x in raw.split(",") if x.strip()]
                elif isinstance(raw, list):
                    out[key] = raw
                else:
                    out[key] = default
            else:
                out[key] = raw
        return out

    def onboarding_completed(self) -> bool:
        return bool(self.store.get_meta("onboarding_completed", False))

    def _normalize_endpoint_settings(self, settings: Dict[str, Any]) -> None:
        keys = [
            "bazarr_movie_lookup_fallback_endpoints",
            "bazarr_manual_search_endpoints",
            "preferred_languages",
        ]
        for key in keys:
            if key not in settings:
                continue
            normalized = parse_list_value(settings.get(key))
            if normalized != settings.get(key):
                settings[key] = normalized
                self.store.set_setting(key, normalized)
