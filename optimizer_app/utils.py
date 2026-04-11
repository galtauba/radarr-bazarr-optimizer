# -*- coding: utf-8 -*-

import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def seconds_since(iso_value: Optional[str]) -> Optional[float]:
    dt = parse_iso(iso_value)
    if not dt:
        return None
    return (utc_now() - dt).total_seconds()


def normalize_string(value: str) -> str:
    if not value:
        return ""
    out = value.strip().lower().replace("_", " ")
    out = re.sub(r"\s+", " ", out)
    return out


def normalize_releaseish(value: str) -> str:
    if not value:
        return ""
    out = value.strip().replace("_", ".")
    out = re.sub(r"\s+", ".", out)
    out = re.sub(r"\.+", ".", out)
    return out.strip(".")


def normalize_release_for_exact(value: str) -> str:
    if not value:
        return ""
    out = normalize_releaseish(value).lower()
    out = re.sub(r"[^a-z0-9\.\- ]+", ".", out)
    out = out.replace("-", ".")
    out = re.sub(r"[.\s]+", ".", out)
    return out.strip(".")


def title_year_string(movie: Dict[str, Any]) -> str:
    title = movie.get("title") or "<unknown>"
    year = movie.get("year") or "?"
    return f"{title} ({year})"


def _clean_subtitle_release_candidate(value: str) -> str:
    if not value:
        return ""
    text = str(value).strip().strip("'\"")
    if not text:
        return ""
    text = text.replace("\\", "/")
    if "/" in text:
        text = text.split("/")[-1]

    while True:
        base, dot, ext = text.rpartition(".")
        if not dot:
            break
        if f".{ext.lower()}" in {".srt", ".ass", ".ssa", ".sub", ".vtt", ".txt"}:
            text = base
            continue
        break

    lang_tail = (
        r"(he|heb|hebrew|iw|en|eng|english|ar|ara|arabic|ru|rus|russian|"
        r"es|spa|spanish|fr|fra|fre|french|de|ger|deu|german|it|ita|italian)"
    )
    for _ in range(2):
        updated = re.sub(rf"(?i)[\.\-_\s]+{lang_tail}$", "", text).strip()
        if updated == text:
            break
        text = updated

    return text.strip(" .-_")


def _looks_like_language_label(value: str) -> bool:
    normalized = normalize_string(value)
    if not normalized:
        return False

    language_labels = {
        "he",
        "heb",
        "hebrew",
        "iw",
        "en",
        "eng",
        "english",
        "ar",
        "ara",
        "arabic",
        "ru",
        "rus",
        "russian",
        "es",
        "spa",
        "spanish",
        "fr",
        "fra",
        "fre",
        "french",
        "de",
        "ger",
        "deu",
        "german",
        "it",
        "ita",
        "italian",
    }
    if normalized in language_labels:
        return True

    if re.fullmatch(r"[a-z ]{2,20}", normalized):
        words = [w for w in normalized.split(" ") if w]
        if 1 <= len(words) <= 2:
            return True
    return False


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
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            candidate = normalize_releaseish(match.group(1))
            if len(candidate) > best_len:
                best = candidate
                best_len = len(candidate)
    return best


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


def subtitle_release_name(subtitle_entry: Optional[Dict[str, Any]]) -> str:
    if not subtitle_entry:
        return ""
    candidates = [
        subtitle_entry.get("release_name"),
        subtitle_entry.get("releaseName"),
        subtitle_entry.get("scene_name"),
        subtitle_entry.get("sceneName"),
        subtitle_entry.get("release_info"),
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
        if isinstance(candidate, list):
            for item in candidate:
                cleaned = _clean_subtitle_release_candidate(str(item))
                if cleaned:
                    return cleaned
            continue
        cleaned = _clean_subtitle_release_candidate(str(candidate))
        if _looks_like_release_name(cleaned):
            return cleaned
    return ""


def subtitle_has_file_reference(sub: Dict[str, Any]) -> bool:
    for key in ("path", "file", "file_path", "filename", "name", "title", "subtitles_path"):
        value = sub.get(key)
        if value is None:
            continue
        text = str(value).strip().lower()
        if text.endswith((".srt", ".ass", ".ssa", ".sub", ".vtt", ".txt")):
            return True
    return False


def subtitle_has_score(sub: Dict[str, Any]) -> bool:
    for key in ("score", "matches", "percent", "match_score"):
        if sub.get(key) is not None:
            return True
    return False


def subtitle_score_value(sub: Dict[str, Any]) -> float:
    def parse_numeric(raw: Any) -> Optional[float]:
        if raw is None:
            return None
        if isinstance(raw, (int, float)):
            return float(raw)
        text = str(raw).strip().replace(",", ".").replace("%", "")
        text = re.sub(r"[^0-9.\-]+", "", text)
        if not text:
            return None
        try:
            return float(text)
        except Exception:
            return None

    for key in ("score", "matches", "percent", "match_score"):
        raw = sub.get(key)
        direct = parse_numeric(raw)
        if direct is not None:
            return direct
        if isinstance(raw, dict):
            for nested in ("score", "value", "percent", "matches", "match_score"):
                value = parse_numeric(raw.get(nested))
                if value is not None:
                    return value
        if isinstance(raw, list):
            for item in raw:
                if isinstance(item, dict):
                    for nested in ("score", "value", "percent", "matches", "match_score"):
                        value = parse_numeric(item.get(nested))
                        if value is not None:
                            return value
                else:
                    value = parse_numeric(item)
                    if value is not None:
                        return value
    return 0.0


def subtitle_language_rank(sub: Dict[str, Any], preferred_languages: List[str]) -> int:
    possible_values: List[str] = []

    def add_value(raw: Any) -> None:
        if raw is None:
            return
        n = normalize_string(str(raw))
        if n:
            possible_values.append(n)

    add_value(sub.get("language"))
    add_value(sub.get("lang"))
    add_value(sub.get("language_code"))
    add_value(sub.get("languageCode"))
    add_value(sub.get("code2"))
    add_value(sub.get("code3"))
    if isinstance(sub.get("language"), dict):
        lang_obj = sub.get("language") or {}
        add_value(lang_obj.get("code2"))
        add_value(lang_obj.get("code3"))
        add_value(lang_obj.get("name"))

    for idx, wanted in enumerate(preferred_languages):
        if normalize_string(wanted) in possible_values:
            return idx
    return len(preferred_languages) + 10


def extract_metadata_tokens(text: str) -> List[str]:
    if not text:
        return []

    known = [
        "2160p",
        "1080p",
        "720p",
        "web-dl",
        "webrip",
        "bluray",
        "brrip",
        "dvdrip",
        "hdrip",
        "remux",
        "uhd",
        "x264",
        "x265",
        "h264",
        "h265",
        "hevc",
        "ddp5.1",
        "aac",
        "dts",
        "truehd",
        "atmos",
        "extended",
        "unrated",
        "proper",
        "repack",
        "imax",
        "criterion",
    ]
    normalized = normalize_releaseish(text).lower()
    flat = normalized.replace(".", "").replace("-", "")
    found: List[str] = []
    for token in known:
        probe = token.replace(".", "").replace("-", "").replace("'", "")
        if probe in flat:
            found.append(token)
    out: List[str] = []
    seen = set()
    for item in found:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


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


def match_quality_between_release_strings(a_value: str, b_value: str) -> float:
    if not a_value or not b_value:
        return 0.0
    a = normalize_releaseish(a_value).lower()
    b = normalize_releaseish(b_value).lower()
    score = 0.0
    if a in b or b in a:
        score += 50.0

    tokens_a = {t for t in re.split(r"[.\-_ ]+", a) if t}
    tokens_b = {t for t in re.split(r"[.\-_ ]+", b) if t}
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
