# Radarr + Bazarr Subtitle Optimizer

סקריפט אוטומציה שמטרתו לטפל במצב שבו Bazarr מוצא כתוביות באיכות לא מספקת לסרט שכבר ירד ב-Radarr, ואז לנסות להחליף לגרסה מתאימה יותר.

## מה הסקריפט עושה בפועל

1. מזהה סרט חדש ב-Radarr ומחכה עד שיש קובץ (`hasFile`/`movieFile`).
2. ממתין `BAZARR_GRACE_SECONDS`.
3. מושך מצב כתוביות מ-Bazarr (כולל history כשמופעל).
4. מדרג כתוביות לפי שפה + score, ומחשב `good/poor` לפי **score של Bazarr** (לא לפי התאמת שם קובץ).
5. אם `poor`:
   - מפעיל Manual Search ב-Bazarr.
   - בודק שוב.
6. אם עדיין `poor`:
   - קורא `GET /api/providers/movies?radarrid=<id>`.
   - מחלץ `release_info` לרשימת שמות release.
7. עובר ל-Radarr:
   - שלב 0: `GET /api/v3/moviefile?movieId=<id>`.
   - שלב 0.1: מוחק קבצים קיימים (אם מופעל).
   - שלב 1: `GET /api/v3/release?movieId=<id>`.
   - שלב 2: בוחר מועמדים רק מתוך pool של `release_info`, עם עדיפות:
     - `downloadAllowed=true` + `rejections=[]`
     - fallback: `downloadAllowed=true` + `rejections!=[]`
   - שלב 3: `POST /api/v3/release`.
8. אם grab לא מתחיל בפועל, מנסה את המועמד הבא ברשימה.
9. מאמת התחלה דרך Queue (ולפי קונפיג גם History) לפני סימון הצלחה.

## נקודה חשובה

`POST /api/v3/release` שחוזר 200 לא מבטיח שההורדה התחילה.  
הסקריפט בודק Queue/History, ואם לא רואה התחלה אמיתית - ממשיך לגרסה אחרת או מסמן `manual_required`.

---

## דרישות

- Python 3.9+
- `requests`
- `python-dotenv`

```bash
pip install -r requirements.txt
```

---

## הרצה

הסקריפט טוען `.env` אוטומטית (באמצעות `python-dotenv`).

```bash
python radarr_bazarr_option1.py
```

---

## קובץ `.env` לדוגמה

```env
RADARR_URL=http://localhost:7878
RADARR_API_KEY=YOUR_RADARR_API_KEY

BAZARR_URL=http://localhost:6767
BAZARR_API_KEY=YOUR_BAZARR_API_KEY
BAZARR_API_KEY_HEADER=X-Api-Key

POLL_SECONDS=300
BAZARR_GRACE_SECONDS=900
RETRY_COOLDOWN_SECONDS=21600
MAX_FOLLOWUP_ATTEMPTS=1

STATE_FILE=state.json
LOG_LEVEL=INFO

HTTP_TIMEOUT=20
HTTP_RETRIES=3
HTTP_BACKOFF_SECONDS=2
VERIFY_SSL=true
USER_AGENT=radarr-bazarr-option1/1.0

PREFERRED_LANGUAGES=he,heb,en,eng
GOOD_SUBTITLE_MIN_SCORE=90
MATCH_SIMILARITY_THRESHOLD=45
EXACT_MATCH_ONLY=true
STRICT_PROFILE_GUARD=true
USE_BAZARR_SCORE_WHEN_RELEASE_MISSING=true
TREAT_FILE_REFERENCE_AS_GOOD_WHEN_SCORE_MISSING=true

BAZARR_MODE=manual_api
BAZARR_MOVIE_LOOKUP_ENDPOINT=/api/movies
BAZARR_MOVIE_LOOKUP_FALLBACK_ENDPOINTS=/api/movies/wanted,/api/movies/history
BAZARR_ENABLE_HISTORY_LOOKUP=true
BAZARR_LOOKUP_STYLE=query_param_radarrid

BAZARR_SEARCH_ENDPOINT=/api/movies/subtitles
ENABLE_BAZARR_SEARCH_TRIGGER=false

ENABLE_BAZARR_MANUAL_SEARCH_ON_POOR=true
BAZARR_MANUAL_SEARCH_ENDPOINTS=/api/movies/subtitles,/api/movies/manual
BAZARR_MANUAL_SEARCH_METHOD=AUTO
BAZARR_MANUAL_SEARCH_WAIT_SECONDS=8
BAZARR_MANUAL_SEARCH_MAX_ATTEMPTS=1
BAZARR_MANUAL_SEARCH_RETRY_COOLDOWN_SECONDS=1800

ENABLE_BAZARR_PROVIDERS_RELEASE_HINT=true
BAZARR_PROVIDERS_MOVIES_ENDPOINT=/api/providers/movies

ENABLE_RADARR_RELEASE_INSPECTION=true
ENABLE_RADARR_MOVIES_SEARCH_FALLBACK=false
ENABLE_RADARR_DELETE_EXISTING_FILE_ON_POOR=true

RADARR_GRAB_VERIFY_ENABLED=true
RADARR_GRAB_VERIFY_TIMEOUT_SECONDS=45
RADARR_GRAB_VERIFY_POLL_SECONDS=5
RADARR_GRAB_VERIFY_USE_HISTORY=false
```

---

## כלי בדיקה

קיים סקריפט עזר לבדיקת API של Bazarr:

```bash
python bazarr_api_probe.py --endpoint /api/movies/history --query radarrId=11
python bazarr_api_probe.py --endpoint /api/providers/movies --query radarrid=11
```

---

## הערות תפעול

- אם סרט סומן `done` ב-`state.json`, הוא לא יעובד שוב עד שתמחק את הרשומה שלו.
- אם Radarr מחזיר שגיאת Download Client (למשל qBittorrent נכשל), הסקריפט יעבור למועמד הבא.
- אם כל המועמדים נכשלו או לא התחילו בפועל, הסרט יסומן `manual_required`.
