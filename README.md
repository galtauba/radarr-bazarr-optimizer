# Subtitle Optimizer (Radarr + Bazarr)

מערכת אוטומציה לזרימת Radarr/Bazarr עם ממשק Web, SQLite וניהול תהליך רקע.

## מה יש במערכת

- ממשק Web מבוסס `Flask + Jinja`.
- `Onboarding` בכניסה ראשונה.
- מסך `Settings` עם שמירה ל־SQLite.
- `Worker` ברקע עם Start/Stop/Auto-start.
- דף סרטים ודף סרט מפורט.
- חיפוש גלובלי + autocomplete.
- עדכון תצוגה אוטומטי ללא רענון ידני (SSE + fallback polling).
- הצגת חותמות זמן לפי הזמן המקומי של הדפדפן.
- לוגו מותאם + favicon.

## התקנה והרצה

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python app.py
```

ברירת מחדל:
- כתובת: `http://127.0.0.1:8686`
- DB: `data/optimizer.db`

## Docker / Docker Compose

הפרויקט כולל תמיכה מלאה בפריסה עם Docker באמצעות:
- `Dockerfile`
- `docker-compose.yml`
- `.dockerignore`

### דרישות מקדימות
- Docker
- Docker Compose (v2)

### הרצה

```bash
docker compose up -d --build
```

ממשק ה־Web יהיה זמין בכתובת:
- `http://127.0.0.1:8686`

### עצירה

```bash
docker compose down
```

### שמירת נתונים (Persistence)

- ה־compose ממפה volume מקומי:
  - `./data` (במכונה שלך) -> `/app/data` (בקונטיינר)
- קובץ ה־SQLite נשמר בנתיב:
  - `./data/optimizer.db`

כך הנתונים נשמרים גם אחרי restart/redeploy לקונטיינר.

### קונפיגורציה דרך משתני סביבה

`docker-compose.yml` טוען משתני סביבה מתוך `.env`.

מינימום נדרש לאינטגרציה מלאה:
- `RADARR_URL`
- `RADARR_API_KEY`
- `BAZARR_URL`
- `BAZARR_API_KEY`
- `DB_PATH=data/optimizer.db`

אם ה־API keys לא מוגדרים, הממשק יעלה, אבל האוטומציה מול Radarr/Bazarr לא תפעל.

### עדכון / בנייה מחדש

אחרי שינוי קוד:

```bash
docker compose up -d --build
```

## קונפיגורציה

אפשר להגדיר ערכי התחלה דרך `.env` (ראה `.env.example`), ואז לנהל הכל דרך ה־UI.

משתנים חשובים:
- `RADARR_URL`
- `RADARR_API_KEY`
- `BAZARR_URL`
- `BAZARR_API_KEY`
- `WEB_HOST`
- `WEB_PORT`
- `WEB_SECRET_KEY`
- `AUTH_MODE` (`none` או `basic`)
- `AUTH_USERNAME`
- `AUTH_PASSWORD_HASH`

הערה:
- בקונפיגורציית `basic`, בלי שם משתמש/סיסמה תקינים, המערכת תחזור אוטומטית ל־`none`.

## Worker Runtime Config

- ה־worker נטען תמיד עם קונפיג עדכני מה־SQLite בכל `start`.
- שינוי הגדרות `worker-related` במסך `Settings` מפעיל restart אוטומטי ל־worker (או start אם הוא היה כבוי).
- לכן שינוי `RADARR_API_KEY` / `BAZARR_API_KEY` דרך ה־UI נכנס לתוקף בלי restart לקונטיינר.
- אם חסרות הגדרות חובה (`RADARR_URL`, `RADARR_API_KEY`, `BAZARR_URL`) ה־restart האוטומטי מדולג ותוצג הודעה מתאימה.

## אחסון נתונים

מקור אמת:
- `data/optimizer.db` (SQLite)

טבלאות עיקריות:
- `settings`
- `app_meta`
- `movies`
- `movie_events`

הערה:
- `state.json` לא בשימוש במערכת ה־Web הנוכחית.

## סנכרון סרטים מול Radarr

- בכל cycle מתבצע reconcile מול Radarr.
- סרט שנמחק ב־Radarr מסומן soft-delete (`removed_at`).
- אם אותו סרט חוזר, נפתח cycle חדש ונשמרת היסטוריה.
- ב־first run סרטים קיימים מסומנים כ־`done` כדי שהמערכת תתמקד בסרטים חדשים.

## Real-time UI (ללא Refresh ידני)

- השרת פותח stream ב־`/events/stream` (SSE).
- הדפדפן מאזין לעדכונים ומעדכן תצוגה אוטומטית.
- אם SSE לא זמין, יש fallback ל־`/events/version` (polling).

## Authentication

- `none`: ללא התחברות.
- `basic`: מסך Login.

התנהגות UI:
- כפתור `Logout` מוצג רק כש־`auth_mode=basic` וגם המשתמש מחובר.

## מסלולים מרכזיים

- `GET /` Dashboard
- `GET|POST /onboarding`
- `GET|POST /settings`
- `GET /movies`
- `GET /movies/suggest`
- `GET /movies/<movie_id>`
- `POST /movies/<movie_id>/recheck`
- `POST /movies/<movie_id>/retry`
- `POST /movies/<movie_id>/state`
- `POST /worker/start`
- `POST /worker/stop`
- `GET /worker/status`
- `GET /events/stream`
- `GET /events/version`
- `GET /favicon.ico`

## Dependencies

- `Flask>=3.0.0`
- `requests>=2.31.0`
- `python-dotenv>=1.0.0`
