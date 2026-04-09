# 🎬 Radarr + Bazarr Subtitle Match Optimizer

## 🧠 Overview

זהו סקריפט Python שמטרתו לשפר התאמה בין סרטים (Radarr) לבין כתוביות (Bazarr).

הבעיה:
- Radarr מוריד גרסה כלשהי של סרט
- Bazarr מוצא כתוביות — אבל לא תמיד לגרסה תואמת
- נוצר מצב של:
  ❌ כתוביות לא מסונכרנות  
  ❌ כתוביות חלקיות  
  ❌ mismatch בין release

הפתרון:
הסקריפט מזהה מתי זה קורה ומנסה **לכוון את Radarr להוריד גרסה שמתאימה לכתוביות**.

---

# ⚙️ Flow מלא (הכי חשוב להבין)

## שלב 1 — זיהוי סרט חדש
- הסקריפט קורא:
```

GET /api/v3/movie

```
- מזהה סרטים שלא קיימים ב־state.json

סטטוס:
```

waiting_for_file

```

---

## שלב 2 — המתנה להורדת הסרט
הסקריפט מחכה עד ש:
```

hasFile == true

```
או:
```

movieFile != null

```

❗ חשוב:
לא עושים כלום לפני שיש קובץ

---

## שלב 3 — Grace Period ל־Bazarr

הסקריפט מחכה:
```

BAZARR_GRACE_SECONDS (ברירת מחדל: 15 דקות)

```

למה?
- לתת ל־Bazarr לעבוד רגיל
- לא להתערב מוקדם מדי

סטטוס:
```

bazarr_waiting

```

---

## שלב 4 — בדיקת מצב כתוביות

הסקריפט שואל את Bazarr:
- האם יש כתוביות?
- איזה?
- מה ה־release name?

⚠️ חשוב:
Bazarr API **לא אחיד בין גרסאות**

לכן יש adapter בקוד שצריך התאמה.

---

## שלב 5 — הערכת איכות התאמה

הסקריפט מחשב:

### ✔️ האם יש כתוביות
### ✔️ שפה
### ✔️ score
### ✔️ metadata בשם הכתובית
### ✔️ דמיון לשם הקובץ

---

### 🧮 איך נמדד הדמיון?

השוואה בין:
```

Movie file name
VS
Subtitle release name

```

כולל:
- resolution (1080p וכו')
- source (WEB, BluRay)
- codec
- release group

---

## שלב 6 — החלטה

### מצב 1 — התאמה טובה
```

status = done

```

### מצב 2 — אין כתוביות
```

subtitle_missing

```

### מצב 3 — התאמה גרועה
```

subtitle_matched_poor

```

---

## שלב 7 — Radarr Follow-up

אם אין התאמה טובה:

### מה הסקריפט עושה:
1. מפיק `release_hint`
2. בודק releases ב־Radarr:
```

GET /api/v3/release?movieId=...

```

3. משווה similarity

4. ואז:
```

POST /api/v3/command
{
"name": "MoviesSearch"
}

```

---

## ⚠️ מגבלה חשובה מאוד

❗ הסקריפט לא מבצע:
> Grab של release ספציפי

למה?
כי זה לא API יציב ללא בדיקה אצלך.

יש TODO בקוד.

---

# 🧱 מבנה הפרויקט

```

project/
├─ radarr_bazarr_option1.py
├─ README.md
├─ .env
├─ requirements.txt
├─ state.json (נוצר לבד)

```

---

# 📦 התקנה

## 1. Python

```

Python 3.9+

````

---

## 2. התקנת תלויות

```bash
pip install requests
````

או:

```bash
pip install -r requirements.txt
```

---

## 3. קובץ requirements.txt

```txt
requests>=2.31.0
```

---

# 🔐 קובץ הגדרות (.env)

```env
RADARR_URL=http://localhost:7878
RADARR_API_KEY=XXXX

BAZARR_URL=http://localhost:6767
BAZARR_API_KEY=XXXX

POLL_SECONDS=300
BAZARR_GRACE_SECONDS=900
RETRY_COOLDOWN_SECONDS=21600
MAX_FOLLOWUP_ATTEMPTS=1

STATE_FILE=state.json
LOG_LEVEL=INFO

HTTP_TIMEOUT=20
HTTP_RETRIES=3

PREFERRED_LANGUAGES=he,heb,en,eng

GOOD_SUBTITLE_MIN_SCORE=80
MATCH_SIMILARITY_THRESHOLD=45

BAZARR_MODE=manual_api

BAZARR_MOVIE_LOOKUP_ENDPOINT=/api/movies
BAZARR_LOOKUP_STYLE=query_param_radarrid

BAZARR_SEARCH_ENDPOINT=/api/movies/subtitles
```

---

# 🔌 טעינת ENV

## Linux

```bash
set -a
source .env
set +a
```

## Windows

```powershell
Get-Content .env | foreach {
 $name, $value = $_ -split '=', 2
 [System.Environment]::SetEnvironmentVariable($name, $value)
}
```

---

# ▶️ הרצה

```bash
python radarr_bazarr_option1.py
```

---

# 🧠 State Management

## למה צריך state.json?

כדי למנוע:

* לולאות
* חיפוש כפול
* overload

---

## מה נשמר?

```json
{
  "movies": {
    "123": {
      "status": "done",
      "release_hint": "...",
      "attempts": 1
    }
  }
}
```

---

# 🔍 התאמת Bazarr (החלק הכי חשוב)

## 🚨 Bazarr API לא אחיד

צריך לבדוק ידנית איך Bazarr שלך עובד.

---

## 🔧 איך למצוא את ה־API של Bazarr שלך

### שלב 1

פתח דפדפן

### שלב 2

פתח:

```
Developer Tools (F12)
```

### שלב 3

לך ל־Network

### שלב 4

בצע:

* חיפוש כתוביות
* או refresh

### שלב 5

מצא request

### שלב 6

בדוק:

* URL
* Method
* Headers
* Payload

---

## 📌 ואז

עדכן בקוד:

```python
BAZARR_MOVIE_LOOKUP_ENDPOINT
BAZARR_SEARCH_ENDPOINT
```

---

# 🧪 Debugging

## אם אין כתוביות

בעיה:

* endpoint לא נכון
* parser לא מתאים

---

## אם תמיד יש follow-up

בעיה:

* thresholds גבוהים מדי

---

## אם לא קורה כלום

בדוק:

* hasFile
* logs

---

# 🧯 מניעת לולאות

הסקריפט כולל:

✔ cooldown
✔ max attempts
✔ state tracking

---

# 🚀 שיפורים עתידיים

## אפשר להוסיף:

### 1. Exact release grab

(צריך Radarr API בדוק)

### 2. Docker

### 3. Web UI

### 4. Webhooks במקום polling

### 5. Matching חכם יותר

---

# 🧠 Design Philosophy

הסקריפט בנוי להיות:

✔ בטוח
✔ לא הורס מערכת קיימת
✔ לא נכנס ללולאות
✔ לא מניח דברים לא ודאיים

---

# ⚠️ סיכום מגבלות

| תחום              | מצב             |
| ----------------- | --------------- |
| Radarr Movie API  | ✔ יציב          |
| Radarr Search     | ✔ יציב          |
| Radarr Exact Grab | ❌ תלוי instance |
| Bazarr API        | ❌ משתנה         |
| Subtitle Matching | ⚠ heuristic     |

---

# ❤️ טיפ חשוב

תתחיל עם:

```
LOG_LEVEL=DEBUG
```

ותראה בדיוק מה קורה.

---

# 🏁 סיום

זה לא רק סקריפט — זה מנוע אופטימיזציה קטן בין Radarr ל־Bazarr.

ככל שתתאים את Bazarr adapter — זה יעבוד יותר טוב.
