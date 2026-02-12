from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import os
import random
import re
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

app = FastAPI(title="Radar Backend", version="0.5")

# =========================
# DEMO CONFIG (seed)
# =========================
TOPICS = ["seguridad", "obras", "basura", "tráfico", "agua", "impuestos", "parques", "empleo"]
SENTIMENTS = ["pos", "neu", "neg"]

# =========================
# SUPABASE
# =========================
def get_sb():
    from supabase import create_client
    url = (os.environ.get("SUPABASE_URL") or "").strip()
    key = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    if not url or not key:
        return None, "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY"
    try:
        return create_client(url, key), None
    except Exception as e:
        return None, str(e)

# =========================
# HELPERS
# =========================
def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()

def clamp_int(x: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, x))

def safe_strip(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s2 = s.strip()
    return s2 if s2 else None

def parse_window(window: str) -> timedelta:
    """
    window examples: "1h", "6h", "24h", "7d"
    """
    window = (window or "24h").strip().lower()
    m = re.match(r"^(\d+)\s*([hd])$", window)
    if not m:
        return timedelta(hours=24)
    n = int(m.group(1))
    unit = m.group(2)
    if unit == "h":
        return timedelta(hours=n)
    return timedelta(days=n)

def classify_sentiment_simple(text: str) -> str:
    """
    MVP: heurística simple (luego lo cambiamos por modelo real).
    """
    t = (text or "").lower()
    pos_words = ["mejor", "avance", "inaugura", "beneficio", "logro", "soluciona", "reduce", "aumenta", "éxito"]
    neg_words = ["crisis", "denuncia", "protesta", "muere", "violencia", "asalt", "robo", "corrup", "caos", "colaps"]
    score = 0
    for w in pos_words:
        if w in t:
            score += 1
    for w in neg_words:
        if w in t:
            score -= 1
    if score >= 1:
        return "pos"
    if score <= -1:
        return "neg"
    return "neu"

def topic_from_text_simple(text: str) -> str:
    """
    MVP: detecta topic por keywords.
    """
    t = (text or "").lower()
    rules = [
        ("seguridad", ["seguridad", "asalto", "robo", "homicidio", "sicariato", "delinc"]),
        ("obras", ["obra", "puente", "vía", "carretera", "construcción", "asfalto"]),
        ("basura", ["basura", "desechos", "recolección", "relleno", "contaminación"]),
        ("tráfico", ["tráfico", "congestión", "choque", "accidente", "movilidad"]),
        ("agua", ["agua", "potable", "corte", "tubería", "alcantarillado"]),
        ("impuestos", ["impuesto", "tasa", "tribut", "sri", "predial"]),
        ("parques", ["parque", "área verde", "recreación", "malecon", "malecón"]),
        ("empleo", ["empleo", "trabajo", "desempleo", "contrat", "vacantes"]),
    ]
    for topic, kws in rules:
        if any(k in t for k in kws):
            return topic
    return "otros"

def stable_id_from_url(url: str) -> str:
    """
    Genera un hash corto para deduplicar noticias por URL.
    """
    h = hashlib.sha256((url or "").encode("utf-8")).hexdigest()
    return h[:24]

# =========================
# HEALTH
# =========================
@app.get("/health")
def health():
    url = (os.environ.get("SUPABASE_URL") or "").strip()
    return {
        "ok": True,
        "supabase_url_preview": url[:35] + ("..." if len(url) > 35 else ""),
        "has_service_role_key": bool((os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()),
        "ts": utc_now().isoformat(),
    }

# =========================
# DEMO SEED (para probar)
# =========================
@app.api_route("/seed", methods=["GET", "POST"])
def seed(n: int = 120):
    sb, err = get_sb()
    if err:
        return JSONResponse(status_code=500, content={"error": err})

    n = clamp_int(n, 1, 1000)
    now = utc_now()
    rows = []

    for _ in range(n):
        created_at = now - timedelta(minutes=random.randint(0, 24 * 60))
        sent = random.choices(SENTIMENTS, weights=[30, 35, 35], k=1)[0]
        topic = random.choice(TOPICS)
        score = random.randint(55, 95) if sent == "pos" else random.randint(35, 75) if sent == "neu" else random.randint(10, 60)

        rows.append({
            "created_at": iso(created_at),
            "source": "demo",
            "country": "EC",
            "city": "Guayaquil",
            "platform": "news",
            "target": "alcaldia_gye",
            "author": f"user{random.randint(1000,9999)}",
            "title": None,
            "text": f"Comentario {sent} sobre {topic} en Guayaquil (demo).",
            "url": None,
            "sentiment": sent,
            "score": score,
            "topic": topic,
            "lang": "es",
            "gender": None,
            "age_range": None,
        })

    sb.table("mentions").insert(rows).execute()
    return {"inserted_mentions": n}

# =========================
# MENTIONS LIST (con filtros)
# =========================
@app.get("/mentions")
def get_mentions(
    limit: int = Query(50, ge=1, le=500),
    q: Optional[str] = None,
    topic: Optional[str] = None,
    sentiment: Optional[str] = None,
    source: Optional[str] = None,
    country: Optional[str] = None,
    city: Optional[str] = None,
    platform: Optional[str] = None,
    target: Optional[str] = None,
    since_hours: Optional[int] = Query(None, ge=1, le=720),
):
    sb, err = get_sb()
    if err:
        return JSONResponse(status_code=500, content={"error": err})

    q = safe_strip(q)
    topic = safe_strip(topic)
    sentiment = safe_strip(sentiment)
    source = safe_strip(source)
    country = safe_strip(country)
    city = safe_strip(city)
    platform = safe_strip(platform)
    target = safe_strip(target)

    query = sb.table("mentions").select("*").order("created_at", desc=True).limit(limit)

    if since_hours:
        since_dt = utc_now() - timedelta(hours=since_hours)
        query = query.gte("created_at", iso(since_dt))

    if topic:
        query = query.eq("topic", topic)
    if sentiment:
        query = query.eq("sentiment", sentiment)
    if source:
        query = query.eq("source", source)
    if country:
        query = query.eq("country", country)
    if city:
        query = query.eq("city", city)
    if platform:
        query = query.eq("platform", platform)
    if target:
        query = query.eq("target", target)

    # Para MVP, "q" filtra en topic o text o title usando ilike
    if q:
        query = query.or_(f"text.ilike.%{q}%,title.ilike.%{q}%,topic.ilike.%{q}%")

    res = query.execute()
    return res.data

# =========================
# SEARCH (palabra/frase + resumen)
# =========================
@app.get("/search")
def search(
    query: str = Query(..., min_length=1),
    limit: int = Query(50, ge=1, le=500),
    since_hours: Optional[int] = Query(24, ge=1, le=720),
    country: Optional[str] = None,
    city: Optional[str] = None,
    platform: Optional[str] = None,
    source: Optional[str] = None,
):
    sb, err = get_sb()
    if err:
        return JSONResponse(status_code=500, content={"error": err})

    query = query.strip()
    country = safe_strip(country)
    city = safe_strip(city)
    platform = safe_strip(platform)
    source = safe_strip(source)

    q = sb.table("mentions").select("*").order("created_at", desc=True).limit(limit)

    if since_hours:
        since_dt = utc_now() - timedelta(hours=since_hours)
        q = q.gte("created_at", iso(since_dt))

    if country:
        q = q.eq("country", country)
    if city:
        q = q.eq("city", city)
    if platform:
        q = q.eq("platform", platform)
    if source:
        q = q.eq("source", source)

    q = q.or_(f"text.ilike.%{query}%,title.ilike.%{query}%,topic.ilike.%{query}%")
    res = q.execute()
    items = res.data or []

    # resumen
    counts = {"pos": 0, "neu": 0, "neg": 0}
    total = len(items)
    score_sum = 0.0
    for it in items:
        s = (it.get("sentiment") or "neu")
        if s not in counts:
            s = "neu"
        counts[s] += 1
        try:
            score_sum += float(it.get("score") or 0)
        except:
            pass

    percentages = {k: (round((v / total) * 100, 2) if total else 0.0) for k, v in counts.items()}
    avg_score = round((score_sum / total), 2) if total else 0.0

    return {
        "query": query,
        "total": total,
        "sentiment_counts": counts,
        "sentiment_percentages": percentages,
        "avg_score": avg_score,
        "items": items,
    }

# =========================
# TRENDING (automático)
# =========================
@app.get("/trending")
def trending(
    window: str = "24h",
    limit: int = Query(10, ge=1, le=50),
    country: Optional[str] = "EC",
    platform: Optional[str] = "news",
):
    """
    Devuelve topics más mencionados en una ventana de tiempo.
    """
    sb, err = get_sb()
    if err:
        return JSONResponse(status_code=500, content={"error": err})

    delta = parse_window(window)
    since_dt = utc_now() - delta

    q = sb.table("mentions").select("topic,sentiment,score,created_at").gte("created_at", iso(since_dt))

    if country:
        q = q.eq("country", country)
    if platform:
        q = q.eq("platform", platform)

    res = q.execute()
    items = res.data or []

    # Agrupar por topic
    agg: Dict[str, Dict[str, Any]] = {}
    for it in items:
        tp = (it.get("topic") or "otros").strip()
        if not tp:
            tp = "otros"
        if tp not in agg:
            agg[tp] = {"topic": tp, "count": 0, "pos": 0, "neu": 0, "neg": 0, "score_sum": 0.0}
        agg[tp]["count"] += 1
        s = it.get("sentiment") or "neu"
        if s not in ("pos", "neu", "neg"):
            s = "neu"
        agg[tp][s] += 1
        try:
            agg[tp]["score_sum"] += float(it.get("score") or 0)
        except:
            pass

    rows = list(agg.values())
    for r in rows:
        total = r["count"]
        r["avg_score"] = round((r["score_sum"] / total), 2) if total else 0.0
        r["sentiment_percentages"] = {
            "pos": round((r["pos"] / total) * 100, 2) if total else 0.0,
            "neu": round((r["neu"] / total) * 100, 2) if total else 0.0,
            "neg": round((r["neg"] / total) * 100, 2) if total else 0.0,
        }
        # limpiar
        del r["score_sum"]

    # ordenar por volumen
    rows.sort(key=lambda x: x["count"], reverse=True)
    rows = rows[:limit]

    return {
        "window": window,
        "since": iso(since_dt),
        "country": country,
        "platform": platform,
        "items": rows,
    }

# =========================
# INGEST: RSS -> mentions
# =========================
@app.post("/ingest/rss")
def ingest_rss(
    feeds: Optional[List[str]] = None,
    target: str = "ecuador_news",
    country: str = "EC",
    city: Optional[str] = None,
    limit_per_feed: int = Query(20, ge=1, le=50),
):
    """
    Ingresa noticias desde RSS.
    - Si no mandas 'feeds', usa una lista base (tú luego la editas).
    - Guarda en mentions como platform='news' y source='rss'
    - Dedup por url (si url existe)
    """
    sb, err = get_sb()
    if err:
        return JSONResponse(status_code=500, content={"error": err})

    try:
        import feedparser
    except Exception:
        return JSONResponse(
            status_code=500,
            content={"error": "Missing dependency feedparser. Add it to requirements.txt"},
        )

    default_feeds = [
        # Puedes reemplazar/añadir tus feeds reales aquí:
        "https://news.google.com/rss?hl=es-419&gl=EC&ceid=EC:es-419",
        "https://news.google.com/rss/search?q=Guayaquil&hl=es-419&gl=EC&ceid=EC:es-419",
        "https://news.google.com/rss/search?q=Quito&hl=es-419&gl=EC&ceid=EC:es-419",
        "https://news.google.com/rss/search?q=Ecuador%20seguridad&hl=es-419&gl=EC&ceid=EC:es-419",
    ]

    feeds = feeds or default_feeds

    inserted = 0
    skipped = 0
    rows = []

    for feed_url in feeds:
        parsed = feedparser.parse(feed_url)
        entries = parsed.entries[:limit_per_feed]

        for e in entries:
            title = (getattr(e, "title", None) or "").strip() or None
            link = (getattr(e, "link", None) or "").strip() or None
            summary = (getattr(e, "summary", None) or "").strip() or None
            published = getattr(e, "published_parsed", None)

            # created_at
            if published:
                dt = datetime(*published[:6], tzinfo=timezone.utc)
            else:
                dt = utc_now()

            text_blob = " ".join([x for x in [title, summary] if x]).strip()
            if not text_blob:
                skipped += 1
                continue

            sentiment = classify_sentiment_simple(text_blob)
            topic = topic_from_text_simple(text_blob)

            # Dedup: si hay URL, evitamos reinsertar la misma
            # (MVP: hacemos una consulta rápida por url exacta)
            if link:
                try:
                    exists = sb.table("mentions").select("id").eq("url", link).limit(1).execute().data
                    if exists:
                        skipped += 1
                        continue
                except:
                    # si falla la consulta por cualquier razón, igual intenta insertar
                    pass

            rows.append({
                "created_at": iso(dt),
                "source": "rss",
                "country": country,
                "city": city,
                "platform": "news",
                "target": target,
                "author": None,
                "title": title,
                "text": text_blob[:2000],
                "url": link,
                "sentiment": sentiment,
                "score": 70 if sentiment == "pos" else 50 if sentiment == "neu" else 35,
                "topic": topic,
                "lang": "es",
                "gender": None,
                "age_range": None,
            })

    if rows:
        sb.table("mentions").insert(rows).execute()
        inserted = len(rows)

    return {
        "feeds_used": len(feeds),
        "inserted": inserted,
        "skipped_estimated": skipped,
        "note": "Para trending real, programa /ingest/rss con un cron cada 5-15 min.",
    }

# =========================
# QUICK TEST (opcional)
# =========================
@app.get("/test_table")
def test_table():
    sb, err = get_sb()
    if err:
        return JSONResponse(status_code=500, content={"error": err})
    res = sb.table("test_table").select("*").limit(50).execute()
    return res.data


