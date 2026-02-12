from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import os, random
from datetime import datetime, timedelta, timezone

app = FastAPI(title="Radar Backend", version="0.4")

TOPICS = ["seguridad", "obras", "basura", "tráfico", "agua", "impuestos", "parques", "empleo"]
SENTIMENTS = ["pos", "neu", "neg"]


# =========================
# Conexión Supabase
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
# Health
# =========================
@app.get("/health")
def health():
    url = (os.environ.get("SUPABASE_URL") or "").strip()
    return {
        "ok": True,
        "supabase_url_preview": url[:35] + ("..." if len(url) > 35 else ""),
        "has_service_role_key": bool((os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()),
        "ts": datetime.now(timezone.utc).isoformat(),
    }


# =========================
# Seed demo
# =========================
@app.api_route("/seed", methods=["GET", "POST"])
def seed(n: int = 120):
    sb, err = get_sb()
    if err:
        return JSONResponse(status_code=500, content={"error": err})

    now = datetime.now(timezone.utc)
    rows = []

    for _ in range(n):
        created_at = now - timedelta(minutes=random.randint(0, 24 * 60))
        sent = random.choices(SENTIMENTS, weights=[30, 35, 35], k=1)[0]
        topic = random.choice(TOPICS)
        score = (
            random.randint(55, 95) if sent == "pos"
            else random.randint(35, 75) if sent == "neu"
            else random.randint(10, 60)
        )

        rows.append({
            "created_at": created_at.isoformat(),
            "source": "demo",
            "country": "EC",
            "target": "alcaldia_gye",
            "author": f"user{random.randint(1000,9999)}",
            "text": f"Comentario {sent} sobre {topic} en Guayaquil (demo).",
            "url": None,
            "sentiment": sent,
            "score": score,
            "topic": topic
        })

    sb.table("mentions").insert(rows).execute()
    return {"inserted_mentions": n}


# =========================
# Search con métricas
# =========================
@app.get("/search")
def search_mentions(
    q: str = Query(..., min_length=2),
    limit: int = Query(100, ge=1, le=500),
    since_hours: int | None = Query(24, ge=1, le=720),
    target: str | None = Query(None),
):
    sb, err = get_sb()
    if err:
        return JSONResponse(status_code=500, content={"error": err})

    query = sb.table("mentions").select("*")

    query = query.ilike("text", f"%{q}%")

    if target:
        query = query.eq("target", target)

    if since_hours:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)
        query = query.gte("created_at", cutoff.isoformat())

    res = (
        query.order("created_at", desc=True)
             .limit(limit)
             .execute()
    )

    data = res.data or []
    total = len(data)

    if total == 0:
        return {
            "query": q,
            "total": 0,
            "sentiment_counts": {},
            "sentiment_percentages": {},
            "avg_score": 0,
            "items": []
        }

    pos = sum(1 for r in data if r["sentiment"] == "pos")
    neu = sum(1 for r in data if r["sentiment"] == "neu")
    neg = sum(1 for r in data if r["sentiment"] == "neg")

    avg_score = round(sum(r["score"] for r in data) / total, 2)

    sentiment_counts = {
        "pos": pos,
        "neu": neu,
        "neg": neg
    }

    sentiment_percentages = {
        "pos": round((pos / total) * 100, 2),
        "neu": round((neu / total) * 100, 2),
        "neg": round((neg / total) * 100, 2),
    }

    return {
        "query": q,
        "total": total,
        "sentiment_counts": sentiment_counts,
        "sentiment_percentages": sentiment_percentages,
        "avg_score": avg_score,
        "items": data
    }


