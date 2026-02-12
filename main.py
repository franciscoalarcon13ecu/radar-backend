from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import os, random
from datetime import datetime, timedelta, timezone

app = FastAPI(title="Radar Backend", version="0.2")

TOPICS = ["seguridad", "obras", "basura", "tráfico", "agua", "impuestos", "parques", "empleo"]
SENTIMENTS = ["pos", "neu", "neg"]


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


@app.get("/health")
def health():
    url = (os.environ.get("SUPABASE_URL") or "").strip()
    return {
        "ok": True,
        "supabase_url_preview": url[:35] + ("..." if len(url) > 35 else ""),
        "has_service_role_key": bool((os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()),
        "ts": datetime.now(timezone.utc).isoformat(),
    }


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

    # ✅ Esto faltaba en tu código: realmente insertar en Supabase
    sb.table("mentions").insert(rows).execute()
    return {"inserted_mentions": n}


@app.get("/test")
def test():
    from supabase import create_client

    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    res = supabase.table("test_table").select("*").execute()
    return res.data


@app.get("/mentions")
def get_mentions(
    limit: int = Query(50, ge=1, le=500),
    sentiment: str | None = Query(None, description="pos|neu|neg"),
    topic: str | None = Query(None),
    target: str | None = Query(None),
    country: str | None = Query(None),
    source: str | None = Query(None),
    since_hours: int | None = Query(None, ge=1, le=720),
):
    sb, err = get_sb()
    if err:
        return JSONResponse(status_code=500, content={"error": err})

    q = sb.table("mentions").select("*")

    if sentiment:
        q = q.eq("sentiment", sentiment)
    if topic:
        q = q.eq("topic", topic)
    if target:
        q = q.eq("target", target)
    if country:
        q = q.eq("country", country)
    if source:
        q = q.eq("source", source)

    if since_hours:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)
        q = q.gte("created_at", cutoff.isoformat())

    res = (
        q.order("created_at", desc=True)
         .limit(limit)
         .execute()
    )
    return res.data



