import os
import random
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY env vars")

sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

app = FastAPI(title="Radar Backend", version="0.1")

TOPICS = ["seguridad", "obras", "basura", "tráfico", "agua", "impuestos", "parques", "empleo"]
SENTIMENTS = ["pos", "neu", "neg"]

def reputation_index(pos: int, neu: int, neg: int) -> int:
    total = max(pos + neu + neg, 1)
    score = ((pos - neg) / total + 1) * 50  # 0..100
    return int(max(0, min(100, round(score))))

@app.get("/health")
def health():
    return {"ok": True, "ts": datetime.now(timezone.utc).isoformat()}

@app.post("/seed")
def seed(n: int = 120):
    """
    Genera 'n' menciones demo para Guayaquil (últimas 24h)
    y actualiza métricas por hora.
    """
    now = datetime.now(timezone.utc)
    rows = []

    for _ in range(n):
        created_at = now - timedelta(minutes=random.randint(0, 24 * 60))
        sent = random.choices(SENTIMENTS, weights=[30, 35, 35], k=1)[0]
        topic = random.choice(TOPICS)
        score = random.randint(55, 95) if sent == "pos" else random.randint(35, 75) if sent == "neu" else random.randint(10, 60)

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

    start = now - timedelta(hours=24)
    data = sb.table("mentions") \
        .select("created_at,sentiment,topic") \
        .eq("target", "alcaldia_gye") \
        .gte("created_at", start.isoformat()) \
        .execute().data or []

    buckets = {}
    for item in data:
        ts = datetime.fromisoformat(item["created_at"].replace("Z", "+00:00"))
        bucket = ts.replace(minute=0, second=0, microsecond=0)
        b = buckets.setdefault(bucket, {"pos":0, "neu":0, "neg":0, "topics_pos":{}, "topics_neg":{}})
        s = item["sentiment"]
        b[s] += 1
        t = item.get("topic") or "otros"
        if s == "pos":
            b["topics_pos"][t] = b["topics_pos"].get(t, 0) + 1
        if s == "neg":
            b["topics_neg"][t] = b["topics_neg"].get(t, 0) + 1

    sb.table("metrics_hourly").delete().eq("target", "alcaldia_gye").eq("source", "demo").execute()

    metric_rows = []
    for bucket_start, b in buckets.items():
        pos, neu, neg = b["pos"], b["neu"], b["neg"]
        total = pos + neu + neg
        top_neg = max(b["topics_neg"], key=b["topics_neg"].get) if b["topics_neg"] else None
        top_pos = max(b["topics_pos"], key=b["topics_pos"].get) if b["topics_pos"] else None
        metric_rows.append({
            "bucket_start": bucket_start.isoformat(),
            "target": "alcaldia_gye",
            "source": "demo",
            "mentions_count": total,
            "pos_count": pos,
            "neu_count": neu,
            "neg_count": neg,
            "reputation_index": reputation_index(pos, neu, neg),
            "top_negative_topic": top_neg,
            "top_positive_topic": top_pos
        })

    if metric_rows:
        sb.table("metrics_hourly").insert(metric_rows).execute()

    return {"inserted_mentions": n, "metric_buckets": len(metric_rows)}
