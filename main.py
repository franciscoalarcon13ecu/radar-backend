@app.get("/search")
def search_mentions(
    q: str = Query(..., min_length=2),
    limit: int = Query(50, ge=1, le=200),
    since_hours: int | None = Query(24, ge=1, le=720),
    target: str | None = Query(None),
):
    sb, err = get_sb()
    if err:
        return JSONResponse(status_code=500, content={"error": err})

    query = sb.table("mentions").select("*")

    # Texto contiene q (case-insensitive)
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
    return {"q": q, "count": len(res.data), "items": res.data}


