from fastapi import FastAPI
import psycopg2

app = FastAPI(title="Reporting API")

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="postgres",
    dbname="analytics",
    user="analytics",
    password="analytics"
)

@app.get("/stats")
def stats(site_id: str, date: str):
    """
    Return aggregated stats for a given site_id and date.
    """
    cur = conn.cursor()

    # Total views
    cur.execute("""
        SELECT COUNT(*) FROM events
        WHERE site_id = %s AND timestamp::date = %s
    """, (site_id, date))
    total_views = cur.fetchone()[0]

    # Unique users
    cur.execute("""
        SELECT COUNT(DISTINCT user_id) FROM events
        WHERE site_id = %s AND timestamp::date = %s
    """, (site_id, date))
    unique_users = cur.fetchone()[0]

    # Top paths
    cur.execute("""
        SELECT path, COUNT(*) 
        FROM events
        WHERE site_id = %s AND timestamp::date = %s
        GROUP BY path
        ORDER BY COUNT(*) DESC
        LIMIT 10
    """, (site_id, date))
    paths_raw = cur.fetchall()
    top_paths = [{"path": p, "views": v} for p, v in paths_raw]

    return {
        "site_id": site_id,
        "date": date,
        "total_views": total_views,
        "unique_users": unique_users,
        "top_paths": top_paths
    }
