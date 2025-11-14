import redis
import json
import psycopg2
import time

# Connect to Redis
r = redis.Redis(host="redis", port=6379, decode_responses=True)

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="postgres",
    dbname="analytics",
    user="analytics",
    password="analytics"
)
cur = conn.cursor()

print("Processor started... Waiting for events...")

while True:
    try:
        # Blocking pop from Redis queue
        event_data = r.brpop("events_queue", timeout=5)
        if not event_data:
            continue  # No event, loop again

        _, payload = event_data
        event = json.loads(payload)

        # Insert into PostgreSQL
        cur.execute("""
            INSERT INTO events (site_id, event_type, path, user_id, timestamp)
            VALUES (%s, %s, %s, %s, %s)
        """, (event["site_id"], event["event_type"], event["path"], event["user_id"], event["timestamp"]))

        conn.commit()
        print("Inserted event:", event)

    except Exception as e:
        print("Error processing event:", e)
        time.sleep(1)
