# File: worker/main.py

import json
import os
import time
import redis
import psycopg2
from psycopg2.extras import Json

# --- 1. Configuration from Environment Variables ---

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_NAME = os.getenv("DB_NAME", "db")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))


# --- 2. The "Business Logic" (The Actual Task Functions) ---

def scan_url(url: str):
    """A dummy function to simulate scanning a URL for vulnerabilities."""
    print(f"WORKER: Starting to scan URL: {url}...")
    # In a real scenario, this would involve network requests, analysis, etc.
    time.sleep(5)
    print(f"WORKER: Finished scanning URL: {url}.")
    return {"status_code": 200, "title": "Example Domain", "vulnerabilities_found": 0}

def fetch_ip(hostname: str):
    """A dummy function to simulate a DNS lookup."""
    print(f"WORK-ER: Starting to fetch IP for: {hostname}...")
    time.sleep(2)
    print(f"WORKER: Finished fetching IP for: {hostname}.")
    return {"ip_address": "93.184.216.34", "hostname": hostname}


# --- 3. The Task Registry (A Simple Dispatcher) ---
TASK_REGISTRY = {
    "scan_url": scan_url,
    "fetch_ip": fetch_ip,
}

def get_db_connection():
    """Establishes and returns a connection to the PostgreSQL database."""
    while True:
        try:
            conn = psycopg2.connect(
                dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
            )
            conn.autocommit = True
            print("WORKER: Database connection established.")
            return conn
        except psycopg2.OperationalError as e:
            print(f"WORKER: Database connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def main():
    """The main loop of the worker process."""
    print("WORKER: Starting up...")
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    db_conn = get_db_connection()

    while True:
        try:
            # --- 4. The Core Loop: Blocking Pop from Redis ---
            print("WORKER: Waiting for a new job from the queue...")
            _, job_json = redis_client.brpop("task_queue")
            job_data = json.loads(job_json)

            task_id = job_data["task_id"]
            task_name = job_data["task_name"]
            params = job_data["params"]
            print(f"WORKER: Picked up job {task_id} ({task_name})")

            # --- 5. Update State: ---
            with db_conn.cursor() as cur:
                cur.execute(
                    "UPDATE tasks SET status = 'IN_PROGRESS', started_at = NOW() WHERE id = %s",
                    (task_id,)
                )

            # --- 6. Execute Task and Handle Outcome ---
            task_function = TASK_REGISTRY.get(task_name)
            if task_function:
                result = task_function(**params)
                with db_conn.cursor() as cur:
                    cur.execute(
                        "UPDATE tasks SET status = 'COMPLETED', completed_at = NOW(), result = %s WHERE id = %s",
                        (Json(result), task_id)
                    )
                print(f"WORKER: Job {task_id} completed successfully.")
            else:
                
                error_result = {"error": f"Task name '{task_name}' not found in registry."}
                with db_conn.cursor() as cur:
                    cur.execute(
                        "UPDATE tasks SET status = 'FAILED', completed_at = NOW(), result = %s WHERE id = %s",
                        (Json(error_result), task_id)
                    )
                print(f"WORKER: Job {task_id} failed: task not found.")

        # --- 7. Error Handling and Resilience ---
        except psycopg2.Error as e:
            print(f"WORKER: Database error: {e}. Attempting to reconnect...")
            db_conn = get_db_connection() 
        except redis.exceptions.RedisError as e:
            print(f"WORKER: Redis error: {e}. Retrying in 5 seconds...")
            time.sleep(5) 
        except Exception as e:
            
            print(f"WORKER: An unexpected error occurred: {e}")
            
            time.sleep(5)

if __name__ == "__main__":
    main()