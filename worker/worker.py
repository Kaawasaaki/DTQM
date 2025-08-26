# File: worker/main.py

import json
import os
import time
import redis
import psycopg2
from psycopg2.extras import Json

# --- 1. Configuration from Environment Variables ---
# The worker runs in its own container and needs its own configuration
# to connect to the shared Redis and PostgreSQL services. Using os.getenv
# allows us to configure it at runtime.
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_NAME = os.getenv("DB_NAME", "db")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))


# --- 2. The "Business Logic" (The Actual Task Functions) ---
# These functions contain the code that performs the actual work. They are
# completely decoupled from the queueing system. They just take arguments
# and return a result.
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
# This dictionary maps the string `task_name` from the job payload to the
# actual Python function that should be executed. This makes the worker
# easily extensible: to add a new task, just define a function and add it here.
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
            # Setting autocommit to True means each DB statement is committed immediately,
            # which is simpler for this worker's logic.
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
            # `brpop` is a "blocking" command. The worker will sleep efficiently
            # until a job is available. This is far better than constantly polling.
            # It returns a tuple: (queue_name, job_json_string)
            print("WORKER: Waiting for a new job from the queue...")
            _, job_json = redis_client.brpop("task_queue")
            job_data = json.loads(job_json)

            task_id = job_data["task_id"]
            task_name = job_data["task_name"]
            params = job_data["params"]
            print(f"WORKER: Picked up job {task_id} ({task_name})")

            # --- 5. Update State: Mark as IN_PROGRESS ---
            with db_conn.cursor() as cur:
                cur.execute(
                    "UPDATE tasks SET status = 'IN_PROGRESS', started_at = NOW() WHERE id = %s",
                    (task_id,)
                )

            # --- 6. Execute Task and Handle Outcome ---
            task_function = TASK_REGISTRY.get(task_name)
            if task_function:
                result = task_function(**params)
                # Update State: Mark as COMPLETED
                with db_conn.cursor() as cur:
                    cur.execute(
                        "UPDATE tasks SET status = 'COMPLETED', completed_at = NOW(), result = %s WHERE id = %s",
                        (Json(result), task_id)
                    )
                print(f"WORKER: Job {task_id} completed successfully.")
            else:
                # Update State: Mark as FAILED (task not found)
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
            db_conn = get_db_connection() # Re-establish the connection for the next loop
        except redis.exceptions.RedisError as e:
            print(f"WORKER: Redis error: {e}. Retrying in 5 seconds...")
            time.sleep(5) # Wait before trying to connect to Redis again
        except Exception as e:
            # This catches bugs in the task functions themselves or other unexpected errors.
            print(f"WORKER: An unexpected error occurred: {e}")
            # Optional: You could update the task to 'FAILED' in the database here.
            time.sleep(5) # Wait before getting the next job

if __name__ == "__main__":
    main()