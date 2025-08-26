# File: api/main.py

import uuid
import json
from typing import Optional, Any

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, BaseSettings, Field, RedisDsn
from asyncpg.pool import create_pool, Pool
import redis.asyncio as aioredis


# --- 1. Configuration Management (Pydantic BaseSettings) ---
# This is the professional way to handle configuration. It reads from environment
# variables, provides defaults, and validates types, ensuring your app
# won't start with invalid settings.
class Settings(BaseSettings):
    postgres_dsn: str = "postgresql://user:password@postgres/db"
    redis_dsn: RedisDsn = "redis://redis:6379"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()


# --- 2. Pydantic Models (API Data Contracts) ---
# These classes define the exact structure and data types for your API's
# inputs and outputs. FastAPI uses them to automatically validate requests
# and serialize responses. This is your API's "contract".

class TaskRequest(BaseModel):
    task_name: str
    params: dict[str, Any]

class TaskCreationResponse(BaseModel):
    task_id: str

class TaskStatusResponse(BaseModel):
    id: str
    task_name: str
    status: str
    submitted_at: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    result: Optional[dict[str, Any]] = None


# --- 3. Application State and Dependency Injection ---
# We manage shared resources like database pools here. Using dependency injection
# makes our code more testable and organized.

db_pool: Pool
redis_client: aioredis.Redis

async def get_db_pool() -> Pool:
    return db_pool

async def get_redis_client() -> aioredis.Redis:
    return redis_client

app = FastAPI(title="Distributed Task Queue API")


# --- 4. Lifecycle Events (Startup and Shutdown) ---
# These functions ensure that connections to databases and other resources are
# established when the application starts and gracefully closed when it stops.
# This is crucial for preventing resource leaks.

@app.on_event("startup")
async def startup_event():
    """Initialize database and Redis connection pools on application startup."""
    print("Connecting to databases...")
    global db_pool, redis_client
    db_pool = await create_pool(settings.postgres_dsn)
    redis_client = aioredis.from_url(settings.redis_dsn, decode_responses=True)
    print("Connections established.")

@app.on_event("shutdown")
async def shutdown_event():
    """Close database and Redis connection pools on application shutdown."""
    print("Closing connections...")
    await db_pool.close()
    await redis_client.close()
    print("Connections closed.")


# --- 5. CORS Middleware ---
# This allows your frontend (running in a browser) to communicate with your backend API.
# It's a security feature required by all modern browsers.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this to your actual frontend domain
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# --- 6. API Endpoints (The Core Logic) ---
# These are the "doors" to your application that the outside world can access.

@app.post("/tasks", response_model=TaskCreationResponse, status_code=202)
async def submit_task(
    task: TaskRequest,
    db: Pool = Depends(get_db_pool),
    redis: aioredis.Redis = Depends(get_redis_client)
):
    """
    Accepts a new task, creates a record in the database, and pushes it to the queue.
    Returns immediately with the task's unique ID.
    """
    task_id = str(uuid.uuid4())
    job_payload = json.dumps({
        "task_id": task_id,
        "task_name": task.task_name,
        "params": task.params
    })

    async with db.acquire() as conn:
        # Using a transaction ensures that we either write to the DB AND push to Redis, or neither.
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO tasks (id, task_name, status) VALUES ($1, $2, 'PENDING')",
                task_id, task.task_name
            )
            await redis.lpush("task_queue", job_payload)

    return TaskCreationResponse(task_id=task_id)


@app.get("/tasks/{task_id}", response_model=TaskStatusResponse)
async def get_task_status(task_id: str, db: Pool = Depends(get_db_pool)):
    """Retrieves the current status and result of a task by its ID."""
    try:
        # A quick check to ensure the task_id is in a valid UUID format before querying the DB.
        uuid.UUID(task_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid Task ID format.")

    async with db.acquire() as conn:
        record = await conn.fetchrow("SELECT * FROM tasks WHERE id = $1", task_id)

    if not record:
        raise HTTPException(status_code=404, detail=f"Task with ID {task_id} not found.")

    # Pydantic model will automatically convert the database record to a clean JSON response
    return record


# --- 7. Serving the Frontend User Interface ---
# These final endpoints are responsible for delivering the static HTML and JavaScript
# files that make up your application's user interface.

@app.get("/", include_in_schema=False)
async def read_index():
    """Serves the main index.html file of the frontend application."""
    return FileResponse('api/frontend/index.html')

# This mounts a directory, allowing FastAPI to serve static files like script.js, styles.css etc.
app.mount("/", StaticFiles(directory="api/frontend"), name="static")