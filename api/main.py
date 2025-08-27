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
class Settings(BaseSettings):
    postgres_dsn: str = "postgresql://user:password@postgres/db"
    redis_dsn: RedisDsn = "redis://redis:6379"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()


# --- 2. Pydantic Models (API Data Contracts) ---

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

db_pool: Pool
redis_client: aioredis.Redis

async def get_db_pool() -> Pool:
    return db_pool

async def get_redis_client() -> aioredis.Redis:
    return redis_client

app = FastAPI(title="Distributed Task Queue API")


# --- 4. Lifecycle Events (Startup and Shutdown) ---

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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this to your actual frontend domain
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# --- 6. API Endpoints ---

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
        
        uuid.UUID(task_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid Task ID format.")

    async with db.acquire() as conn:
        record = await conn.fetchrow("SELECT * FROM tasks WHERE id = $1", task_id)

    if not record:
        raise HTTPException(status_code=404, detail=f"Task with ID {task_id} not found.")


    return record


# --- 7. Serving the Frontend User Interface ---

@app.get("/", include_in_schema=False)
async def read_index():
    """Serves the main index.html file of the frontend application."""
    return FileResponse('api/frontend/index.html')


app.mount("/", StaticFiles(directory="api/frontend"), name="static")