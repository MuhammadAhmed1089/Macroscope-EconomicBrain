"""
GDP Analyzer Web API
====================
FastAPI application entrypoint.
Run with:  uvicorn app.main:app --reload --port 8000
"""

import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routes.api import router

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="GDP Analyzer API",
    description="REST wrapper around the GDP Analyzer Python core engine.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Allow requests from the React dev server and any local origin
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api")

logger.info("GDP Analyzer API started")
