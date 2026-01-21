"""
FastAPI application with static file serving capability.

This module demonstrates how to serve static files (e.g., member profile images)
alongside the main API endpoints using FastAPI's StaticFiles middleware.
"""

import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.api import graph, groups

app = FastAPI(title="SocialCollateral AI - Backend", version="1.0.0")

# Configure CORS middleware to allow cross-origin requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static file serving: Mount images directory for member profile photos
os.makedirs("data/images", exist_ok=True)
app.mount("/static", StaticFiles(directory="data/images"), name="static")

# Register API route handlers
app.include_router(graph.router, prefix="/api/v1")
app.include_router(groups.router, prefix="/api/v1")


@app.get("/")
def root():
    """Root endpoint to verify server status."""
    return {"message": "Backend & Static File Server Running!"}
