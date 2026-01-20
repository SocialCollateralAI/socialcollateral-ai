import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles  # <--- IMPORT INI

from app.api import graph, groups

app = FastAPI(title="SocialCollateral AI - Backend", version="1.0.0")

# Setup CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- UPDATE KRUSIAL: SERVE STATIC FILES ---
# Pastikan folder 'data/images' ada. Di sinilah foto sample panitia akan ditaruh.
os.makedirs("data/images", exist_ok=True)
app.mount("/static", StaticFiles(directory="data/images"), name="static")

app.include_router(graph.router, prefix="/api/v1")
app.include_router(groups.router, prefix="/api/v1")


@app.get("/")
def root():
    return {"message": "Backend & Static File Server Running!"}
