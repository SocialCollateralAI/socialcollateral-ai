import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from app.api import graph, groups

app = FastAPI(title="SocialCollateral AI - Backend", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(graph.router, prefix="/api/v1")
app.include_router(groups.router, prefix="/api/v1")


@app.get("/")
def root():
    return {"message": "Backend is Running!"}


@app.get("/health")
def health_check():
    return {"status": "healthy", "message": "SocialCollateral AI Backend is running"}


@app.get("/images/{filename}")
async def get_image(filename: str):
    """
    Simple endpoint to serve images directly
    Usage: /images/placeholder_home.jpg or /images/sample1.jpg
    """
    # Try different locations
    possible_paths = [
        f"data/images/{filename}",
        f"data/images/home/{filename}",
        f"data/images/bisnis/{filename}"
    ]
    
    for image_path in possible_paths:
        if os.path.exists(image_path):
            return FileResponse(image_path)
    
    # If not found, return 404
    raise HTTPException(status_code=404, detail=f"Image {filename} not found")


# Serve static images folder so URLs like /static/images/<subpath>
# map to files under `data/images/<subpath>` inside the container.
# Only mount if the directory exists to prevent startup errors
if os.path.exists("data/images"):
    app.mount("/static/images", StaticFiles(directory="data/images"), name="static_images")
