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


@app.get("/api/v1/images/{image_type}/{filename}")
async def get_image(image_type: str, filename: str):
    """
    Serve images with fallback to placeholder
    image_type: 'home' or 'bisnis'
    """
    if image_type not in ["home", "bisnis"]:
        raise HTTPException(status_code=400, detail="Invalid image type")
    
    image_path = f"data/images/{image_type}/{filename}"
    
    # Check if specific image exists
    if os.path.exists(image_path):
        return FileResponse(image_path)
    
    # Fallback to placeholder
    placeholder_path = f"data/images/placeholder_{image_type}.jpg"
    if os.path.exists(placeholder_path):
        return FileResponse(placeholder_path)
    
    # Final fallback - return 404 if no placeholder exists
    raise HTTPException(status_code=404, detail="Image not found")


# Serve static images folder so URLs like /static/images/<subpath>
# map to files under `data/images/<subpath>` inside the container.
# Only mount if the directory exists to prevent startup errors
if os.path.exists("data/images"):
    app.mount("/static/images", StaticFiles(directory="data/images"), name="static_images")
