import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import logging

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
    # Try different directories and common extensions so requests like
    # /images/house_0.jpg will match files named house_0.jpeg, house_0.png, etc.
    possible_dirs = [
        os.path.join("data", "images"),
        os.path.join("data", "images", "home"),
        os.path.join("data", "images", "bisnis"),
    ]

    # Common extensions to try when the requested name might differ
    common_exts = [".jpg", ".jpeg", ".png", ".webp"]

    # First, try the filename exactly as provided in each directory
    for d in possible_dirs:
        path_exact = os.path.join(d, filename)
        logging.info("Looking for image: %s", path_exact)
        if os.path.exists(path_exact):
            logging.info("Found image: %s", path_exact)
            return FileResponse(path_exact)

    # Split name/extension and try variations
    name, ext = os.path.splitext(filename)

    # If caller didn't provide an extension, try common ones
    if not ext:
        for d in possible_dirs:
            for e in common_exts:
                p = os.path.join(d, name + e)
                if os.path.exists(p):
                    return FileResponse(p)
    else:
        # Caller provided an extension; also try alternative extensions
        for d in possible_dirs:
            for e in common_exts:
                if e == ext.lower():
                    continue
                p = os.path.join(d, name + e)
                if os.path.exists(p):
                    return FileResponse(p)

    # Not found
    logging.warning("Image not found for request '%s'. Attempted dirs: %s", filename, possible_dirs)
    raise HTTPException(status_code=404, detail=f"Image {filename} not found")


@app.get("/debug/images")
def list_images():
    """Debug endpoint: returns files under data/images and subdirectories.
    Use to verify which images are packaged into the running container.
    """
    root = os.path.join("data", "images")
    result = {}
    if not os.path.exists(root):
        return {"exists": False, "message": "data/images not present in container"}

    for dirpath, dirnames, filenames in os.walk(root):
        rel = os.path.relpath(dirpath, root)
        key = rel if rel != "." else "root"
        result[key] = filenames

    return {"exists": True, "files": result}


# Serve static images folder so URLs like /static/images/<subpath>
# map to files under `data/images/<subpath>` inside the container.
# Only mount if the directory exists to prevent startup errors
if os.path.exists("data/images"):
    app.mount("/static/images", StaticFiles(directory="data/images"), name="static_images")
