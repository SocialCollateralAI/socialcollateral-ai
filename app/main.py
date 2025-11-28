from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

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


# Serve static images folder so URLs like /static/images/<subpath>
# map to files under `data/images/<subpath>` inside the container.
app.mount("/static/images", StaticFiles(directory="data/images"), name="static_images")
