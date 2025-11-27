from fastapi import APIRouter, HTTPException

from app.models.schemas import GroupDetail
from app.services.data_service import data_service

router = APIRouter()


@router.get("/groups/{group_id}", response_model=GroupDetail)
async def get_group_detail(group_id: str):
    data = data_service.get_group_detail(group_id)
    if not data:
        raise HTTPException(status_code=404, detail="Group not found")
    return data
