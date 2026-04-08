from typing import Annotated, Optional

from fastapi import APIRouter, HTTPException, status
from fastapi.params import Query
from pydantic import BaseModel
from pymongo import MongoClient

from bdi_api.settings import Settings

settings = Settings()

s6 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s6",
    tags=["s6"],
)


class AircraftPosition(BaseModel):
    icao: str
    registration: Optional[str] = None
    type: Optional[str] = None
    lat: float
    lon: float
    alt_baro: Optional[float] = None
    ground_speed: Optional[float] = None
    timestamp: str


def _get_collection():
    client = MongoClient(settings.mongo_url)
    db = client["bdi_aircraft"]
    return db["positions"]


@s6.post("/aircraft")
def create_aircraft(position: AircraftPosition) -> dict:
    """Store an aircraft position document in MongoDB.

    Use the BDI_MONGO_URL environment variable to configure the connection.
    Start MongoDB with: make mongo
    Database name: bdi_aircraft
    Collection name: positions
    """
    collection = _get_collection()
    doc = position.model_dump()
    collection.insert_one(doc)
    return {"status": "ok"}


@s6.get("/aircraft/stats")
def aircraft_stats() -> list[dict]:
    """Return aggregated statistics: count of positions grouped by aircraft type.

    Response example: [{"type": "B738", "count": 42}, {"type": "A320", "count": 38}]

    Use MongoDB's aggregation pipeline with $group.
    """
    collection = _get_collection()
    pipeline = [
        {"$group": {"_id": "$type", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$project": {"_id": 0, "type": "$_id", "count": 1}},
    ]
    results = list(collection.aggregate(pipeline))
    return results


@s6.get("/aircraft/")
def list_aircraft(
    page: Annotated[
        int,
        Query(description="Page number (1-indexed)", ge=1),
    ] = 1,
    page_size: Annotated[
        int,
        Query(description="Number of results per page", ge=1, le=100),
    ] = 20,
) -> list[dict]:
    """List all aircraft with pagination.

    Each result should include: icao, registration, type.
    Use MongoDB's skip() and limit() for pagination.
    """
    collection = _get_collection()
    skip = (page - 1) * page_size

    pipeline = [
        {"$group": {
            "_id": "$icao",
            "registration": {"$first": "$registration"},
            "type": {"$first": "$type"},
        }},
        {"$sort": {"_id": 1}},
        {"$skip": skip},
        {"$limit": page_size},
        {"$project": {"_id": 0, "icao": "$_id", "registration": 1, "type": 1}},
    ]
    results = list(collection.aggregate(pipeline))
    return results


@s6.get("/aircraft/{icao}")
def get_aircraft(icao: str) -> dict:
    """Get the latest position data for a specific aircraft.

    Return the most recent document matching the given ICAO code.
    If not found, return 404.
    """
    collection = _get_collection()
    doc = collection.find_one(
        {"icao": icao},
        sort=[("timestamp", -1)],
        projection={"_id": 0},
    )
    if not doc:
        raise HTTPException(status_code=404, detail=f"Aircraft '{icao}' not found")
    return doc


@s6.delete("/aircraft/{icao}")
def delete_aircraft(icao: str) -> dict:
    """Remove all position records for an aircraft.

    Returns the number of deleted documents.
    """
    collection = _get_collection()
    result = collection.delete_many({"icao": icao})
    return {"deleted": result.deleted_count}
