from typing import Annotated

from fastapi import APIRouter, HTTPException, status
from fastapi.params import Query
from pydantic import BaseModel
from pymongo import DESCENDING, MongoClient

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


def get_collection():
    client = MongoClient(settings.mongo_url)
    return client["bdi_aircraft"]["positions"]


class AircraftPosition(BaseModel):
    icao: str
    registration: str | None = None
    type: str | None = None
    lat: float
    lon: float
    alt_baro: float | None = None
    ground_speed: float | None = None
    timestamp: str


@s6.post("/aircraft")
def create_aircraft(position: AircraftPosition) -> dict:
    """Store an aircraft position document in MongoDB.

    Use the BDI_MONGO_URL environment variable to configure the connection.
    Start MongoDB with: make mongo
    Database name: bdi_aircraft
    Collection name: positions
    """
    collection = get_collection()
    collection.insert_one(position.model_dump())
    return {"status": "ok"}


@s6.get("/aircraft/stats")
def aircraft_stats() -> list[dict]:
    """Return aggregated statistics: count of positions grouped by aircraft type.

    Response example: [{"type": "B738", "count": 42}, {"type": "A320", "count": 38}]

    Use MongoDB's aggregation pipeline with $group.
    """
    collection = get_collection()
    pipeline = [
        {"$group": {"_id": "$type", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$project": {"_id": 0, "type": "$_id", "count": 1}},
    ]
    return list(collection.aggregate(pipeline))


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
    collection = get_collection()
    pipeline = [
        {"$sort": {"timestamp": DESCENDING}},
        {"$group": {"_id": "$icao", "registration": {"$first": "$registration"}, "type": {"$first": "$type"}}},
        {"$project": {"_id": 0, "icao": "$_id", "registration": 1, "type": 1}},
        {"$skip": (page - 1) * page_size},
        {"$limit": page_size},
    ]
    return list(collection.aggregate(pipeline))


@s6.get("/aircraft/{icao}")
def get_aircraft(icao: str) -> dict:
    """Get the latest position data for a specific aircraft.

    Return the most recent document matching the given ICAO code.
    If not found, return 404.
    """
    collection = get_collection()
    doc = collection.find_one({"icao": icao}, sort=[("timestamp", DESCENDING)], projection={"_id": 0})
    if doc is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Aircraft not found")
    return doc


@s6.delete("/aircraft/{icao}")
def delete_aircraft(icao: str) -> dict:
    """Remove all position records for an aircraft.

    Returns the number of deleted documents.
    """
    collection = get_collection()
    result = collection.delete_many({"icao": icao})
    return {"deleted": result.deleted_count}
