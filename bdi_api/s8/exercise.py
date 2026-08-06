import os
from typing import Optional

import duckdb
from fastapi import APIRouter, status
from pydantic import BaseModel

from bdi_api.settings import Settings

settings = Settings()

s8 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s8",
    tags=["s8"],
)

DB_PATH = os.path.join(settings.prepared_dir, "aircraft.duckdb")

# Fuel consumption rates (gallons per hour) by ICAO aircraft type designator
FUEL_RATES_GALPH = {
    "A319": 630, "A320": 660, "A321": 750, "A332": 1200, "A333": 1250,
    "A339": 1100, "A340": 1800, "A343": 1700, "A345": 2000, "A346": 2100,
    "A359": 1200, "A35K": 1250, "A388": 3100,
    "B712": 500, "B733": 550, "B734": 580, "B735": 520, "B737": 600,
    "B738": 620, "B739": 630, "B742": 2700, "B743": 2800, "B744": 2900,
    "B748": 2800, "B752": 750, "B753": 800, "B762": 900, "B763": 950,
    "B764": 1000, "B772": 1400, "B773": 1500, "B77L": 1450, "B77W": 1500,
    "B788": 1100, "B789": 1150, "B78X": 1100,
    "C172": 8, "C208": 50, "C510": 80, "C525": 100, "C560": 150,
    "C680": 200, "C750": 250,
    "CRJ2": 250, "CRJ7": 280, "CRJ9": 320, "CRJX": 330,
    "E135": 200, "E145": 220, "E170": 300, "E175": 320, "E190": 380, "E195": 400,
    "E290": 380, "E295": 400,
    "MD11": 2100, "MD80": 600, "MD82": 600, "MD83": 600, "MD88": 600,
    "GLF4": 250, "GLF5": 280, "GLF6": 290, "GLEX": 300,
    "LJ31": 150, "LJ35": 160, "LJ45": 170, "LJ60": 200,
    "PC12": 65, "PA28": 10, "PA32": 15,
    "SF50": 40, "SR22": 15, "TBM7": 50, "TBM8": 50, "TBM9": 50,
}


class AircraftReturn(BaseModel):
    icao: str
    registration: Optional[str]
    type: Optional[str]
    owner: Optional[str]
    manufacturer: Optional[str]
    model: Optional[str]


class AircraftCO2Return(BaseModel):
    icao: str
    hours_flown: float
    co2: Optional[float]


@s8.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[AircraftReturn]:
    """List all aircraft with enriched data, ordered by ICAO ascending.

    The data should come from the silver layer (processed by the Airflow DAG).
    Paginated with `num_results` per page and `page` number (0-indexed).
    """
    if not os.path.exists(DB_PATH):
        return []

    con = duckdb.connect(DB_PATH, read_only=True)
    offset = page * num_results
    result = con.execute(
        """
        SELECT DISTINCT icao, registration, type
        FROM aircraft_positions
        ORDER BY icao ASC
        LIMIT ? OFFSET ?
        """,
        [num_results, offset],
    ).fetchall()
    con.close()

    return [
        AircraftReturn(
            icao=r[0],
            registration=r[1],
            type=r[2],
            owner=None,
            manufacturer=None,
            model=None,
        )
        for r in result
    ]


@s8.get("/aircraft/{icao}/co2")
def get_aircraft_co2(icao: str, day: str) -> AircraftCO2Return:
    """Calculate CO2 emissions for a given aircraft on a specific day.

    Computation:
    - Each row in the tracking data represents a 5-second observation
    - hours_flown = (number_of_observations * 5) / 3600
    - Look up `galph` (gallons per hour) from fuel consumption rates using the aircraft's ICAO type
    - fuel_used_kg = hours_flown * galph * 3.04
    - co2_tons = (fuel_used_kg * 3.15) / 907.185
    - If fuel consumption rate is not available for this aircraft type, return None for co2
    """
    if not os.path.exists(DB_PATH):
        return AircraftCO2Return(icao=icao, hours_flown=0.0, co2=None)

    con = duckdb.connect(DB_PATH, read_only=True)

    # Count observations and get aircraft type
    result = con.execute(
        """
        SELECT COUNT(*) as obs_count, MAX(type) as aircraft_type
        FROM aircraft_positions
        WHERE icao = ?
        """,
        [icao],
    ).fetchone()
    con.close()

    if not result or result[0] == 0:
        return AircraftCO2Return(icao=icao, hours_flown=0.0, co2=None)

    obs_count = result[0]
    aircraft_type = result[1]

    hours_flown = (obs_count * 5) / 3600

    galph = FUEL_RATES_GALPH.get(aircraft_type)
    if galph is None:
        return AircraftCO2Return(icao=icao, hours_flown=hours_flown, co2=None)

    fuel_used_kg = hours_flown * galph * 3.04
    co2_tons = (fuel_used_kg * 3.15) / 907.185

    return AircraftCO2Return(icao=icao, hours_flown=hours_flown, co2=co2_tons)
