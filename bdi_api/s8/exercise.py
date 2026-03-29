import json
import os
import sqlite3

import requests
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

# Path to the SQLite database written by the Airflow DAG.
# The DAG writes to BDI_S8_DB_PATH inside the container; set that env var to
# the same absolute path that the API host sees (e.g. via a bind mount).
_DB_PATH = os.getenv(
    "BDI_S8_DB_PATH",
    os.path.join(settings.local_dir, "s8", "aircraft.db"),
)

_FUEL_URL = (
    "https://raw.githubusercontent.com/martsec/flight_co2_analysis/"
    "main/data/aircraft_type_fuel_consumption_rates.json"
)
# Simple in-process cache for fuel consumption rates (loaded once per process).
_fuel_rates: dict | None = None


def _get_fuel_rates() -> dict:
    global _fuel_rates
    if _fuel_rates is None:
        # Try a local cache file first so the API doesn't hit the network on
        # every cold start.
        cache_path = os.path.join(settings.local_dir, "s8", "fuel_rates.json")
        if os.path.exists(cache_path):
            with open(cache_path) as f:
                _fuel_rates = json.load(f)
        else:
            try:
                resp = requests.get(_FUEL_URL, timeout=30)
                resp.raise_for_status()
                _fuel_rates = resp.json()
                os.makedirs(os.path.dirname(cache_path), exist_ok=True)
                with open(cache_path, "w") as f:
                    json.dump(_fuel_rates, f)
            except Exception:
                _fuel_rates = {}
    return _fuel_rates


def _get_conn():
    if not os.path.exists(_DB_PATH):
        return None
    return sqlite3.connect(_DB_PATH)


class AircraftReturn(BaseModel):
    icao: str
    registration: str | None
    type: str | None
    owner: str | None
    manufacturer: str | None
    model: str | None


class AircraftCO2Return(BaseModel):
    icao: str
    hours_flown: float
    co2: float | None


@s8.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[AircraftReturn]:
    """List all aircraft with enriched data, ordered by ICAO ascending.

    The data should come from the silver layer (processed by the Airflow DAG).
    Paginated with `num_results` per page and `page` number (0-indexed).
    """
    conn = _get_conn()
    if conn is None:
        return []

    offset = page * num_results
    try:
        rows = conn.execute(
            """
            SELECT icao, registration, type, owner, manufacturer, model
            FROM aircraft
            ORDER BY icao ASC
            LIMIT ? OFFSET ?
            """,
            (num_results, offset),
        ).fetchall()
    except Exception:
        return []
    finally:
        conn.close()

    return [
        AircraftReturn(
            icao=r[0],
            registration=r[1],
            type=r[2],
            owner=r[3],
            manufacturer=r[4],
            model=r[5],
        )
        for r in rows
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
    conn = _get_conn()
    if conn is None:
        return AircraftCO2Return(icao=icao, hours_flown=0.0, co2=None)

    try:
        # Get observation count for this aircraft on the given day
        obs_row = conn.execute(
            "SELECT count FROM observations WHERE icao = ? AND day = ?",
            (icao, day),
        ).fetchone()

        # Get the aircraft type so we can look up fuel consumption
        ac_row = conn.execute(
            "SELECT type FROM aircraft WHERE icao = ?",
            (icao,),
        ).fetchone()
    except Exception:
        return AircraftCO2Return(icao=icao, hours_flown=0.0, co2=None)
    finally:
        conn.close()

    observation_count = obs_row[0] if obs_row else 0
    hours_flown = (observation_count * 5) / 3600

    ac_type = ac_row[0] if ac_row else None
    co2: float | None = None

    if ac_type:
        fuel_rates = _get_fuel_rates()
        rate = fuel_rates.get(ac_type)
        if rate is not None:
            galph = rate if isinstance(rate, (int, float)) else rate.get("galph")
            if galph is not None:
                fuel_used_kg = hours_flown * galph * 3.04
                co2 = (fuel_used_kg * 3.15) / 907.185

    return AircraftCO2Return(icao=icao, hours_flown=hours_flown, co2=co2)
