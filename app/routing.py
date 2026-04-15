"""
BrokerOps AI – Route mileage via Google Maps Directions API.

Used by the quote pipeline to calculate road miles between origin and
destination for rate math. Google Maps Directions API handles truck-reasonable
routing; for hazmat / bridge / weight-restricted routing upgrade to PC*Miler
later.

Requires GOOGLE_MAPS_API_KEY in config (restricted to directions-backend.googleapis.com).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional

import httpx

from app.config import get_settings

logger = logging.getLogger("brokerops.routing")

_DIRECTIONS_URL = "https://maps.googleapis.com/maps/api/directions/json"
_DEFAULT_TIMEOUT_SEC = 10.0


@dataclass(frozen=True)
class Route:
    """Driving route summary."""
    origin: str
    destination: str
    miles: float
    duration_minutes: int
    summary: str  # human-readable route name (e.g. "I-95 N")


class RouteLookupError(Exception):
    """Raised when the Directions API returns an unrecoverable error."""


@lru_cache(maxsize=512)
def get_route(origin: str, destination: str) -> Optional[Route]:
    """Fetch driving route for origin → destination.

    Origin/destination can be any format Google Maps accepts:
      - "Miami, FL"
      - "33101"
      - "1600 Pennsylvania Ave NW, Washington, DC"

    Returns None if the API key is missing OR no route is found. Raises
    RouteLookupError only on hard failures (invalid request, auth error).
    Network/timeout errors return None so the caller can degrade gracefully.
    """
    key = get_settings().GOOGLE_MAPS_API_KEY
    if not key:
        logger.warning("GOOGLE_MAPS_API_KEY not configured — route lookup skipped")
        return None

    params = {
        "origin": origin,
        "destination": destination,
        "mode": "driving",
        "units": "imperial",
        "key": key,
    }
    try:
        resp = httpx.get(_DIRECTIONS_URL, params=params, timeout=_DEFAULT_TIMEOUT_SEC)
    except httpx.HTTPError as exc:
        logger.warning("Directions API network error for %s → %s: %s", origin, destination, exc)
        return None

    if resp.status_code != 200:
        logger.warning(
            "Directions API returned HTTP %d for %s → %s: %s",
            resp.status_code, origin, destination, resp.text[:200],
        )
        return None

    data = resp.json()
    status = data.get("status", "UNKNOWN")

    if status == "ZERO_RESULTS":
        logger.info("No route found: %s → %s", origin, destination)
        return None
    if status == "NOT_FOUND":
        logger.info("Address not found: %s → %s", origin, destination)
        return None
    if status in ("REQUEST_DENIED", "INVALID_REQUEST"):
        raise RouteLookupError(
            f"Directions API rejected request ({status}): {data.get('error_message', 'no detail')}"
        )
    if status != "OK":
        logger.warning("Directions API status %s for %s → %s", status, origin, destination)
        return None

    routes = data.get("routes", [])
    if not routes:
        return None
    leg = routes[0]["legs"][0]
    # leg.distance.value is meters; leg.duration.value is seconds
    miles = leg["distance"]["value"] / 1609.344
    duration_min = round(leg["duration"]["value"] / 60)
    summary = routes[0].get("summary", "")

    return Route(
        origin=origin,
        destination=destination,
        miles=round(miles, 1),
        duration_minutes=duration_min,
        summary=summary,
    )


def get_route_miles(origin: str, destination: str) -> Optional[float]:
    """Convenience wrapper returning just the driving distance in miles."""
    route = get_route(origin, destination)
    return route.miles if route else None
