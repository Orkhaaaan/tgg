"""
Async Reverse Geocoding Module with Rate Limiting and Cache

Features:
- Non-blocking async geocoding
- In-memory cache with TTL
- Global rate limiting (respects Nominatim 1 req/sec)
- Multiple provider support (Nominatim, Photon)
- Graceful fallback on errors
- Environment-based configuration
"""

import asyncio
import os
import time
from typing import Optional, Dict, Tuple
from datetime import datetime, timedelta
import aiohttp


# ================== CONFIGURATION ==================

GEOCODING_ENABLED = os.getenv("GEOCODING_ENABLED", "false").lower() == "true"
GEOCODING_PROVIDER = os.getenv("GEOCODING_PROVIDER", "nominatim").lower()
GEOCODING_URL = os.getenv("GEOCODING_URL", "")  # Custom URL for Photon or other
GEOCODING_TIMEOUT_SEC = int(os.getenv("GEOCODING_TIMEOUT_SEC", "3"))
GEOCODING_RPS = float(os.getenv("GEOCODING_RPS", "1.0"))  # Requests per second
GEOCODING_CACHE_TTL_SEC = int(os.getenv("GEOCODING_CACHE_TTL_SEC", "86400"))  # 24 hours

# User-Agent for Nominatim (required by their policy)
USER_AGENT = os.getenv("GEOCODING_USER_AGENT", "tgbotcuk/2.0 (attendance bot)")


# ================== CACHE ==================

class GeocodingCache:
    """In-memory cache with TTL for geocoding results"""
    
    def __init__(self, ttl_seconds: int = 86400):
        self._cache: Dict[Tuple[float, float], Tuple[str, float]] = {}
        self._ttl = ttl_seconds
        self._lock = asyncio.Lock()
    
    def _is_expired(self, timestamp: float) -> bool:
        """Check if cache entry is expired"""
        return time.time() - timestamp > self._ttl
    
    async def get(self, lat: float, lon: float) -> Optional[str]:
        """Get cached address if available and not expired"""
        key = (round(lat, 5), round(lon, 5))
        async with self._lock:
            if key in self._cache:
                address, timestamp = self._cache[key]
                if not self._is_expired(timestamp):
                    return address
                else:
                    # Remove expired entry
                    del self._cache[key]
        return None
    
    async def set(self, lat: float, lon: float, address: str) -> None:
        """Store address in cache with current timestamp"""
        key = (round(lat, 5), round(lon, 5))
        async with self._lock:
            self._cache[key] = (address, time.time())
    
    async def clear_expired(self) -> int:
        """Remove all expired entries, return count removed"""
        count = 0
        async with self._lock:
            expired_keys = [
                k for k, (_, ts) in self._cache.items()
                if self._is_expired(ts)
            ]
            for k in expired_keys:
                del self._cache[k]
                count += 1
        return count


# ================== RATE LIMITER ==================

class RateLimiter:
    """Global async rate limiter for geocoding requests"""
    
    def __init__(self, requests_per_second: float = 1.0):
        self._rps = requests_per_second
        self._min_interval = 1.0 / requests_per_second if requests_per_second > 0 else 0
        self._last_request_time = 0.0
        self._lock = asyncio.Lock()
    
    async def acquire(self) -> None:
        """Wait if necessary to respect rate limit"""
        async with self._lock:
            if self._min_interval > 0:
                now = time.time()
                time_since_last = now - self._last_request_time
                if time_since_last < self._min_interval:
                    wait_time = self._min_interval - time_since_last
                    await asyncio.sleep(wait_time)
                self._last_request_time = time.time()


# ================== GLOBAL INSTANCES ==================

_cache = GeocodingCache(ttl_seconds=GEOCODING_CACHE_TTL_SEC)
_rate_limiter = RateLimiter(requests_per_second=GEOCODING_RPS)


# ================== PROVIDERS ==================

async def _fetch_nominatim(lat: float, lon: float, session: aiohttp.ClientSession) -> Optional[str]:
    """Fetch address from Nominatim API"""
    url = "https://nominatim.openstreetmap.org/reverse"
    params = {
        "format": "jsonv2",
        "lat": lat,
        "lon": lon,
        "zoom": 18,
        "addressdetails": 1
    }
    headers = {"User-Agent": USER_AGENT}
    
    try:
        async with session.get(url, params=params, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                
                # Try display_name first
                addr = data.get("display_name", "")
                if addr:
                    return addr
                
                # Build from components
                address = data.get("address", {})
                if isinstance(address, dict):
                    parts = []
                    
                    # Road/street
                    road = address.get("road") or address.get("street") or address.get("pedestrian")
                    if road:
                        parts.append(road)
                    
                    # House number
                    house = address.get("house_number")
                    if house:
                        parts.append(house)
                    
                    # City
                    city = address.get("city") or address.get("town") or address.get("village")
                    if city and city not in parts:
                        parts.append(city)
                    
                    if parts:
                        return ", ".join(parts)
                    
                    # Fallback to suburb or neighbourhood
                    suburb = address.get("suburb") or address.get("neighbourhood")
                    if suburb:
                        return suburb
            
            return None
    except Exception as e:
        print(f"[geocoding] Nominatim error: {e}")
        return None


async def _fetch_photon(lat: float, lon: float, session: aiohttp.ClientSession) -> Optional[str]:
    """Fetch address from Photon API (custom or public)"""
    base_url = GEOCODING_URL or "https://photon.komoot.io"
    url = f"{base_url}/reverse"
    params = {"lat": lat, "lon": lon}
    
    try:
        async with session.get(url, params=params) as resp:
            if resp.status == 200:
                data = await resp.json()
                features = data.get("features", [])
                if features:
                    props = features[0].get("properties", {})
                    # Build address from Photon properties
                    parts = []
                    for key in ["name", "street", "housenumber", "city", "district"]:
                        val = props.get(key)
                        if val and val not in parts:
                            parts.append(str(val))
                    if parts:
                        return ", ".join(parts)
            return None
    except Exception as e:
        print(f"[geocoding] Photon error: {e}")
        return None


# ================== MAIN API ==================

async def reverse_geocode(lat: float, lon: float) -> Optional[str]:
    """
    Async reverse geocoding with cache, rate limiting, and graceful fallback.
    
    Args:
        lat: Latitude
        lon: Longitude
    
    Returns:
        Address string or None if geocoding is disabled/failed
    """
    # Check if geocoding is enabled
    if not GEOCODING_ENABLED:
        return None
    
    # Check cache first
    cached = await _cache.get(lat, lon)
    if cached:
        return cached
    
    # Acquire rate limit token
    await _rate_limiter.acquire()
    
    # Fetch from provider
    address = None
    timeout = aiohttp.ClientTimeout(total=GEOCODING_TIMEOUT_SEC)
    
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            if GEOCODING_PROVIDER == "photon":
                address = await _fetch_photon(lat, lon, session)
            else:  # Default to nominatim
                address = await _fetch_nominatim(lat, lon, session)
    except asyncio.TimeoutError:
        print(f"[geocoding] Timeout for {lat}, {lon}")
    except Exception as e:
        print(f"[geocoding] Unexpected error: {e}")
    
    # Cache result if successful
    if address:
        await _cache.set(lat, lon, address)
    
    return address


async def reverse_geocode_background(lat: float, lon: float, callback=None) -> None:
    """
    Background task for reverse geocoding with optional callback.
    
    Args:
        lat: Latitude
        lon: Longitude
        callback: Optional async function to call with result: callback(address: str)
    """
    try:
        address = await reverse_geocode(lat, lon)
        if address and callback:
            await callback(address)
    except Exception as e:
        print(f"[geocoding] Background task error: {e}")


# ================== MAINTENANCE ==================

async def cleanup_expired_cache() -> int:
    """Remove expired cache entries. Returns count removed."""
    return await _cache.clear_expired()


# ================== INFO ==================

def get_config_info() -> dict:
    """Get current geocoding configuration"""
    return {
        "enabled": GEOCODING_ENABLED,
        "provider": GEOCODING_PROVIDER,
        "custom_url": GEOCODING_URL or "default",
        "timeout_sec": GEOCODING_TIMEOUT_SEC,
        "rate_limit_rps": GEOCODING_RPS,
        "cache_ttl_sec": GEOCODING_CACHE_TTL_SEC,
        "user_agent": USER_AGENT,
    }
