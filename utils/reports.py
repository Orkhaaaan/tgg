"""
Hesabat funksiyaları - qayda yoxlamaları və formatlar
"""
from datetime import datetime, timedelta
from typing import Optional, Tuple
from utils.distance import haversine_m


# Qayda konstantaları (main_aiogram.py-dən import olunacaq)
CHECKIN_DEADLINE_HOUR = 11
CHECKOUT_DEADLINE_HOUR = 19
MIN_WORK_DURATION_HOURS = 3
WORKPLACE_LAT = 40.4093
WORKPLACE_LON = 49.8671
WORKPLACE_RADIUS_M = 100
LOCATION_TOLERANCE_M = 50


def check_rules_violation(
    giris_time: Optional[str],
    cixis_time: Optional[str],
    giris_lat: Optional[float],
    giris_lon: Optional[float],
    cixis_lat: Optional[float],
    cixis_lon: Optional[float],
    is_active: int = 1,
    checkin_deadline: int = 11,
    checkout_deadline: int = 19,
    min_work_hours: float = 3.0,
    workplace_lat: float = 40.4093,
    workplace_lon: float = 49.8671,
    workplace_radius: float = 100.0,
    location_tolerance: float = 50.0
) -> Tuple[str, list[str]]:
    """
    Qayda pozuntularını yoxlayır.
    Returns: (status, violations)
    status: "ok" (yaşıl), "violation" (qırmızı), "inactive" (boz)
    violations: pozuntuların siyahısı
    """
    violations = []
    
    # Kursdan çıxarılanlar
    if is_active == 0:
        return ("inactive", ["Kursdan çıxarılıb"])
    
    # Giriş yoxdursa
    if not giris_time:
        return ("violation", ["Giriş yoxdur"])
    
    try:
        # Giriş vaxtını parse et
        if isinstance(giris_time, str):
            # Format: HH:MM:SS və ya HH:MM
            time_parts = giris_time.split(':')
            if len(time_parts) >= 2:
                giris_hour = int(time_parts[0])
                # Qayda: Giriş deadline-a qədər
                if giris_hour >= checkin_deadline:
                    violations.append(f"Giriş {checkin_deadline}:00-dan sonra ({giris_time})")
        
        # Çıxış varsa, yoxla
        if cixis_time:
            time_parts = cixis_time.split(':')
            if len(time_parts) >= 2:
                cixis_hour = int(time_parts[0])
                # Qayda: Çıxış deadline-a qədər
                if cixis_hour >= checkout_deadline:
                    violations.append(f"Çıxış {checkout_deadline}:00-dan sonra ({cixis_time})")
            
            # Minimum iş müddəti yoxla
            try:
                giris_dt = datetime.strptime(giris_time, "%H:%M:%S")
            except:
                try:
                    giris_dt = datetime.strptime(giris_time, "%H:%M")
                except:
                    giris_dt = None
            
            try:
                cixis_dt = datetime.strptime(cixis_time, "%H:%M:%S")
            except:
                try:
                    cixis_dt = datetime.strptime(cixis_time, "%H:%M")
                except:
                    cixis_dt = None
            
            if giris_dt and cixis_dt:
                duration = (cixis_dt - giris_dt).total_seconds() / 3600.0
                if duration < min_work_hours:
                    violations.append(f"Minimum iş müddəti pozulub ({duration:.1f} saat < {min_work_hours} saat)")
            
            # Lokasiya yoxlaması - çıxış girişdən fərqli yerdədirsə
            if giris_lat is not None and giris_lon is not None and cixis_lat is not None and cixis_lon is not None:
                dist = haversine_m(float(giris_lat), float(giris_lon), float(cixis_lat), float(cixis_lon))
                if dist > location_tolerance:
                    violations.append(f"Çıxış fərqli yerdə ({int(dist)}m > {location_tolerance}m)")
        
        # İş yeri mərkəzinə görə yoxlama deaktiv edilib
        
    except Exception as e:
        # Parse xətası olsa, sadəcə violations-a əlavə et
        pass
    
    if violations:
        return ("violation", violations)
    else:
        return ("ok", [])


def get_status_color(status: str) -> str:
    """Status-a görə rəng hex kodu qaytarır"""
    colors = {
        "ok": "00FF00",  # Yaşıl
        "violation": "FF0000",  # Qırmızı
        "inactive": "808080",  # Boz
    }
    return colors.get(status, "FFFFFF")


def get_status_name(status: str) -> str:
    """Status-a görə ad qaytarır"""
    names = {
        "ok": "Qaydalara uyğundur",
        "violation": "Qayda pozuntusu",
        "inactive": "Kursdan çıxarılıb",
    }
    return names.get(status, "Naməlum")

