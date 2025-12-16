"""
Hesabat ixrac funksiyaları - Excel, CSV, PDF
"""
import csv
import os
from datetime import datetime, timedelta
from typing import List, Dict
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill
from openpyxl.utils import get_column_letter


def generate_csv_report(report_data: List[Dict], filename: str) -> str:
    """Generate CSV report. Returns path to the CSV file."""
    filepath = os.path.join(os.getcwd(), filename)
    
    if not report_data:
        # Create empty CSV with headers
        headers = [
            "Tarix", "FIN Kodu", "Ad", "Soyad", "Vəsiqə Seriya", "Telefon",
            "Qrup Kodu", "Peşə", "Giriş Saatı", "Çıxış Saatı", 
            "GPS Koordinatları", "Lokasiya", "Status", "Qayda Pozuntuları"
        ]
        with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
        return filepath
    
    # Get headers from first row keys
    headers = [
        "Tarix", "FIN Kodu", "Ad", "Soyad", "Vəsiqə Seriya", "Telefon",
        "Qrup Kodu", "Peşə", "Giriş Saatı", "Çıxış Saatı",
        "GPS Koordinatları", "Lokasiya", "Xəritə Linki", "Status", "Qayda Pozuntuları"
    ]
    
    with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        
        for row in report_data:
            # Extract data from row dict
            date = row.get('date', '')
            fin = row.get('fin', '')
            name = row.get('name', '')
            name_parts = name.strip().split(maxsplit=1)
            ad = name_parts[0] if name_parts else name
            soyad = name_parts[1] if len(name_parts) > 1 else ""
            seriya = row.get('seriya', '') or ''
            phone = row.get('phone_number', '') or ''
            code = row.get('code', '')
            profession = row.get('profession', '')
            giris_time = row.get('giris_time') or ''
            cixis_time = row.get('cixis_time') or ''

            # Prefer precomputed fields from caller (period export computes these)
            gps_coords = row.get('gps_coords') or ''
            address = row.get('address') or ''
            maps_link = row.get('maps_link') or ''

            # Fallback: compute from raw GPS/loc fields if not provided
            if not gps_coords:
                start_lat = row.get('start_lat')
                start_lon = row.get('start_lon')
                end_lat = row.get('end_lat')
                end_lon = row.get('end_lon')
                if start_lat is not None and start_lon is not None:
                    gps_coords = f"{start_lat}, {start_lon}"
                    maps_link = maps_link or f"https://maps.google.com/?q={start_lat},{start_lon}"
                elif end_lat is not None and end_lon is not None:
                    gps_coords = f"{end_lat}, {end_lon}"
                    maps_link = maps_link or f"https://maps.google.com/?q={end_lat},{end_lon}"

            if not address:
                giris_loc = (row.get('giris_loc', '') or '').strip()
                cixis_loc = (row.get('cixis_loc', '') or '').strip()
                address = giris_loc or cixis_loc or ''

            # Status and violations (pre-filled by caller)
            status = row.get('status', '')
            violations = row.get('violations', '')
            
            writer.writerow([
                date, fin, ad, soyad, seriya, phone, code, profession,
                giris_time, cixis_time, gps_coords, address, maps_link, status, violations
            ])
    
    return filepath

