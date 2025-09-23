#!/usr/bin/env python3
"""
SGW Essen Wasserball Kalender Generator
Einfaches Tool zum Verwalten von Terminen und Generieren von ICS-Kalendern
"""

import sqlite3
import hashlib
import argparse
import re
import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from typing import List, Dict

class SGWTermineScraper:
    def __init__(self, db_path: str = "sgw_termine.db"):
        self.db_path = db_path
        # URL für die neue Saison (wird aktiviert sobald verfügbar)
        self.base_url = "https://dsvdaten.dsv.de/Modules/WB/League.aspx?Season=2025&LeagueID=197&Group=&LeagueKind=L"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.init_database()
    
    def init_database(self):
        """Initialisiert die SQLite-Datenbank"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS games (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT UNIQUE NOT NULL,
                home TEXT,
                guest TEXT,
                date TEXT NOT NULL,
                time TEXT,
                location TEXT,
                result TEXT,
                last_change TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_event_id ON games(event_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON games(date)')
        
        # Füge location-Spalte hinzu falls sie nicht existiert (Migration)
        try:
            cursor.execute('ALTER TABLE games ADD COLUMN location TEXT')
            print("✅ Location-Spalte zur Datenbank hinzugefügt")
        except sqlite3.OperationalError:
            # Spalte existiert bereits
            pass
        
        conn.commit()
        conn.close()
    
    def generate_event_id(self, home: str, guest: str) -> str:
        """Generiert eindeutige Event-ID basierend auf Teams"""
        content = f"{home}_vs_{guest}".strip()
        return hashlib.md5(content.encode('utf-8')).hexdigest()
    
    def scrape_termine(self, enable_scraping=False) -> List[Dict]:
        """Scraping von DSV-Website (aktivierbar sobald neue Saison online)"""
        if not enable_scraping:
            print("ℹ️  Scraping deaktiviert - Website für neue Saison noch nicht verfügbar")
            print("💡 Verwenden Sie --enable-scraping um zu testen oder --add für manuelle Eingabe")
            return []
        
        print(f"🔍 Versuche Scraping von: {self.base_url}")
        
        try:
            response = self.session.get(self.base_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            termine = []
            tables = soup.find_all('table')
            print(f"📋 {len(tables)} Tabellen gefunden")
            
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 8:  # Mindestens 8 Spalten für vollständige Spielinfos
                        row_text = ' '.join([cell.get_text(strip=True) for cell in cells])
                        
                        # Prüfe ob SG Wasserball Essen in der Zeile vorkommt
                        if 'SG Wasserball Essen' in row_text or 'SGW Essen' in row_text:
                            termin = self._parse_game_row(cells, row_text)
                            if termin:
                                termine.append(termin)
            
            print(f"🎯 {len(termine)} SGW Essen Spiele gefunden")
            return termine
            
        except requests.RequestException as e:
            print(f"❌ Netzwerk-Fehler: {e}")
            print("💡 Website möglicherweise noch nicht verfügbar für neue Saison")
            return []
        except Exception as e:
            print(f"❌ Scraping-Fehler: {e}")
            return []
    
    def _parse_game_row(self, cells: List, row_text: str) -> Dict:
        """Parst eine Tabellenzeile mit Spielansetzungen"""
        try:
            # Bewährte Parsing-Logik von der alten Website
            # Struktur: [Spielnummer, Datum/leer, leer, Heim, leer, Gast, Ort, Ergebnis, Viertel/leer]
            
            if len(cells) < 8:
                return None
            
            # Extrahiere Spielnummer
            game_number = cells[0].get_text(strip=True)
            if not game_number.isdigit():
                return None
            
            # Extrahiere Datum und Uhrzeit
            date_cell = cells[1].get_text(strip=True)
            date = ""
            time = ""
            
            if date_cell:
                # Suche nach Datum im Format DD.MM.YY, HH:MM Uhr
                date_match = re.search(r'(\d{1,2}\.\d{1,2}\.\d{2,4})', date_cell)
                time_match = re.search(r'(\d{1,2}:\d{2})', date_cell)
                
                date = date_match.group(1) if date_match else ""
                time = time_match.group(1) if time_match else ""
                
                # Konvertiere 2-stelliges Jahr zu 4-stellig
                if date and len(date.split('.')[-1]) == 2:
                    year = int(date.split('.')[-1])
                    if year < 50:
                        year += 2000
                    else:
                        year += 1900
                    date = date[:-2] + str(year)
            
            # Extrahiere Mannschaften
            home_raw = cells[3].get_text(strip=True)
            guest_raw = cells[5].get_text(strip=True)
            
            home = self._clean_team_name(home_raw)
            guest = self._clean_team_name(guest_raw)
            
            # Prüfe ob SG Wasserball Essen dabei ist
            if 'SG Wasserball Essen' not in home and 'SG Wasserball Essen' not in guest:
                return None
            
            # Bestimme wer Heim und wer Gast ist
            if 'SG Wasserball Essen' in home:
                final_home = home
                final_guest = guest
            else:
                final_home = guest
                final_guest = home
            
            # Extrahiere Ergebnis
            result_cell = cells[7]
            result_link = result_cell.find('a')
            result = ""
            if result_link:
                result = result_link.get_text(strip=True)
            else:
                result = result_cell.get_text(strip=True)
            
            return {
                'date': date,
                'time': time,
                'home': final_home,
                'guest': final_guest,
                'result': result
            }
            
        except Exception as e:
            print(f"⚠️  Fehler beim Parsen der Zeile: {e}")
            return None
    
    def _clean_team_name(self, team_name: str) -> str:
        """Bereinigt Team-Namen"""
        # Entferne Zahlen und Sonderzeichen am Anfang
        cleaned = re.sub(r'^\d+[^\w]*', '', team_name)
        cleaned = cleaned.strip()
        
        if not cleaned or cleaned.isdigit():
            return ""
            
        # Entferne mehrfache Leerzeichen
        cleaned = re.sub(r'\s+', ' ', cleaned)
        return cleaned
    
    def save_termine(self, termine: List[Dict]) -> int:
        """Speichert Termine in der Datenbank"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        updated_count = 0
        
        for termin in termine:
            event_id = self.generate_event_id(
                termin.get('home', ''),
                termin.get('guest', '')
            )
            
            # Prüfe ob Event bereits existiert
            cursor.execute('SELECT id FROM games WHERE event_id = ?', (event_id,))
            existing = cursor.fetchone()
            
            if existing:
                # Aktualisiere bestehenden Eintrag
                cursor.execute('''
                    UPDATE games 
                    SET home = ?, guest = ?, date = ?, time = ?, location = ?, result = ?, 
                        last_change = CURRENT_TIMESTAMP
                    WHERE event_id = ?
                ''', (
                    termin.get('home', ''),
                    termin.get('guest', ''),
                    termin.get('date', ''),
                    termin.get('time', ''),
                    termin.get('location', ''),
                    termin.get('result', ''),
                    event_id
                ))
                print(f"🔄 Aktualisiert: {termin.get('home', '')} vs {termin.get('guest', '')}")
            else:
                # Füge neuen Eintrag hinzu
                cursor.execute('''
                    INSERT INTO games 
                    (event_id, home, guest, date, time, location, result)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    event_id,
                    termin.get('home', ''),
                    termin.get('guest', ''),
                    termin.get('date', ''),
                    termin.get('time', ''),
                    termin.get('location', ''),
                    termin.get('result', '')
                ))
                updated_count += 1
        
        conn.commit()
        conn.close()
        return updated_count
    
    def generate_ics(self, output_file: str = "sgw_termine.ics") -> str:
        """Generiert ICS-Kalenderdatei"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, event_id, home, guest, date, time, location, result
            FROM games 
            ORDER BY date, time
        ''')
        
        termine = cursor.fetchall()
        conn.close()
        
        ics_content = self._create_ics_content(termine)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(ics_content)
        
        return output_file
    
    def _create_ics_content(self, termine: List) -> str:
        """Erstellt ICS-Inhalt"""
        now = datetime.now()
        
        ics_lines = [
            "BEGIN:VCALENDAR",
            "VERSION:2.0",
            "PRODID:-//SGW Essen//Termine Scraper//DE",
            "CALSCALE:GREGORIAN",
            "METHOD:PUBLISH",
            "X-WR-CALNAME:SGW Essen Wasserball Termine",
            "X-WR-CALDESC:Automatisch generierte Termine für SGW Essen",
            "X-WR-TIMEZONE:Europe/Berlin"
        ]
        
        for termin in termine:
            (id, event_id, home, guest, date, time, location, result) = termin
            
            uid = f"sgw-{event_id}@essen.de"
            title = f"{home} vs {guest}"
            
            # Parse Datum
            try:
                if '.' in date:
                    dt = datetime.strptime(date, '%d.%m.%Y')
                else:
                    dt = datetime.strptime(date, '%Y-%m-%d')
            except:
                continue
            
            # Parse Zeit
            start_time = dt
            if time and ':' in time:
                try:
                    time_parts = time.split(':')
                    start_time = dt.replace(
                        hour=int(time_parts[0]),
                        minute=int(time_parts[1])
                    )
                except:
                    pass
            
            end_time = start_time + timedelta(hours=2)
            
            # ICS Format
            dtstart = start_time.strftime('%Y%m%dT%H%M%S')
            dtend = end_time.strftime('%Y%m%dT%H%M%S')
            dtstamp = now.strftime('%Y%m%dT%H%M%SZ')
            
            # Beschreibung
            description = f"Result: {result}" if result else ""
            
            # Location
            location_text = location if location and location.strip() else "TBA"
            
            # Event
            ics_lines.extend([
                "BEGIN:VEVENT",
                f"UID:{uid}",
                f"DTSTAMP:{dtstamp}",
                f"DTSTART:{dtstart}",
                f"DTEND:{dtend}",
                f"SUMMARY:{title}",
                f"DESCRIPTION:{description}",
                f"LOCATION:{location_text}",
                "STATUS:CONFIRMED",
                "TRANSP:OPAQUE",
                "END:VEVENT"
            ])
        
        ics_lines.append("END:VCALENDAR")
        return "\n".join(ics_lines)
    
    def list_termine(self, limit: int = 10):
        """Zeigt Termine aus der Datenbank"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, date, time, home, guest, location, result, last_change
            FROM games 
            ORDER BY date DESC, time DESC
            LIMIT ?
        ''', (limit,))
        
        termine = cursor.fetchall()
        conn.close()
        
        if not termine:
            print("Keine Termine in der Datenbank gefunden.")
            print("💡 Verwenden Sie --add um Termine hinzuzufügen")
            return
        
        print(f"\n=== {len(termine)} Termine ===")
        print("-" * 80)
        for termin in termine:
            (id, date, time, home, guest, location, result, last_change) = termin
            time_str = f" {time}" if time else ""
            location_str = f" @ {location}" if location and location.strip() else ""
            result_str = f" | {result}" if result else ""
            
            print(f"ID {id:3d} | {date}{time_str}{location_str}")
            print(f"      | {home} vs {guest}{result_str}")
            print(f"      | {last_change}")
            print("-" * 80)
    
    def add_manual_termine(self) -> List[Dict]:
        """Interaktive Eingabe neuer Termine"""
        print("\n=== Manuelle Termineingabe ===")
        print("Geben Sie die Termindetails ein (Enter für leere Felder):")
        
        termine = []
        while True:
            print(f"\n--- Termin {len(termine) + 1} ---")
            
            date = input("Datum (DD.MM.YYYY): ").strip()
            if not date:
                break
            
            time = input("Uhrzeit (HH:MM) [optional]: ").strip()
            home = input("Heimmannschaft: ").strip()
            if not home:
                print("Heimmannschaft ist erforderlich!")
                continue
            guest = input("Gastmannschaft: ").strip()
            if not guest:
                print("Gastmannschaft ist erforderlich!")
                continue
            location = input("Ort [optional]: ").strip()
            result = input("Ergebnis [optional]: ").strip()
            
            termin = {
                'date': date,
                'time': time,
                'home': home,
                'guest': guest,
                'location': location,
                'result': result
            }
            
            termine.append(termin)
            
            weiter = input("\nWeiteren Termin hinzufügen? (j/n): ").strip().lower()
            if weiter not in ['j', 'ja', 'y', 'yes']:
                break
        
        return termine
    
    def run(self, scrape=True, add_new=False, enable_scraping=False):
        """Hauptausführung"""
        print("🏊‍♂️ SGW Essen Termine Scraper gestartet...")
        
        alle_termine = []
        
        # Scraping (mit Feature-Flag)
        if scrape:
            print("Extrahiere Termine von DSV-Seite...")
            termine = self.scrape_termine(enable_scraping=enable_scraping)
            alle_termine.extend(termine)
        
        # Manuelle Eingabe
        if add_new:
            manual_termine = self.add_manual_termine()
            alle_termine.extend(manual_termine)
        
        # Speichere in Datenbank
        if alle_termine:
            print("Speichere Termine in Datenbank...")
            count = self.save_termine(alle_termine)
            print(f"Neue/aktualisierte Termine: {count}")
        
        # Generiere ICS
        print("Generiere ICS-Kalender...")
        ics_file = self.generate_ics()
        print(f"ICS-Datei erstellt: {ics_file}")
        
        if not alle_termine and not scrape and not add_new:
            print("\n💡 Verwenden Sie:")
            print("  --add DATUM ZEIT HEIM GAST ERGEBNIS  # Termin direkt hinzufügen")
            print("  -new                                  # Interaktive Eingabe")
            print("  --list                                # Termine anzeigen")

def main():
    parser = argparse.ArgumentParser(
        description='SGW Essen Wasserball Kalender Generator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Beispiele:
  python sgw_essen_scraper.py --add "20.12.2025" "15:00" "SGW Essen" "Weihnachtsfeier" ""
  python sgw_essen_scraper.py -new
  python sgw_essen_scraper.py --list
        """
    )
    
    parser.add_argument('-new', '--add-new', action='store_true',
                       help='Interaktive Eingabe neuer Termine')
    parser.add_argument('--add', nargs=6, metavar=('DATE', 'TIME', 'HOME', 'GUEST', 'LOCATION', 'RESULT'),
                       help='Termin direkt hinzufügen')
    parser.add_argument('--enable-scraping', action='store_true',
                       help='Aktiviert Web-Scraping (für Tests oder wenn neue Website online)')
    parser.add_argument('--db', default='sgw_termine.db',
                       help='Pfad zur SQLite-Datenbank')
    parser.add_argument('--ics', default='sgw_termine.ics',
                       help='Ausgabedatei für ICS-Kalender')
    parser.add_argument('--list', action='store_true',
                       help='Zeigt Termine aus der Datenbank')
    parser.add_argument('--limit', type=int, default=10,
                       help='Anzahl der anzuzeigenden Termine')
    
    args = parser.parse_args()
    
    scraper = SGWTermineScraper(db_path=args.db)
    
    # Liste anzeigen
    if args.list:
        scraper.list_termine(limit=args.limit)
        return
    
    # Direkter Termin
    if args.add:
        date, time, home, guest, location, result = args.add
        termin = {
            'date': date,
            'time': time,
            'home': home,
            'guest': guest,
            'location': location,
            'result': result
        }
        count = scraper.save_termine([termin])
        print(f"✅ Termin hinzugefügt: {home} vs {guest} am {date} {time}")
        print(f"Neue/aktualisierte Termine: {count}")
        
        ics_file = scraper.generate_ics(args.ics)
        print(f"ICS-Datei erstellt: {ics_file}")
        return
    
    # Standard oder manuelle Eingabe
    scraper.run(scrape=args.enable_scraping, add_new=args.add_new, enable_scraping=args.enable_scraping)

if __name__ == "__main__":
    main()