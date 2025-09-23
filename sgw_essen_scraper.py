#!/usr/bin/env python3
"""
SGW Essen Termine Scraper
Extrahiert Termine von der DSV-Seite und speichert sie in SQLite mit ICS-Export
Unterstützt auch manuelle Eingabe neuer Termine über -new Flag
"""

import requests
from bs4 import BeautifulSoup
import sqlite3
import hashlib
import re
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import os

class SGWTermineScraper:
    def __init__(self, db_path: str = "sgw_termine.db"):
        self.db_path = db_path
        # Verwende die echte DSV-URL
        self.base_url = "https://dsvdaten.dsv.de/Modules/WB/League.aspx?Season=2024&LeagueID=197&Group=&LeagueKind=L"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.init_database()
    
    def init_database(self):
        """Initialisiert die SQLite-Datenbank mit dem erforderlichen Schema"""
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
                result TEXT,
                last_change TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Index für bessere Performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_event_id ON games(event_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON games(date)')
        
        conn.commit()
        conn.close()
    
    def generate_event_id(self, home: str, guest: str) -> str:
        """Generiert eine eindeutige Event-ID basierend nur auf den Teams (für Updates)"""
        content = f"{home}_vs_{guest}".strip()
        return hashlib.md5(content.encode('utf-8')).hexdigest()
    
    def scrape_termine(self) -> List[Dict]:
        """Extrahiert Termine von der DSV-Seite"""
        try:
            if self.base_url.startswith('file://'):
                # Lese lokale HTML-Datei
                file_path = self.base_url.replace('file://', '')
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                soup = BeautifulSoup(content, 'html.parser')
            else:
                # Lade von URL
                response = self.session.get(self.base_url)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
            
            termine = []
            
            # Suche nach Tabellen mit Spielansetzungen
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    
                    row_text = ' '.join([cell.get_text(strip=True) for cell in cells])
                    
                    # Prüfe ob SG Wasserball Essen in der Zeile vorkommt
                    if 'SG Wasserball Essen' in row_text or 'SGW Essen' in row_text or 'Essen' in row_text:
                        termin = self._parse_game_row(cells, row_text)
                        if termin:
                            termine.append(termin)
            
            return termine
            
        except Exception as e:
            print(f"Fehler beim Scraping: {e}")
            return []
    
    def _parse_game_row(self, cells: List, row_text: str) -> Dict:
        """Parst eine Tabellenzeile mit Spielansetzungen"""
        try:
            # Basierend auf der echten HTML-Struktur:
            # Spalte 0: Spielnummer (z.B. "87")
            # Spalte 1: Datum (kann leer sein)
            # Spalte 2: Heimmannschaft (z.B. "SV Lünen 08 II")
            # Spalte 3: Gastmannschaft (z.B. "SG Wasserball Essen II")
            # Spalte 4: Ort (z.B. "Lünen")
            # Spalte 5: Ergebnis (z.B. "10:0")
            # Spalte 6: Viertelergebnisse (kann leer sein)
            
            if len(cells) < 8:
                return None
            
            # Extrahiere Spielnummer
            game_number = cells[0].get_text(strip=True)
            
            # Prüfe ob es eine gültige Spielnummer ist (nur Zahlen)
            if not game_number.isdigit():
                return None
            
            # Extrahiere Datum und Uhrzeit (falls vorhanden)
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
                    if year < 50:  # Annahme: 00-49 = 2000-2049, 50-99 = 1950-1999
                        year += 2000
                    else:
                        year += 1900
                    date = date[:-2] + str(year)
            
            # Extrahiere Mannschaften - basierend auf der echten HTML-Struktur
            # Die Struktur ist: [Spielnummer, Datum/leer, leer, Heim, leer, Gast, Ort, Ergebnis, Viertel/leer]
            home_raw = cells[3].get_text(strip=True)  # Heim steht in Spalte 3
            guest_raw = cells[5].get_text(strip=True)  # Gast steht in Spalte 5
            
            home = self._clean_team_name(home_raw)
            guest = self._clean_team_name(guest_raw)
            
            # Prüfe ob SG Wasserball Essen dabei ist
            if 'SG Wasserball Essen' not in home and 'SG Wasserball Essen' not in guest:
                return None  # Kein SG Wasserball Essen Spiel
            
            # Bestimme wer Heim und wer Gast ist basierend auf SG Wasserball Essen
            if 'SG Wasserball Essen' in home:
                # SG Wasserball Essen ist Heim
                final_home = home
                final_guest = guest
            else:
                # SG Wasserball Essen ist Gast - tausche um
                final_home = guest
                final_guest = home
            
            # Extrahiere Ergebnis aus Spalte 7
            result_cell = cells[7]
            result_link = result_cell.find('a')
            result = ""
            if result_link:
                result = result_link.get_text(strip=True)
            else:
                # Falls kein Link, nimm den Text der Zelle
                result = result_cell.get_text(strip=True)
            
            return {
                'date': date,
                'time': time,
                'home': final_home,
                'guest': final_guest,
                'result': result
            }
            
        except Exception as e:
            print(f"Fehler beim Parsen der Zeile: {e}")
            return None
    
    
    def _clean_team_name(self, team_name: str) -> str:
        """Bereinigt Team-Namen von Bildern und anderen Elementen"""
        # Entferne Zahlen und Sonderzeichen am Anfang
        cleaned = re.sub(r'^\d+[^\w]*', '', team_name)
        
        # Entferne Leerzeichen am Anfang und Ende
        cleaned = cleaned.strip()
        
        # Entferne leere Strings oder nur Zahlen
        if not cleaned or cleaned.isdigit():
            return ""
        
        # Entferne zusätzliche Leerzeichen und Sonderzeichen
        cleaned = re.sub(r'\s+', ' ', cleaned)  # Mehrfache Leerzeichen zu einem
        
        return cleaned
    
    
    def is_date_like(self, text: str) -> bool:
        """Prüft ob ein Text wie ein Datum aussieht"""
        date_patterns = [
            r'\d{1,2}\.\d{1,2}\.\d{4}',  # DD.MM.YYYY
            r'\d{1,2}\.\d{1,2}',         # DD.MM
            r'\d{4}-\d{2}-\d{2}',        # YYYY-MM-DD
        ]
        return any(re.search(pattern, text) for pattern in date_patterns)
    
    def is_time_like(self, text: str) -> bool:
        """Prüft ob ein Text wie eine Zeit aussieht"""
        time_patterns = [
            r'\d{1,2}:\d{2}',  # HH:MM
            r'\d{1,2}:\d{2}:\d{2}',  # HH:MM:SS
        ]
        return any(re.search(pattern, text) for pattern in time_patterns)
    
    def save_termine(self, termine: List[Dict]) -> int:
        """Speichert Termine in der Datenbank und gibt Anzahl der neuen/aktualisierten Einträge zurück"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        updated_count = 0
        
        for termin in termine:
            # Generiere Event-ID basierend nur auf den Teams
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
                    SET home = ?, guest = ?, date = ?, time = ?, result = ?, 
                        last_change = CURRENT_TIMESTAMP
                    WHERE event_id = ?
                ''', (
                    termin.get('home', ''),
                    termin.get('guest', ''),
                    termin.get('date', ''),
                    termin.get('time', ''),
                    termin.get('result', ''),
                    event_id
                ))
            else:
                # Füge neuen Eintrag hinzu
                cursor.execute('''
                    INSERT INTO games 
                    (event_id, home, guest, date, time, result)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    event_id,
                    termin.get('home', ''),
                    termin.get('guest', ''),
                    termin.get('date', ''),
                    termin.get('time', ''),
                    termin.get('result', '')
                ))
                updated_count += 1
        
        conn.commit()
        conn.close()
        return updated_count
    
    def generate_ics(self, output_file: str = "sgw_termine.ics") -> str:
        """Generiert ICS-Kalenderdatei aus der Datenbank"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, event_id, home, guest, date, time, result
            FROM games 
            ORDER BY date, time
        ''')
        
        termine = cursor.fetchall()
        conn.close()
        
        ics_content = self._create_ics_content(termine)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(ics_content)
        
        # Versuche GitHub Upload falls konfiguriert
        self._upload_to_github(output_file)
        
        return output_file
    
    def _upload_to_github(self, ics_file: str):
        """Lädt ICS-Datei zu GitHub Gist hoch"""
        try:
            from gist_uploader import GistUploader
            
            uploader = GistUploader()
            calendar_url = uploader.upload_calendar(ics_file)
            
            if calendar_url:
                print("✅ Kalender automatisch zu GitHub Gist hochgeladen!")
                print(f"📱 Android Kalender URL: {calendar_url}")
                return calendar_url
            
        except ImportError:
            print("Gist Uploader nicht verfügbar")
        except Exception as e:
            print(f"Gist Upload fehlgeschlagen: {e}")
        
        print("\n💡 Für automatischen Upload:")
        print("python gist_uploader.py")
    
    def _create_ics_content(self, termine: List) -> str:
        """Erstellt den ICS-Inhalt"""
        now = datetime.now()
        
        ics_lines = [
            "BEGIN:VCALENDAR",
            "VERSION:2.0",
            "PRODID:-//SGW Essen//Termine Scraper//DE",
            "CALSCALE:GREGORIAN",
            "METHOD:PUBLISH",
            f"X-WR-CALNAME:SGW Essen Wasserball Termine",
            f"X-WR-CALDESC:Automatisch generierte Termine für SGW Essen",
            f"X-WR-TIMEZONE:Europe/Berlin"
        ]
        
        for termin in termine:
            (id, event_id, home, guest, date, time, result) = termin
            
            # Erstelle UID basierend auf event_id
            uid = f"sgw-{event_id}@essen.de"
            
            # Erstelle Titel
            title = f"{home} vs {guest}"
            
            # Parse Datum
            try:
                if '.' in date:
                    if len(date.split('.')) == 3:
                        dt = datetime.strptime(date, '%d.%m.%Y')
                    else:
                        dt = datetime.strptime(f"{date}.{now.year}", '%d.%m.%Y')
                else:
                    dt = datetime.strptime(date, '%Y-%m-%d')
            except:
                continue
            
            # Parse Zeit falls vorhanden
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
            
            # Ende-Zeit (Standard: 2 Stunden später)
            end_time = start_time + timedelta(hours=2)
            
            # Format für ICS (mit Zeitzone)
            dtstart = start_time.strftime('%Y%m%dT%H%M%S')
            dtend = end_time.strftime('%Y%m%dT%H%M%S')
            dtstamp = now.strftime('%Y%m%dT%H%M%SZ')
            
            # Erstelle Beschreibung
            desc_parts = []
            if result:
                desc_parts.append(f"Result: {result}")
            
            description_text = "\\n".join(desc_parts) if desc_parts else ""
            
            # Event hinzufügen mit Sequence für Updates
            ics_lines.extend([
                "BEGIN:VEVENT",
                f"UID:{uid}",
                f"DTSTAMP:{dtstamp}",
                f"DTSTART:{dtstart}",
                f"DTEND:{dtend}",
                f"SUMMARY:{title}",
                f"DESCRIPTION:{description_text}",
                f"LOCATION:TBA",
                "STATUS:CONFIRMED",
                "TRANSP:OPAQUE",
                f"SEQUENCE:1",
                f"LAST-MODIFIED:{dtstamp}",
                "END:VEVENT"
            ])
        
        ics_lines.append("END:VCALENDAR")
        
        return "\n".join(ics_lines)
    
    def list_termine(self, limit: int = 10):
        """Zeigt vorhandene Termine aus der Datenbank an"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, date, time, home, guest, result, last_change
            FROM games 
            ORDER BY date DESC, time DESC
            LIMIT ?
        ''', (limit,))
        
        termine = cursor.fetchall()
        conn.close()
        
        if not termine:
            print("Keine Termine in der Datenbank gefunden.")
            return
        
        print(f"\n=== Letzte {len(termine)} Games ===")
        print("-" * 100)
        for termin in termine:
            (id, date, time, home, guest, result, last_change) = termin
            time_str = f" {time}" if time else ""
            result_str = f" | {result}" if result else ""
            
            print(f"ID {id:3d} | {date}{time_str}")
            print(f"      | {home} vs {guest}{result_str}")
            print(f"      | Last Change: {last_change}")
            print("-" * 100)
    
    def add_manual_termine(self) -> List[Dict]:
        """Interaktive Eingabe neuer Termine"""
        print("\n=== Manuelle Termineingabe ===")
        print("Geben Sie die Termindetails ein (Enter für leere Felder):")
        
        termine = []
        while True:
            print(f"\n--- Termin {len(termine) + 1} ---")
            
            date = input("Date (DD.MM.YYYY or DD.MM): ").strip()
            if not date:
                break
            
            time = input("Time (HH:MM) [optional]: ").strip()
            home = input("Home team: ").strip()
            if not home:
                print("Home team is required!")
                continue
            guest = input("Guest team: ").strip()
            if not guest:
                print("Guest team is required!")
                continue
            result = input("Result [optional]: ").strip()
            
            termin = {
                'date': date,
                'time': time,
                'home': home,
                'guest': guest,
                'result': result
            }
            
            termine.append(termin)
            
            # Frage ob weiterer Termin
            weiter = input("\nWeiteren Termin hinzufügen? (j/n): ").strip().lower()
            if weiter not in ['j', 'ja', 'y', 'yes']:
                break
        
        return termine
    
    def run(self, scrape: bool = True, add_new: bool = False):
        """Hauptfunktion zum Ausführen des Scrapers"""
        print("SGW Essen Termine Scraper gestartet...")
        
        all_termine = []
        
        # Scrape Termine von Website
        if scrape:
            print("Extrahiere Termine von DSV-Seite...")
            scraped_termine = self.scrape_termine()
            print(f"Gefunden: {len(scraped_termine)} Termine")
            all_termine.extend(scraped_termine)
        
        # Manuelle Termineingabe
        if add_new:
            manual_termine = self.add_manual_termine()
            print(f"Manuell eingegeben: {len(manual_termine)} Termine")
            all_termine.extend(manual_termine)
        
        if all_termine:
            # Speichere in Datenbank
            print("Speichere Termine in Datenbank...")
            updated_count = self.save_termine(all_termine)
            print(f"Neue/aktualisierte Termine: {updated_count}")
            
            # Generiere ICS
            print("Generiere ICS-Kalender...")
            ics_file = self.generate_ics()
            print(f"ICS-Datei erstellt: {ics_file}")
        else:
            print("Keine Termine gefunden oder eingegeben.")

def main():
    parser = argparse.ArgumentParser(
        description='SGW Essen Termine Scraper - Extrahiert Termine von DSV-Seite und verwaltet sie in SQLite',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Beispiele:
  python sgw_essen_scraper.py                    # Normales Scraping von DSV-Seite
  python sgw_essen_scraper.py -new               # Nur manuelle Termineingabe
  python sgw_essen_scraper.py -new --no-scrape   # Manuelle Eingabe ohne Scraping
  python sgw_essen_scraper.py --scrape-only      # Nur Scraping ohne manuelle Eingabe
        """
    )
    
    parser.add_argument('-new', '--add-new', action='store_true',
                       help='Interaktive Eingabe neuer Termine')
    parser.add_argument('--no-scrape', action='store_true',
                       help='Überspringe das Scraping von der DSV-Seite')
    parser.add_argument('--scrape-only', action='store_true',
                       help='Nur Scraping, keine manuelle Eingabe')
    parser.add_argument('--db', default='sgw_termine.db',
                       help='Pfad zur SQLite-Datenbank (Standard: sgw_termine.db)')
    parser.add_argument('--ics', default='sgw_termine.ics',
                       help='Ausgabedatei für ICS-Kalender (Standard: sgw_termine.ics)')
    parser.add_argument('--list', action='store_true',
                       help='Zeigt vorhandene Termine aus der Datenbank an')
    parser.add_argument('--limit', type=int, default=10,
                       help='Anzahl der anzuzeigenden Termine (Standard: 10)')
    
    args = parser.parse_args()
    
    scraper = SGWTermineScraper(db_path=args.db)
    
    # Wenn --list gesetzt, zeige nur Termine an
    if args.list:
        scraper.list_termine(limit=args.limit)
        return
    
    # Logik für Scraping vs. manuelle Eingabe
    scrape = not args.no_scrape
    add_new = args.add_new
    
    # Wenn nur --scrape-only gesetzt, dann nur Scraping
    if args.scrape_only:
        scrape = True
        add_new = False
    
    # Wenn weder Scraping noch manuelle Eingabe, dann Standard (nur Scraping)
    if not scrape and not add_new:
        scrape = True
    
    scraper.run(scrape=scrape, add_new=add_new)

if __name__ == "__main__":
    main()
