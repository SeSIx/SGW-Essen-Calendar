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
import sys
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import urllib.parse

class SGWTermineScraper:
    def __init__(self, db_path: str = "sgw_termine.db"):
        self.db_path = db_path
        # URL für die aktuelle Saison
        self.base_url = "https://dsvdaten.dsv.de/Modules/WB/League.aspx"
        
        # Parameter für verschiedene Wettbewerbe
        self.competitions = {
            'pokal': {
                'name': 'Bezirkspokal',
                'params': {
                    'Season': '2024',
                    'LeagueID': '250',
                    'Group': '',
                    'LeagueKind': 'C'
                }
            },
            'nrw_pokal': {
                'name': 'NRW Pokal',
                'params': {
                    'Season': '2025',
                    'LeagueID': '132',
                    'Group': '',
                    'LeagueKind': 'C'
                }
            },
            'verbandsliga': {
                'name': 'Verbandsliga',
                'params': {
                    'Season': '2025',
                    'LeagueID': '197',
                    'Group': 'B',
                    'LeagueKind': 'L'
                }
            },
            'ruhrgebietsliga': {
                'name': 'Ruhrgebietsliga',
                'params': {
                    'Season': '2025',
                    'LeagueID': '212',
                    'Group': '',
                    'LeagueKind': 'L'
                }
            }
        }
        self.session = requests.Session()
        self.session.headers.update({
            'Host': 'dsvdaten.dsv.de',
            'Sec-Ch-Ua': '"Not?A_Brand";v="99", "Chromium";v="130"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Accept-Language': 'de-DE,de;q=0.9',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.6723.70 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-User': '?1',
            'Sec-Fetch-Dest': 'document',
            'Referer': 'https://dsvdaten.dsv.de/Modules/WB/Index.aspx',
            'Accept-Encoding': 'gzip, deflate, br',
            'Priority': 'u=0, i'
        })
        # URL für einzelne Spiele
        self.game_detail_url = "https://dsvdaten.dsv.de/Modules/WB/Game.aspx"
        self.init_database()
    
    def init_database(self):
        """Initialisiert die SQLite-Datenbank"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Games table (Spiele mit Heim/Gast)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS games (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT UNIQUE NOT NULL,
                home TEXT,
                guest TEXT,
                date TEXT NOT NULL,
                time TEXT,
                location TEXT,
                description TEXT,
                dsv_game_id TEXT,
                competition_type TEXT,
                last_change TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Migration: Add dsv_game_id column if not exists
        try:
            cursor.execute('ALTER TABLE games ADD COLUMN dsv_game_id TEXT')
        except sqlite3.OperationalError:
            pass
        try:
            cursor.execute('ALTER TABLE games ADD COLUMN competition_type TEXT')
        except sqlite3.OperationalError:
            pass
        
        # Events table (Termine ohne Heim/Gast - z.B. Weihnachtsmarkt)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT UNIQUE NOT NULL,
                title TEXT NOT NULL,
                date TEXT NOT NULL,
                time TEXT,
                location TEXT,
                description TEXT,
                last_change TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_event_id ON games(event_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON games(date)')
        
        # Indexes for events table
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_events_event_id ON events(event_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_events_date ON events(date)')
        
        # Game statistics table (Überzahl/Unterzahl pro Spiel)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS game_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id INTEGER NOT NULL,
                team TEXT NOT NULL,
                goals INTEGER DEFAULT 0,
                power_play_goals INTEGER DEFAULT 0,
                power_play_attempts INTEGER DEFAULT 0,
                penalty_kill_success INTEGER DEFAULT 0,
                penalty_kill_attempts INTEGER DEFAULT 0,
                exclusions INTEGER DEFAULT 0,
                FOREIGN KEY (game_id) REFERENCES games(id),
                UNIQUE(game_id, team)
            )
        ''')
        
        # Player statistics table (Spieler-Stats pro Spiel)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS player_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id INTEGER NOT NULL,
                player_name TEXT NOT NULL,
                team TEXT NOT NULL,
                goals INTEGER DEFAULT 0,
                exclusions INTEGER DEFAULT 0,
                yellow_cards INTEGER DEFAULT 0,
                red_cards INTEGER DEFAULT 0,
                FOREIGN KEY (game_id) REFERENCES games(id)
            )
        ''')
        
        # Füge location-Spalte hinzu falls sie nicht existiert (Migration)
        try:
            cursor.execute('ALTER TABLE games ADD COLUMN location TEXT')
        except sqlite3.OperationalError:
            # Spalte existiert bereits
            pass
        
        # Füge description-Spalte hinzu und migriere result-Daten (Migration)
        try:
            cursor.execute('ALTER TABLE games ADD COLUMN description TEXT')
        except sqlite3.OperationalError:
            # Spalte existiert bereits
            pass
        
        
        # Migriere result zu description falls result-Spalte existiert
        try:
            cursor.execute("SELECT name FROM pragma_table_info('games') WHERE name='result'")
            if cursor.fetchone():
                # Migriere alle Daten von result zu description
                cursor.execute('UPDATE games SET description = result WHERE result IS NOT NULL AND result != ""')
                
                # SQLite unterstützt kein DROP COLUMN direkt, also erstelle neue Tabelle
                cursor.execute('''
                    CREATE TABLE games_new (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        event_id TEXT UNIQUE NOT NULL,
                        home TEXT,
                        guest TEXT,
                        date TEXT NOT NULL,
                        time TEXT,
                        location TEXT,
                        description TEXT,
                        last_change TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Kopiere Daten in neue Tabelle
                cursor.execute('''
                    INSERT INTO games_new (id, event_id, home, guest, date, time, location, description, last_change)
                    SELECT id, event_id, home, guest, date, time, location, description, last_change
                    FROM games
                ''')
                
                # Lösche alte Tabelle und benenne neue um
                cursor.execute('DROP TABLE games')
                cursor.execute('ALTER TABLE games_new RENAME TO games')
                
                # Erstelle Indizes neu
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_event_id ON games(event_id)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON games(date)')
        except sqlite3.OperationalError as e:
            pass
        
        conn.commit()
        conn.close()
    
    def generate_event_id(self, home: str, guest: str, competition: str = "") -> str:
        """Generiert eindeutige Event-ID basierend auf Teams und Wettbewerb (normalisiert)"""
        # Normalisiere Teamnamen für konsistente Event-IDs
        home_norm = self._normalize_team_name(home)
        guest_norm = self._normalize_team_name(guest)
        content = f"{competition}_{home_norm}_vs_{guest_norm}".strip()
        return hashlib.md5(content.encode('utf-8')).hexdigest()
    
    def _normalize_team_name(self, team_name: str) -> str:
        """Normalisiert Teamnamen für konsistente Event-IDs"""
        if not team_name:
            return ""
        
        # Entferne häufige Variationen
        normalized = team_name.strip()
        
        # Normalisiere SGW Essen Varianten
        if "SGW Essen" in normalized or "SG Wasserball Essen" in normalized:
            # Extrahiere Team-Nummer (I, II, III, etc.)
            if " III" in normalized:
                return "SGW Essen III"
            elif " II" in normalized:
                return "SGW Essen II"
            else:
                return "SGW Essen"
        
        # Normalisiere andere Teams (entferne Jahreszahlen und Zusätze)
        # SV Rheinhausen 1913 II -> SV Rheinhausen II
        normalized = re.sub(r'\s+\d{4}', '', normalized)  # Entferne Jahreszahlen
        normalized = re.sub(r'\s+', ' ', normalized).strip()  # Normalisiere Leerzeichen
        
        return normalized
    
    def _is_valid_game(self, game: Dict) -> bool:
        """Prüft ob ein Spiel gültige Daten hat (mindestens Datum)"""
        if not game:
            return False
        
        date = game.get('date', '').strip()
        
        # Spiel muss ein gültiges Datum haben
        if not date or date.lower() in ['unbekannt', 'unknown', '', '-']:
            return False
        
        # Prüfe ob Datum ein gültiges Format hat
        try:
            if '.' in date:
                # Format: DD.MM.YYYY
                parts = date.split('.')
                if len(parts) == 3:
                    day, month, year = parts
                    if int(day) > 0 and int(month) > 0 and int(year) > 2020:
                        return True
            return False
        except (ValueError, IndexError):
            return False
    
    def scrape_termine(self, enable_scraping=False) -> List[Dict]:
        """Scraping von DSV-Website für alle Wettbewerbe"""
        if not enable_scraping:
            print("Scraping disabled - use --enable-scraping to activate")
            return []
        
        all_termine = []
        
        print("Scraping DSV website...")
        # Scrape alle konfigurierten Wettbewerbe
        for comp_key, comp_info in self.competitions.items():
            comp_termine = self._scrape_competition(comp_info['params'], comp_key)
            all_termine.extend(comp_termine)
        
        return all_termine
    
    def _scrape_competition(self, params: Dict, competition_type: str) -> List[Dict]:
        """Scraping einer spezifischen Competition (Pokal oder Liga)"""
        try:
            response = self.session.get(self.base_url, params=params)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Finde alle Tabellenzeilen
            rows = soup.find_all('tr')
            
            termine = []
            current_round = ""
            
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) < 6:
                    continue
                
                # Prüfe auf Runden-Header
                if len(cells) == 1 and 'Runde' in cells[0].get_text():
                    current_round = cells[0].get_text(strip=True)
                    continue
                
                # Extrahiere Spiel-Daten
                row_text = ' '.join([cell.get_text(strip=True) for cell in cells])
                
                # Suche nach SGW Essen Spielen
                if 'SG Wasserball Essen' in row_text or 'Essen' in row_text:
                    # Filtere Tabellen-/Statistik-Zeilen aus
                    if ('Gesamttabelle' in row_text or 
                        'kein dir. Vergleich' in row_text or
                        'Pkt:' in row_text or
                        'TD:' in row_text or
                        'Tore:' in row_text):
                        continue
                    
                    game = self._parse_simple_game_row(cells, current_round, competition_type)
                    if game and self._is_valid_game(game):
                        termine.append(game)
            
            return termine
            
        except Exception as e:
            print(f"Error scraping {competition_type}: {e}")
            return []
    
    def _parse_simple_game_row(self, cells, current_round: str, competition_type: str = "cup") -> Dict:
        """Einfaches Parsing einer Spielzeile"""
        try:
            # Extrahiere Daten aus den Zellen
            game_id = cells[0].get_text(strip=True)
            date_time = cells[1].get_text(strip=True)
            home_team = cells[3].get_text(strip=True)
            guest_team = cells[5].get_text(strip=True)
            location = cells[6].get_text(strip=True)
            result = cells[7].get_text(strip=True)
            
            # Parse Datum und Zeit
            date_match = re.search(r'(\d{1,2}\.\d{1,2}\.\d{2,4})', date_time)
            time_match = re.search(r'(\d{1,2}:\d{2})', date_time)
            
            date = ""
            time = ""
            if date_match:
                date = date_match.group(1)
                # Konvertiere 2-stelliges Jahr
                if len(date.split('.')[-1]) == 2:
                    year = int(date.split('.')[-1])
                    year += 2000 if year < 50 else 1900
                    date = date[:-2] + str(year)
            
            if time_match:
                time = time_match.group(1)
            
            # Bestimme Home/Guest basierend auf Location
            if location and 'essen' in location.lower():
                # SG Wasserball Essen ist zu Hause
                final_home = home_team if 'SG Wasserball Essen' in home_team else guest_team
                final_guest = guest_team if 'SG Wasserball Essen' in home_team else home_team
            else:
                # SG Wasserball Essen ist Gast
                final_home = guest_team if 'SG Wasserball Essen' in home_team else home_team
                final_guest = home_team if 'SG Wasserball Essen' in home_team else guest_team
            
            # Extrahiere Game-ID für detaillierte Informationen
            game_id = None
            # Suche nach Links in allen Zellen
            for cell in cells:
                links = cell.find_all('a')
                for link in links:
                    if link.get('href'):
                        href = link.get('href')
                        # Extrahiere GameID aus dem Link
                        game_id_match = re.search(r'GameID=(\d+)', href)
                        if game_id_match:
                            game_id = game_id_match.group(1)
                            break
                if game_id:
                    break
            
            # Bereinige Ergebnis und prüfe auf "mehr..."
            clean_result = "" if result == "mehr..." else result
            # Immer Details holen wenn game_id verfügbar (für Schiedsrichter und Ort)
            needs_detail_fetch = game_id is not None
            
            return {
                'round': current_round,
                'date': date,
                'time': time,
                'home': final_home,
                'guest': final_guest,
                'location': location,
                'result': clean_result,
                'game_id': game_id,
                'needs_detail_fetch': needs_detail_fetch,
                'competition': competition_type
            }
            
        except Exception as e:
            return None

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
    
    def fetch_game_details(self, game_id: str, competition_type: str = "cup") -> Optional[Dict]:
        """Holt detaillierte Spielinformationen von der Einzelspiel-Seite"""
        if not game_id:
            return None
            
        try:
            # Parameter für die Game-Detail-URL (verwende entsprechende Competition-Parameter)
            if competition_type in self.competitions:
                base_params = self.competitions[competition_type]['params']
            else:
                # Fallback auf ersten Wettbewerb
                base_params = list(self.competitions.values())[0]['params']
                
            game_params = {
                'Season': base_params['Season'],
                'LeagueID': base_params['LeagueID'],
                'Group': base_params['Group'],
                'LeagueKind': base_params['LeagueKind'],
                'GameID': game_id
            }
            
            response = self.session.get(self.game_detail_url, params=game_params)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extrahiere detaillierte Informationen
            details = {
                'location_address': '',
                'location_maps_link': '',
                'detailed_result': '',
                'referee1': '',
                'referee2': '',
                'is_played': False
            }
            
            # Suche nach Adressinformationen
            address_info = self._extract_location_info(soup)
            if address_info:
                details.update(address_info)
            
            # Suche nach detailliertem Ergebnis
            result_info = self._extract_detailed_result(soup)
            if result_info:
                details['detailed_result'] = result_info
                details['is_played'] = True
            
            # Suche nach Schiedsrichter-Informationen
            referee_info = self._extract_referee_info(soup)
            if referee_info:
                details.update(referee_info)
            
            return details
            
        except Exception as e:
            return None
    
    def _extract_location_info(self, soup: BeautifulSoup) -> Optional[Dict]:
        """Extrahiert Adress- und Google Maps-Informationen aus der Spieldetail-Seite"""
        location_info = {
            'location_address': '',
            'location_maps_link': ''
        }
        
        try:
            # Suche nach "Google Maps:" in Tabellen
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    for i, cell in enumerate(cells):
                        cell_text = cell.get_text(strip=True).lower()
                        if 'google maps' in cell_text and i + 1 < len(cells):
                            link = cells[i + 1].find('a', href=True)
                            if link:
                                location_info['location_maps_link'] = link.get('href', '')
                                # Versuche Adresse aus URL zu extrahieren
                                try:
                                    parsed_url = urllib.parse.urlparse(location_info['location_maps_link'])
                                    query_params = urllib.parse.parse_qs(parsed_url.query)
                                    if 'q' in query_params:
                                        location_info['location_address'] = query_params['q'][0]
                                    elif 'query' in query_params:
                                        location_info['location_address'] = query_params['query'][0]
                                except:
                                    pass
                                break
                    if location_info['location_maps_link']:
                        break
                if location_info['location_maps_link']:
                    break
            
            # Suche nach Adresse in Tabellen falls nicht aus URL extrahiert
            if not location_info['location_address']:
                for table in tables:
                    rows = table.find_all('tr')
                    for row in rows:
                        cells = row.find_all(['td', 'th'])
                        for i, cell in enumerate(cells):
                            cell_text = cell.get_text(strip=True).lower()
                            if any(keyword in cell_text for keyword in ['adresse', 'bad-adresse']):
                                if i + 1 < len(cells):
                                    address = cells[i + 1].get_text(strip=True)
                                    if address and len(address) > 3:
                                        location_info['location_address'] = address
                                        break
                        if location_info['location_address']:
                            break
                    if location_info['location_address']:
                        break
            
            return location_info if location_info['location_address'] or location_info['location_maps_link'] else None
            
        except Exception as e:
            return None
    
    def _extract_detailed_result(self, soup: BeautifulSoup) -> Optional[str]:
        """Extrahiert detailliertes Spielergebnis aus der Spieldetail-Seite"""
        try:
            # Suche nach Ergebnis in Tabellen
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    for i, cell in enumerate(cells):
                        cell_text = cell.get_text(strip=True).lower()
                        if any(keyword in cell_text for keyword in ['ergebnis', 'result', 'endstand']):
                            for j in range(i + 1, len(cells)):
                                result_text = cells[j].get_text(strip=True)
                                match = re.search(r'\b(\d{1,2}[:\-]\d{1,2})\b', result_text)
                                if match:
                                    result = match.group(1)
                                    parts = result.replace('-', ':').split(':')
                                    if len(parts) == 2:
                                        first, second = int(parts[0]), int(parts[1])
                                        # Filtere Zeit-Patterns aus
                                        if first > 23 or second > 59 or (first <= 30 and second <= 30):
                                            return result.replace('-', ':')
            return None
            
        except Exception as e:
            return None
    
    def _extract_referee_info(self, soup: BeautifulSoup) -> Optional[Dict]:
        """Extrahiert Schiedsrichter-Informationen aus der Spieldetail-Seite"""
        referee_info = {
            'referee1': '',
            'referee2': ''
        }
        
        try:
            all_ref_names = []
            
            # Suche in Tabellen nach Schiedsrichter-Keywords
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    for i, cell in enumerate(cells):
                        cell_text = cell.get_text(strip=True).lower()
                        if any(keyword in cell_text for keyword in ['schiedsrichter', 'referee', 'ref', 'sr']):
                            for j in range(i + 1, len(cells)):
                                ref_name = cells[j].get_text(strip=True)
                                if (ref_name and len(ref_name) > 2 and not ref_name.isdigit() and
                                    ',' in ref_name and
                                    not any(word in ref_name.lower() for word in [
                                        'essen', 'oberhausen', 'vs', 'mehr', 'spiel', 'solingen', 
                                        'wuppertal', 'bochum', 'duisburg', 'rheinhausen', 'kevelaer',
                                        'tpsk', 'sgw', 'sv', 'asc', 'wsg', 'blau-weiß'
                                    ])):
                                    all_ref_names.append(ref_name)
            
            unique_refs = list(dict.fromkeys(all_ref_names))
            
            if unique_refs:
                referee_info['referee1'] = unique_refs[0] if len(unique_refs) > 0 else ''
                referee_info['referee2'] = unique_refs[1] if len(unique_refs) > 1 else ''
                return referee_info
            
            return None
            
        except Exception as e:
            return None
    
    def _extract_player_stats(self, soup: BeautifulSoup) -> List[Dict]:
        """Extrahiert Spielerstatistiken aus der #players Sektion"""
        player_stats = []
        
        try:
            # Suche nach Spieler-Tabellen (meist mit Spielernamen, Tore, Ausschlüsse)
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                header_found = False
                current_team = ""
                
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if not cells:
                        continue
                    
                    row_text = ' '.join([c.get_text(strip=True) for c in cells])
                    
                    # Team-Header erkennen
                    if 'SG Wasserball Essen' in row_text or 'SGW Essen' in row_text:
                        current_team = "SGW Essen"
                        header_found = True
                        continue
                    elif any(club in row_text for club in ['SC Solingen', 'Kevelaer', 'Bochum', 'Rheinhausen', 'Oberhausen']):
                        # Gegner-Team
                        for club in ['SC Solingen', 'Kevelaer', 'Bochum', 'Rheinhausen', 'Oberhausen', 
                                    'Duisburg', 'Wuppertal', 'Gladbeck', 'Hagen']:
                            if club in row_text:
                                current_team = club
                                break
                        header_found = True
                        continue
                    
                    # Spieler-Zeile parsen (Name, Nummer, Tore, Ausschlüsse)
                    if header_found and len(cells) >= 3:
                        # Typisches Format: Nr, Name, Tore, Ausschlüsse
                        try:
                            name_cell = None
                            goals = 0
                            exclusions = 0
                            
                            for i, cell in enumerate(cells):
                                text = cell.get_text(strip=True)
                                
                                # Spielername (enthält Buchstaben, keine reine Zahl)
                                if text and not text.isdigit() and len(text) > 2 and ',' in text:
                                    name_cell = text
                                
                                # Tore/Ausschlüsse (Zahlen in späteren Spalten)
                                if text.isdigit() and i > 0:
                                    num = int(text)
                                    if i == len(cells) - 2:  # Vorletzte Spalte meist Tore
                                        goals = num
                                    elif i == len(cells) - 1:  # Letzte Spalte meist Ausschlüsse
                                        exclusions = num
                            
                            if name_cell and current_team:
                                player_stats.append({
                                    'name': name_cell,
                                    'team': current_team,
                                    'goals': goals,
                                    'exclusions': exclusions
                                })
                        except:
                            continue
            
            return player_stats
            
        except Exception as e:
            return []
    
    def _extract_team_stats(self, soup: BeautifulSoup) -> Dict:
        """Extrahiert Mannschaftsstatistiken aus der #stats Sektion"""
        stats = {
            'sgw': {'power_play_goals': 0, 'power_play_attempts': 0, 
                    'penalty_kill_success': 0, 'penalty_kill_attempts': 0},
            'opponent': {'power_play_goals': 0, 'power_play_attempts': 0,
                        'penalty_kill_success': 0, 'penalty_kill_attempts': 0}
        }
        
        try:
            # Suche nach Statistik-Tabellen
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    row_text = ' '.join([c.get_text(strip=True).lower() for c in cells])
                    
                    # Überzahl-Statistik
                    if 'überzahl' in row_text or 'power play' in row_text or 'pp' in row_text:
                        # Extrahiere Zahlen (Format: X/Y oder X von Y)
                        for cell in cells:
                            text = cell.get_text(strip=True)
                            match = re.search(r'(\d+)\s*/\s*(\d+)', text)
                            if match:
                                goals, attempts = int(match.group(1)), int(match.group(2))
                                # Bestimme ob SGW oder Gegner basierend auf Position
                                if 'essen' in row_text.lower():
                                    stats['sgw']['power_play_goals'] = goals
                                    stats['sgw']['power_play_attempts'] = attempts
                                else:
                                    stats['opponent']['power_play_goals'] = goals
                                    stats['opponent']['power_play_attempts'] = attempts
                    
                    # Unterzahl-Statistik  
                    if 'unterzahl' in row_text or 'penalty kill' in row_text or 'pk' in row_text:
                        for cell in cells:
                            text = cell.get_text(strip=True)
                            match = re.search(r'(\d+)\s*/\s*(\d+)', text)
                            if match:
                                success, attempts = int(match.group(1)), int(match.group(2))
                                if 'essen' in row_text.lower():
                                    stats['sgw']['penalty_kill_success'] = success
                                    stats['sgw']['penalty_kill_attempts'] = attempts
                                else:
                                    stats['opponent']['penalty_kill_success'] = success
                                    stats['opponent']['penalty_kill_attempts'] = attempts
            
            return stats
            
        except Exception as e:
            return stats
    
    def save_game_stats(self, game_db_id: int, team_stats: Dict, player_stats: List[Dict]):
        """Speichert Spielstatistiken in der Datenbank"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Team-Stats speichern
            for team_key, team in [('sgw', 'SGW Essen'), ('opponent', 'Opponent')]:
                if team_key in team_stats:
                    ts = team_stats[team_key]
                    cursor.execute('''
                        INSERT OR REPLACE INTO game_stats 
                        (game_id, team, power_play_goals, power_play_attempts, 
                         penalty_kill_success, penalty_kill_attempts)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (game_db_id, team, ts.get('power_play_goals', 0), 
                          ts.get('power_play_attempts', 0), ts.get('penalty_kill_success', 0),
                          ts.get('penalty_kill_attempts', 0)))
            
            # Player-Stats speichern
            for ps in player_stats:
                cursor.execute('''
                    INSERT INTO player_stats (game_id, player_name, team, goals, exclusions)
                    VALUES (?, ?, ?, ?, ?)
                ''', (game_db_id, ps['name'], ps['team'], ps.get('goals', 0), ps.get('exclusions', 0)))
            
            conn.commit()
        except Exception as e:
            print(f"Error saving stats: {e}")
        finally:
            conn.close()
    
    def save_termine(self, termine: List[Dict]) -> Dict:
        """Speichert Termine in der Datenbank"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        new_games = []
        updated_games = []
        unchanged_games = []
        
        for termin in termine:
            # Replace "SG Wasserball Essen" with "SGW Essen" in team names before saving
            home_clean = termin.get('home', '').replace("SG Wasserball Essen", "SGW Essen")
            guest_clean = termin.get('guest', '').replace("SG Wasserball Essen", "SGW Essen")
            
            event_id = self.generate_event_id(home_clean, guest_clean, termin.get('competition', ''))
            
            # Hole detaillierte Informationen falls nötig
            game_details = None
            if termin.get('needs_detail_fetch', False) and termin.get('game_id'):
                game_details = self.fetch_game_details(termin['game_id'], termin.get('competition', 'cup'))
            
            # Bestimme finale Werte für Location und Description
            base_location = termin.get('location', '')
            base_result = termin.get('result', '')
            
            # Kombiniere Location: Adresse + Google Maps Link
            final_location = base_location
            final_description = ""
            
            if game_details:
                # Kombiniere Adresse und Maps Link für Location
                location_parts = []
                if game_details.get('location_address'):
                    location_parts.append(game_details['location_address'])
                elif base_location.strip():
                    location_parts.append(base_location)
                
                if game_details.get('location_maps_link'):
                    location_parts.append(game_details['location_maps_link'])
                
                if location_parts:
                    final_location = ' | '.join(location_parts)
                
                # Formatiere Description basierend auf Spielstatus
                description_parts = []
                
                # Ergebnis hinzufügen
                if game_details.get('is_played') and game_details.get('detailed_result'):
                    # Gespieltes Spiel: Ergebnis von Detail-Seite
                    description_parts.append(f"Result: {game_details['detailed_result']}")
                elif base_result.strip():
                    # Fallback auf base result von Übersichtsseite
                    description_parts.append(f"Result: {base_result}")
                else:
                    # Kein Ergebnis verfügbar
                    description_parts.append("Result: -")
                
                # Schiedsrichter hinzufügen
                if game_details.get('referee1'):
                    description_parts.append(f"Ref 1: {game_details['referee1']}")
                if game_details.get('referee2'):
                    description_parts.append(f"Ref 2: {game_details['referee2']}")
                
                final_description = '\n'.join(description_parts)
            else:
                # Keine Details verfügbar, verwende base values
                if base_result.strip():
                    final_description = f"Result: {base_result}"
                else:
                    final_description = "Result: -"
            
            # Füge Competition-Information zur Description hinzu (falls noch nicht vorhanden)
            competition_type = termin.get('competition', 'pokal')
            
            # Bestimme Competition-Prefix basierend auf dem tatsächlichen Wettbewerb
            if competition_type == 'pokal':
                comp_prefix = "[POKAL]"
            elif competition_type == 'nrw_pokal':
                comp_prefix = "[NRW POKAL]"
            elif competition_type == 'verbandsliga':
                comp_prefix = "[VERBANDSLIGA]"
            elif competition_type == 'ruhrgebietsliga':
                comp_prefix = "[RUHRGEBIETSLIGA]"
            else:
                comp_prefix = f"[{competition_type.upper()}]"
            
            # Prüfe ob Competition-Info bereits vorhanden ist
            existing_prefixes = ["[LIGA]", "[POKAL]", "[NRW POKAL]", "[VERBANDSLIGA]", "[RUHRGEBIETSLIGA]"]
            has_prefix = any(final_description.startswith(prefix) for prefix in existing_prefixes)
            
            if not has_prefix:
                final_description = f"{comp_prefix}\n{final_description}"
            
            # Prüfe ob Event bereits existiert
            cursor.execute('SELECT home, guest, date, time, location, description FROM games WHERE event_id = ?', (event_id,))
            existing = cursor.fetchone()
            
            if existing:
                # Vergleiche Daten um zu prüfen ob sich etwas geändert hat
                old_home, old_guest, old_date, old_time, old_location, old_description = existing
                
                # Prüfe ob sich irgendwelche Daten geändert haben
                data_changed = (
                    old_home != home_clean or
                    old_guest != guest_clean or
                    old_date != termin.get('date', '') or
                    old_time != termin.get('time', '') or
                    old_location != final_location or
                    old_description != final_description
                )
                
                if data_changed:
                    # Sammle detaillierte Änderungen für die Ausgabe
                    changes = []
                    
                    # Check team name changes (rare but possible)
                    if old_home != home_clean:
                        changes.append(f"home team: {old_home} -> {home_clean}")
                    if old_guest != guest_clean:
                        changes.append(f"guest team: {old_guest} -> {guest_clean}")
                    
                    # Check date and time changes
                    if old_date != termin.get('date', ''):
                        old_d = old_date if old_date else '(empty)'
                        new_d = termin.get('date', '') if termin.get('date', '') else '(empty)'
                        changes.append(f"date: {old_d} -> {new_d}")
                    if old_time != termin.get('time', ''):
                        old_t = old_time if old_time else '(empty)'
                        new_t = termin.get('time', '') if termin.get('time', '') else '(empty)'
                        changes.append(f"time: {old_t} -> {new_t}")
                    
                    # Check location changes
                    if old_location != final_location:
                        # Show location change with details
                        old_loc = old_location.split('|')[0].strip() if old_location else '(empty)'
                        new_loc = final_location.split('|')[0].strip() if final_location else '(empty)'
                        if old_loc != new_loc:
                            changes.append(f"location: {old_loc} -> {new_loc}")
                        elif '|' in old_location and '|' in final_location:
                            # Location address same, but maps link might differ
                            changes.append("location: maps link updated")
                        else:
                            changes.append("location: additional data added")
                    if old_description != final_description:
                        # Parse both descriptions to compare individual fields
                        def parse_description(desc):
                            fields = {}
                            if desc:
                                for line in desc.split('\n'):
                                    if line.startswith('Result:'):
                                        fields['result'] = line.replace('Result:', '').strip()
                                    elif line.startswith('Ref 1:'):
                                        fields['ref1'] = line.replace('Ref 1:', '').strip()
                                    elif line.startswith('Ref 2:'):
                                        fields['ref2'] = line.replace('Ref 2:', '').strip()
                            return fields
                        
                        old_fields = parse_description(old_description)
                        new_fields = parse_description(final_description)
                        
                        # Compare each field
                        if old_fields.get('result', '') != new_fields.get('result', ''):
                            old_res = old_fields.get('result', '-')
                            new_res = new_fields.get('result', '-')
                            changes.append(f"result: {old_res} -> {new_res}")
                        
                        if old_fields.get('ref1', '') != new_fields.get('ref1', ''):
                            old_ref = old_fields.get('ref1', 'none')
                            new_ref = new_fields.get('ref1', 'none')
                            if old_ref == 'none' and new_ref != 'none':
                                changes.append(f"referee 1 added: {new_ref}")
                            elif old_ref != 'none' and new_ref == 'none':
                                changes.append(f"referee 1 removed")
                            else:
                                changes.append(f"referee 1: {old_ref} -> {new_ref}")
                        
                        if old_fields.get('ref2', '') != new_fields.get('ref2', ''):
                            old_ref = old_fields.get('ref2', 'none')
                            new_ref = new_fields.get('ref2', 'none')
                            if old_ref == 'none' and new_ref != 'none':
                                changes.append(f"referee 2 added: {new_ref}")
                            elif old_ref != 'none' and new_ref == 'none':
                                changes.append(f"referee 2 removed")
                            else:
                                changes.append(f"referee 2: {old_ref} -> {new_ref}")
                    
                    # Fallback: If description changed but no specific changes detected, note it
                    if not changes and old_description != final_description:
                        changes.append("description updated (unknown field)")
                    
                    # Aktualisiere nur wenn sich tatsächlich etwas geändert hat
                    cursor.execute('''
                        UPDATE games 
                        SET home = ?, guest = ?, date = ?, time = ?, location = ?, description = ?,
                            dsv_game_id = COALESCE(?, dsv_game_id), competition_type = COALESCE(?, competition_type),
                            last_change = CURRENT_TIMESTAMP
                        WHERE event_id = ?
                    ''', (
                        home_clean,
                        guest_clean,
                        termin.get('date', ''),
                        termin.get('time', ''),
                        final_location,
                        final_description,
                        termin.get('game_id', None),
                        termin.get('competition', None),
                        event_id
                    ))
                    
                    updated_games.append({
                        'match': f"{home_clean} vs {guest_clean}",
                        'date': termin.get('date', ''),
                        'changes': changes
                    })
                else:
                    # Keine Aenderungen - aber dsv_game_id updaten falls vorhanden
                    if termin.get('game_id'):
                        cursor.execute('''
                            UPDATE games SET dsv_game_id = ?, competition_type = ?
                            WHERE event_id = ? AND (dsv_game_id IS NULL OR dsv_game_id = '')
                        ''', (termin.get('game_id'), termin.get('competition', ''), event_id))
                    unchanged_games.append(f"{home_clean} vs {guest_clean}")
            else:
                # Füge neuen Eintrag hinzu
                cursor.execute('''
                    INSERT INTO games 
                    (event_id, home, guest, date, time, location, description, dsv_game_id, competition_type)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    event_id,
                    home_clean,
                    guest_clean,
                    termin.get('date', ''),
                    termin.get('time', ''),
                    final_location,
                    final_description,
                    termin.get('game_id', ''),
                    termin.get('competition', '')
                ))
                new_games.append({
                    'match': f"{home_clean} vs {guest_clean}",
                    'date': termin.get('date', ''),
                    'time': termin.get('time', ''),
                    'competition': comp_prefix
                })
        
        conn.commit()
        conn.close()
        
        return {
            'new': new_games,
            'updated': updated_games,
            'unchanged': unchanged_games
        }
    
    def delete_games_and_recalculate_ids(self, ids_to_delete: List[int]) -> int:
        """Löscht Spiele mit den angegebenen IDs und berechnet IDs neu"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Prüfe welche IDs existieren
        placeholders = ','.join(['?' for _ in ids_to_delete])
        cursor.execute(f'SELECT id FROM games WHERE id IN ({placeholders})', ids_to_delete)
        existing_ids = [row[0] for row in cursor.fetchall()]
        
        if not existing_ids:
            print("No games found with specified IDs")
            conn.close()
            return 0
        
        print(f"Deleting {len(existing_ids)} games with IDs: {existing_ids}")
        
        # Lösche die Spiele
        cursor.execute(f'DELETE FROM games WHERE id IN ({placeholders})', ids_to_delete)
        deleted_count = cursor.rowcount
        
        # Hole alle verbleibenden Spiele und sortiere sie nach ID
        cursor.execute('SELECT * FROM games ORDER BY id')
        remaining_games = cursor.fetchall()
        
        # Lösche alle Spiele und füge sie mit neuen IDs ein
        cursor.execute('DELETE FROM games')
        
        for i, game in enumerate(remaining_games, 1):
            # Neue ID ist i, restliche Daten bleiben gleich
            (old_id, event_id, home, guest, date, time, location, description, last_change) = game
            cursor.execute('''
                INSERT INTO games 
                (id, event_id, home, guest, date, time, location, description, last_change)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (i, event_id, home, guest, date, time, location, description, last_change))
        
        # Setze den Auto-Increment Counter
        max_id = len(remaining_games)
        cursor.execute('DELETE FROM sqlite_sequence WHERE name="games"')
        cursor.execute('INSERT INTO sqlite_sequence (name, seq) VALUES ("games", ?)', (max_id,))
        
        conn.commit()
        conn.close()
        
        print(f"{deleted_count} games deleted")
        print(f"IDs recalculated, next ID: {max_id + 1}")
        
        return deleted_count
    
    # ==================== EVENTS CRUD ====================
    
    def generate_event_id_for_event(self, title: str, date: str) -> str:
        """Generiert eindeutige Event-ID für Events (nicht Spiele)"""
        content = f"event_{title}_{date}".strip()
        return hashlib.md5(content.encode('utf-8')).hexdigest()
    
    def add_event(self, title: str, date: str, time: str = "", location: str = "", description: str = "") -> Dict:
        """Fügt ein neues Event hinzu (z.B. Weihnachtsmarkt)"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        event_id = self.generate_event_id_for_event(title, date)
        
        # Prüfe ob Event bereits existiert
        cursor.execute('SELECT id FROM events WHERE event_id = ?', (event_id,))
        existing = cursor.fetchone()
        
        result = {'status': 'unchanged', 'title': title, 'date': date}
        
        if existing:
            # Update existing event
            cursor.execute('''
                UPDATE events 
                SET title = ?, date = ?, time = ?, location = ?, description = ?, 
                    last_change = CURRENT_TIMESTAMP
                WHERE event_id = ?
            ''', (title, date, time, location, description, event_id))
            result['status'] = 'updated'
            result['id'] = existing[0]
        else:
            # Insert new event
            cursor.execute('''
                INSERT INTO events (event_id, title, date, time, location, description)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (event_id, title, date, time, location, description))
            result['status'] = 'new'
            result['id'] = cursor.lastrowid
        
        conn.commit()
        conn.close()
        
        return result
    
    def delete_events(self, ids_to_delete: List[int]) -> int:
        """Löscht Events mit den angegebenen IDs und berechnet IDs neu"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Prüfe welche IDs existieren
        placeholders = ','.join(['?' for _ in ids_to_delete])
        cursor.execute(f'SELECT id FROM events WHERE id IN ({placeholders})', ids_to_delete)
        existing_ids = [row[0] for row in cursor.fetchall()]
        
        if not existing_ids:
            print("No events found with specified IDs")
            conn.close()
            return 0
        
        print(f"Deleting {len(existing_ids)} events with IDs: {existing_ids}")
        
        # Lösche die Events
        cursor.execute(f'DELETE FROM events WHERE id IN ({placeholders})', ids_to_delete)
        deleted_count = cursor.rowcount
        
        # Hole alle verbleibenden Events und sortiere sie nach ID
        cursor.execute('SELECT * FROM events ORDER BY id')
        remaining = cursor.fetchall()
        
        # Lösche alle und füge mit neuen IDs ein
        cursor.execute('DELETE FROM events')
        
        for i, event in enumerate(remaining, 1):
            (old_id, event_id, title, date, time, location, description, last_change) = event
            cursor.execute('''
                INSERT INTO events 
                (id, event_id, title, date, time, location, description, last_change)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (i, event_id, title, date, time, location, description, last_change))
        
        # Setze Auto-Increment Counter
        max_id = len(remaining)
        cursor.execute('DELETE FROM sqlite_sequence WHERE name="events"')
        if max_id > 0:
            cursor.execute('INSERT INTO sqlite_sequence (name, seq) VALUES ("events", ?)', (max_id,))
        
        conn.commit()
        conn.close()
        
        print(f"{deleted_count} events deleted")
        return deleted_count
    
    def list_events(self, limit: int = 20, future_only: bool = False):
        """Zeigt Events aus der Datenbank"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT id, title, date, time, location, description, last_change FROM events')
        all_events = cursor.fetchall()
        conn.close()
        
        if not all_events:
            print("No events found in database.")
            print("Use --add-event to add events")
            return
        
        today_dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Sort and filter
        def get_sort_key(event):
            (id, title, date, time, location, description, last_change) = event
            try:
                if '.' in date:
                    dt = datetime.strptime(date, '%d.%m.%Y')
                else:
                    dt = datetime.strptime(date, '%Y-%m-%d')
                if time:
                    time_parts = time.split(':')
                    dt = dt.replace(hour=int(time_parts[0]), minute=int(time_parts[1]))
                return dt
            except:
                return datetime.max
        
        sorted_events = sorted(all_events, key=get_sort_key)
        
        if future_only:
            sorted_events = [e for e in sorted_events if get_sort_key(e) >= today_dt]
        
        events = sorted_events[:limit]
        
        if not events:
            print("No upcoming events found.")
            return
        
        title_str = "Upcoming Events" if future_only else "All Events"
        print(f"\n=== {title_str} ({len(events)}) ===")
        print("-" * 50)
        
        for event in events:
            (id, title, date, time, location, description, last_change) = event
            time_str = f" {time}" if time else ""
            location_str = f" @ {location}" if location else ""
            
            print(f"[ID:{id}] {date}{time_str}{location_str}")
            print(f"      | {title}")
            if description:
                print(f"      | {description}")
            print("-" * 50)
    
    # ==================== END EVENTS CRUD ====================
    
    # ==================== STATISTICS ====================
    
    def get_season_stats(self) -> Dict:
        """Berechnet Saison-Statistiken für SGW Essen"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Gesamte Überzahl/Unterzahl Stats
        cursor.execute('''
            SELECT 
                SUM(power_play_goals) as pp_goals,
                SUM(power_play_attempts) as pp_attempts,
                SUM(penalty_kill_success) as pk_success,
                SUM(penalty_kill_attempts) as pk_attempts
            FROM game_stats WHERE team = 'SGW Essen'
        ''')
        
        row = cursor.fetchone()
        
        stats = {
            'power_play': {
                'goals': row[0] or 0,
                'attempts': row[1] or 0,
                'percentage': (row[0] / row[1] * 100) if row[1] and row[1] > 0 else 0
            },
            'penalty_kill': {
                'success': row[2] or 0,
                'attempts': row[3] or 0,
                'percentage': (row[2] / row[3] * 100) if row[3] and row[3] > 0 else 0
            }
        }
        
        # Top-Torschützen
        cursor.execute('''
            SELECT player_name, SUM(goals) as total_goals
            FROM player_stats 
            WHERE team = 'SGW Essen' AND goals > 0
            GROUP BY player_name
            ORDER BY total_goals DESC
            LIMIT 10
        ''')
        stats['top_scorers'] = [{'name': r[0], 'goals': r[1]} for r in cursor.fetchall()]
        
        # Ausschluss-Statistik
        cursor.execute('''
            SELECT player_name, SUM(exclusions) as total_excl
            FROM player_stats 
            WHERE team = 'SGW Essen' AND exclusions > 0
            GROUP BY player_name
            ORDER BY total_excl DESC
            LIMIT 10
        ''')
        stats['exclusions'] = [{'name': r[0], 'exclusions': r[1]} for r in cursor.fetchall()]
        
        conn.close()
        return stats
    
    def print_season_stats(self):
        """Gibt Saison-Statistiken aus"""
        stats = self.get_season_stats()
        
        print("\n=== SGW Essen - Season Statistics ===\n")
        
        # Überzahl
        pp = stats['power_play']
        print(f"POWER PLAY (Ueberzahl):")
        print(f"  {pp['goals']}/{pp['attempts']} ({pp['percentage']:.1f}%)")
        
        # Unterzahl
        pk = stats['penalty_kill']
        print(f"\nPENALTY KILL (Unterzahl):")
        print(f"  {pk['success']}/{pk['attempts']} ({pk['percentage']:.1f}%)")
        
        # Top-Torschützen
        if stats['top_scorers']:
            print(f"\nTOP SCORERS:")
            for i, s in enumerate(stats['top_scorers'][:5], 1):
                print(f"  {i}. {s['name']}: {s['goals']} goals")
        
        # Ausschlüsse
        if stats['exclusions']:
            print(f"\nEXCLUSIONS:")
            for s in stats['exclusions'][:5]:
                print(f"  {s['name']}: {s['exclusions']}")
        
        print()
    
    def get_game_stats(self, game_db_id: int) -> Dict:
        """Holt Statistiken für ein einzelnes Spiel"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Game info
        cursor.execute('SELECT home, guest, date FROM games WHERE id = ?', (game_db_id,))
        game = cursor.fetchone()
        if not game:
            conn.close()
            return None
        
        result = {
            'home': game[0],
            'guest': game[1],
            'date': game[2],
            'team_stats': {},
            'player_stats': []
        }
        
        # Team stats
        cursor.execute('SELECT * FROM game_stats WHERE game_id = ?', (game_db_id,))
        for row in cursor.fetchall():
            result['team_stats'][row[2]] = {
                'power_play_goals': row[4],
                'power_play_attempts': row[5],
                'penalty_kill_success': row[6],
                'penalty_kill_attempts': row[7]
            }
        
        # Player stats
        cursor.execute('''
            SELECT player_name, team, goals, exclusions 
            FROM player_stats WHERE game_id = ? ORDER BY goals DESC
        ''', (game_db_id,))
        result['player_stats'] = [
            {'name': r[0], 'team': r[1], 'goals': r[2], 'exclusions': r[3]}
            for r in cursor.fetchall()
        ]
        
        conn.close()
        return result
    
    def scrape_game_statistics(self, game_id: str, competition_type: str) -> bool:
        """Scraped Spielstatistiken von der DSV-Detailseite"""
        try:
            if competition_type in self.competitions:
                base_params = self.competitions[competition_type]['params']
            else:
                base_params = list(self.competitions.values())[0]['params']
            
            game_params = {
                'Season': base_params['Season'],
                'LeagueID': base_params['LeagueID'],
                'Group': base_params['Group'],
                'LeagueKind': base_params['LeagueKind'],
                'GameID': game_id
            }
            
            response = self.session.get(self.game_detail_url, params=game_params)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extrahiere Stats
            player_stats = self._extract_player_stats(soup)
            team_stats = self._extract_team_stats(soup)
            
            return {
                'player_stats': player_stats,
                'team_stats': team_stats
            }
            
        except Exception as e:
            print(f"Error scraping stats for game {game_id}: {e}")
            return None
    
    def scrape_all_played_games_stats(self):
        """Scraped Stats fuer alle gespielten Spiele"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Finde Spiele mit Ergebnis und DSV Game-ID aber ohne Stats
        cursor.execute('''
            SELECT g.id, g.dsv_game_id, g.competition_type, g.home, g.guest, g.date
            FROM games g
            LEFT JOIN game_stats gs ON g.id = gs.game_id
            WHERE g.description LIKE '%Result:%'
            AND g.description NOT LIKE '%Result: -%'
            AND g.dsv_game_id IS NOT NULL
            AND g.dsv_game_id != ''
            AND gs.id IS NULL
        ''')
        
        games_to_scrape = cursor.fetchall()
        conn.close()
        
        if not games_to_scrape:
            print("No played games with DSV game_id without stats found.")
            print("Run --enable-scraping first to fetch DSV game IDs.")
            return 0
        
        print(f"Found {len(games_to_scrape)} games to scrape stats for...")
        
        scraped_count = 0
        for game in games_to_scrape:
            db_id, dsv_game_id, comp_type, home, guest, date = game
            print(f"  Scraping: {home} vs {guest} ({date}) [DSV:{dsv_game_id}]...")
            
            stats = self.scrape_game_statistics(dsv_game_id, comp_type or 'verbandsliga')
            
            if stats:
                self.save_game_stats(db_id, stats.get('team_stats', {}), stats.get('player_stats', []))
                scraped_count += 1
                print(f"    -> Stats saved")
            else:
                print(f"    -> No stats found")
        
        print(f"Scraped stats for {scraped_count} games.")
        return scraped_count
    
    # ==================== END STATISTICS ====================
    
    def generate_ics(self, output_file: str = "sgw_termine.ics") -> str:
        """Generiert ICS-Kalenderdatei (Games + Events)"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Hole Games
        cursor.execute('''
            SELECT id, event_id, home, guest, date, time, location, description
            FROM games 
            ORDER BY date, time
        ''')
        games = cursor.fetchall()
        
        # Hole Events
        cursor.execute('''
            SELECT id, event_id, title, date, time, location, description
            FROM events 
            ORDER BY date, time
        ''')
        events = cursor.fetchall()
        
        conn.close()
        
        ics_content = self._create_ics_content(games, events)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(ics_content)
        
        return output_file
    
    def _create_ics_content(self, games: List, events: List = None) -> str:
        """Erstellt ICS-Inhalt (Games + Events)"""
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
        
        # === GAMES ===
        for termin in games:
            (id, event_id, home, guest, date, time, location, description) = termin
            
            uid = f"sgw-game-{event_id}@essen.de"
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
            
            ics_description = description.replace('\n', '\\n') if description else ""
            
            # Location
            if location and '|' in location:
                parts = location.split('|', 1)
                address = parts[0].strip()
                maps_link = parts[1].strip()
                if address and maps_link:
                    location_text = f"{address}\\nGoogle Maps: {maps_link}"
                else:
                    location_text = address if address else maps_link
            else:
                location_text = location.strip() if location else "TBA"
            
            location_text = location_text if location_text else "TBA"
            
            ics_lines.extend([
                "BEGIN:VEVENT",
                f"UID:{uid}",
                f"DTSTAMP:{dtstamp}",
                f"DTSTART:{dtstart}",
                f"DTEND:{dtend}",
                f"SUMMARY:{title}",
                f"DESCRIPTION:{ics_description}",
                f"LOCATION:{location_text}",
                "STATUS:CONFIRMED",
                "TRANSP:OPAQUE",
                "END:VEVENT"
            ])
        
        # === EVENTS (Weihnachtsmarkt etc.) ===
        if events:
            for event in events:
                (id, event_id, title, date, time, location, description) = event
                
                uid = f"sgw-event-{event_id}@essen.de"
                summary = f"[EVENT] {title}"
                
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
                
                # Events dauern standardmäßig 3 Stunden
                end_time = start_time + timedelta(hours=3)
                
                dtstart = start_time.strftime('%Y%m%dT%H%M%S')
                dtend = end_time.strftime('%Y%m%dT%H%M%S')
                dtstamp = now.strftime('%Y%m%dT%H%M%SZ')
                
                ics_description = description.replace('\n', '\\n') if description else ""
                location_text = location.strip() if location else "TBA"
                
                ics_lines.extend([
                    "BEGIN:VEVENT",
                    f"UID:{uid}",
                    f"DTSTAMP:{dtstamp}",
                    f"DTSTART:{dtstart}",
                    f"DTEND:{dtend}",
                    f"SUMMARY:{summary}",
                    f"DESCRIPTION:{ics_description}",
                    f"LOCATION:{location_text}",
                    "STATUS:CONFIRMED",
                    "TRANSP:OPAQUE",
                    "END:VEVENT"
                ])
        
        ics_lines.append("END:VCALENDAR")
        return "\n".join(ics_lines)
    
    def list_next_termine(self, limit: int = 10, format: str = "full"):
        """Zeigt die nächsten anstehenden Termine (ab heute)
        
        Args:
            limit: Anzahl der Termine
            format: "full" (alle Details), "compact" (für Bot), "minimal" (nur Basics)
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        today_dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        cursor.execute('''
            SELECT id, date, time, home, guest, location, description, last_change
            FROM games
        ''')
        
        all_termine = cursor.fetchall()
        conn.close()
        
        # Filter and sort
        future_termine = []
        for termin in all_termine:
            (id, date, time, home, guest, location, description, last_change) = termin
            try:
                dt = datetime.strptime(date, '%d.%m.%Y') if '.' in date else datetime.strptime(date, '%Y-%m-%d')
                if time:
                    h, m = time.split(':')
                    dt = dt.replace(hour=int(h), minute=int(m))
                if dt >= today_dt:
                    future_termine.append((dt, termin))
            except:
                continue
        
        future_termine.sort(key=lambda x: x[0])
        future_termine = [t for _, t in future_termine[:limit]]
        
        if not future_termine:
            print("No upcoming games.")
            return
        
        # Compact format for bot responses
        if format == "compact":
            for t in future_termine:
                (id, date, time, home, guest, location, description, _) = t
                time_str = time if time else ""
                loc = location.split('|')[0].strip()[:25] if location else ""
                
                # Extract result if played
                result = ""
                if description and "Result:" in description:
                    for line in description.split('\n'):
                        if line.startswith("Result:"):
                            r = line.replace("Result:", "").strip()
                            if r and r != "-":
                                result = f" ({r})"
                            break
                
                print(f"[{id}] {date} {time_str} | {home} vs {guest}{result}")
                if loc:
                    print(f"    @ {loc}")
            return
        
        # Minimal format - just one line per game
        if format == "minimal":
            for t in future_termine:
                (id, date, time, home, guest, _, description, _) = t
                result = ""
                if description and "Result:" in description:
                    for line in description.split('\n'):
                        if "Result:" in line:
                            r = line.replace("Result:", "").strip()
                            if r and r != "-":
                                result = f" [{r}]"
                print(f"[{id}] {date} {time or ''} {home} vs {guest}{result}")
            return
        
        # Full format
        print(f"\n=== Next {len(future_termine)} Games ===\n")
        
        for t in future_termine:
            (id, date, time, home, guest, location, description, _) = t
            
            # Competition tag
            comp = ""
            if description:
                for tag in ["[VERBANDSLIGA]", "[RUHRGEBIETSLIGA]", "[NRW POKAL]", "[POKAL]", "[LIGA]"]:
                    if description.startswith(tag):
                        comp = tag + " "
                        break
            
            loc = location.split('|')[0].strip() if location else ""
            time_str = time if time else ""
            
            print(f"[{id}] {comp}{date} {time_str}")
            print(f"    {home} vs {guest}")
            if loc:
                print(f"    @ {loc}")
            
            # Result and refs
            if description:
                for line in description.split('\n'):
                    if line.startswith("Result:"):
                        r = line.replace("Result:", "").strip()
                        if r and r != "-":
                            print(f"    Result: {r}")
                    elif line.startswith("Ref"):
                        print(f"    {line}")
            print()
    
    def list_termine(self, limit: int = 10):
        """Zeigt Termine aus der Datenbank"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, date, time, home, guest, location, description, last_change
            FROM games
        ''')
        
        all_termine = cursor.fetchall()
        conn.close()
        
        if not all_termine:
            print("No games found in database.")
            print("Use --add to add games")
            return
        
        # Sort by actual date in Python (not as string)
        def get_sort_key(termin):
            (id, date, time, home, guest, location, description, last_change) = termin
            try:
                if '.' in date:
                    dt = datetime.strptime(date, '%d.%m.%Y')
                else:
                    dt = datetime.strptime(date, '%Y-%m-%d')
                
                # Add time if available
                if time:
                    time_parts = time.split(':')
                    dt = dt.replace(hour=int(time_parts[0]), minute=int(time_parts[1]))
                
                return dt
            except:
                return datetime.max  # Put invalid dates at the end
        
        # Sort and limit
        termine = sorted(all_termine, key=get_sort_key)[:limit]
        
        if not termine:
            print("No games found in database.")
            print("Use --add to add games")
            return
        
        print(f"\n=== {len(termine)} Termine ===")
        print("-" * 69)
        for termin in termine:
            (id, date, time, home, guest, location, description, last_change) = termin
            time_str = f" {time}" if time else ""
            
            # Competition indicator aus Description extrahieren
            comp_str = ""
            if description:
                if description.startswith("[VERBANDSLIGA]"):
                    comp_str = "[VERBANDSLIGA] "
                elif description.startswith("[RUHRGEBIETSLIGA]"):
                    comp_str = "[RUHRGEBIETSLIGA] "
                elif description.startswith("[NRW POKAL]"):
                    comp_str = "[NRW POKAL] "
                elif description.startswith("[POKAL]"):
                    comp_str = "[POKAL] "
                elif description.startswith("[LIGA]"):  # Fallback für alte Einträge
                    comp_str = "[LIGA] "
            
            # Location: Zeige nur Adress-Teil (vor "|"), Maps-Link wird separat angezeigt
            display_location = location.split('|')[0].strip() if location else ""
            location_str = f" @ {display_location}" if display_location else ""
            maps_str = f" [Maps]" if '|' in location else ""
            
            print(f"[ID:{id}] {comp_str}{date}{time_str}{location_str}{maps_str}")
            print(f"      | {home} vs {guest}")
            
            # Zeige Description (Result/Refs) wenn vorhanden
            if description and description.strip():
                desc_lines = description.split('\n')
                for desc_line in desc_lines:
                    print(f"      | {desc_line}")
            
            # Zeige Google Maps Link wenn vorhanden
            if '|' in location:
                maps_link = location.split('|', 1)[1].strip()
                if maps_link.startswith('http'):
                    print(f"      | Maps: {maps_link}")
            
            print(f"      | Updated: {last_change}")
            print("-" * 69)    
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
    
    def run(self, scrape=True, add_new=False, enable_scraping=False) -> int:
        """Hauptausführung
        
        Returns:
            0 - No changes detected
            1 - Changes detected (new or updated games)
        """
        print("SGW Essen Scraper started\n")
        
        alle_termine = []
        
        # Scraping (mit Feature-Flag)
        if scrape:
            termine = self.scrape_termine(enable_scraping=enable_scraping)
            alle_termine.extend(termine)
        
        # Manuelle Eingabe
        if add_new:
            manual_termine = self.add_manual_termine()
            alle_termine.extend(manual_termine)
        
        # Speichere in Datenbank
        results = None
        has_changes = False
        
        if alle_termine:
            results = self.save_termine(alle_termine)
            has_changes = bool(results['new'] or results['updated'])
            
            # Print summary
            print("\n" + "="*36)
            print(f"SUMMARY: {len(alle_termine)} games scraped")
            print("="*36)
            
            # New games
            if results['new']:
                print(f"\nNEW GAMES ({len(results['new'])}):")
                for game in results['new']:
                    print(f"  + {game['competition']} {game['match']}")
                    print(f"    Date: {game['date']} {game['time']}")
            else:
                print("\nNEW GAMES: None")
            
            # Updated games
            if results['updated']:
                print(f"\nUPDATED GAMES ({len(results['updated'])}):")
                for game in results['updated']:
                    print(f"  * {game['match']} ({game['date']})")
                    for change in game['changes']:
                        print(f"    - {change}")
            else:
                print("\nUPDATED GAMES: None (all data unchanged)")
            
            print("\n" + "="*36)
        
        # Generiere ICS nur bei Änderungen
        if has_changes:
            ics_file = self.generate_ics()
            print(f"\nICS calendar updated: {ics_file}")
        else:
            print("\nICS calendar and database are up to date")
        
        if not alle_termine and not scrape and not add_new:
            print("\nUsage:")
            print("  --add DATUM ZEIT HEIM GAST ERGEBNIS  # Add directly")
            print("  -new                                  # Interactive input")
            print("  --list                                # Show games")
        
        # Return exit code based on changes
        return 1 if has_changes else 0

def main():
    parser = argparse.ArgumentParser(
        description='SGW Essen Wasserball Kalender Generator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Beispiele:
  # Games (Spiele mit Heim/Gast)
  python sgw_essen_scraper.py --add "20.12.2025" "15:00" "SGW Essen" "Gegner" "Halle" "Beschreibung"
  python sgw_essen_scraper.py --list
  python sgw_essen_scraper.py --list-next 5
  python sgw_essen_scraper.py --delete 5 7 9
  
  # Events (Termine ohne Heim/Gast)
  python sgw_essen_scraper.py --add-event "20.12.2025" "18:00" "Weihnachtsmarkt" "Grugahalle" "Glühwein!"
  python sgw_essen_scraper.py --list-events
  python sgw_essen_scraper.py --list-events-next 5
  python sgw_essen_scraper.py --delete-event 1 2
  
  python sgw_essen_scraper.py --enable-scraping
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
    parser.add_argument('--list-next', type=int, metavar='N',
                       help='Zeigt die nächsten N anstehenden Termine (ab heute)')
    parser.add_argument('--limit', type=int, default=10,
                       help='Anzahl der anzuzeigenden Termine')
    parser.add_argument('--delete', nargs='+', type=int, metavar='ID',
                       help='Löscht Spiele mit den angegebenen IDs')
    
    # Event arguments (Termine ohne Heim/Gast)
    parser.add_argument('--add-event', nargs=5, metavar=('DATE', 'TIME', 'TITLE', 'LOCATION', 'DESC'),
                       help='Event hinzufügen (z.B. Weihnachtsmarkt)')
    parser.add_argument('--list-events', action='store_true',
                       help='Zeigt alle Events')
    parser.add_argument('--list-events-next', type=int, metavar='N',
                       help='Zeigt die nächsten N Events')
    parser.add_argument('--delete-event', nargs='+', type=int, metavar='ID',
                       help='Loescht Events mit den angegebenen IDs')
    
    # Statistics arguments
    parser.add_argument('--stats', action='store_true',
                       help='Zeigt Saison-Statistiken (Ueberzahl, Top-Scorer)')
    parser.add_argument('--game-stats', type=int, metavar='GAME_ID',
                       help='Zeigt Statistiken fuer ein bestimmtes Spiel')
    parser.add_argument('--scrape-stats', action='store_true',
                       help='Scraped Statistiken von DSV fuer gespielte Spiele')
    parser.add_argument('--format', choices=['full', 'compact', 'minimal'], default='full',
                       help='Ausgabeformat fuer Listen (default: full)')
    
    args = parser.parse_args()
    
    scraper = SGWTermineScraper(db_path=args.db)
    
    # ========== GAMES ==========
    
    # Spiele löschen
    if args.delete:
        deleted_count = scraper.delete_games_and_recalculate_ids(args.delete)
        if deleted_count > 0:
            ics_file = scraper.generate_ics(args.ics)
            print(f"ICS calendar updated: {ics_file}")
            sys.exit(1)  # Changes made
        sys.exit(0)  # No changes
    
    # Liste anzeigen
    if args.list:
        scraper.list_termine(limit=args.limit)
        sys.exit(0)
    
    # Naechste Termine anzeigen
    if args.list_next:
        scraper.list_next_termine(limit=args.list_next, format=args.format)
        sys.exit(0)
    
    # ========== EVENTS ==========
    
    # Event hinzufügen
    if args.add_event:
        date, time, title, location, description = args.add_event
        result = scraper.add_event(title, date, time, location, description)
        if result['status'] == 'new':
            print(f"Event added: {title} on {date}")
        elif result['status'] == 'updated':
            print(f"Event updated: {title} on {date}")
        else:
            print(f"Event unchanged: {title}")
        
        if result['status'] in ['new', 'updated']:
            ics_file = scraper.generate_ics(args.ics)
            print(f"ICS calendar updated: {ics_file}")
            sys.exit(1)  # Changes made
        sys.exit(0)
    
    # Events löschen
    if args.delete_event:
        deleted_count = scraper.delete_events(args.delete_event)
        if deleted_count > 0:
            ics_file = scraper.generate_ics(args.ics)
            print(f"ICS calendar updated: {ics_file}")
            sys.exit(1)  # Changes made
        sys.exit(0)
    
    # Events auflisten
    if args.list_events:
        scraper.list_events(limit=args.limit, future_only=False)
        sys.exit(0)
    
    # Nächste Events
    if args.list_events_next:
        scraper.list_events(limit=args.list_events_next, future_only=True)
        sys.exit(0)
    
    # ========== STATISTICS ==========
    
    # Saison-Statistiken
    if args.stats:
        scraper.print_season_stats()
        sys.exit(0)
    
    # Statistiken scrapen
    if args.scrape_stats:
        count = scraper.scrape_all_played_games_stats()
        sys.exit(1 if count > 0 else 0)
    
    # Spiel-Statistiken
    if args.game_stats:
        stats = scraper.get_game_stats(args.game_stats)
        if stats:
            print(f"\n=== {stats['home']} vs {stats['guest']} ({stats['date']}) ===\n")
            
            if stats['team_stats']:
                for team, ts in stats['team_stats'].items():
                    print(f"{team}:")
                    if ts['power_play_attempts'] > 0:
                        pct = ts['power_play_goals'] / ts['power_play_attempts'] * 100
                        print(f"  Power Play: {ts['power_play_goals']}/{ts['power_play_attempts']} ({pct:.0f}%)")
                    if ts['penalty_kill_attempts'] > 0:
                        pct = ts['penalty_kill_success'] / ts['penalty_kill_attempts'] * 100
                        print(f"  Penalty Kill: {ts['penalty_kill_success']}/{ts['penalty_kill_attempts']} ({pct:.0f}%)")
            
            if stats['player_stats']:
                print("\nPlayer Stats:")
                for ps in stats['player_stats']:
                    if ps['goals'] > 0 or ps['exclusions'] > 0:
                        print(f"  {ps['name']} ({ps['team']}): {ps['goals']}G {ps['exclusions']}E")
        else:
            print(f"No stats found for game ID {args.game_stats}")
        sys.exit(0)
    
    # ========== COMBINED ==========
    
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
        results = scraper.save_termine([termin])
        if results['new']:
            print(f"Added: {results['new'][0]['match']}")
            ics_file = scraper.generate_ics(args.ics)
            print(f"ICS calendar updated: {ics_file}")
            sys.exit(1)  # Changes made
        elif results['updated']:
            print(f"Updated: {results['updated'][0]['match']}")
            ics_file = scraper.generate_ics(args.ics)
            print(f"ICS calendar updated: {ics_file}")
            sys.exit(1)  # Changes made
        sys.exit(0)  # No changes
    
    # Standard oder manuelle Eingabe
    exit_code = scraper.run(scrape=args.enable_scraping, add_new=args.add_new, enable_scraping=args.enable_scraping)
    sys.exit(exit_code)

if __name__ == "__main__":
    main()