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
                last_change TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_event_id ON games(event_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON games(date)')
        
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
                                    ',' in ref_name and  # Schiedsrichter haben meist Format "Nachname, Vorname"
                                    not any(word in ref_name.lower() for word in [
                                        'essen', 'oberhausen', 'vs', 'mehr', 'spiel', 'solingen', 
                                        'wuppertal', 'bochum', 'duisburg', 'rheinhausen', 'kevelaer',
                                        'tpsk', 'sgw', 'sv', 'asc', 'wsg', 'blau-weiß'
                                    ])):
                                    all_ref_names.append(ref_name)
            
            # Entferne Duplikate
            unique_refs = list(dict.fromkeys(all_ref_names))
            
            if unique_refs:
                referee_info['referee1'] = unique_refs[0] if len(unique_refs) > 0 else ''
                referee_info['referee2'] = unique_refs[1] if len(unique_refs) > 1 else ''
                return referee_info
            
            return None
            
        except Exception as e:
            return None
    
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
            elif competition_type == 'verbandsliga':
                comp_prefix = "[VERBANDSLIGA]"
            elif competition_type == 'ruhrgebietsliga':
                comp_prefix = "[RUHRGEBIETSLIGA]"
            else:
                comp_prefix = f"[{competition_type.upper()}]"
            
            # Prüfe ob Competition-Info bereits vorhanden ist
            existing_prefixes = ["[LIGA]", "[POKAL]", "[VERBANDSLIGA]", "[RUHRGEBIETSLIGA]"]
            has_prefix = any(final_description.startswith(prefix) for prefix in existing_prefixes)
            
            if not has_prefix:
                final_description = f"{comp_prefix}\n{final_description}"
            
            # Prüfe ob Event bereits existiert
            cursor.execute('SELECT home, guest, date, time, location, description FROM games WHERE event_id = ?', (event_id,))
            existing = cursor.fetchone()
            
            if existing:
                # Vergleiche Daten um zu prüfen ob sich etwas geändert hat
                old_home, old_guest, old_date, old_time, old_location, old_description = existing
                
                changes = []
                if old_date != termin.get('date', ''):
                    changes.append(f"date: {old_date} -> {termin.get('date', '')}")
                if old_time != termin.get('time', ''):
                    changes.append(f"time: {old_time} -> {termin.get('time', '')}")
                if old_location != final_location:
                    changes.append("location updated")
                if old_description != final_description:
                    # Check if result changed
                    old_result = ""
                    new_result = ""
                    if old_description:
                        for line in old_description.split('\n'):
                            if line.startswith('Result:'):
                                old_result = line.replace('Result:', '').strip()
                    if final_description:
                        for line in final_description.split('\n'):
                            if line.startswith('Result:'):
                                new_result = line.replace('Result:', '').strip()
                    if old_result != new_result:
                        changes.append(f"result: {old_result} -> {new_result}")
                
                # Aktualisiere bestehenden Eintrag
                cursor.execute('''
                    UPDATE games 
                    SET home = ?, guest = ?, date = ?, time = ?, location = ?, description = ?, 
                        last_change = CURRENT_TIMESTAMP
                    WHERE event_id = ?
                ''', (
                    home_clean,
                    guest_clean,
                    termin.get('date', ''),
                    termin.get('time', ''),
                    final_location,
                    final_description,
                    event_id
                ))
                
                if changes:
                    updated_games.append({
                        'match': f"{home_clean} vs {guest_clean}",
                        'date': termin.get('date', ''),
                        'changes': changes
                    })
                else:
                    unchanged_games.append(f"{home_clean} vs {guest_clean}")
            else:
                # Füge neuen Eintrag hinzu
                cursor.execute('''
                    INSERT INTO games 
                    (event_id, home, guest, date, time, location, description)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    event_id,
                    home_clean,
                    guest_clean,
                    termin.get('date', ''),
                    termin.get('time', ''),
                    final_location,
                    final_description
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
    
    def generate_ics(self, output_file: str = "sgw_termine.ics") -> str:
        """Generiert ICS-Kalenderdatei"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, event_id, home, guest, date, time, location, description
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
            (id, event_id, home, guest, date, time, location, description) = termin
            
            uid = f"sgw-{event_id}@essen.de"
            # Extrahiere Competition-Info aus Description für Titel
            # Kalender-Titel ohne Competition-Tags
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
            
            # Verwende Description direkt (bereits formatiert mit Result/Refs)
            # Ersetze \n durch \\n für korrekte ICS-Formatierung
            ics_description = description.replace('\n', '\\n') if description else ""
            
            # Location: Kombiniere Adresse und Google Maps Link für bessere Kalender-Integration
            if location and '|' in location:
                parts = location.split('|', 1)
                address = parts[0].strip()
                maps_link = parts[1].strip()
                if address and maps_link:
                    # Format: "Address\nGoogle Maps: Link" für bessere Darstellung in Kalendern
                    location_text = f"{address}\\nGoogle Maps: {maps_link}"
                else:
                    location_text = address if address else maps_link
            else:
                location_text = location.strip() if location else "TBA"
            
            location_text = location_text if location_text else "TBA"
            
            # Event
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
        
        ics_lines.append("END:VCALENDAR")
        return "\n".join(ics_lines)
    
    def list_termine(self, limit: int = 10):
        """Zeigt Termine aus der Datenbank"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, date, time, home, guest, location, description, last_change
            FROM games 
            ORDER BY date DESC, time DESC
            LIMIT ?
        ''', (limit,))
        
        termine = cursor.fetchall()
        conn.close()
        
        if not termine:
            print("No games found in database.")
            print("Use --add to add games")
            return
        
        print(f"\n=== {len(termine)} Termine ===")
        print("-" * 80)
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
                elif description.startswith("[POKAL]"):
                    comp_str = "[POKAL] "
                elif description.startswith("[LIGA]"):  # Fallback für alte Einträge
                    comp_str = "[LIGA] "
            
            # Location: Zeige nur Adress-Teil (vor "|"), Maps-Link wird separat angezeigt
            display_location = location.split('|')[0].strip() if location else ""
            location_str = f" @ {display_location}" if display_location else ""
            maps_str = f" [Maps]" if '|' in location else ""
            
            print(f"ID {id:3d} | {comp_str}{date}{time_str}{location_str}{maps_str}")
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
            print("\n" + "="*60)
            print(f"SUMMARY: {len(alle_termine)} games scraped")
            print("="*60)
            
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
            
            print("\n" + "="*60)
        
        # Generiere ICS nur bei Änderungen
        if has_changes:
            ics_file = self.generate_ics()
            print(f"\nICS calendar updated: {ics_file}")
        else:
            print("\nNo changes detected - ICS calendar not regenerated")
        
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
  python sgw_essen_scraper.py --add "20.12.2025" "15:00" "SGW Essen" "Weihnachtsfeier" ""
  python sgw_essen_scraper.py -new
  python sgw_essen_scraper.py --list
  python sgw_essen_scraper.py --delete 5 7 9
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
    parser.add_argument('--limit', type=int, default=10,
                       help='Anzahl der anzuzeigenden Termine')
    parser.add_argument('--delete', nargs='+', type=int, metavar='ID',
                       help='Löscht Termine mit den angegebenen IDs und berechnet IDs neu')
    
    args = parser.parse_args()
    
    scraper = SGWTermineScraper(db_path=args.db)
    
    # Spiele löschen
    if args.delete:
        deleted_count = scraper.delete_games_and_recalculate_ids(args.delete)
        if deleted_count > 0:
            # Generiere ICS nach dem Löschen
            ics_file = scraper.generate_ics(args.ics)
            print(f"ICS calendar updated: {ics_file}")
            sys.exit(1)  # Changes made
        sys.exit(0)  # No changes
    
    # Liste anzeigen
    if args.list:
        scraper.list_termine(limit=args.limit)
        sys.exit(0)
    
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