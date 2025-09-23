#!/usr/bin/env python3
"""
Upload ICS zu GitHub Gist - Private Repo, Public Kalender
"""

import requests
import json
import os
from datetime import datetime

class GistUploader:
    def __init__(self):
        self.token = None
        self.gist_id = None
        
    def setup(self):
        """Setup für GitHub Gist"""
        print("🔒 GitHub Gist Setup (Private Repo + Public Kalender)")
        print("=" * 60)
        
        print("1. Gehen Sie zu: https://github.com/settings/tokens")
        print("2. 'Generate new token (classic)'")
        print("3. Wählen Sie nur 'gist' permission")
        print("4. Kopieren Sie das Token")
        
        self.token = input("\nIhr GitHub Token: ").strip()
        if not self.token:
            print("❌ Token erforderlich!")
            return False
            
        # Teste Token
        headers = {"Authorization": f"token {self.token}"}
        response = requests.get("https://api.github.com/user", headers=headers)
        
        if response.status_code != 200:
            print("❌ Ungültiges Token!")
            return False
            
        user_info = response.json()
        username = user_info["login"]
        print(f"✅ Angemeldet als: {username}")
        
        # Speichere Konfiguration
        config = {
            "token": self.token,
            "username": username
        }
        
        with open("gist_config.json", "w") as f:
            json.dump(config, f, indent=2)
            
        print("✅ Gist-Konfiguration gespeichert!")
        return True
    
    def upload_calendar(self, ics_file):
        """Lädt ICS-Datei zu GitHub Gist hoch"""
        if not os.path.exists("gist_config.json"):
            print("❌ Gist nicht konfiguriert! Führen Sie setup() aus.")
            return False
            
        # Lade Konfiguration
        with open("gist_config.json", "r") as f:
            config = json.load(f)
            
        self.token = config["token"]
        username = config["username"]
        
        # Lade ICS-Datei
        if not os.path.exists(ics_file):
            print(f"❌ ICS-Datei nicht gefunden: {ics_file}")
            return False
            
        with open(ics_file, "r", encoding="utf-8") as f:
            ics_content = f.read()
            
        # Lade existierende Gist-ID falls vorhanden
        gist_id = None
        if os.path.exists("gist_id.txt"):
            with open("gist_id.txt", "r") as f:
                gist_id = f.read().strip()
        
        headers = {
            "Authorization": f"token {self.token}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        gist_data = {
            "description": f"SGW Essen Wasserball Kalender - {datetime.now().strftime('%d.%m.%Y %H:%M')}",
            "public": True,  # Public Gist für öffentlichen Zugang
            "files": {
                "sgw_termine.ics": {
                    "content": ics_content
                }
            }
        }
        
        if gist_id:
            # Update existierende Gist
            url = f"https://api.github.com/gists/{gist_id}"
            response = requests.patch(url, headers=headers, json=gist_data)
        else:
            # Neue Gist erstellen
            url = "https://api.github.com/gists"
            response = requests.post(url, headers=headers, json=gist_data)
        
        if response.status_code in [200, 201]:
            gist_info = response.json()
            gist_id = gist_info["id"]
            
            # Speichere Gist-ID für Updates
            with open("gist_id.txt", "w") as f:
                f.write(gist_id)
                
            # Raw URL für direkten Kalender-Zugang
            raw_url = f"https://gist.githubusercontent.com/{username}/{gist_id}/raw/sgw_termine.ics"
            
            print("✅ Kalender erfolgreich zu GitHub Gist hochgeladen!")
            print(f"📱 Android Kalender URL: {raw_url}")
            print(f"🌐 Gist URL: {gist_info['html_url']}")
            
            # Speichere URL
            with open("calendar_url.txt", "w") as f:
                f.write(raw_url)
                
            return raw_url
        else:
            print(f"❌ Upload fehlgeschlagen: {response.status_code}")
            print(response.text)
            return False

def setup_gist():
    """Setup-Assistent für GitHub Gist"""
    uploader = GistUploader()
    return uploader.setup()

if __name__ == "__main__":
    setup_gist()
