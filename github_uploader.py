#!/usr/bin/env python3
"""
Automatischer Upload zu GitHub Pages - einfach und zuverlässig
"""

import os
import subprocess
import json
from datetime import datetime

class GitHubPagesUploader:
    def __init__(self, repo_name="sgw-kalender"):
        self.repo_name = repo_name
        self.github_username = None
        
    def setup_repository(self):
        """Erstellt GitHub Repository für Kalender"""
        print("🔧 GitHub Repository Setup")
        print("=" * 50)
        
        # GitHub Username abfragen
        self.github_username = input("Ihr GitHub Username: ").strip()
        if not self.github_username:
            print("❌ GitHub Username erforderlich!")
            return False
        
        print(f"\n📁 Repository wird erstellt: {self.github_username}/{self.repo_name}")
        print("\n🔑 Sie benötigen ein GitHub Personal Access Token:")
        print("1. Gehen Sie zu: https://github.com/settings/tokens")
        print("2. 'Generate new token (classic)'")
        print("3. Wählen Sie 'repo' permissions")
        print("4. Kopieren Sie das Token")
        
        token = input("\nIhr GitHub Token: ").strip()
        if not token:
            print("❌ GitHub Token erforderlich!")
            return False
        
        # Speichere Konfiguration
        config = {
            "username": self.github_username,
            "repo": self.repo_name,
            "token": token
        }
        
        with open("github_config.json", "w") as f:
            json.dump(config, f, indent=2)
        
        print("✅ Konfiguration gespeichert!")
        
        # Repository erstellen
        return self._create_repository(token)
    
    def _create_repository(self, token):
        """Erstellt das GitHub Repository"""
        try:
            # Git Repository initialisieren
            if not os.path.exists(".git"):
                subprocess.run(["git", "init"], check=True)
                subprocess.run(["git", "branch", "-M", "main"], check=True)
            
            # README erstellen
            readme_content = f"""# SGW Essen Wasserball Kalender

Automatisch generierter Kalender für SG Wasserball Essen Termine.

## 📱 Kalender abonnieren:

**Android/iOS/Desktop:**
```
https://{self.github_username}.github.io/{self.repo_name}/sgw_termine.ics
```

## 🔄 Letztes Update:
{datetime.now().strftime('%d.%m.%Y %H:%M')}

---
*Automatisch generiert vom SGW Termine Scraper*
"""
            
            with open("README.md", "w", encoding="utf-8") as f:
                f.write(readme_content)
            
            # Index.html für GitHub Pages
            index_content = f"""<!DOCTYPE html>
<html>
<head>
    <title>SGW Essen Kalender</title>
    <meta charset="utf-8">
</head>
<body>
    <h1>SGW Essen Wasserball Kalender</h1>
    <p>Kalender-URL für Ihre Apps:</p>
    <code>https://{self.github_username}.github.io/{self.repo_name}/sgw_termine.ics</code>
    <br><br>
    <a href="sgw_termine.ics">📅 ICS-Datei herunterladen</a>
</body>
</html>"""
            
            with open("index.html", "w", encoding="utf-8") as f:
                f.write(index_content)
            
            # Git Konfiguration
            subprocess.run(["git", "add", "."], check=True)
            subprocess.run(["git", "commit", "-m", "Initial commit"], check=True)
            subprocess.run(["git", "remote", "add", "origin", f"https://{token}@github.com/{self.github_username}/{self.repo_name}.git"], check=True)
            subprocess.run(["git", "push", "-u", "origin", "main"], check=True)
            
            print(f"✅ Repository erstellt: https://github.com/{self.github_username}/{self.repo_name}")
            print(f"🌐 Kalender URL: https://{self.github_username}.github.io/{self.repo_name}/sgw_termine.ics")
            
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Git-Fehler: {e}")
            return False
    
    def upload_calendar(self, ics_file):
        """Lädt ICS-Datei zu GitHub hoch"""
        if not os.path.exists("github_config.json"):
            print("❌ GitHub nicht konfiguriert! Führen Sie setup_repository() aus.")
            return False
        
        try:
            # Lade Konfiguration
            with open("github_config.json", "r") as f:
                config = json.load(f)
            
            # Kopiere ICS-Datei
            if os.path.exists(ics_file):
                with open(ics_file, "r", encoding="utf-8") as f:
                    ics_content = f.read()
                
                with open("sgw_termine.ics", "w", encoding="utf-8") as f:
                    f.write(ics_content)
            
            # Update README mit Timestamp
            readme_content = f"""# SGW Essen Wasserball Kalender

Automatisch generierter Kalender für SG Wasserball Essen Termine.

## 📱 Kalender abonnieren:

**Android/iOS/Desktop:**
```
https://{config['username']}.github.io/{config['repo']}/sgw_termine.ics
```

## 🔄 Letztes Update:
{datetime.now().strftime('%d.%m.%Y %H:%M')}

---
*Automatisch generiert vom SGW Termine Scraper*
"""
            
            with open("README.md", "w", encoding="utf-8") as f:
                f.write(readme_content)
            
            # Git Push
            subprocess.run(["git", "add", "."], check=True)
            subprocess.run(["git", "commit", "-m", f"Update calendar {datetime.now().strftime('%Y-%m-%d %H:%M')}"], check=True)
            subprocess.run(["git", "push"], check=True)
            
            calendar_url = f"https://{config['username']}.github.io/{config['repo']}/sgw_termine.ics"
            
            print("✅ Kalender automatisch hochgeladen!")
            print(f"📱 Android Kalender URL: {calendar_url}")
            
            # Speichere URL für später
            with open("calendar_url.txt", "w") as f:
                f.write(calendar_url)
            
            return calendar_url
            
        except Exception as e:
            print(f"❌ Upload fehlgeschlagen: {e}")
            return False

def setup_github_calendar():
    """Setup-Assistent für GitHub Kalender"""
    print("🚀 GitHub Pages Kalender Setup")
    print("=" * 50)
    print("Vorteile:")
    print("✅ Kostenlos und zuverlässig")
    print("✅ Funktioniert auf allen Geräten")
    print("✅ Automatische Updates")
    print("✅ Keine API-Keys nötig")
    print()
    
    uploader = GitHubPagesUploader()
    return uploader.setup_repository()

if __name__ == "__main__":
    setup_github_calendar()
