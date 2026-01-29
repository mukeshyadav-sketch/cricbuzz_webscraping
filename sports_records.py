
import requests
from bs4 import BeautifulSoup
import sqlite3
import re
import time
from typing import List, Dict, Optional

class SportsMatchScraper:
    """Scraper for specific cricket match data with refined schema"""
    
    BASE_URL = "https://www.cricbuzz.com"
    
    # Specific list of matches provided by user
    MATCH_IDS = [
        116441, 121389, 121400, 121406, 133000, 133011, 133017, 
        137826, 137831, 140537, 140548, 140559
    ]
    
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        
    def fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        try:
            time.sleep(0.5) 
            response = self.session.get(url, timeout=30)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except Exception as e:
            print(f"❌ Error fetching {url}: {e}")
            return None

    def clean_text(self, text: str) -> str:
        if not text: return ""
        return re.sub(r"\s+", " ", text).strip()

    def get_match_details(self, match_id: int) -> Dict:
        """Fetch details using match_id"""
        # We need a slug to form a valid URL for standard pages, 
        # BUT for some pages we might get redirected or we can find it.
        # Strategy: Use the 'squads' URL strategy or similar to get the title/header first?
        # Actually, simpler: Use the live-scores URL with a dummy slug, catch redirect or parse.
        
        url = f"{self.BASE_URL}/live-cricket-scores/{match_id}/match"
        soup = self.fetch_page(url)
        
        data = {
            "match_id": str(match_id),
            "team1": "Unknown",
            "team2": "Unknown",
            "winner": None,
            "venue": "Unknown"
        }
        
        if not soup: return data
        
        # 1. Extract Teams from Title or Header
        # Title format usually: "Team A vs Team B, Match Description..."
        title = soup.title.string if soup.title else ""
        data["team1"], data["team2"] = self.parse_teams(title)
        
        # 2. Venue
        # Look for venue link
        venue_el = soup.select_one('a[href*="/venues/"]')
        if venue_el:
            data["venue"] = self.clean_text(venue_el.get_text())
        else:
             # Fallback
             match_info = soup.select_one(".cb-nav-subhdr") # sometimes holds venue?
             pass

        # 3. Winner
        # Look for the result status
        # <div class="cb-col cb-col-100 cb-min-stts cb-text-complete">Team A won by X runs</div>
        result_el = soup.select_one(".cb-text-complete")
        if result_el:
            result_text = self.clean_text(result_el.get_text())
            data["winner"] = self.extract_winner_name(result_text, data["team1"], data["team2"])
        else:
            # Try finding "won by" text node
            won_node = soup.find(string=re.compile(r"won by", re.I))
            if won_node:
                data["winner"] = self.extract_winner_name(won_node.strip(), data["team1"], data["team2"])

        return data


    def parse_teams(self, title: str):
        # "India vs New Zealand, 3rd T20I - Live Cricket Score..."
        t1, t2 = "Unknown", "Unknown"
        try:
            if " vs " in title:
                parts = title.split(" vs ")
                t1 = parts[0].strip()
                # Cleanup specific prefix seen in logs
                t1 = t1.replace("Cricket commentary | ", "")
                t1 = t1.replace("Live Cricket Score, ", "")
                
                remainder = parts[1]
                # Split by comma or known separators
                separators = [",", " Live ", " Match ", " Scorecard", " - Live"]
                idx = len(remainder)
                for sep in separators:
                    if sep in remainder:
                        i = remainder.find(sep)
                        if i != -1 and i < idx:
                            idx = i
                t2 = remainder[:idx].strip()
        except:
            pass
        return t1, t2


    def extract_winner_name(self, result_text: str, t1: str, t2: str) -> str:
        """
        'India won by 7 wkts' -> 'India'
        'Match tied' -> 'Tied'
        """
        txt = result_text.lower()
        
        # Check explicit win
        if "won by" in txt:
            # Usually starts with the team name
            # "India won by..."
            if t1.lower() in txt:
                return t1
            if t2.lower() in txt:
                return t2
            
            # Heuristic: split by " won by "
            parts = result_text.split(" won by ")
            return parts[0].strip()
            
        if "tied" in txt:
            return "Tied"
            
        if "no result" in txt:
            return "No Result"

        # Fallback: if text starts with team name
        if t1 and result_text.startswith(t1): return t1
        if t2 and result_text.startswith(t2): return t2
        
        return result_text # Return full string if unsure, better than null

    def scrape(self) -> List[Dict]:
        matches = []
        for mid in self.MATCH_IDS:
            print(f"Processing {mid}...")
            details = self.get_match_details(mid)
            matches.append(details)
            print(f"   -> {details['team1']} vs {details['team2']} | Winner: {details['winner']}")
        return matches


class SportsMatchRecords:
    def __init__(self, db_path: str = "cricbuzz.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        # Re-create table with new schema
        conn.execute("DROP TABLE IF EXISTS sports_match_records_new") 
        # Check if we should migrate data? No, we are re-scraping specific IDs.
        # But wait, other tables reference 'sports_match_records' (match_id).
        # Dropping the table might violate FK constraints if enforced, 
        # but SQLite default usually doesn't enforce unless enabled. 
        # Safest is to Drop and Recreate.
        
        # NOTE: If we drop the main table, we might lose data if not careful.
        # But here we are re-populating the specific user list.
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sports_match_records (
                match_id TEXT PRIMARY KEY,
                team1 TEXT,
                team2 TEXT,
                winner TEXT,
                venue TEXT
            )
        """)
        
        # Check if columns exist (migration hack for existing table)
        # If table exists from previous run, it might have 'teams' col.
        # Let's check schema.
        try:
            cur = conn.execute("SELECT * FROM sports_match_records LIMIT 0")
            cols = [description[0] for description in cur.description]
            if "teams" in cols and "team1" not in cols:
                print("⚠️ Migrating schema: Dropping old table to recreate with new columns.")
                conn.execute("DROP TABLE sports_match_records")
                conn.execute("""
                    CREATE TABLE sports_match_records (
                        match_id TEXT PRIMARY KEY,
                        team1 TEXT,
                        team2 TEXT,
                        winner TEXT,
                        venue TEXT
                    )
                """)
        except Exception as e:
            print(e)
            
        conn.commit()
        conn.close()

    def save_matches(self, matches: List[Dict]):
        conn = sqlite3.connect(self.db_path)
        # We use INSERT OR REPLACE to update existing entries
        conn.executemany("""
            INSERT OR REPLACE INTO sports_match_records (match_id, team1, team2, winner, venue)
            VALUES (:match_id, :team1, :team2, :winner, :venue)
        """, matches)
        conn.commit()
        conn.close()

    def display(self):
        conn = sqlite3.connect(self.db_path)
        rows = conn.execute("SELECT * FROM sports_match_records").fetchall()
        print("\n" + "="*100)
        print(f"{'ID':<10} {'TEAM 1':<20} {'TEAM 2':<20} {'WINNER':<20} {'VENUE'}")
        print("-" * 100)
        for r in rows:
            # Row index depends on schema order: match_id, team1, team2, winner, venue
            print(f"{r[0]:<10} {r[1]:<20} {r[2]:<20} {r[3]:<20} {r[4]}")
        print("="*100)
        conn.close()

if __name__ == "__main__":
    scraper = SportsMatchScraper()
    data = scraper.scrape()
    
    db = SportsMatchRecords()
    db.save_matches(data)
    db.display()
