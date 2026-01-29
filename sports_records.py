
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
            "venue": "Unknown",
            "match_name": "Unknown"
        }
        
        if not soup: return data
        
        # 1. Extract Teams and Match Name from Title or Header
        # Title format: "India vs Sri Lanka, 3rd T20I - Live Cricket Score..."
        # or header: "India vs Sri Lanka, 3rd T20I"
        title = soup.title.string if soup.title else ""
        data["team1"], data["team2"] = self.parse_teams(title)
        
        # Parse Match Name (e.g. 3rd T20I)
        # Usually found in breadcrumb or subheader or title parts
        # Try splitting title by comma
        if "," in title:
            parts = title.split(",")
            if len(parts) > 1:
                 # "Ind vs SL, 3rd T20I - Live..."
                 sub = parts[1]
                 if "-" in sub:
                     data["match_name"] = sub.split("-")[0].strip()
                 else:
                     data["match_name"] = sub.strip()

        
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
        conn.execute("DROP TABLE IF EXISTS master_new") 
        # Check if we should migrate data? No, we are re-scraping specific IDs.
        # But wait, other tables reference 'master' (match_id).
        # Dropping the table might violate FK constraints if enforced, 
        # but SQLite default usually doesn't enforce unless enabled. 
        # Safest is to Drop and Recreate.
        
        # NOTE: If we drop the main table, we might lose data if not careful.
        # But here we are re-populating the specific user list.
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS master (
                match_id INTEGER PRIMARY KEY,
                team1 TEXT,
                team2 TEXT,
                winner TEXT,
                venue TEXT,
                match_name TEXT
            )
        """)
        
        # Check if columns exist (migration hack for existing table)
        # If table exists from previous run, it might have 'teams' col.
        # Let's check schema.
        try:
            cur = conn.execute("SELECT * FROM master LIMIT 0")
            cols = [description[0] for description in cur.description]
            if "teams" in cols and "team1" not in cols:
                print("⚠️ Migrating schema: Dropping old table to recreate with new columns.")
                conn.execute("DROP TABLE master")
                conn.execute("""
                    CREATE TABLE master (
                        match_id INTEGER PRIMARY KEY,
                        team1 TEXT,
                        team2 TEXT,
                        winner TEXT,
                        venue TEXT,
                        match_name TEXT
                    )
                """)
        except Exception as e:
            print(e)
            
        conn.commit()
        conn.close()

    def save_matches(self, matches: List[Dict]):
        conn = sqlite3.connect(self.db_path)
        # We use INSERT OR REPLACE to update existing entries
        # Ensure match_name is passed
        conn.executemany("""
            UPDATE master 
            SET team1=:team1, team2=:team2, winner=:winner, venue=:venue, match_name=:match_name
            WHERE match_id=:match_id
        """, matches)
        
        # For new records or if update missed (though we initialized with migration)
        # It's better to use INSERT OR REPLACE but we need to list all cols.
        # Since we added a col via ALTER, existing rows have NULL.
        # The UPDATE above handles it.
        # But for full upsert:
        conn.executemany("""
            INSERT OR REPLACE INTO master (match_id, team1, team2, winner, venue, match_name)
            VALUES (:match_id, :team1, :team2, :winner, :venue, :match_name)
        """, matches)
        
        conn.commit()
        conn.close()

    def display(self):
        conn = sqlite3.connect(self.db_path)
        rows = conn.execute("SELECT * FROM master").fetchall()
        print("\n" + "="*120)
        print(f"{'ID':<10} {'MATCH':<15} {'TEAM 1':<20} {'TEAM 2':<20} {'WINNER':<20} {'VENUE'}")
        print("-" * 120)
        for r in rows:
            # Columns: match_id, team1, team2, winner, venue, match_name (added at end)
            # Careful with index if ALTER added it at the end
            # r[0]=id, r[1]=t1, r[2]=t2, r[3]=win, r[4]=ven, r[5]=mname
            mname = r[5] if len(r) > 5 else "N/A"
            print(f"{r[0]:<10} {mname:<15} {r[1]:<20} {r[2]:<20} {r[3]:<20} {r[4]}")
        print("="*120)
        conn.close()

if __name__ == "__main__":
    scraper = SportsMatchScraper()
    data = scraper.scrape()
    
    db = SportsMatchRecords()
    db.save_matches(data)
    db.display()
