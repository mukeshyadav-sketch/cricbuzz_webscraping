
import requests
from bs4 import BeautifulSoup
import sqlite3
import re
import time

DB_PATH = "cricbuzz.db"
BASE_URL = "https://www.cricbuzz.com"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS leaders (
        match_id INTEGER,
        team TEXT,
        player_id INTEGER,
        player_name TEXT,
        role TEXT,
        FOREIGN KEY (match_id) REFERENCES master(match_id),
        FOREIGN KEY (player_id) REFERENCES players(player_id)
    )
    """)
    conn.commit()
    conn.close()

def get_matches():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT match_id, team1, team2 FROM master")
    rows = cursor.fetchall()
    conn.close()
    return rows

def extract_id_from_url(url):
    # /profiles/1114/paul-stirling
    match = re.search(r"/profiles/(\d+)/", url)
    if match:
        return int(match.group(1))
    return None

def process_match(match_id, team1, team2):
    # Scorecard page is better for finding (c)
    url = f"https://www.cricbuzz.com/live-cricket-scorecard/{match_id}/match"
    print(f"Checking {url}...")
    
    try:
        r = requests.get(url, headers=HEADERS)
        if r.status_code != 200:
            print(f"   ⚠️ Status {r.status_code}. Trying alternate...")
            url2 = f"https://www.cricbuzz.com/live-cricket-scorecard/{match_id}/something"
            r = requests.get(url2, headers=HEADERS)
            if r.status_code != 200: return []
            
        soup = BeautifulSoup(r.text, "html.parser")
        
        captains = []
        
        # In scorecard, players are listed in rows. 
        # Look for "(c)" in the text of the link or cell.
        
        # Find all anchors with profiles
        anchors = soup.select("a[href^='/profiles/']")
        
        for a in anchors:
            text = a.get_text(strip=True)
            is_captain = False
            
            # Check 1: Inside anchor text: "Name (c)"
            if re.search(r"\(\s*c\s*\)", text, re.IGNORECASE):
                is_captain = True
            
            # Check 2: Immediate text sibling: <a...>Name</a> (c)
            # We need to be careful not to consume the whole parent text if it's shared.
            # Only check siblings if parent has multiple links? 
            # Or just check string/stripped strings.
            elif a.next_sibling and isinstance(a.next_sibling, str):
                if re.search(r"\(\s*c\s*\)", a.next_sibling, re.IGNORECASE):
                    is_captain = True
            
            # Check 3: Parent text, BUT ONLY if parent doesn't contain other profile links
            # This handles cases where structure is <div>Name (c)</div>
            elif a.parent and len(a.parent.find_all("a", href=re.compile(r"^/profiles/"))) == 1:
                parent_text = a.parent.get_text(strip=True)
                if re.search(r"\(\s*c\s*\)", parent_text, re.IGNORECASE):
                    is_captain = True

            if is_captain:
                # Found a captain
                pid = extract_id_from_url(a['href'])
                
                # Check team via match_squads table (most reliable)
                if pid:
                    team_for_player = get_team_for_player(match_id, pid)
                    if not team_for_player:
                        team_for_player = "Unknown"
                    
                    # Clean name for storage: "Name (c)" -> "Name"
                    # Also remove (wk) if present
                    clean_name = re.sub(r"\s*\((?:c|wk|c\s*\&\s*wk|wk\s*\&\s*c)\)", "", text, flags=re.IGNORECASE).strip()
                        
                    print(f"   Found Captain: {clean_name} (ID: {pid}) -> Team: {team_for_player}")
                    captains.append((match_id, team_for_player, pid, clean_name))
                    
        return captains
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return []

def get_team_for_player(match_id, player_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT team FROM match_players WHERE match_id=? AND player_id=?", (match_id, player_id))
    row = cursor.fetchone()
    conn.close()
    if row: return row[0]
    return None

def save_leaders(leaders):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Ensure this is running AFTER squads populated match_players.
    # We update the is_captain flag.
    
    count = 0
    for mid, team, pid, pname in leaders:
        cursor.execute("UPDATE match_players SET is_captain=1 WHERE match_id=? AND player_id=?", (mid, pid))
        if cursor.rowcount > 0:
            count += 1
        
    conn.commit()
    print(f"Updated {count} captain flags.")
    conn.close()

def main():
    init_db()
    matches = get_matches()
    print(f"Scanning {len(matches)} matches for captains...")
    
    all_leaders = []
    
    for mid, t1, t2 in matches:
        leaders = process_match(mid, t1, t2)
        all_leaders.extend(leaders)
        time.sleep(1.0)
        
    save_leaders(all_leaders)
    print(f"Saved {len(all_leaders)} captain records.")

if __name__ == "__main__":
    main()
