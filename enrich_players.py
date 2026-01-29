
import sqlite3
import requests
from bs4 import BeautifulSoup
import re
import time

DB_PATH = "cricbuzz.db"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def get_players_missing_info():
    """Fetch players who don't have country set yet (or missing other info)."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # We prioritize those with missing country, but really we want to fill any missing gap
    cursor.execute("SELECT player_id, name FROM players WHERE country IS NULL")
    players = cursor.fetchall()
    conn.close()
    return players

def clean_text(text):
    if not text: return None
    return re.sub(r"\s+", " ", text).strip()

def fetch_player_details(player_id, name):
    # Construct URL: cricbuzz.com requires a slug, but usually redirects correct ID
    slug = name.lower().replace(" ", "-")
    url = f"https://www.cricbuzz.com/profiles/{player_id}/{slug}"
    
    print(f"   Fetching {url}...")
    
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code != 200:
            print(f"   ❌ Status {r.status_code}")
            return None, None, None, None
            
        soup = BeautifulSoup(r.text, "html.parser")
        
        born_val = None
        place_val = None
        role_val = None
        country_val = None
        
        # 1. Country (Header Badge)
        # Strategy: Look for the flag or the text next to it in the header.
        # Mobile view: <span class="text-white text-[10px]">Country</span>
        # Desktop view: <span class="text-base text-gray-800">Country</span> inside a rounded-lg container
        
        # Try finding the country by common classes seen in dump
        # Option A: The text-base one near the name
        country_node = soup.find("span", class_="text-base text-gray-800")
        if country_node:
             country_val = clean_text(country_node.get_text())
        else:
             # Option B: The white text one
             country_node = soup.find("span", class_="text-white text-[10px]")
             if country_node:
                 country_val = clean_text(country_node.get_text())

        # Helper to find value by label
        def find_value_by_label(label_text):
            node = soup.find(string=re.compile(label_text, re.I))
            if node:
                row = node.parent
                if row:
                    container = row.parent
                    if container:
                        cols = container.find_all("div", recursive=False)
                        if len(cols) >= 2:
                            return clean_text(cols[1].get_text())
            return None

        born_val = find_value_by_label("Born")
        if born_val:
            # Parse date immediately: September 03, 1990 (35 years) -> 03/09/1990
            clean_str = re.sub(r"\s*\(.*\)", "", born_val).strip()
            try:
                dt_obj = datetime.datetime.strptime(clean_str, "%B %d, %Y")
                born_val = dt_obj.strftime("%d/%m/%Y")
            except Exception:
                pass # Keep original if parse fails

        place_val = find_value_by_label("Birth Place")
        new_role = find_value_by_label("Role")
        
        return born_val, place_val, new_role, country_val
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return None, None, None, None

def update_player(player_id, born, place, role, country):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Dynamic update query
    updates = []
    params = []
    
    if born: 
        updates.append("birth_date=?")
        params.append(born)
    if place:
        updates.append("birth_place=?")
        params.append(place)
    if role:
        updates.append("role=?")
        params.append(role)
    if country:
        updates.append("country=?")
        params.append(country)
        
    if updates:
        sql = f"UPDATE players SET {', '.join(updates)} WHERE player_id=?"
        params.append(player_id)
        cursor.execute(sql, tuple(params))
        conn.commit()
    
    conn.close()

def main():
    players = get_players_missing_info()
    print(f"Found {len(players)} players to enrich.")
    
    for pid, name in players:
        print(f"Processing {name} ({pid})...")
        born, place, role, country = fetch_player_details(pid, name)
        
        info = []
        if born: info.append(f"Born: {born}")
        if country: info.append(f"Country: {country}")
        
        if info:
            print(f"   ✅ {', '.join(info)}")
            update_player(pid, born, place, role, country)
        else:
            print("   ⚠️ No new info found.")
            
        time.sleep(1.0) # Be polite
        
    print("Done.")

if __name__ == "__main__":
    main()
