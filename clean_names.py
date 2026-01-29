
import sqlite3
import re

DB_PATH = "cricbuzz.db"

def clean_names():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. Get all players with parens
    cursor.execute("SELECT player_id, name FROM players WHERE name LIKE '%(%'")
    rows = cursor.fetchall()
    
    print(f"Found {len(rows)} potentially dirty names.")
    
    count = 0
    updates = []
    
    # Regex to handle (C), (WK), (c), (wk), (c & wk) etc.
    # We want to remove the parenthesis group at the end of the string if it contains these roles.
    # Be careful not to remove (Country) if that ever happens, but current issue is Role suffixes.
    # Pattern: Space(optional) + "(" + content + ")" at end of string
    pattern = r"\s*\((?:c|wk|c\s*\&\s*wk|wk\s*\&\s*c)\)$"
    
    for pid, name in rows:
        # Check if it matches our target suffixes
        # use re.IGNORECASE
        new_name = re.sub(pattern, "", name, flags=re.IGNORECASE)
        
        if new_name != name:
            print(f"   '{name}' -> '{new_name}'")
            updates.append((new_name, pid))
            count += 1
            
    if updates:
        print(f"Updating {len(updates)} records...")
        cursor.executemany("UPDATE players SET name=? WHERE player_id=?", updates)
        conn.commit()
    else:
        print("No changes needed based on regex.")
        
    conn.close()
    print("Done.")

if __name__ == "__main__":
    clean_names()
