
import sqlite3
import datetime
import re

DB_PATH = "cricbuzz.db"

def parse_date(date_str):
    if not date_str:
        return None
    
    # Clean string: "September 03, 1990 (35 years)" -> "September 03, 1990"
    # Remove parens and extra spaces
    clean_str = re.sub(r"\s*\(.*\)", "", date_str).strip()
    
    try:
        # Parse "September 03, 1990"
        dt = datetime.datetime.strptime(clean_str, "%B %d, %Y")
        # Return "03/09/1990"
        return dt.strftime("%d/%m/%Y")
    except ValueError as e:
        print(f"   ⚠️ Could not parse: '{date_str}' ({e})")
        return None

def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT player_id, birth_date FROM players WHERE birth_date IS NOT NULL")
    rows = cursor.fetchall()
    
    print(f"Found {len(rows)} dates to check/format.")
    
    updates = []
    
    for pid, bdate in rows:
        # Skip if already formatted (check regex for dd/mm/yyyy)
        if re.match(r"^\d{2}/\d{2}/\d{4}$", bdate):
            continue
            
        new_date = parse_date(bdate)
        if new_date and new_date != bdate:
            print(f"   {pid}: '{bdate}' -> '{new_date}'")
            updates.append((new_date, pid))
            
    if updates:
        print(f"Updating {len(updates)} records...")
        cursor.executemany("UPDATE players SET birth_date=? WHERE player_id=?", updates)
        conn.commit()
    else:
        print("No updates needed.")
        
    conn.close()
    print("Done.")

if __name__ == "__main__":
    main()
