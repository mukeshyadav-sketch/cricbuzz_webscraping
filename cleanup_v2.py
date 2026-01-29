
import sqlite3

DB_PATH = "cricbuzz.db"

def cleanup():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("Starting V2 Cleanup...")
    
    # 1. Drop Old Tables
    old_tables = ["match_squads", "batter_scorecard", "bowler_scorecard", "leaders", "match_awards"]
    for t in old_tables:
        cursor.execute(f"DROP TABLE IF EXISTS {t}")
        print(f"Dropped {t}")
        
    # 2. Rename New Tables if needed
    # We used 'match_awards_v2' in migration, but 'match_awards' in updated awards.py
    # Let's check if match_awards_v2 exists and move data to match_awards if needed.
    # Or just rename it.
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='match_awards_v2'")
    if cursor.fetchone():
        print("Migrating match_awards_v2 -> match_awards...")
        cursor.execute("CREATE TABLE IF NOT EXISTS match_awards (match_id INTEGER, player_id INTEGER, award_name TEXT, PRIMARY KEY (match_id, award_name, player_id), FOREIGN KEY (match_id) REFERENCES master(match_id), FOREIGN KEY (player_id) REFERENCES players(player_id))")
        cursor.execute("INSERT OR IGNORE INTO match_awards (match_id, player_id, award_name) SELECT match_id, player_id, award_name FROM match_awards_v2")
        cursor.execute("DROP TABLE match_awards_v2")
        print("Renamed match_awards_v2 data to match_awards.")
        
    conn.commit()
    conn.close()
    print("Cleanup Complete. V2 is live.")

if __name__ == "__main__":
    cleanup()
