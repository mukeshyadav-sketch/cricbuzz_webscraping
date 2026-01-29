
import sqlite3

DB_PATH = "cricbuzz.db"

def migrate_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Check if columns exist
    cursor.execute("PRAGMA table_info(players)")
    cols = [info[1] for info in cursor.fetchall()]
    
    if "birth_date" not in cols:
        print("Adding 'birth_date' column...")
        cursor.execute("ALTER TABLE players ADD COLUMN birth_date TEXT")
        
    if "country" not in cols:
        print("Adding 'country' column...")
        cursor.execute("ALTER TABLE players ADD COLUMN country TEXT")

    # Leaders table updates
    cursor.execute("PRAGMA table_info(leaders)")
    l_cols = [info[1] for info in cursor.fetchall()]

    if "player_name" not in l_cols:
        print("Adding 'player_name' to leaders...")
        cursor.execute("ALTER TABLE leaders ADD COLUMN player_name TEXT")
        
    if "role" not in l_cols:
        print("Adding 'role' to leaders...")
        cursor.execute("ALTER TABLE leaders ADD COLUMN role TEXT")
        
    # Master table updates
    cursor.execute("PRAGMA table_info(master)")
    m_cols = [info[1] for info in cursor.fetchall()]

    if "match_name" not in m_cols:
        print("Adding 'match_name' to master...")
        cursor.execute("ALTER TABLE master ADD COLUMN match_name TEXT")
        
    conn.commit()
    conn.close()
    print("Schema migration complete.")

if __name__ == "__main__":
    migrate_db()
