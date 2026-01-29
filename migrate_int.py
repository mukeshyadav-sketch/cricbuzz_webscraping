
import sqlite3

DB_PATH = "cricbuzz.db"

def migrate():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Tables to migrate
    tables = [
        "master", 
        "match_squads", 
        "batter_scorecard", 
        "bowler_scorecard", 
        "match_awards",
        "leaders"
    ]
    
    print("Starting migration to INTEGER match_id...")
    
    try:
        cursor.execute("PRAGMA foreign_keys = OFF;")
        
        for table in tables:
            print(f"Migrating {table}...")
            
            # Check if table exists
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
            if not cursor.fetchone():
                print(f"  Skipping {table} (not found)")
                continue
            
            # 1. Rename old table
            cursor.execute(f"ALTER TABLE {table} RENAME TO {table}_old")
            
            # 2. Create new table with correct schema
            if table == "master":
                cursor.execute("""
                    CREATE TABLE master (
                        match_id INTEGER PRIMARY KEY,
                        team1 TEXT,
                        team2 TEXT,
                        winner TEXT,
                        venue TEXT,
                        match_name TEXT
                    )
                """)
            elif table == "match_squads":
                cursor.execute("""
                    CREATE TABLE match_squads (
                        squad_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        match_id INTEGER NOT NULL,
                        player_id INTEGER NOT NULL,
                        team TEXT NOT NULL,
                        FOREIGN KEY (match_id) REFERENCES master(match_id),
                        FOREIGN KEY (player_id) REFERENCES players(player_id)
                    )
                """)
            elif table == "batter_scorecard":
                cursor.execute("""
                    CREATE TABLE batter_scorecard (
                        match_id INTEGER,
                        player_id INTEGER,
                        player_name TEXT,
                        R INTEGER,
                        B INTEGER,
                        fours INTEGER,
                        sixes INTEGER,
                        SR REAL,
                        FOREIGN KEY (match_id) REFERENCES master(match_id)
                    )
                """)
            elif table == "bowler_scorecard":
                cursor.execute("""
                    CREATE TABLE bowler_scorecard (
                        match_id INTEGER,
                        player_id INTEGER,
                        player_name TEXT,
                        O REAL,
                        M INTEGER,
                        R INTEGER,
                        W INTEGER,
                        NB INTEGER,
                        WB INTEGER,
                        ECO REAL,
                        FOREIGN KEY (match_id) REFERENCES master(match_id)
                    )
                """)
            elif table == "match_awards":
                cursor.execute("""
                    CREATE TABLE match_awards (
                        match_id INTEGER,
                        player_id INTEGER,
                        player_name TEXT,
                        award_name TEXT,
                        FOREIGN KEY (match_id) REFERENCES master(match_id)
                    )
                """)
            elif table == "leaders":
                cursor.execute("""
                    CREATE TABLE leaders (
                        match_id INTEGER,
                        team TEXT,
                        player_id INTEGER,
                        player_name TEXT,
                        role TEXT,
                        FOREIGN KEY (match_id) REFERENCES master(match_id),
                        FOREIGN KEY (player_id) REFERENCES players(player_id)
                    )
                """)
            
            # 3. Copy data
            # We assume column names match, or use explicit insert if schema evolved.
            # Simpler: INSERT INTO new SELECT CAST(match_id AS INTEGER), ... FROM old
            # But columns might vary. Safe bet: Select * from old, re-insert.
            
            cursor.execute(f"SELECT * FROM {table}_old")
            rows = cursor.fetchall()
            old_cols = [description[0] for description in cursor.description]
            
            # Build insert query dynamically based on columns in OLD table
            # Assuming expected columns exist in new table too.
            ph = ",".join(["?"] * len(old_cols))
            cols_str = ",".join(old_cols)
            
            insert_sql = f"INSERT INTO {table} ({cols_str}) VALUES ({ph})"
            
            # Process rows: Convert match_id index to int
            match_id_idx = -1
            if "match_id" in old_cols:
                match_id_idx = old_cols.index("match_id")
            
            new_rows = []
            for r in rows:
                r_list = list(r)
                if match_id_idx != -1 and r_list[match_id_idx] is not None:
                    try:
                        r_list[match_id_idx] = int(r_list[match_id_idx])
                    except:
                        r_list[match_id_idx] = 0 # Fallback
                new_rows.append(tuple(r_list))
                
            cursor.executemany(insert_sql, new_rows)
            print(f"  Migrated {len(new_rows)} rows.")
            
            # 4. Drop old table
            cursor.execute(f"DROP TABLE {table}_old")
            
        cursor.execute("PRAGMA foreign_keys = ON;")
        conn.commit()
        print("Migration complete!")
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
