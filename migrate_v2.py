
import sqlite3

DB_PATH = "cricbuzz.db"

def migrate_v2():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. New Tables Creation
    # master (unchanged roughly, but let's confirm schema match)
    # players (unchanged)
    
    print("Creating new V2 tables...")
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS match_players (
        match_id INTEGER,
        player_id INTEGER,
        team TEXT NOT NULL,
        is_captain INTEGER DEFAULT 0,
        is_vice_captain INTEGER DEFAULT 0,
        PRIMARY KEY (match_id, player_id),
        FOREIGN KEY (match_id) REFERENCES master(match_id),
        FOREIGN KEY (player_id) REFERENCES players(player_id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS batting_scorecard (
        match_id INTEGER,
        player_id INTEGER,
        runs INTEGER,
        balls INTEGER,
        fours INTEGER,
        sixes INTEGER,
        strike_rate REAL,
        PRIMARY KEY (match_id, player_id),
        FOREIGN KEY (match_id) REFERENCES master(match_id),
        FOREIGN KEY (player_id) REFERENCES players(player_id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS bowling_scorecard (
        match_id INTEGER,
        player_id INTEGER,
        overs REAL,
        maidens INTEGER,
        runs INTEGER,
        wickets INTEGER,
        no_balls INTEGER,
        wides INTEGER,
        economy REAL,
        PRIMARY KEY (match_id, player_id),
        FOREIGN KEY (match_id) REFERENCES master(match_id),
        FOREIGN KEY (player_id) REFERENCES players(player_id)
    )
    """)
    
    # Re-create match awards with new minimal schema if needed, or migration
    # The requested schema has PK (match_id, award_name, player_id)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS match_awards_v2 (
        match_id INTEGER,
        player_id INTEGER,
        award_name TEXT,
        PRIMARY KEY (match_id, award_name, player_id),
        FOREIGN KEY (match_id) REFERENCES master(match_id),
        FOREIGN KEY (player_id) REFERENCES players(player_id)
    )
    """)
    
    # 2. Data Migration
    
    # A. match_squads -> match_players
    print("Migrating match_squads -> match_players...")
    cursor.execute("SELECT match_id, player_id, team FROM match_squads")
    squads = cursor.fetchall()
    for row in squads:
        cursor.execute("""
            INSERT OR IGNORE INTO match_players (match_id, player_id, team) 
            VALUES (?, ?, ?)
        """, row)
        
    # B. leaders -> match_players (is_captain flag)
    # leaders schema: match_id, team, player_id, player_name, role
    print("Migrating leaders -> match_players (Captaincy)...")
    cursor.execute("SELECT match_id, player_id FROM leaders")
    leaders = cursor.fetchall()
    for row in leaders:
        cursor.execute("""
            UPDATE match_players 
            SET is_captain = 1 
            WHERE match_id = ? AND player_id = ?
        """, row)
        
    # C. batter_scorecard -> batting_scorecard
    # Old: match_id, player_id, player_name, R, B, fours, sixes, SR
    print("Migrating batter_scorecard -> batting_scorecard...")
    cursor.execute("SELECT match_id, player_id, R, B, fours, sixes, SR FROM batter_scorecard")
    bats = cursor.fetchall()
    for row in bats:
        cursor.execute("""
            INSERT OR IGNORE INTO batting_scorecard (match_id, player_id, runs, balls, fours, sixes, strike_rate) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, row)
        
    # D. bowler_scorecard -> bowling_scorecard
    # Old: match_id, player_id, player_name, O, M, R, W, NB, WB, ECO
    print("Migrating bowler_scorecard -> bowling_scorecard...")
    cursor.execute("SELECT match_id, player_id, O, M, R, W, NB, WB, ECO FROM bowler_scorecard")
    bowls = cursor.fetchall()
    for row in bowls:
        cursor.execute("""
            INSERT OR IGNORE INTO bowling_scorecard (match_id, player_id, overs, maidens, runs, wickets, no_balls, wides, economy) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, row)
        
    # E. match_awards -> match_awards_v2
    # Old: match_id, player_id, player_name, award_name
    print("Migrating match_awards -> match_awards_v2...")
    cursor.execute("SELECT match_id, player_id, award_name FROM match_awards")
    awards = cursor.fetchall()
    for row in awards:
        cursor.execute("""
            INSERT OR IGNORE INTO match_awards_v2 (match_id, player_id, award_name) 
            VALUES (?, ?, ?)
        """, row)
        
    conn.commit()
    
    # 3. Cleanup (Drop Old Tables) 
    # Use with caution!
    print("verify the data before dropping manually.")
    # cursor.execute("DROP TABLE match_squads")
    # cursor.execute("DROP TABLE leaders")
    # cursor.execute("DROP TABLE batter_scorecard")
    # cursor.execute("DROP TABLE bowler_scorecard")
    # cursor.execute("DROP TABLE match_awards")
    # cursor.execute("ALTER TABLE match_awards_v2 RENAME TO match_awards")
    
    conn.close()
    print("Migration V2 (Data Copy) complete.")

if __name__ == "__main__":
    migrate_v2()
