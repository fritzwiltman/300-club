import sys
import os
import psycopg2
import statsapi

# Add the project root directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

# Import the database configuration from config.py
from config.config import DATABASE

# Season year for stats to update
SEASON = 2025

def get_db_connection():
    """
    Connects to the PostgreSQL database.

    Returns:
        psycopg2.extensions.connection: The database connection object.
    """
    conn = psycopg2.connect(
        dbname=DATABASE['dbname'],
        user=DATABASE['user'],
        password=DATABASE['password'],
        host=DATABASE['host']
    )
    return conn

def fetch_hitter_stats(api_player_id):
    """
    Fetches hitter stats from the MLB Stats API.

    Args:
        api_player_id (int): The player's ID in the MLB Stats API.

    Returns:
        tuple: A tuple containing the player's average, OPS, plate appearances, home runs, RBIs, and stolen bases.
               If an error occurs, returns None.
    """
    try:
        player_stats = statsapi.player_stat_data(api_player_id, group="hitting", type="season")
        stats = player_stats['stats'][0]['stats']
        
        # Extract relevant stats
        average = stats.get('avg', 0)
        ops = stats.get('ops', 0)
        plate_appearances = stats.get('plateAppearances', 0)
        home_runs = stats.get('homeRuns', 0)
        rbis = stats.get('rbi', 0)
        stolen_bases = stats.get('stolenBases', 0)
        
        return average, ops, plate_appearances, home_runs, rbis, stolen_bases
    except Exception as e:
        print(f"Error fetching hitter stats for player {api_player_id}: {e}")
        return None

def fetch_pitcher_stats(api_player_id):
    """
    Fetches pitcher stats from the MLB Stats API.

    Args:
        api_player_id (int): The player's ID in the MLB Stats API.

    Returns:
        tuple: A tuple containing the player's wins, losses, ERA, and strikeouts.
               If an error occurs, returns None.
    """
    try:
        player_stats = statsapi.player_stat_data(api_player_id, group="pitching", type="season")
        stats = player_stats['stats'][0]['stats']
        
        # Extract relevant stats
        wins = stats.get('wins', 0)
        losses = stats.get('losses', 0)
        era = stats.get('era', 0)
        strikeouts = stats.get('strikeOuts', 0)
        
        return wins, losses, era, strikeouts
    except Exception as e:
        print(f"Error fetching pitcher stats for player {api_player_id}: {e}")
        return None

def update_hitter_stats(player_id, stats):
    """
    Updates the hitter stats in the database for the current season.

    Args:
        player_id (int): The ID of the player in the database.
        stats (tuple): A tuple containing the player's average, OPS, plate appearances, home runs, RBIs, and stolen bases.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            UPDATE hitters
            SET average = %s, ops = %s, plate_appearances = %s, home_runs = %s, rbis = %s, stolen_bases = %s
            WHERE player_id = %s AND season = %s;
        """, (stats[0], stats[1], stats[2], stats[3], stats[4], stats[5], player_id, SEASON))

        conn.commit()
    except Exception as e:
        print(f"Error updating hitter stats for player {player_id}: {e}")
    finally:
        cur.close()
        conn.close()

def update_pitcher_stats(player_id, stats):
    """
    Updates the pitcher stats in the database for the current season.

    Args:
        player_id (int): The ID of the player in the database.
        stats (tuple): A tuple containing the player's wins, losses, ERA, and strikeouts.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            UPDATE pitchers
            SET wins = %s, losses = %s, era = %s, strikeouts = %s
            WHERE player_id = %s AND season = %s;
        """, (stats[0], stats[1], stats[2], stats[3], player_id, SEASON))

        conn.commit()
    except Exception as e:
        print(f"Error updating pitcher stats for player {player_id}: {e}")
    finally:
        cur.close()
        conn.close()

def update_player_stats():
    """
    Fetches and updates all player stats in the database for the current season.
    Only updates players that have hitter/pitcher rows for the current season.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Fetch hitters for the current season and update their stats
        cur.execute("""
            SELECT p.id, p.api_player_id
            FROM players p
            JOIN hitters h ON h.player_id = p.id
            WHERE p.player_type = 'hitter'
              AND p.api_player_id IS NOT NULL
              AND h.season = %s;
        """, (SEASON,))
        hitters = cur.fetchall()

        print(f"Updating stats for {len(hitters)} hitters in {SEASON} season...")
        for player_id, api_player_id in hitters:
            stats = fetch_hitter_stats(api_player_id)
            if stats:
                update_hitter_stats(player_id, stats)

        # Fetch pitchers for the current season and update their stats
        cur.execute("""
            SELECT p.id, p.api_player_id
            FROM players p
            JOIN pitchers pt ON pt.player_id = p.id
            WHERE p.player_type = 'pitcher'
              AND p.api_player_id IS NOT NULL
              AND pt.season = %s;
        """, (SEASON,))
        pitchers = cur.fetchall()

        print(f"Updating stats for {len(pitchers)} pitchers in {SEASON} season...")
        for player_id, api_player_id in pitchers:
            stats = fetch_pitcher_stats(api_player_id)
            if stats:
                update_pitcher_stats(player_id, stats)

        print("Stats update complete.")

    except Exception as e:
        print(f"Error during the update process: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    update_player_stats()