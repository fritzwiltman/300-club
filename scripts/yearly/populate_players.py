import json
import psycopg2
import statsapi
import sys
import os

# Add the project root directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from config.config import DATABASE

# Season year for the data being populated
SEASON = 2025


def populate_player_tables():
    """
    Populates the players, pitchers, and hitters tables in the three_hundred_club database.

    For new seasons:
    1. Adds any new players to the players table (skips existing)
    2. Creates hitter/pitcher stat rows for the new season
    3. Fetches api_player_id from MLB API for player lookups

    Safe to run multiple times - uses ON CONFLICT to avoid duplicates.
    """
    conn = psycopg2.connect(
        dbname=DATABASE['dbname'],
        user=DATABASE['user'],
        password=DATABASE['password'],
        host=DATABASE['host']
    )
    cur = conn.cursor()

    # Step 1: Extract unique players from the picks table for the current season
    cur.execute("""
        SELECT DISTINCT player_name, category_id
        FROM picks
        WHERE player_name IS NOT NULL AND season = %s;
    """, (SEASON,))
    unique_players = cur.fetchall()
    
    # Step 2: Insert unique players into the players table with player_type
    # First, get unique player names (not player+category pairs)
    seen_players = set()
    players_to_insert = []
    for player_name, category_id in unique_players:
        if player_name not in seen_players:
            seen_players.add(player_name)
            # Determine player type based on category_id
            if category_id in (1, 2, 4, 5, 6, 7):  # Hitter-related categories
                player_type = 'hitter'
            elif category_id == 3:  # Pitcher-related category
                player_type = 'pitcher'
            else:
                player_type = 'hitter'  # Default
            players_to_insert.append((player_name, player_type))

    print(f"Found {len(players_to_insert)} unique players to process")

    for player_name, player_type in players_to_insert:
        # Check if player already exists
        cur.execute("SELECT id FROM players WHERE player_name = %s", (player_name,))
        if cur.fetchone() is None:
            print(f"  Inserting: {player_name} ({player_type})")
            cur.execute("""
                INSERT INTO players (player_name, player_type)
                VALUES (%s, %s);
            """, (player_name, player_type))
        else:
            print(f"  Exists: {player_name}")

    # Commit player inserts before proceeding
    conn.commit()
    print(f"\nProcessed {len(players_to_insert)} player entries")

    # Step 3.5: Get api_player_id for each player, write to json, insert into player table
    player_id_map = {}
    all_players = fetch_players_ids()

    not_found_players = []
    for player in unique_players:
        if player[0] in all_players:
            print(f"Success! Player {player} found in MLB API")
            player_id = all_players[player[0]]
            player_id_map[player[0]] = player_id
        else:
            print(f"Failure. Player {player[0]} NOT found in MLB API")
            not_found_players.append(player[0])
            player_id_map[player[0]] = None  # Will be NULL in database

    # Write the player_id_map to a JSON file for reference
    with open('player_ids.json', 'w') as f:
        json.dump(player_id_map, f, default=str)

    # Commit before updates to ensure inserts are persisted
    conn.commit()

    # Update api_player_id for each player (skip NOT_FOUND ones)
    updated_count = 0
    for player_name, api_id in player_id_map.items():
        if api_id is not None:
            try:
                cur.execute("""
                    UPDATE players
                    SET api_player_id = %s
                    WHERE player_name = %s;
                """, (api_id, player_name))
                updated_count += 1
            except Exception as e:
                print(f"Error updating {player_name} with api_id {api_id}: {e}")
                conn.rollback()

    conn.commit()
    print(f"Updated api_player_id for {updated_count} players")

    if not_found_players:
        print(f"\nWarning: {len(not_found_players)} players not found in MLB API:")
        for p in set(not_found_players):
            print(f"  - {p}")

    
    # Step 4: Insert players into hitters or pitchers table for the current season
    # Only insert for players that were picked this season
    cur.execute("""
        SELECT DISTINCT p.id, p.player_name, p.player_type
        FROM players p
        JOIN picks pk ON pk.player_name = p.player_name
        WHERE pk.season = %s AND pk.player_name IS NOT NULL;
    """, (SEASON,))
    players = cur.fetchall()

    for player_id, player_name, player_type in players:
        if player_type == 'hitter':
            cur.execute("""
                INSERT INTO hitters (player_id, season)
                VALUES (%s, %s)
                ON CONFLICT (player_id, season) DO NOTHING;
            """, (player_id, SEASON))
        elif player_type == 'pitcher':
            cur.execute("""
                INSERT INTO pitchers (player_id, season)
                VALUES (%s, %s)
                ON CONFLICT (player_id, season) DO NOTHING;
            """, (player_id, SEASON))

    conn.commit()
    cur.close()
    conn.close()


def fetch_players_ids():
    """Fetch all MLB player IDs from the Stats API for the current season."""
    players = statsapi.get('sports_players', {'season': SEASON})

    # map player name to player_id
    player_ids = {}
    for player in players["people"]:
        player_ids[player['fullName']] = player['id']

    return player_ids


if __name__ == "__main__":
    populate_player_tables()
