import json
import psycopg2
import statsapi
import sys
import os

# Add the project root directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from config.config import DATABASE

def populate_player_tables():
    """
    Populates the players, pitchers, and hitters tables in the three_hundred_club database.
    This script is run once to populate the tables and does not need to be run again.
    """
    conn = psycopg2.connect(
        dbname=DATABASE['dbname'],
        user=DATABASE['user'],
        password=DATABASE['password'],
        host=DATABASE['host']
    )
    cur = conn.cursor()

    # Step 1: Extract unique players from the picks table
    cur.execute("""
        SELECT DISTINCT player_name, category_id
        FROM picks
        WHERE player_name IS NOT NULL;
    """)
    unique_players = cur.fetchall()
    
    # Step 2: Insert unique players into the players table with player_type
    for player_name, category_id in unique_players:
        # Determine player type based on category_id
        if category_id in (1, 2, 4, 5, 6, 7):  # Assuming these are hitter-related categories
            player_type = 'hitter'
        elif category_id == 3:  # Assuming this is the pitcher-related category
            player_type = 'pitcher'
        
        print (player_name, player_type)
        # Step 3: Insert player into the players table
        cur.execute("""
            INSERT INTO players (player_name, player_type)
            VALUES (%s, %s)
            ON CONFLICT (player_name) DO NOTHING;
        """, (player_name, player_type))
    
    # Step 3.5: Get api_player_id for each player, write to json, insert into player table
    player_id_map = {}
    all_players = fetch_players_ids()

    for player in unique_players:
        if player[0] in all_players:
            print(f"Success! Player {player} found in MLB API")
            player_id = all_players[player[0]]
            player_id_map[player[0]] = player_id
            # Code to add player_id to players table
        else:
            print(f"Failure. Player {player[0]} NOT found in MLB API")
            player_id_map[player[0]] = "NOT_FOUND"
    # Write the player_id_map to a JSON file
    with open('player_ids.json', 'w') as f:
        json.dump(player_id_map, f)

    # Load the JSON file containing player names and api_player_ids
    with open('player_ids.json', 'r') as f:
        player_api_ids = json.load(f)
    
    for player_name, api_id in player_api_ids.items():
        # Update the api_player_id for each player
        cur.execute("""
            UPDATE players
            SET api_player_id = %s
            WHERE player_name = %s;
        """, (api_id, player_name))

    
    # Step 4: Insert players into hitters or pitchers table based on player_type
    cur.execute("""
        SELECT id, player_name, player_type
        FROM players;
    """)
    players = cur.fetchall()

    for player_id, player_name, player_type in players:
        if player_type == 'hitter':
            cur.execute("""
                INSERT INTO hitters (player_id)
                VALUES (%s)
                ON CONFLICT (player_id) DO NOTHING;
            """, (player_id,))
        elif player_type == 'pitcher':
            cur.execute("""
                INSERT INTO pitchers (player_id)
                VALUES (%s)
                ON CONFLICT (player_id) DO NOTHING;
            """, (player_id,))

    conn.commit()
    cur.close()
    conn.close()


def fetch_players_ids():
    players = statsapi.get('sports_players',{'season':2024})

    # map player name to player_id
    player_ids = {}
    for player in players["people"]:
        player_ids[player['fullName']] = player['id']

    return player_ids


if __name__ == "__main__":
    populate_player_tables()
