import cloudscraper
from bs4 import BeautifulSoup
import psycopg2
import sys
import os

# Add the project root directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from config.config import DATABASE

# Season year for the data being scraped
SEASON = 2026

# Create cloudscraper instance to bypass Cloudflare protection
scraper = cloudscraper.create_scraper()


def scrape_and_store_user_selections():
    """
    Scrapes and stores user selections by iterating through a list of users,
    scraping data for each user, and organizing the data into categories and picks.

    Returns:
        None
    """
    users = scrape_mbr_ids()

    for user in users:
        print(f"Scraping data for {user['user']}")
        mbr_id = user['mbr_id']

        user['batters'] = scrape_selected_batters_data(mbr_id)
        user['alternate_batters'] = scrape_selected_alternate_batters(mbr_id)
        user['pitchers'] = scrape_selected_pitchers_data(mbr_id)
        user['home_run_hitters'] = scrape_selected_home_run_data(mbr_id)
        user['rbi_champion'] = scrape_selected_rbi_champion_data(mbr_id)
        user['stolen_base_champion'] = scrape_selected_stolen_base_champion_data(mbr_id)
        user['dimaggio'] = scrape_selected_dimaggio_data(mbr_id)

    # create categories for each category: batters, alternate_batters, pitchers, home_run_hitters, rbi_champion, stolen_base_champion, dimaggio
    categories = [{'name': 'batters'}, {'name': 'alternate_batters'}, {'name': 'pitchers'}, {'name': 'home_run_hitters'}, {'name': 'rbi_champion'}, {'name': 'stolen_base_champion'}, {'name': 'dimaggio'}]
   
    # create picks for each user matching the fields for picks: id, user_id, category_id, player_name, pick_order, pick_value, is_alternate
    picks = {}
    batter_picks = []
    alternate_batter_picks = []
    pitcher_picks = []
    home_run_hitter_picks = []
    rbi_champion_picks = []
    stolen_base_champion_picks = []
    dimaggio_picks = []

    for user in users:
        for category in categories:
            if category['name'] == 'batters':
                # print(user['batters'])
                for selection in user['batters']:
                    batter_picks.append({
                        'user_id': user['mbr_id'],
                        'category_id': category['name'],
                        'player_name': selection['player'],
                        'pick_order': selection['selection_number'],
                        'is_alternate': False
                    })
            elif category['name'] == 'alternate_batters': 
                for selection in user['alternate_batters']:
                    alternate_batter_picks.append({
                        'user_id': user['mbr_id'],
                        'category_id': category['name'],
                        'player_name': selection['player'],
                        'pick_order': selection['selection_number'],
                        'is_alternate': True
                    })
            elif category['name'] == 'pitchers':
                for selection in user['pitchers']:
                    pitcher_picks.append({
                        'user_id': user['mbr_id'],
                        'category_id': category['name'],
                        'player_name': selection['player'],
                        'pick_order': selection['selection_number'],
                    })
            elif category['name'] == 'home_run_hitters':
                for selection in user['home_run_hitters']:
                    home_run_hitter_picks.append({
                        'user_id': user['mbr_id'],
                        'category_id': category['name'],
                        'player_name': selection['player'],
                        'pick_order': selection['selection_number'],
                    })
            elif category['name'] == 'rbi_champion':
                for selection in user['rbi_champion']:
                    rbi_champion_picks.append({
                        'user_id': user['mbr_id'],
                        'category_id': category['name'],
                        'player_name': selection['player'],
                        'pick_value': selection['ballot_rbi'],  # User's predicted RBI count
                    })
            elif category['name'] == 'stolen_base_champion':
                for selection in user['stolen_base_champion']:
                    stolen_base_champion_picks.append({
                        'user_id': user['mbr_id'],
                        'category_id': category['name'],
                        'player_name': selection['player'],
                        'pick_value': selection['ballot_stolen_bases'],
                    })
            elif category['name'] == 'dimaggio':
                for selection in user['dimaggio']:
                    dimaggio_picks.append({
                        'user_id': user['mbr_id'],
                        'category_id': category['name'],
                        'pick_value': selection['ballot_longest_hitting_streak'],
                    })
            
    picks = {
        'batters': batter_picks,
        'alternate_batters': alternate_batter_picks,
        'pitchers': pitcher_picks,
        'home_run_hitters': home_run_hitter_picks,
        'rbi_champion': rbi_champion_picks,
        'stolen_base_champion': stolen_base_champion_picks,
        'dimaggio': dimaggio_picks
    }
    print("Scraping and storing user selections complete.")

    # Insert data into the database
    print("Inserting stagnant data into the database...")
    insert_stagnant_data(users, categories, picks)
    print("Stagnant data (users, categories, picks) inserted into the database.")

def insert_stagnant_data(users, categories, picks):
    '''
    Insert stagnant data into the database. Stagnant data is data that does not change from week to week.
    This includes users, categories, and picks.

    Parameters:
    - users (list): A list of dictionaries representing the users to be inserted.
    - categories (list): A list of dictionaries representing the categories to be inserted.
    - picks (dict): A dictionary containing the picks to be inserted, categorized by pick type.

    Returns:
    None
    '''
    conn = psycopg2.connect(
        dbname=DATABASE['dbname'],
        user=DATABASE['user'],
        password=DATABASE['password'],
        host=DATABASE['host']
    )
    cur = conn.cursor()

    # Insert users (skip if already exists)
    for user in users:
        cur.execute(
            "INSERT INTO users (mbr_id, name) VALUES (%s, %s) ON CONFLICT (mbr_id) DO NOTHING",
            (user['mbr_id'], user['user'])
        )
    conn.commit()

    # Categories are static - skip insertion as they already exist
    # (Categories: 1=batters, 2=alternate_batters, 3=pitchers, 4=home_run_hitters,
    #  5=rbi_champion, 6=stolen_base_champion, 7=dimaggio)

    # Check if picks already exist for this season to avoid duplicates
    cur.execute("SELECT COUNT(*) FROM picks WHERE season = %s", (SEASON,))
    existing_picks = cur.fetchone()[0]
    if existing_picks > 0:
        print(f"Warning: {existing_picks} picks already exist for season {SEASON}. Skipping pick insertion to avoid duplicates.")
        print("To re-import picks, first delete existing picks: DELETE FROM picks WHERE season = %s", SEASON)
        cur.close()
        conn.close()
        return

    # Insert batter picks with season
    for pick in picks['batters']:
        cur.execute(
            "INSERT INTO picks (user_id, category_id, player_name, is_alternate, pick_order, season) VALUES (%s, %s, %s, %s, %s, %s)",
            (pick['user_id'], 1, pick['player_name'], pick['is_alternate'], pick['pick_order'], SEASON)
        )

    # Insert alternate batter picks with season
    for pick in picks['alternate_batters']:
        cur.execute(
            "INSERT INTO picks (user_id, category_id, player_name, is_alternate, pick_order, season) VALUES (%s, %s, %s, %s, %s, %s)",
            (pick['user_id'], 2, pick['player_name'], pick['is_alternate'], pick['pick_order'], SEASON)
        )

    # Insert pitcher picks with season
    for pick in picks['pitchers']:
        cur.execute(
            "INSERT INTO picks (user_id, category_id, player_name, pick_order, season) VALUES (%s, %s, %s, %s, %s)",
            (pick['user_id'], 3, pick['player_name'], pick['pick_order'], SEASON)
        )

    # Insert home run hitter picks with season
    for pick in picks['home_run_hitters']:
        cur.execute(
            "INSERT INTO picks (user_id, category_id, player_name, pick_order, season) VALUES (%s, %s, %s, %s, %s)",
            (pick['user_id'], 4, pick['player_name'], pick['pick_order'], SEASON)
        )

    # Insert rbi champion picks with season
    for pick in picks['rbi_champion']:
        cur.execute(
            "INSERT INTO picks (user_id, category_id, player_name, pick_value, season) VALUES (%s, %s, %s, %s, %s)",
            (pick['user_id'], 5, pick['player_name'], pick['pick_value'], SEASON)
        )

    # Insert stolen base champion picks with season
    for pick in picks['stolen_base_champion']:
        cur.execute(
            "INSERT INTO picks (user_id, category_id, player_name, pick_value, season) VALUES (%s, %s, %s, %s, %s)",
            (pick['user_id'], 6, pick['player_name'], pick['pick_value'], SEASON)
        )

    # Insert dimaggio picks with season
    for pick in picks['dimaggio']:
        cur.execute(
            "INSERT INTO picks (user_id, category_id, pick_value, season) VALUES (%s, %s, %s, %s)",
            (pick['user_id'], 7, pick['pick_value'], SEASON)
        )

    conn.commit()
    cur.close()
    conn.close()


def scrape_mbr_ids():
    """
    Scrapes the mbr_ids (member IDs) of users from 300 Club and returns a list of dictionaries
    containing the user names and their corresponding mbr_ids.

    Returns:
        list: A list of dictionaries, where each dictionary contains the 'user' and 'mbr_id' keys.
    """
    url = 'https://www.300club.org/CntstRanking.asp?contest_id=2&contest_name=Batters'
    response = scraper.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # For each user, extract mbr_id from href of user name column
    users = []
    tables = soup.find_all('table', {'id': 'ranking'})

    # First table is qualified users
    if len(tables) >= 1:
        qualified_table = tables[0]
        for row in qualified_table.find_all('tr')[1:]:
            columns = row.find_all('td')
            if len(columns) > 1:
                user = columns[1].text.strip()
                link = columns[1].find('a')
                if link and 'href' in link.attrs:
                    mbr_id = link['href'].split('=')[1].split('&')[0]
                    users.append({'user': user, 'mbr_id': mbr_id})

    # Second table is disqualified users (may not exist at start of season)
    if len(tables) >= 2:
        disqualified_table = tables[1]
        for row in disqualified_table.find_all('tr')[1:]:
            columns = row.find_all('td')
            if len(columns) > 0:
                user = columns[0].text.strip()
                link = columns[0].find('a')
                if link and 'href' in link.attrs:
                    mbr_id = link['href'].split('=')[1].split('&')[0]
                    users.append({'user': user, 'mbr_id': mbr_id})

    print(f"Found {len(users)} users")
    return users


def scrape_selected_batters_data(mbr_id):
    """
    Scrapes the selected batters data for a given member ID.

    Args:
        mbr_id (int): The member ID.

    Returns:
        list: A list of dictionaries containing the selected batters data.
            Each dictionary represents a selection and contains the following keys:
            - 'selection_number': The selection number.
            - 'player': The player's name.
            - 'team': The player's team.
            - 'average': The player's batting average.
            - 'plate_appearances': The player's plate appearances.
            - 'ops': The player's OPS (OBP Plus Slugging).
            - 'disqualified': A boolean indicating if the player is disqualified or not (by not meeting plate appearance minimum).
    """
    url = f'https://www.300club.org/RankingPerMember.asp?mbr_id={mbr_id}&contest_id=2&contest_name=Batters'
    response = scraper.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    selections = []
    try:
        table = soup.find_all('table')
        if len(table) <= 10:
            return selections

        for row in table[10].find_all('tr')[3:13]:
            try:
                columns = row.find_all('td')
                selection = {
                    'selection_number': columns[0].text.strip(),
                    'player': columns[1].text.strip(),
                    'team': columns[2].text.strip(),
                    'average': columns[3].text.strip(),
                    'plate_appearances': columns[4].text.strip(),
                    'ops': columns[5].text.strip(),
                    'disqualified': False if columns[6].text.strip() == '' else True,
                }
                selections.append(selection)
            except IndexError:
                break
    except (IndexError, AttributeError) as e:
        print(f"Warning: Could not scrape batters for mbr_id {mbr_id}: {e}")
    return selections


def scrape_selected_alternate_batters(mbr_id):
    """
    Scrapes the selected alternate batters for a given member ID.

    Args:
        mbr_id (int): The member ID.

    Returns:
        list: A list of dictionaries representing the selected alternate batters. Each dictionary contains the following keys:
            - 'selection_number': The selection number.
            - 'player': The name of the player.
            - 'team': The team of the player.
            - 'average': The batting average of the player.
            - 'plate_appearances': The number of plate appearances of the player.
            - 'ops': The OPS (On-base Plus Slugging) of the player.
            - 'disqualified': A boolean indicating if the player is disqualified or not.
    """
    url = f'https://www.300club.org/RankingPerMember.asp?mbr_id={mbr_id}&contest_id=5&contest_name=Alternates'
    response = scraper.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    selections = []
    try:
        table = soup.find_all('table')
        if len(table) <= 11:
            return selections

        selection_number = 1
        for row in table[11].find_all('tr')[1:6]:
            try:
                columns = row.find_all('td')
                selection = {
                    'selection_number': selection_number,
                    'player': columns[0].text.strip(),
                    'team': columns[1].text.strip(),
                    'average': columns[2].text.strip(),
                    'plate_appearances': columns[3].text.strip(),
                    'ops': columns[4].text.strip(),
                    'disqualified': False if columns[5].text.strip() == '' else True,
                }
                selection_number += 1
                selections.append(selection)
            except IndexError:
                break
    except (IndexError, AttributeError) as e:
        print(f"Warning: Could not scrape alternate batters for mbr_id {mbr_id}: {e}")
    return selections


def scrape_selected_pitchers_data(mbr_id):
    """
    Scrapes the selected pitchers data for a given member ID.

    Args:
        mbr_id (int): The member ID.

    Returns:
        list: A list of dictionaries containing the selected pitchers data.
            Each dictionary contains the following keys:
            - selection_number (int): The selection number.
            - player (str): The name of the player.
            - team (str): The team of the player.
            - wins (str): The number of wins for the player.
    """
    url = f'https://www.300club.org/RankingPerMember.asp?mbr_id={mbr_id}&contest_id=3&contest_name=Pitchers'
    response = scraper.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    selections = []
    try:
        table = soup.find_all('table')
        if len(table) <= 11:
            return selections

        selection_number = 1
        for row in table[11].find_all('tr')[1:5]:
            try:
                columns = row.find_all('td')
                selection = {
                    'selection_number': selection_number,
                    'player': columns[0].text.strip(),
                    'team': columns[1].text.strip(),
                    'wins': columns[2].text.strip(),
                }
                selection_number += 1
                selections.append(selection)
            except IndexError:
                break
    except (IndexError, AttributeError) as e:
        print(f"Warning: Could not scrape pitchers for mbr_id {mbr_id}: {e}")
    return selections


def scrape_selected_home_run_data(mbr_id):
    """
    Scrapes the selected home run data for a given member ID.

    Parameters:
    mbr_id (int): The member ID.

    Returns:
    list: A list of dictionaries containing the selected home run data.
          Each dictionary contains the following keys:
          - selection_number: The player's selection number by the user.
          - player: The player's name.
          - team: The player's team.
          - home_runs: The number of home runs.
    """
    url = f'https://www.300club.org/RankingPerMember.asp?mbr_id={mbr_id}&contest_id=6&contest_name=Home+Run+Hitters'
    response = scraper.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    selections = []
    try:
        table = soup.find_all('table')
        if len(table) <= 11:
            return selections

        selection_number = 1
        for row in table[11].find_all('tr')[1:5]:
            try:
                columns = row.find_all('td')
                selection = {
                    'selection_number': selection_number,
                    'player': columns[0].text.strip(),
                    'team': columns[1].text.strip(),
                    'home_runs': columns[2].text.strip(),
                }
                selection_number += 1
                selections.append(selection)
            except IndexError:
                break
    except (IndexError, AttributeError) as e:
        print(f"Warning: Could not scrape home run hitters for mbr_id {mbr_id}: {e}")
    return selections


def scrape_selected_rbi_champion_data(mbr_id):
    """
    Scrapes the selected RBI champion data for a given member ID.

    Args:
        mbr_id (int): The member ID.

    Returns:
        list: A list of dictionaries containing the selected RBI champion data.
            Each dictionary represents a selection and contains the following keys:
            - 'selection_number': The selection number (always 1).
            - 'player': The name of the player.
            - 'team': The team of the player.
            - 'actual_rbi': The actual RBI count.
            - 'ballot_rbi': The RBI count from the ballot.
    """
    url = f'https://www.300club.org/RankingPerMember.asp?mbr_id={mbr_id}&contest_id=7&contest_name=RBI+Champion'
    response = scraper.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    selections = []
    try:
        table = soup.find_all('table')
        if len(table) > 11:
            rows = table[11].find_all('tr')
            if len(rows) > 1:
                row = rows[1]
                columns = row.find_all('td')
                if len(columns) >= 4:
                    selection = {
                        'selection_number': 1,
                        'player': columns[0].text.strip(),
                        'team': columns[1].text.strip(),
                        'actual_rbi': columns[2].text.strip(),
                        'ballot_rbi': columns[3].text.strip(),
                    }
                    selections.append(selection)
    except (IndexError, AttributeError) as e:
        print(f"Warning: Could not scrape RBI champion data for mbr_id {mbr_id}: {e}")
    return selections


def scrape_selected_stolen_base_champion_data(mbr_id):
    """
    Scrapes the selected stolen base champion data for a given member ID.

    Args:
        mbr_id (int): The member ID.

    Returns:
        list: A list of dictionaries containing the selected stolen base champion data.
            Each dictionary represents a selection and contains the following keys:
            - 'selection_number': The selection number (always 1).
            - 'player': The name of the player.
            - 'team': The team of the player.
            - 'actual_stolen_bases': The actual stolen bases of the player.
            - 'ballot_stolen_bases': The ballot stolen bases of the player.
    """
    url = f'https://www.300club.org/RankingPerMember.asp?mbr_id={mbr_id}&contest_id=8&contest_name=Stolen+Base+Champion'
    response = scraper.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    selections = []
    try:
        table = soup.find_all('table')
        if len(table) > 11:
            rows = table[11].find_all('tr')
            if len(rows) > 1:
                row = rows[1]
                columns = row.find_all('td')
                if len(columns) >= 4:
                    selection = {
                        'selection_number': 1,
                        'player': columns[0].text.strip(),
                        'team': columns[1].text.strip(),
                        'actual_stolen_bases': columns[2].text.strip(),
                        'ballot_stolen_bases': columns[3].text.strip(),
                    }
                    selections.append(selection)
    except (IndexError, AttributeError) as e:
        print(f"Warning: Could not scrape stolen base champion for mbr_id {mbr_id}: {e}")
    return selections


def scrape_selected_dimaggio_data(mbr_id):
    """
    Scrapes the selected DiMaggio Prize data for a given member ID.

    Args:
        mbr_id (int): The member ID.

    Returns:
        list: A list of dictionaries containing the selected data.
            Each dictionary represents a selection and contains the following keys:
            - 'selection_number': The selection number (always 1).
            - 'actual_longest_hitting_streak': The actual longest hitting streak.
            - 'ballot_longest_hitting_streak': The ballot longest hitting streak.
    """
    url = f'https://www.300club.org/RankingPerMember.asp?mbr_id={mbr_id}&contest_id=9&contest_name=DiMaggio+Prize'
    response = scraper.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    selections = []
    try:
        table = soup.find_all('table')
        if len(table) > 11:
            rows = table[11].find_all('tr')
            if len(rows) > 1:
                row = rows[1]
                columns = row.find_all('td')
                if len(columns) >= 2:
                    selection = {
                        'selection_number': 1,
                        'actual_longest_hitting_streak': columns[0].text.strip(),
                        'ballot_longest_hitting_streak': columns[1].text.strip(),
                    }
                    selections.append(selection)
    except (IndexError, AttributeError) as e:
        print(f"Warning: Could not scrape DiMaggio data for mbr_id {mbr_id}: {e}")
    return selections


if __name__ == "__main__":
    scrape_and_store_user_selections()
