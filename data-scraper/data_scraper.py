import requests
from bs4 import BeautifulSoup
import psycopg2  # or any other database connector

def scrape_batters_data():
    url = 'https://www.300club.org/CntstRanking.asp?contest_id=2&contest_name=Batters'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

     # Extract data from HTML
    rankings = []
    table = soup.find('table', {'id': 'ranking'})
    for row in table.find_all('tr')[1:]:
        columns = row.find_all('td')
        ranking = {
            'rank': columns[0].text.strip(),
            'user': columns[1].text.strip(),
            'eligible_batters_average': columns[2].text.strip(),
            'altername_batters_average': columns[3].text.strip(),
            'rank_last_week': columns[4].text.strip(),
            'rank_four_weeks_ago': columns[5].text.strip(),
            'eligible_batters_ops': columns[6].text.strip(),
        }
        rankings.append(ranking)
    return rankings

def scrape_pitchers_data():
    url = 'https://www.300club.org/CntstRanking.asp?contest_id=3&contest_name=Pitchers'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

     # Extract data from HTML
    rankings = []
    table = soup.find('table', {'id': 'ranking'})
    for row in table.find_all('tr')[1:]:
        columns = row.find_all('td')
        ranking = {
            'rank': columns[0].text.strip(),
            'user': columns[1].text.strip(),
            'total_wins_of_best_three': columns[2].text.strip(),
            'first_tiebreaker': columns[3].text.strip(),
            'second_tiebreaker': columns[4].text.strip(),
            'third_tiebreaker': columns[5].text.strip(),
            'fourth_tiebreaker': columns[6].text.strip(),
            'rank_last_week': columns[7].text.strip(),
            'rank_four_weeks_ago': columns[8].text.strip(),
        }
        rankings.append(ranking)
    return rankings

def scrape_home_run_data():
    url = 'https://www.300club.org/CntstRanking.asp?contest_id=6&contest_name=Home+Run+Hitters'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

     # Extract data from HTML
    rankings = []
    table = soup.find('table', {'id': 'ranking'})
    for row in table.find_all('tr')[1:]:
        columns = row.find_all('td')
        ranking = {
            'rank': columns[0].text.strip(),
            'user': columns[1].text.strip(),
            'total_home_runs_of_best_three': columns[2].text.strip(),
            'first_tiebreaker': columns[3].text.strip(),
            'second_tiebreaker': columns[4].text.strip(),
            'rank_last_week': columns[5].text.strip(),
            'rank_four_weeks_ago': columns[6].text.strip(),
        }
        rankings.append(ranking)
    return rankings

def scrape_rbi_data():
    url = 'https://www.300club.org/CntstRanking.asp?contest_id=7&contest_name=RBI+Champion'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

     # Extract data from HTML
    rankings = []
    table = soup.find('table', {'id': 'ranking'})
    for row in table.find_all('tr')[1:]:
        columns = row.find_all('td')
        ranking = {
            'rank': columns[0].text.strip(),
            'user': columns[1].text.strip(),
            'player': columns[2].text.strip(),
            'guessed_rbis': columns[3].text.strip(),
            'deviation_of_prorated_guess': columns[4].text.strip(),
            'ytd_rbis': columns[5].text.strip(),
            'tiebreaker': columns[6].text.strip(),
            'rank_last_week': columns[7].text.strip(),
            'rank_four_weeks_ago': columns[8].text.strip(),
        }
        rankings.append(ranking)
    return rankings

def scrape_stolen_base_data():
    url = 'https://www.300club.org/CntstRanking.asp?contest_id=8&contest_name=Stolen+Base+Champion'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

     # Extract data from HTML
    rankings = []
    table = soup.find('table', {'id': 'ranking'})
    for row in table.find_all('tr')[1:]:
        columns = row.find_all('td')
        ranking = {
            'rank': columns[0].text.strip(),
            'user': columns[1].text.strip(),
            'player': columns[2].text.strip(),
            'guessed_stolen_bases': columns[3].text.strip(),
            'deviation_of_prorated_guess': columns[4].text.strip(),
            'ytd_stolen_bases': columns[5].text.strip(),
            'tiebreaker': columns[6].text.strip(),
            'rank_last_week': columns[7].text.strip(),
            'rank_four_weeks_ago': columns[8].text.strip(),
        }
        rankings.append(ranking)
    return rankings

def scrape_dimaggio_data():
    url = 'https://www.300club.org/CntstRanking.asp?contest_id=9&contest_name=DiMaggio+Prize'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

     # Extract data from HTML
    rankings = []
    table = soup.find('table', {'id': 'ranking'})
    for row in table.find_all('tr')[1:]:
        columns = row.find_all('td')
        ranking = {
            'rank': columns[0].text.strip(),
            'user': columns[1].text.strip(),
            'longest_hitting_streak': columns[2].text.strip(),
            'tiebreaker': columns[3].text.strip(),
        }
        rankings.append(ranking)
    return rankings

def store_data():
    # Store data in the database
    conn = psycopg2.connect("dbname=yourdbname user=youruser password=yourpassword")
    cur = conn.cursor()
    for ranking in rankings:
        cur.execute(
            "INSERT INTO rankings (rank, user, average) VALUES (%s, %s, %s)",
            (ranking['rank'], ranking['user'], ranking['average'])
        )
    conn.commit()
    cur.close()
    conn.close()

def scrape_ranking_data():
    batters = scrape_batters_data()
    pitchers = scrape_pitchers_data()
    home_runs = scrape_home_run_data()
    rbis = scrape_rbi_data()
    stolen_bases = scrape_stolen_base_data()
    dimaggio = scrape_dimaggio_data()
    # function to update user selections numbers


if __name__ == "__main__":
    scrape_ranking_data()
