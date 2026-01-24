# 300-Club Project

This repository manages a fantasy baseball leaderboard system for the 300-Club. It includes **scripts** to populate and update PostgreSQL tables, as well as a **Django** backend to serve relevant data via API endpoints.

---

## Table of Contents

- [Data Population Scripts](#data-population-scripts)
  - [user_selections_scraper.py](#user_selections_scraperpy)
  - [populate_players.py](#populate_playerspy)
  - [data_scraper.py](#data_scraperpy)
- [Running the Backend Server](#running-the-backend-server)
- [Existing API Endpoint](#existing-api-endpoints)

---

## Data Population Scripts

Several python3 scripts exist to **initialize** and **populate** tables in the PostgreSQL database:

### `user_selections_scraper.py`

- **Purpose**: Scrapes user picks (e.g., from a CSV or website) and populates the `picks` table.
- **Usage**:
  ```bash
  python3 scripts/yearly/user_selections_scraper.py
  ```
  Typically run at the start of the season or whenever user picks change significantly.

### `populate_players.py`

- **Purpose**: Reads unique players selected by users and populates the players table, optionally assigning api_player_id values.
- **Usage**:
  ```bash
  python3 scripts/yearly/populate_players.py
  ```
  Run this **after** collecting user picks to ensure the players table is filled.

### `data_scraper.py`

- **Purpose**: Fetches stats (batting average, OPS, ERA, wins, etc.) from an external API ([MLB Stats API](https://github.com/toddrob99/MLB-StatsAPI)) and updates the hitters and pitchers tables.
- **Usage**:
  ``` bash
  python scripts/daily/data_scraper.py
  ```
  Typically automated via a cron job or scheduled task (daily/weekly) to keep stats current.

---

## Running the Backend Server

The backend is built with **Django** and interfaces with a PostgreSQL database.

1. **Install Dependencies**

   ```bash
   pip install -r requirements.txt
   ```

2. **Set Environment Variables**
   If using a .env.local file:
   ```bash
   export ENV_FILE=config/.env.local
   ```
   Adjust for production or other environments as needed.

3. **Apply Migrations** (Optional)

   ```bash
   python3 manage.py makemigrations
   python3 manage.py migrate
   ```

   If your tables already exist and you’ve used `managed=False` or `--fake-initial`, this step might do nothing. Otherwise, Django updates your database.

4. **Start the Development Server**
   ```bash
   python3 manage.py runserver
   ```
   The server runs at http://127.0.0.1:8000 by default.

---

## API Endpoints

All leaderboard endpoints support an optional `?season=YYYY` query parameter to retrieve data for a specific season (defaults to 2024).

### Leaderboard Endpoints

- `GET /leaderboard/batters/`
  - **Description**: Returns batting average leaderboard rankings. Users are ranked by aggregate batting average of their 10 qualified batters (alternates fill in for disqualified picks).
  - **Query Parameters**: `?season=YYYY` (optional)

- `GET /leaderboard/ops/`
  - **Description**: Returns OPS leaderboard rankings. Uses the same batters as the batting average contest, ranked by aggregate OPS.
  - **Query Parameters**: `?season=YYYY` (optional)

- `GET /leaderboard/homeruns/`
  - **Description**: Returns home run leaderboard rankings. Top 3 of 4 picked players count toward total home runs.
  - **Query Parameters**: `?season=YYYY` (optional)

- `GET /leaderboard/pitchers/`
  - **Description**: Returns pitcher leaderboard rankings. Top 3 of 4 pitchers count toward total wins. Tiebreakers: 4th pick wins → win percentage → aggregate ERA → alternates average.
  - **Query Parameters**: `?season=YYYY` (optional)

- `GET /leaderboard/rbi-champion/`
  - **Description**: Returns RBI champion leaderboard. Users must pick the correct player who leads MLB in RBIs. Among correct pickers, closest to actual RBI count wins.
  - **Query Parameters**: `?season=YYYY` (optional)

- `GET /leaderboard/stolen-bases/`
  - **Description**: Returns stolen base champion leaderboard. Users must pick the correct player who leads MLB in stolen bases. Among correct pickers, closest to actual SB count wins.
  - **Query Parameters**: `?season=YYYY` (optional)

- `GET /leaderboard/dimaggio/`
  - **Description**: Returns DiMaggio Prize leaderboard. Users predict the longest hitting streak of the season. Exact number required to win.
  - **Query Parameters**: `?season=YYYY` (optional)

### Supporting Endpoints

- `GET /leaderboard/players/`
  - **Description**: Returns a JSON list of all players from the players table.
  - **Sample Response**:
  ```json
  [
    {
      "id": 1,
      "player_name": "Aaron Judge",
      "player_type": "hitter",
      "api_player_id": 123456
    }
  ]
  ```

- `GET /leaderboard/users/`
  - **Description**: Returns a list of all contestants/users.

- `GET /leaderboard/categories/`
  - **Description**: Returns a list of competition categories with metadata (display name, picks per user, description).
