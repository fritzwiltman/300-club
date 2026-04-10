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

All leaderboard endpoints support an optional `?season=YYYY` query parameter to retrieve data for a specific season (defaults to 2025).

### Leaderboard Endpoints

#### `GET /leaderboard/batters/`

Returns batting average leaderboard rankings. Users are ranked by aggregate batting average of their 10 qualified batters (alternates fill in for disqualified picks).

**Query Parameters**: `?season=YYYY` (optional)

**Response Schema**:
```json
[
  {
    "user_name": "string",
    "aggregate_average": "number | null",
    "alternate_average": "number | null",
    "aggregate_ops": "number | null",
    "qualified_picks": [
      { "player_name": "string", "average": "number", "ops": "number" }
    ],
    "disqualified_picks": [
      { "player_name": "string", "plate_appearances": "number" }
    ],
    "rank": "number (0 = disqualified)"
  }
]
```

---

#### `GET /leaderboard/ops/`

Returns OPS leaderboard rankings. Uses the same batters as the batting average contest, ranked by aggregate OPS.

**Query Parameters**: `?season=YYYY` (optional)

**Response Schema**:
```json
[
  {
    "user_name": "string",
    "aggregate_ops": "number | null",
    "alternate_average": "number | null",
    "qualified_picks": [
      { "player_name": "string", "ops": "number", "average": "number" }
    ],
    "disqualified_picks": [
      { "player_name": "string", "plate_appearances": "number" }
    ],
    "rank": "number (0 = disqualified)"
  }
]
```

---

#### `GET /leaderboard/homeruns/`

Returns home run leaderboard rankings. Top 3 of 4 picked players count toward total home runs.

**Query Parameters**: `?season=YYYY` (optional)

**Response Schema**:
```json
[
  {
    "user_name": "string",
    "top_three_total_homeruns": "number",
    "first_tiebreaker_homeruns": "number | null",
    "second_tiebreaker_average": "number",
    "rank": "number",
    "all_homerun_picks": [
      { "player_name": "string", "home_runs": "number" }
    ],
    "alternate_batters_picks": [
      { "player_name": "string", "average": "number", "is_disqualified": "boolean" }
    ]
  }
]
```

---

#### `GET /leaderboard/pitchers/`

Returns pitcher leaderboard rankings. Top 3 of 4 pitchers count toward total wins.

**Tiebreakers**: 4th pick wins → win percentage → aggregate ERA → alternates average

**Query Parameters**: `?season=YYYY` (optional)

**Response Schema**:
```json
[
  {
    "user_name": "string",
    "top_three_total_wins": "number",
    "first_tiebreaker_wins": "number | null",
    "second_tiebreaker_win_pct": "number | null",
    "third_tiebreaker_era": "number | null",
    "fourth_tiebreaker_alt_avg": "number | null",
    "rank": "number",
    "pitcher_picks": [
      {
        "player_name": "string",
        "wins": "number",
        "losses": "number",
        "era": "number | null",
        "strikeouts": "number"
      }
    ]
  }
]
```

---

#### `GET /leaderboard/rbi-champion/`

Returns RBI champion leaderboard. Users must pick the correct player who leads MLB in RBIs. Among correct pickers, closest to actual RBI count wins.

**Query Parameters**: `?season=YYYY` (optional)

**Response Schema**:
```json
{
  "actual_rbi_leader": {
    "player_name": "string",
    "rbis": "number"
  },
  "leaderboard": [
    {
      "user_name": "string",
      "predicted_player": "string",
      "predicted_rbis": "number | null",
      "predicted_correct_player": "boolean",
      "rbi_difference": "number | null",
      "alternates_average": "number | null",
      "rank": "number | null"
    }
  ]
}
```

---

#### `GET /leaderboard/stolen-bases/`

Returns stolen base champion leaderboard. Users must pick the correct player who leads MLB in stolen bases. Among correct pickers, closest to actual SB count wins.

**Query Parameters**: `?season=YYYY` (optional)

**Response Schema**:
```json
{
  "actual_sb_leader": {
    "player_name": "string",
    "stolen_bases": "number"
  },
  "leaderboard": [
    {
      "user_name": "string",
      "predicted_player": "string",
      "predicted_stolen_bases": "number | null",
      "predicted_correct_player": "boolean",
      "sb_difference": "number | null",
      "alternates_average": "number | null",
      "rank": "number | null"
    }
  ]
}
```

---

#### `GET /leaderboard/dimaggio/`

Returns DiMaggio Prize leaderboard. Users predict the longest hitting streak of the season. Exact number required to win.

**Query Parameters**: `?season=YYYY` (optional)

**Response Schema**:
```json
{
  "actual_longest_streak": "number | null",
  "streak_holder_name": "string | null",
  "leaderboard": [
    {
      "user_name": "string",
      "predicted_streak": "number | null",
      "is_exact_match": "boolean",
      "alternates_average": "number | null",
      "rank": "number | null"
    }
  ]
}
```

### MLB League Leaders Endpoints

These endpoints return the top 20 MLB players for each statistical category, fetched live from the MLB Stats API. Useful for displaying league-wide leaders alongside user picks.

#### `GET /leaderboard/batters/mlb-leaders/`

Returns top 20 MLB batting average leaders.

**Query Parameters**: `?season=YYYY` (optional)

**Response Schema**:
```json
{
  "leaders": [
    {
      "rank": 1,
      "player_name": "Luis Arraez",
      "team": "SD",
      "value": 0.354,
      "headshot_url": "https://img.mlbstatic.com/mlb-photos/image/upload/w_180,q_100/v1/people/660670/headshot/silo/current"
    }
  ]
}
```

---

#### `GET /leaderboard/ops/mlb-leaders/`

Returns top 20 MLB OPS leaders.

**Query Parameters**: `?season=YYYY` (optional)

**Response Schema**: Same as batters/mlb-leaders (value is OPS, e.g., 0.945)

---

#### `GET /leaderboard/homeruns/mlb-leaders/`

Returns top 20 MLB home run leaders.

**Query Parameters**: `?season=YYYY` (optional)

**Response Schema**: Same as batters/mlb-leaders (value is HR count, e.g., 58)

---

#### `GET /leaderboard/pitchers/mlb-leaders/`

Returns top 20 MLB pitching wins leaders.

**Query Parameters**: `?season=YYYY` (optional)

**Response Schema**: Same as batters/mlb-leaders (value is win count, e.g., 18)

---

#### `GET /leaderboard/rbi-champion/mlb-leaders/`

Returns top 20 MLB RBI leaders.

**Query Parameters**: `?season=YYYY` (optional)

**Response Schema**: Same as batters/mlb-leaders (value is RBI count, e.g., 144)

---

#### `GET /leaderboard/stolen-bases/mlb-leaders/`

Returns top 20 MLB stolen base leaders.

**Query Parameters**: `?season=YYYY` (optional)

**Response Schema**: Same as batters/mlb-leaders (value is SB count, e.g., 67)

---

### Supporting Endpoints

#### `GET /leaderboard/players/`

Returns a JSON list of all players from the players table.

**Response Schema**:
```json
[
  {
    "id": "number",
    "player_name": "string",
    "player_type": "hitter | pitcher",
    "api_player_id": "number | null"
  }
]
```

---

#### `GET /leaderboard/users/`

Returns a list of all contestants/users.

**Response Schema**:
```json
[
  {
    "mbr_id": "number",
    "name": "string"
  }
]
```

---

#### `GET /leaderboard/categories/`

Returns a list of competition categories with metadata.

**Response Schema**:
```json
[
  {
    "id": "number",
    "name": "string",
    "display_name": "string",
    "picks_per_user": "number",
    "description": "string"
  }
]
```
