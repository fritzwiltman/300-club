"""
Management command: collect_stats

Fetches current MLB stats from the Stats API and updates the hitters,
pitchers, and mlb_leaders tables for the configured season.

This replaces scripts/daily/stat_collection.py with a proper Django
management command so it can run inside the Docker container using the
same database connection and environment variables as the web process.

Usage:
    python manage.py collect_stats              # uses DEFAULT_SEASON constant
    python manage.py collect_stats --season 2025

Cron entry (on the Lightsail host, runs daily at 6 AM ET = 10 AM UTC):
    0 10 * * * cd /opt/leaderboard/backend && docker compose exec -T web \
        python manage.py collect_stats >> /var/log/leaderboard/collect_stats.log 2>&1
"""

import statsapi
from django.core.management.base import BaseCommand, CommandError
from django.db import connection

# Default season — update this at the start of each MLB season
DEFAULT_SEASON = 2025

# Categories to fetch for MLB league leaders
LEADER_CATEGORIES = [
    ("battingAverage", "hitting"),
    ("onBasePlusSlugging", "hitting"),
    ("homeRuns", "hitting"),
    ("wins", "pitching"),
    ("runsBattedIn", "hitting"),
    ("stolenBases", "hitting"),
]


class Command(BaseCommand):
    help = "Fetch MLB stats from the Stats API and update the database."

    def add_arguments(self, parser):
        parser.add_argument(
            "--season",
            type=int,
            default=DEFAULT_SEASON,
            help=f"Season year to update (default: {DEFAULT_SEASON})",
        )

    def handle(self, *args, **options):
        season = options["season"]
        self.stdout.write(f"Collecting stats for {season} season...")

        try:
            self._update_player_stats(season)
            self._update_mlb_leaders(season)
        except Exception as exc:
            raise CommandError(f"Stats collection failed: {exc}") from exc

        self.stdout.write(self.style.SUCCESS(f"Stats collection complete for {season}."))

    # ------------------------------------------------------------------
    # Player stats (hitters / pitchers)
    # ------------------------------------------------------------------

    def _update_player_stats(self, season: int) -> None:
        with connection.cursor() as cur:
            # Hitters
            cur.execute(
                """
                SELECT p.id, p.api_player_id
                FROM players p
                JOIN hitters h ON h.player_id = p.id
                WHERE p.player_type = 'hitter'
                  AND p.api_player_id IS NOT NULL
                  AND h.season = %s
                """,
                (season,),
            )
            hitters = cur.fetchall()

        self.stdout.write(f"  Updating {len(hitters)} hitters...")
        for player_id, api_player_id in hitters:
            stats = self._fetch_hitter_stats(api_player_id, season)
            if stats:
                self._update_hitter(player_id, stats, season)

        with connection.cursor() as cur:
            # Pitchers
            cur.execute(
                """
                SELECT p.id, p.api_player_id
                FROM players p
                JOIN pitchers pt ON pt.player_id = p.id
                WHERE p.player_type = 'pitcher'
                  AND p.api_player_id IS NOT NULL
                  AND pt.season = %s
                """,
                (season,),
            )
            pitchers = cur.fetchall()

        self.stdout.write(f"  Updating {len(pitchers)} pitchers...")
        for player_id, api_player_id in pitchers:
            stats = self._fetch_pitcher_stats(api_player_id, season)
            if stats:
                self._update_pitcher(player_id, stats, season)

    def _fetch_hitter_stats(self, api_player_id: int, season: int):
        try:
            response = statsapi.get(
                "person",
                {
                    "personId": api_player_id,
                    "hydrate": (
                        f"stats(group=[hitting],type=[season],"
                        f"season={season},gameType=R)"
                    ),
                },
            )
            stats = response["people"][0]["stats"][0]["splits"][0]["stat"]
            return (
                stats.get("avg", 0),
                stats.get("ops", 0),
                stats.get("plateAppearances", 0),
                stats.get("homeRuns", 0),
                stats.get("rbi", 0),
                stats.get("stolenBases", 0),
            )
        except Exception as exc:
            self.stderr.write(
                f"    Warning: could not fetch hitter stats for {api_player_id}: {exc}"
            )
            return None

    def _fetch_pitcher_stats(self, api_player_id: int, season: int):
        try:
            response = statsapi.get(
                "person",
                {
                    "personId": api_player_id,
                    "hydrate": (
                        f"stats(group=[pitching],type=[season],"
                        f"season={season},gameType=R)"
                    ),
                },
            )
            stats = response["people"][0]["stats"][0]["splits"][0]["stat"]
            return (
                stats.get("wins", 0),
                stats.get("losses", 0),
                stats.get("era", 0),
                stats.get("strikeOuts", 0),
            )
        except Exception as exc:
            self.stderr.write(
                f"    Warning: could not fetch pitcher stats for {api_player_id}: {exc}"
            )
            return None

    def _update_hitter(self, player_id: int, stats: tuple, season: int) -> None:
        with connection.cursor() as cur:
            cur.execute(
                """
                UPDATE hitters
                SET average=%s, ops=%s, plate_appearances=%s,
                    home_runs=%s, rbis=%s, stolen_bases=%s
                WHERE player_id=%s AND season=%s
                """,
                (*stats, player_id, season),
            )

    def _update_pitcher(self, player_id: int, stats: tuple, season: int) -> None:
        with connection.cursor() as cur:
            cur.execute(
                """
                UPDATE pitchers
                SET wins=%s, losses=%s, era=%s, strikeouts=%s
                WHERE player_id=%s AND season=%s
                """,
                (*stats, player_id, season),
            )

    # ------------------------------------------------------------------
    # MLB league leaders
    # ------------------------------------------------------------------

    def _update_mlb_leaders(self, season: int) -> None:
        self.stdout.write("  Updating MLB league leaders...")
        with connection.cursor() as cur:
            cur.execute("DELETE FROM mlb_leaders WHERE season = %s", (season,))

        total = 0
        for category, stat_group in LEADER_CATEGORIES:
            leaders = self._fetch_leaders(category, stat_group, season)
            self.stdout.write(f"    {category}: {len(leaders)} leaders")
            with connection.cursor() as cur:
                for rank, player_name, team, value, api_player_id in leaders:
                    cur.execute(
                        """
                        INSERT INTO mlb_leaders
                            (season, category, rank, player_name, team, value, api_player_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (season, category, rank, player_name, team, value, api_player_id),
                    )
                    total += 1
        self.stdout.write(f"  Inserted {total} MLB leader records.")

    def _fetch_leaders(
        self, category: str, stat_group: str, season: int, limit: int = 20
    ) -> list:
        try:
            response = statsapi.get(
                "stats_leaders",
                {
                    "leaderCategories": category,
                    "season": season,
                    "limit": limit,
                    "statGroup": stat_group,
                    "gameTypes": "R",
                    "hydrate": "team",
                },
            )
            if not response.get("leagueLeaders") or not response["leagueLeaders"][0].get(
                "leaders"
            ):
                return []

            leaders = []
            for leader in response["leagueLeaders"][0]["leaders"]:
                person = leader.get("person", {})
                team = leader.get("team", {})
                raw_value = leader.get("value", "0")
                try:
                    value = float(raw_value) if "." in str(raw_value) else float(int(raw_value))
                except (ValueError, TypeError):
                    value = 0.0
                leaders.append(
                    (
                        leader.get("rank"),
                        person.get("fullName"),
                        team.get("abbreviation"),
                        value,
                        person.get("id"),
                    )
                )
            return leaders
        except Exception as exc:
            self.stderr.write(f"    Warning: could not fetch {category} leaders: {exc}")
            return []
