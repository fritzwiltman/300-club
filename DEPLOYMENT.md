# 300 Club — Deployment Guide

## Architecture Overview

```
Internet
    │
    ▼
Lightsail instance (single box)
    │
    ├── nginx (host process, port 80/443)
    │     ├── /api/*        → Django container (port 8000, localhost only)
    │     ├── /admin/*      → Django container
    │     ├── /static/*     → /opt/leaderboard/staticfiles/
    │     └── /*            → /opt/leaderboard/frontend/  (Angular SPA)
    │
    ├── Docker Compose
    │     ├── web  (Django/Gunicorn, port 127.0.0.1:8000)
    │     └── db   (PostgreSQL, internal only, named volume)
    │
    └── Cron
          └── daily: docker compose exec web python manage.py collect_stats
```

**Frontend deployment choice:** Static files served by nginx on the same Lightsail box.
Reasons: single box to manage, no extra CDN/S3 cost, nginx already handles static
serving efficiently. The Angular SSR server.mjs is _not_ deployed — we use the
prerendered/static browser build with nginx `try_files` for SPA routing.

---

## Repositories

| Repo | Path on server |
|------|---------------|
| Backend (Django) | `/opt/leaderboard/backend` |
| Frontend (Angular) | Built in CI and rsync'd to `/opt/leaderboard/frontend/` |

---

## One-Time Server Setup (manual steps on Lightsail)

### 1. Create Lightsail instance

- Plan: $10/month (2 vCPU, 1 GB RAM) is fine for this traffic level
- OS: Ubuntu 24.04 LTS
- Enable ports: SSH (22), HTTP (80), HTTPS (443)
- Attach a static IP address
- Point DNS: `300clubleaderboard.com` → static IP

### 2. Install dependencies

```bash
sudo apt-get update && sudo apt-get upgrade -y

# Docker
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
# Log out and back in to apply docker group

# nginx
sudo apt-get install -y nginx rsync

# Verify
docker --version
docker compose version
nginx -v
```

### 3. Clone the backend repo

```bash
sudo mkdir -p /opt/leaderboard
sudo chown $USER:$USER /opt/leaderboard

cd /opt/leaderboard
git clone https://github.com/<your-org>/300-club backend
```

### 4. Create the production environment file

```bash
cp /opt/leaderboard/backend/deploy/.env.production.example /opt/leaderboard/.env.production
chmod 600 /opt/leaderboard/.env.production

# Edit and fill in all real values:
nano /opt/leaderboard/.env.production
```

Required variables in `/opt/leaderboard/.env.production`:

| Variable | Description |
|----------|-------------|
| `DJANGO_SECRET_KEY` | 50+ character random string (use `openssl rand -base64 50`) |
| `DJANGO_DEBUG` | Must be `False` |
| `DJANGO_ALLOWED_HOSTS` | `300clubleaderboard.com www.300clubleaderboard.com` |
| `DB_NAME` | `three_hundred_club` |
| `DB_USER` | `leaderboard` |
| `DB_PASSWORD` | Strong random password |
| `DB_HOST` | `db` (Docker service name) |
| `DB_PORT` | `5432` |
| `CORS_ALLOWED_ORIGINS` | `https://300clubleaderboard.com` |
| `CSRF_TRUSTED_ORIGINS` | `https://300clubleaderboard.com,https://www.300clubleaderboard.com` |

### 5. Configure nginx

```bash
sudo cp /opt/leaderboard/backend/deploy/nginx/nginx.conf /etc/nginx/nginx.conf
sudo nginx -t
sudo systemctl enable nginx
sudo systemctl start nginx
```

### 6. Create required host directories

```bash
# Angular frontend (rsync'd by CI)
sudo mkdir -p /opt/leaderboard/frontend
sudo mkdir -p /opt/leaderboard/frontend-staging
# Django static files (bind-mounted from the web container)
sudo mkdir -p /opt/leaderboard/staticfiles
# Backup storage
sudo mkdir -p /opt/leaderboard/backups
sudo chown $USER:$USER /opt/leaderboard/frontend \
    /opt/leaderboard/frontend-staging \
    /opt/leaderboard/staticfiles \
    /opt/leaderboard/backups
```

### 7. First-time Docker Compose start

```bash
cd /opt/leaderboard/backend/deploy
docker compose --env-file /opt/leaderboard/.env.production up --build -d
docker compose --env-file /opt/leaderboard/.env.production exec web python manage.py migrate
docker compose --env-file /opt/leaderboard/.env.production exec web python manage.py createsuperuser
```

### 8. Configure TLS with Let's Encrypt

```bash
sudo apt-get install -y certbot python3-certbot-nginx
sudo certbot --nginx -d 300clubleaderboard.com -d www.300clubleaderboard.com

# After cert is issued, uncomment the TLS blocks in nginx.conf and reload:
sudo nginx -t && sudo systemctl reload nginx
```

### 9. Allow nginx to reload without password (for CI)

The GitHub Actions workflow runs `sudo systemctl reload nginx`. Add a passwordless
sudoers entry for the deploy user:

```bash
# Replace 'ubuntu' with your actual username
echo "ubuntu ALL=(ALL) NOPASSWD: /bin/systemctl reload nginx, /usr/sbin/nginx -t" \
  | sudo tee /etc/sudoers.d/nginx-reload
sudo chmod 440 /etc/sudoers.d/nginx-reload
```

### 10. Configure daily stats collection

```bash
# Open crontab for the ubuntu user
crontab -e

# Add this line (runs at 10:00 UTC = 6:00 AM ET, roughly):
0 10 * * * cd /opt/leaderboard/backend/deploy && \
  docker compose --env-file /opt/leaderboard/.env.production exec -T web \
  python manage.py collect_stats >> /var/log/leaderboard/collect_stats.log 2>&1
```

Create the log directory:
```bash
sudo mkdir -p /var/log/leaderboard
sudo chown $USER:$USER /var/log/leaderboard
```

---

## GitHub Secrets Required

Configure these in each GitHub repository under Settings → Secrets and variables → Actions.

### Backend repo (`300-club`)

| Secret | Value |
|--------|-------|
| `LIGHTSAIL_HOST` | Public IP or hostname of the Lightsail instance |
| `LIGHTSAIL_USER` | SSH username (e.g., `ubuntu`) |
| `LIGHTSAIL_SSH_KEY` | Full PEM content of the private SSH key |

### Frontend repo (`300-club-frontend`)

| Secret | Value |
|--------|-------|
| `LIGHTSAIL_HOST` | Same as backend |
| `LIGHTSAIL_USER` | Same as backend |
| `LIGHTSAIL_SSH_KEY` | Same as backend (or a separate deploy key) |

To generate a deployment key pair:
```bash
ssh-keygen -t ed25519 -C "github-actions-deploy" -f ~/.ssh/deploy_key -N ""
# Add deploy_key.pub to ~/.ssh/authorized_keys on the server
cat ~/.ssh/deploy_key.pub >> ~/.ssh/authorized_keys
# Add deploy_key content to GitHub secret LIGHTSAIL_SSH_KEY
cat ~/.ssh/deploy_key
```

---

## Local Development Setup

### Backend

```bash
cd 300-club

# Install dependencies (use pipenv or venv)
pip install -r requirements.txt

# Environment is loaded from config/.env.local automatically
# Edit config/.env.local with your local postgres credentials

# Start Django
python manage.py runserver
# API available at http://localhost:8000
```

### Frontend

```bash
cd 300club/300-club-frontend

npm install
npm start
# App available at http://localhost:4200
# API requests to /api/* are proxied to http://localhost:8000
```

---

## Staging Setup

Staging runs on the same Lightsail instance but on `staging.300clubleaderboard.com`.

1. Create `config/.env.staging` on your local machine (gitignored):
   ```
   DJANGO_ENV=staging
   DJANGO_SECRET_KEY=<staging-secret>
   DJANGO_DEBUG=False
   DJANGO_ALLOWED_HOSTS=staging.300clubleaderboard.com
   DB_NAME=three_hundred_club_staging
   DB_USER=leaderboard_staging
   DB_PASSWORD=<staging-db-password>
   DB_HOST=db
   DB_PORT=5432
   CORS_ALLOWED_ORIGINS=https://staging.300clubleaderboard.com
   CSRF_TRUSTED_ORIGINS=https://staging.300clubleaderboard.com
   ```

2. For a staging database, you would run a second Docker Compose stack or
   use a separate database. For a small app, a separate Postgres database
   on the same container with a different `DB_NAME` is simplest.

3. Frontend staging build:
   ```bash
   npx ng build --configuration staging
   rsync dist/300-club-frontend/browser/ user@host:/opt/leaderboard/frontend-staging/
   ```

---

## Deployment Flow

### Backend (automatic on push to main)

1. GitHub Actions connects to Lightsail via SSH
2. `git reset --hard origin/main` (safe: server changes are never the source of truth)
3. `docker compose up --build -d` rebuilds the image and restarts containers
4. `python manage.py migrate --noinput` runs any pending migrations
5. Old image layers are pruned

### Frontend (automatic on push to main)

1. GitHub Actions runs `ng build --configuration production`
2. Output `dist/300-club-frontend/browser/` is rsync'd to `/opt/leaderboard/frontend/`
3. `sudo systemctl reload nginx` makes nginx pick up new `index.html` immediately
4. Old hashed assets are automatically cleaned up by rsync `--delete`

---

## Database Migrations

Migrations run automatically during every backend deploy. To run manually:

```bash
cd /opt/leaderboard/backend/deploy
docker compose --env-file /opt/leaderboard/.env.production exec web python manage.py migrate
```

To create a new migration after model changes:
```bash
# Local only — never create migrations on the server
python manage.py makemigrations
git add leaderboard/migrations/
git commit -m "Add migration: <description>"
```

---

## Database Backup and Restore

### Manual backup (run on the server)

```bash
cd /opt/leaderboard/backend/deploy
docker compose --env-file /opt/leaderboard/.env.production exec db \
  pg_dump -U leaderboard three_hundred_club \
  > /opt/leaderboard/backups/$(date +%Y%m%d_%H%M%S).sql
```

### Restore from a dump

```bash
# Stop the web container to prevent writes during restore
docker compose --env-file /opt/leaderboard/.env.production stop web

docker compose --env-file /opt/leaderboard/.env.production exec -T db \
  psql -U leaderboard three_hundred_club \
  < /opt/leaderboard/backups/20250401_120000.sql

# Restart web
docker compose --env-file /opt/leaderboard/.env.production start web
```

### Automated daily backup (optional but recommended)

```bash
# Add to crontab alongside the stats collection job:
0 9 * * * cd /opt/leaderboard/backend/deploy && \
  docker compose --env-file /opt/leaderboard/.env.production exec -T db \
  pg_dump -U leaderboard three_hundred_club \
  > /opt/leaderboard/backups/$(date +%Y%m%d).sql && \
  # Keep only the last 14 days:
  find /opt/leaderboard/backups -name "*.sql" -mtime +14 -delete
```

---

## Rollback

### Backend rollback

```bash
# On the server — roll back to previous commit
cd /opt/leaderboard/backend
git log --oneline -5         # Find the commit to roll back to
git checkout <commit-hash>
cd deploy
docker compose --env-file /opt/leaderboard/.env.production up --build -d
```

### Frontend rollback

Re-run the GitHub Actions workflow on the previous commit:
1. GitHub → Actions → "Deploy Frontend" → "Re-run jobs" on the desired run

Or manually:
```bash
git checkout <previous-tag>
npm ci
npx ng build --configuration production
rsync dist/300-club-frontend/browser/ user@host:/opt/leaderboard/frontend/
ssh user@host "sudo systemctl reload nginx"
```

---

## Scripts and Scheduled Jobs

### Daily: `collect_stats` (MLB stats update)

This is a Django management command that replaces `scripts/daily/stat_collection.py`.

- **Location:** `leaderboard/management/commands/collect_stats.py`
- **What it does:** Fetches hitter and pitcher stats from the MLB Stats API,
  updates the `hitters`, `pitchers`, and `mlb_leaders` tables.
- **Season variable:** Hardcoded as `DEFAULT_SEASON = 2025` in the command.
  Update this constant at the start of each new MLB season.
- **Production:** Runs daily via cron (see server setup step 10 above).
- **Manual run:**
  ```bash
  docker compose --env-file /opt/leaderboard/.env.production exec web \
    python manage.py collect_stats --season 2025
  ```

### Yearly: `populate_players.py` and `user_selections_scraper.py`

These scripts run once at the start of each MLB season. They are **not** management
commands and still use the `config/config.py` database connection. They should be
run locally against a local database or from the server after updating `config/config.py`
with production credentials.

**Recommended approach for production runs:**

1. SSH into the server
2. Run inside the web container:
   ```bash
   docker compose exec web python scripts/yearly/populate_players.py
   docker compose exec web python scripts/yearly/user_selections_scraper.py
   ```
   Note: These scripts import `config.config` which does not exist in the container.
   You need to either:
   - Add a `config/config.py` with the real DB credentials temporarily (then remove it), or
   - Convert them to management commands (recommended for the next season prep)

**Simplest safe approach:** Run these locally pointing at the production database directly
with a temporary SSH tunnel:
```bash
# On your local machine, open a tunnel to the database
ssh -L 5433:localhost:5432 ubuntu@<server-ip> \
  "docker compose -f /opt/leaderboard/backend/deploy/docker-compose.yaml \
   --env-file /opt/leaderboard/.env.production port db 5432"

# Then run the script locally with DB_HOST=localhost, DB_PORT=5433
```

---

## Monitoring and Logs

```bash
# Django/Gunicorn logs
docker compose logs -f web

# Postgres logs
docker compose logs -f db

# nginx logs
sudo tail -f /var/log/nginx/access.log
sudo tail -f /var/log/nginx/error.log

# Stats collection logs
tail -f /var/log/leaderboard/collect_stats.log

# Check all containers
docker compose ps
```
