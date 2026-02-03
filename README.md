# VolGuard 3.3 FastAPI - Docker Deployment Guide

## 📁 Project Structure

```
volguard-deployment/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── entrypoint.sh
├── .env
├── .dockerignore
├── volguard_3.3.py          # Your main application file
├── config/
│   └── grafana_datasources.yaml
├── volguard_data/           # Created automatically
└── volguard_logs/           # Created automatically
```

## 🚀 Quick Start

### 1. Prepare Files

```bash
# Create project directory
mkdir volguard-deployment
cd volguard-deployment

# Create subdirectories
mkdir -p config volguard_data volguard_logs

# Copy all files from artifacts:
# - Dockerfile
# - docker-compose.yml
# - requirements.txt
# - entrypoint.sh
# - .env
# - .dockerignore
# - volguard_3.3.py (rename your file to this)
# - config/grafana_datasources.yaml
```

### 2. Configure Environment

Edit `.env` file with your actual credentials:

```bash
# Required: Replace these values
UPSTOX_ACCESS_TOKEN=your_actual_token
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
GROQ_API_KEY=your_groq_key

# Important: Set to FALSE for live trading
VG_DRY_RUN=TRUE
```

### 3. Fix Line Endings (Windows Users)

```bash
# If on Windows, convert line endings
dos2unix entrypoint.sh

# Or use Git Bash
sed -i 's/\r$//' entrypoint.sh
```

### 4. Build and Start

```bash
# Build the Docker image
docker-compose build

# Start services
docker-compose up -d

# View logs
docker-compose logs -f volguard
```

## 🔍 Testing the API

```bash
# Check health
curl http://localhost:8000/health

# Get service info
curl http://localhost:8000/

# Check market status
curl http://localhost:8000/market/status

# Start trading
curl -X POST http://localhost:8000/trading/start

# Check portfolio
curl http://localhost:8000/portfolio/status

# Run analysis
curl -X POST http://localhost:8000/analysis/run

# Trigger morning brief
curl -X POST http://localhost:8000/morning-brief

# Emergency: Exit all positions
curl -X POST http://localhost:8000/positions/exit-all

# Stop trading
curl -X POST http://localhost:8000/trading/stop
```

## 📊 Access Grafana Dashboard

1. Open browser: `http://localhost:3000`
2. Login: `admin` / `volguard2024` (change this!)
3. Add SQLite data source pointing to: `/var/lib/grafana/data/volguard_mount/volguard.db`

## 🛠️ Management Commands

```bash
# View logs
docker-compose logs -f volguard

# Restart service
docker-compose restart volguard

# Stop all services
docker-compose down

# Stop and remove volumes (WARNING: deletes data)
docker-compose down -v

# Rebuild after code changes
docker-compose up -d --build

# Execute commands inside container
docker-compose exec volguard bash

# View resource usage
docker stats volguard_fastapi
```

## 🔒 Security Checklist

- [ ] Change Grafana admin password in `docker-compose.yml`
- [ ] Set strong passwords in `.env`
- [ ] Keep `.env` out of version control (add to `.gitignore`)
- [ ] Start with `VG_DRY_RUN=TRUE` for testing
- [ ] Monitor Telegram notifications closely
- [ ] Set up regular backups of `volguard_data/`
- [ ] Use firewall to restrict port 8000 access
- [ ] Consider adding API authentication for production

## ⚠️ Before Going Live

1. **Test in Dry Run Mode**
   ```bash
   VG_DRY_RUN=TRUE
   ```
   - Monitor for at least 1 week
   - Verify all strategies work correctly
   - Check Telegram notifications

2. **Verify All Credentials**
   - Test Upstox API access
   - Confirm Telegram bot works
   - Test Groq API key

3. **Set Capital Limits**
   ```bash
   VG_BASE_CAPITAL=1000000        # Your actual capital
   VG_MAX_LOSS_PER_TRADE=50000    # Max loss per trade
   MAX_TRADES_PER_DAY=3           # Daily trade limit
   ```

4. **Enable Live Trading**
   ```bash
   # In .env file
   VG_DRY_RUN=FALSE
   
   # Restart container
   docker-compose restart volguard
   ```

## 📈 Monitoring

### Health Check
```bash
# Should return "healthy"
curl http://localhost:8000/health | jq
```

### Portfolio Status
```bash
# Real-time positions and Greeks
curl http://localhost:8000/portfolio/status | jq
```

### Container Status
```bash
# Check if container is running
docker ps | grep volguard

# Check resource usage
docker stats volguard_fastapi

# Check health status
docker inspect volguard_fastapi | grep -A 5 Health
```

## 🐛 Troubleshooting

### Container won't start
```bash
# Check logs
docker-compose logs volguard

# Common issues:
# - Line ending problems (use dos2unix)
# - Missing .env file
# - Permission issues on volumes
```

### Can't connect to API
```bash
# Check if port is accessible
curl http://localhost:8000/health

# Check if container is running
docker ps

# Check firewall rules
sudo ufw status
```

### Database locked errors
```bash
# Stop all services
docker-compose down

# Remove lock files
rm volguard_data/*.db-shm
rm volguard_data/*.db-wal

# Restart
docker-compose up -d
```

## 📦 Backup Strategy

### Manual Backup
```bash
# Backup data directory
tar -czf volguard_backup_$(date +%Y%m%d).tar.gz volguard_data/

# Backup to remote location
rsync -avz volguard_data/ user@remote:/backups/volguard/
```

### Automated Backup (cron)
```bash
# Add to crontab: Backup daily at 2 AM
0 2 * * * cd /path/to/volguard && tar -czf backups/volguard_$(date +\%Y\%m\%d).tar.gz volguard_data/
```

## 🚨 Emergency Stop

### Kill Switch (File-based)
```bash
# Create kill switch file
touch volguard_data/KILL_SWITCH

# System will stop taking new trades
# Existing positions remain until manual exit
```

### API Emergency Stop
```bash
# Exit all positions immediately
curl -X POST http://localhost:8000/positions/exit-all

# Stop trading engine
curl -X POST http://localhost:8000/trading/stop

# Or stop container entirely
docker-compose stop volguard
```

## 📝 File Descriptions

| File | Purpose |
|------|---------|
| `Dockerfile` | Container image definition |
| `docker-compose.yml` | Multi-container orchestration |
| `requirements.txt` | Python dependencies |
| `entrypoint.sh` | Container startup script |
| `.env` | Environment variables (credentials) |
| `.dockerignore` | Files to exclude from image |
| `grafana_datasources.yaml` | Grafana data source config |

## 🔗 API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Service status |
| GET | `/health` | Health check |
| POST | `/trading/start` | Start trading |
| POST | `/trading/stop` | Stop trading |
| POST | `/analysis/run` | Run market analysis |
| POST | `/morning-brief` | Generate AI brief |
| GET | `/portfolio/status` | Current positions |
| POST | `/positions/exit-all` | Emergency exit |
| GET | `/market/status` | Market open/closed |

## 📞 Support

- Check logs: `docker-compose logs -f volguard`
- Monitor Telegram for alerts
- Review database: `sqlite3 volguard_data/volguard.db`
- Grafana dashboard: `http://localhost:3000`

---

**⚠️ DISCLAIMER**: This is a real money trading system. Test thoroughly in dry-run mode before enabling live trading. Only trade with capital you can afford to lose.
