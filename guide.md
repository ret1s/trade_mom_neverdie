# Redeployment Guide

## Requirements

- Ubuntu 22.04+ (or any Linux distro)
- Node.js 18+ and npm
- Docker and Docker Compose
- Git

---

## 1. Clone both repos

```bash
git clone https://github.com/ret1s/trade_mom_neverdie.git
git clone https://github.com/ret1s/h2v.git
```

---

## 2. trade_mom_neverdie

The database (`trade_mom.db`) is included in the repo.

```bash
cd trade_mom_neverdie
npm install          # installs deps and rebuilds better-sqlite3 for this machine
node index.js        # runs on port 3000
```

To keep it running after logout:

```bash
nohup node index.js > nohup.out 2>&1 &
```

Or with pm2 (recommended):

```bash
npm install -g pm2
pm2 start index.js --name trade_mom
pm2 save
pm2 startup           # follow the printed command to auto-start on reboot
```

---

## 3. h2v

h2v runs in Docker. The database (`sector_rotation.db`) is included in the repo but needs to be copied into the Docker volume after the container is created.

```bash
cd h2v

# Build and start the container (creates the volume)
docker compose up -d --build

# Copy the database into the container volume
docker cp sector_rotation.db h2v:/app/data/sector_rotation.db

# Restart so the app picks up the database
docker compose restart
```

Runs on port **8000**. Check health:

```bash
docker ps
curl http://localhost:8000/healthz
```

---

## Ports summary

| Project           | Port |
|-------------------|------|
| trade_mom_neverdie | 3000 |
| h2v               | 8000 |

---

## Notes

- `venv/` is not committed — it is not needed (h2v deps are installed inside Docker).
- `node_modules/` is not committed — `npm install` recreates it and compiles the native `better-sqlite3` binary for the current machine.
- If you change the port for trade_mom, set the `PORT` environment variable: `PORT=4000 node index.js`.
