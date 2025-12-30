# Deployment Guide — distributed-sync-system

Dokumen ini menjelaskan cara menjalankan sistem terdistribusi (3 node + Redis) menggunakan Docker Compose, serta cara mendemonstrasikan fitur Lock Manager (Raft), Distributed Queue (Redis Streams), dan Cache Coherence (MESI demo).

---

## Prasyarat
- Docker Desktop + Docker Compose.
- Port host tersedia:
  - 8001, 8002, 8003 (HTTP API masing-masing node)
  - 6379 (Redis)
  - 8089/8090 (opsional untuk Swagger UI)

---

## Menjalankan Sistem
Dari root project:

docker compose -f docker/docker-compose.yml up --build -d
docker compose -f docker/docker-compose.yml ps


Melihat log:
docker compose -f docker/docker-compose.yml logs --tail=50 node1
docker compose -f docker/docker-compose.yml logs --tail=50 node2
docker compose -f docker/docker-compose.yml logs --tail=50 node3



Stop:
docker compose -f docker/docker-compose.yml down

t

## Verifikasi Cluster (Raft)
Cek status Raft pada tiap node:
curl http://localhost:8001/raft/status
curl http://localhost:8002/raft/status
curl http://localhost:8003/raft/status

text

Pilih node yang `role=leader` untuk operasi lock (acquire/release). Jika request lock terkena HTTP 409 `not_leader`, ulangi ke node leader.

---