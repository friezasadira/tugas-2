from __future__ import annotations

from typing import List
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_env: str = Field(default="dev", alias="APP_ENV")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    node_id: str = Field(default="node1", alias="NODE_ID")
    role: str = Field(default="all", alias="ROLE")

    http_host: str = Field(default="0.0.0.0", alias="HTTP_HOST")
    http_port: int = Field(default=8000, alias="HTTP_PORT")

    peers: str = Field(default="", alias="PEERS")
    redis_url: str = Field(default="redis://localhost:6379/0", alias="REDIS_URL")

    raft_election_timeout_min_ms: int = Field(default=800, alias="RAFT_ELECTION_TIMEOUT_MIN_MS")
    raft_election_timeout_max_ms: int = Field(default=1500, alias="RAFT_ELECTION_TIMEOUT_MAX_MS")
    raft_heartbeat_interval_ms: int = Field(default=250, alias="RAFT_HEARTBEAT_INTERVAL_MS")
    raft_data_dir: str = Field(default="/data", alias="RAFT_DATA_DIR")


    metrics_port: int = Field(default=9000, alias="METRICS_PORT")

    def peer_list(self) -> List[str]:
        if not self.peers.strip():
            return []
        return [p.strip() for p in self.peers.split(",") if p.strip()]


settings = Settings()
