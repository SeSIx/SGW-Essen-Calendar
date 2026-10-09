"""The deployment files keep the promises the spec makes (and Phase 3 relies on)."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def read(rel):
    return (ROOT / rel).read_text(encoding="utf-8")


def test_compose_hardening_and_names():
    c = read("admin/compose.yml")
    for needle in (
        "container_name: sgw-admin", "SGW_ADMIN_DB: /data/sgw-admin.db", "SGW_ADMIN_ENV: production",
        "env_file: .env", "read_only: true", "- ALL", "no-new-privileges:true", "sgw-admin-data:/data",
        "traefik.enable=true", "traefik.docker.network=root_default",
        "traefik.http.routers.sgw-admin.rule=Host(`sgw-admin.srv1136792.hstgr.cloud`)",
        "traefik.http.routers.sgw-admin.entrypoints=websecure",
        "traefik.http.routers.sgw-admin.tls.certresolver=mytlschallenge",
        "traefik.http.services.sgw-admin.loadbalancer.server.port=8000", "external: true",
    ):
        assert needle in c, needle
    assert "ports:" not in c, "no host ports: there is no host firewall"
    assert "docker.sock" not in c


def test_dockerfile_runs_as_non_root_and_ships_the_cli():
    d = read("admin/Dockerfile")
    assert "USER sgw" in d and "/usr/local/bin/sgw-admin" in d and "admin.wsgi:app" in d
    assert "SGW_ADMIN_DB=/data/sgw-admin.db" in d


def test_healthcheck_sends_the_production_host_and_proto():
    d = read("admin/Dockerfile")
    assert "HEALTHCHECK" in d and "sgw-admin.srv1136792.hstgr.cloud" in d and "X-Forwarded-Proto" in d


def test_secrets_never_reach_git_or_the_image():
    assert "admin/.env" in read(".gitignore")
    ignore = read("admin/Dockerfile.dockerignore")
    assert "admin/.env" in ignore and "admin/e2e/" in ignore


def test_e2e_only_binds_localhost():
    e = read("admin/e2e/compose.e2e.yml")
    assert '"127.0.0.1:8099:8000"' in e and "SGW_ADMIN_ENV: e2e" in e
    assert "container_name" not in e and "root_default" not in e
