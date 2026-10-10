"""Die systemd-Units sind Betriebsvertrag: Zeitplan, Pfade, Härtung, keine Geheimnisse."""

import configparser
import re
from pathlib import Path

import pytest

OPS = Path(__file__).resolve().parents[2] / "admin" / "ops"


def unit(name):
    parser = configparser.ConfigParser(strict=False, interpolation=None)
    parser.optionxform = str
    parser.read(OPS / name, encoding="utf-8")
    return parser


def test_token_watch_timer_runs_daily_at_nine_berlin_and_catches_up():
    timer = unit("sgw-token-watch.timer")
    assert timer["Timer"]["OnCalendar"] == "*-*-* 09:00:00 Europe/Berlin"
    assert timer["Timer"]["Persistent"] == "true"
    assert timer["Install"]["WantedBy"] == "timers.target"


def test_backup_timer_runs_daily_at_night_berlin():
    timer = unit("sgw-admin-backup.timer")
    assert timer["Timer"]["OnCalendar"] == "*-*-* 03:30:00 Europe/Berlin"
    assert timer["Timer"]["Persistent"] == "true"
    assert timer["Install"]["WantedBy"] == "timers.target"


def test_token_watch_service_uses_host_python_isolated_and_is_hardened():
    service = unit("sgw-token-watch.service")["Service"]
    assert service["Type"] == "oneshot"
    assert service["ExecStart"] == "/usr/bin/python3 -I /root/sgw-admin/admin/ops/token_watch.py"
    assert service["StateDirectory"] == "sgw-token-watch"
    assert service["StateDirectoryMode"] == "0700"
    assert service["ProtectSystem"] == "strict"
    assert service["ProtectHome"] == "read-only"
    assert service["NoNewPrivileges"] == "yes"
    # Loopback (Evolution) und api.github.com brauchen IP-Sockets.
    assert "AF_INET" in service["RestrictAddressFamilies"].split()
    assert "PrivateNetwork" not in service


def test_backup_service_runs_script_after_docker():
    parsed = unit("sgw-admin-backup.service")
    assert parsed["Service"]["ExecStart"] == "/usr/bin/bash /root/sgw-admin/admin/ops/backup_db.sh"
    assert "docker.service" in parsed["Unit"]["After"]


def test_backup_service_keeps_docker_socket_and_backup_dir_writable():
    service = unit("sgw-admin-backup.service")["Service"]
    assert service["ProtectSystem"] == "full"
    assert "ProtectHome" not in service
    assert "PrivateNetwork" not in service
    assert "ReadWritePaths" not in service


@pytest.mark.parametrize("name", ["sgw-token-watch.service", "sgw-token-watch.timer",
                                  "sgw-admin-backup.service", "sgw-admin-backup.timer"])
def test_units_contain_no_phone_numbers_or_keys(name):
    text = (OPS / name).read_text(encoding="utf-8")
    assert not re.search(r"\d{10,}", text)
    assert "AUTHENTICATION_API_KEY" not in text and "GITHUB_TOKEN" not in text


def test_readme_restore_block_grants_needed_caps_and_chmods_before_chown():
    readme = (OPS.parent / "README.md").read_text(encoding="utf-8")
    block = readme.split("Backup wiederherstellen", 1)[1].split("```bash", 1)[1].split("```", 1)[0]
    assert "--cap-add DAC_OVERRIDE" in block
    assert "--cap-add CHOWN" in block
    assert block.index("chmod 600") < block.index("chown sgw:sgw")


def test_units_have_start_timeouts():
    assert unit("sgw-admin-backup.service")["Service"]["TimeoutStartSec"] == "10min"
    assert unit("sgw-token-watch.service")["Service"]["TimeoutStartSec"] == "5min"


def test_token_watch_retries_hourly_but_not_on_config_errors():
    parsed = unit("sgw-token-watch.service")
    service = parsed["Service"]
    assert service["Restart"] == "on-failure"
    assert service["RestartSec"] == "1h"
    assert service["RestartPreventExitStatus"] == "2 3"
    assert parsed["Unit"]["StartLimitIntervalSec"] == "12h"
    assert parsed["Unit"]["StartLimitBurst"] == "4"


def test_readme_restore_revokes_old_sessions():
    readme = (OPS.parent / "README.md").read_text(encoding="utf-8")
    section = readme.split("Backup wiederherstellen", 1)[1]
    assert "sgw-admin logout-all <login>" in section
    assert "Einladungen" in section


def test_readme_conf_template_has_no_inline_comment():
    readme = (OPS.parent / "README.md").read_text(encoding="utf-8")
    assert not re.search(r"^RECIPIENT=\S+[ \t]+#", readme, re.M)
