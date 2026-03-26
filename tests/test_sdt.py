from datetime import datetime, timedelta, timezone

from core.simulator.sdt import SwingingDoorCompressor


def _cfg(enabled=True, eps_abs=0.5, eps_pct=0.0, heartbeat_ms=60000):
    return {
        "sdt": {
            "enabled": enabled,
            "epsilon_abs": eps_abs,
            "epsilon_pct": eps_pct,
            "heartbeat_ms": heartbeat_ms,
        }
    }


def _cfg_with_override():
    return {
        "sdt": {
            "enabled": True,
            "epsilon_abs": 1.0,
            "epsilon_pct": 0.0,
            "heartbeat_ms": 60000,
            "tag_overrides": {
                "fast_tag": {"epsilon_abs": 0.05, "epsilon_pct": 0.0, "heartbeat_ms": 60000}
            },
        }
    }


def test_sdt_emits_first_point():
    sdt = SwingingDoorCompressor(_cfg())
    now = datetime.now(timezone.utc)
    assert sdt.should_emit("A::temp", "temp", 100.0, "good", now) is True


def test_sdt_suppresses_small_changes_inside_door():
    sdt = SwingingDoorCompressor(_cfg(eps_abs=1.0, heartbeat_ms=10_000))
    now = datetime.now(timezone.utc)
    assert sdt.should_emit("A::temp", "temp", 100.0, "good", now) is True
    # Small movement under epsilon should be compressed away.
    assert sdt.should_emit("A::temp", "temp", 100.4, "good", now + timedelta(seconds=1)) is False


def test_sdt_emits_when_trend_breaks():
    sdt = SwingingDoorCompressor(_cfg(eps_abs=0.2, heartbeat_ms=60_000))
    now = datetime.now(timezone.utc)
    assert sdt.should_emit("A::temp", "temp", 100.0, "good", now) is True
    assert sdt.should_emit("A::temp", "temp", 100.1, "good", now + timedelta(seconds=1)) is False
    # Big jump breaks door and should emit.
    assert sdt.should_emit("A::temp", "temp", 104.0, "good", now + timedelta(seconds=2)) is True


def test_sdt_heartbeat_forces_emit():
    sdt = SwingingDoorCompressor(_cfg(eps_abs=5.0, heartbeat_ms=1000))
    now = datetime.now(timezone.utc)
    assert sdt.should_emit("A::temp", "temp", 100.0, "good", now) is True
    # Within door and before heartbeat -> dropped.
    assert sdt.should_emit("A::temp", "temp", 100.1, "good", now + timedelta(milliseconds=500)) is False
    # After heartbeat -> emitted.
    assert sdt.should_emit("A::temp", "temp", 100.2, "good", now + timedelta(milliseconds=1200)) is True


def test_sdt_never_drops_bad_quality():
    sdt = SwingingDoorCompressor(_cfg())
    now = datetime.now(timezone.utc)
    assert sdt.should_emit("A::temp", "temp", 100.0, "good", now) is True
    assert sdt.should_emit("A::temp", "temp", 100.1, "bad", now + timedelta(seconds=1)) is True


def test_sdt_tag_override_tightens_compression_door():
    sdt = SwingingDoorCompressor(_cfg_with_override())
    now = datetime.now(timezone.utc)

    assert sdt.should_emit("A::slow_tag", "slow_tag", 100.0, "good", now) is True
    assert sdt.should_emit("A::fast_tag", "fast_tag", 100.0, "good", now) is True

    # Same delta for both tags; default (slow_tag) should compress it away,
    # while fast_tag override with tighter epsilon should emit.
    assert sdt.should_emit("A::slow_tag", "slow_tag", 100.2, "good", now + timedelta(seconds=1)) is False
    assert sdt.should_emit("A::fast_tag", "fast_tag", 100.2, "good", now + timedelta(seconds=1)) is True
