from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class _TagState:
    anchor_ts: float
    anchor_value: float
    candidate_ts: float
    candidate_value: float
    lower_slope: float
    upper_slope: float
    last_emitted_ts: float


class SwingingDoorCompressor:
    """
    Swinging Door Trending (SDT) compressor for simulator telemetry.

    The compressor emits:
    - first point per tag
    - points that cause the door to "break" (trend change)
    - heartbeat points when max silence is exceeded
    - non-good quality points (never compressed away)
    """

    def __init__(self, simulator_cfg: dict[str, Any]):
        sdt_cfg = (simulator_cfg or {}).get("sdt", {}) or {}
        self.enabled = bool(sdt_cfg.get("enabled", False))
        self.default_eps_abs = float(sdt_cfg.get("epsilon_abs", 0.0) or 0.0)
        self.default_eps_pct = float(sdt_cfg.get("epsilon_pct", 0.01) or 0.01)
        self.heartbeat_s = float(sdt_cfg.get("heartbeat_ms", 60000) or 60000) / 1000.0
        self.tag_overrides = sdt_cfg.get("tag_overrides", {}) or {}
        self._state: dict[str, _TagState] = {}

    @staticmethod
    def _to_epoch_seconds(ts: datetime) -> float:
        return float(ts.timestamp())

    def _epsilon_for(self, tag_name: str, value: float) -> float:
        override = self.tag_overrides.get(tag_name, {}) or {}
        eps_abs = float(override.get("epsilon_abs", self.default_eps_abs) or 0.0)
        eps_pct = float(override.get("epsilon_pct", self.default_eps_pct) or 0.0)
        return max(eps_abs, abs(value) * eps_pct, 1e-12)

    def _new_state(self, ts_s: float, value: float, epsilon: float) -> _TagState:
        return _TagState(
            anchor_ts=ts_s,
            anchor_value=value,
            candidate_ts=ts_s,
            candidate_value=value,
            lower_slope=-epsilon,
            upper_slope=epsilon,
            last_emitted_ts=ts_s,
        )

    def should_emit(self, key: str, tag_name: str, value: float, quality: str, ts: datetime) -> bool:
        if not self.enabled:
            return True

        # Keep non-good quality samples for PdM signal integrity.
        if quality != "good":
            ts_s = self._to_epoch_seconds(ts)
            epsilon = self._epsilon_for(tag_name, value)
            self._state[key] = self._new_state(ts_s, value, epsilon)
            return True

        ts_s = self._to_epoch_seconds(ts)
        epsilon = self._epsilon_for(tag_name, value)
        state = self._state.get(key)
        if state is None:
            self._state[key] = self._new_state(ts_s, value, epsilon)
            return True

        # Heartbeat to avoid long silent periods for flat signals.
        if (ts_s - state.last_emitted_ts) >= self.heartbeat_s:
            self._state[key] = self._new_state(ts_s, value, epsilon)
            return True

        dt = ts_s - state.anchor_ts
        if dt <= 0:
            return False

        upper = (value + epsilon - state.anchor_value) / dt
        lower = (value - epsilon - state.anchor_value) / dt
        next_upper = min(state.upper_slope, upper)
        next_lower = max(state.lower_slope, lower)

        # Door crossed -> emit the current point and reset from it.
        if next_lower > next_upper:
            self._state[key] = self._new_state(ts_s, value, epsilon)
            return True

        state.upper_slope = next_upper
        state.lower_slope = next_lower
        state.candidate_ts = ts_s
        state.candidate_value = value
        return False
