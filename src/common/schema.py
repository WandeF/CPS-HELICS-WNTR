from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class SensorSnapshot:
    # 示例：{"TANK1": 3.14}
    tank_level: Dict[str, float]

    def to_dict(self) -> Dict[str, Any]:
        return {"tank_level": dict(self.tank_level)}

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "SensorSnapshot":
        tl = d.get("tank_level", {}) or {}
        return SensorSnapshot(tank_level={str(k): float(v) for k, v in tl.items()})


@dataclass
class ActuatorCommand:
    # 示例：{"PUMP1": "OPEN"/"CLOSED"}
    pumps: Dict[str, str]

    def to_dict(self) -> Dict[str, Any]:
        return {"pumps": dict(self.pumps or {})}

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "ActuatorCommand":
        pumps = d.get("pumps", {}) or {}
        # 允许 OPEN/CLOSED 字符串（或其他可转成字符串的值）
        return ActuatorCommand(pumps={str(k): str(v) for k, v in pumps.items()})
