from __future__ import annotations
from typing import List, Dict, Tuple
import os

import wntr


def run_wntr_baseline(inp_path: str, dt_s: int, t_end_s: int) -> Tuple[List[Dict], List[str]]:
    """
    一次性跑完 WNTR 仿真（不做 step-by-step），返回 time series:
      [
        {
          "t": 0,
          "tank_level": <float>,
          "<pump_name>_status": "OPEN|CLOSED|ACTIVE|UNKNOWN",
          ...
        },
        ...
      ]
    注意：这里的 tank_level 是“水位 level（相对标高）”，不是 head。
          level = head - elevation
    """
    if not os.path.exists(inp_path):
        raise FileNotFoundError(f"WNTR inp not found: {inp_path}")

    wn = wntr.network.WaterNetworkModel(inp_path)
    pump_names: List[str] = sorted(getattr(wn, "pump_name_list", []) or [])
    sim = wntr.sim.EpanetSimulator(wn)
    results = sim.run_sim()

    if not hasattr(results, "node") or "head" not in results.node:
        raise ValueError("WNTR results missing node head data for tank level extraction")
    pump_status_df = results.link["status"] if hasattr(results, "link") and "status" in results.link else None

    tank_name = "TANK"
    head_df = results.node["head"]
    if tank_name not in head_df.columns:
        raise ValueError(f"TANK '{tank_name}' not found in head results columns: {list(head_df.columns)}")

    # ✅ 关键修复：head -> level
    tank_obj = wn.get_node(tank_name)
    elev = float(getattr(tank_obj, "elevation", 0.0))
    min_level = float(getattr(tank_obj, "min_level", 0.0))
    max_level = float(getattr(tank_obj, "max_level", 0.0))

    head_series = head_df[tank_name]
    times = list(range(0, int(t_end_s) + 1, int(dt_s)))
    series: List[Dict] = []

    def _pump_status_at_time(pump: str, t: int) -> str:
        if pump_status_df is None or pump not in pump_status_df.columns:
            return "UNKNOWN"
        status_series = pump_status_df[pump]
        idx = status_series.index.get_indexer([t], method="nearest")
        if idx.size == 0 or idx[0] == -1:
            return "UNKNOWN"
        raw_val = status_series.iloc[idx[0]]
        if isinstance(raw_val, str):
            return raw_val.upper()
        try:
            val = int(round(float(raw_val)))
        except Exception:
            return str(raw_val)
        if val == 1:
            return "OPEN"
        if val == 0:
            return "CLOSED"
        if val == 2:
            return "ACTIVE"
        return str(raw_val)

    for t in times:
        idx = head_series.index.get_indexer([t], method="nearest")
        if idx.size == 0 or idx[0] == -1:
            level = 0.0
        else:
            head_val = float(head_series.iloc[idx[0]])
            level = head_val - elev
        row: Dict = {"t": int(t), "tank_level": float(level)}
        for pump in pump_names:
            row[f"{pump}_status"] = _pump_status_at_time(pump, t)
        series.append(row)

    # ✅ sanity check：如果 level 明显超出 (min,max) 很多，说明变量取错了
    if series:
        levels = [p["tank_level"] for p in series]
        lvl_min, lvl_max = min(levels), max(levels)
        # 给一点宽容：允许略微超出
        if max_level > 0 and (lvl_max > max_level + 5 or lvl_min < min_level - 5):
            print(
                "[WARN] tank_level seems out of expected range. "
                f"computed level range=({lvl_min:.3f},{lvl_max:.3f}), "
                f"expected approx=({min_level:.3f},{max_level:.3f}), "
                f"elevation={elev:.3f}. Check whether you are using head vs level."
            )

    return series, pump_names
