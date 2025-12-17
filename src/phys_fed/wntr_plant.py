from __future__ import annotations

from dataclasses import dataclass, field

from typing import Dict, Any, List, Optional

from wntr.network.base import LinkStatus



import os

import wntr





@dataclass

class WNTRState:

    # 你可以后续扩展 pressure/flow 等

    tank_level: Dict[str, float]

    pump_status: Dict[str, str] = field(default_factory=dict)





class WNTRPlant:

    def __init__(self, inp_path: str, sensors: Dict[str, Any], actuators: Dict[str, Any]):

        if not os.path.exists(inp_path):

            raise FileNotFoundError(f"WNTR inp not found: {inp_path}")



        self.inp_path = inp_path

        self.wn = wntr.network.WaterNetworkModel(inp_path)

        self.sensors = sensors

        self.actuators = actuators



        # 预解析出 tank/pump 名称，兼容 {"tank_level": [{"tank": "TANK"}]} 或直接字符串列表

        self._tank_names = []

        for entry in self.sensors.get("tank_level", []):

            if isinstance(entry, dict):

                name = entry.get("tank") or entry.get("id") or entry.get("name")

            else:

                name = entry

            if name:

                self._tank_names.append(str(name))



        self._pump_names = []

        for entry in self.actuators.get("pumps", []):

            if isinstance(entry, dict):

                name = entry.get("pump") or entry.get("id") or entry.get("name")

            else:

                name = entry

            if name:

                self._pump_names.append(str(name))



        self.sim_time = 0.0

        self._last_cmd: Dict[str, Any] = {"pumps": {}}

        self._default_tank: Optional[str] = self._tank_names[0] if self._tank_names else None



    def reset(self) -> WNTRState:

        self.sim_time = 0.0

        # WNTR 的“状态”通常通过运行仿真得到；最小实现：先跑 0~dt 或直接读初值

        # 这里先直接读 tank 初始水位（如果存在）

        return self._observe()



    def step(self, dt: float, cmd: Dict[str, Any]) -> WNTRState:

        """

        推进物理一步：

        - 应用泵的 OPEN/CLOSED

        - 将 EPANET/WNTR 仿真推进到 self.sim_time + dt（通过设置 duration）

        - 从 results 中取目标时刻的 TANK level（head - elevation）

        """

        self._last_cmd = cmd or {"pumps": {}}



        # 1) 应用泵命令到 wn

        pumps_cmd = (self._last_cmd.get("pumps") or {}) if isinstance(self._last_cmd, dict) else {}

        for pump_name, state in pumps_cmd.items():

            if pump_name not in self._pump_names:

                continue

            try:

                pump = self.wn.get_link(pump_name)

            except Exception:

                continue



            s = str(state).upper()

            if s in ("OPEN", "1", "ON", "TRUE"):

                pump.initial_status = LinkStatus.Opened

            elif s in ("CLOSED", "0", "OFF", "FALSE"):

                pump.initial_status = LinkStatus.Closed



        # 2) 推进时间

        self.sim_time += float(dt)

        target_t = int(round(self.sim_time))



        # 3) 设置仿真只跑到 target_t

        #    注意：这里是最小可行做法（每步重跑 0..target_t），先求正确性再谈性能/连续状态

        self.wn.options.time.duration = target_t

        # 让报表步长/水力步长更贴合你的 dt（可选，但建议）

        try:

            self.wn.options.time.hydraulic_timestep = int(round(dt))

            self.wn.options.time.report_timestep = int(round(dt))

        except Exception:

            pass



        # 4) 运行仿真并抽取 target_t 时刻的状态

        sim = wntr.sim.EpanetSimulator(self.wn)

        results = sim.run_sim()



        return self._observe_from_results(results, target_t)



    def make_record(self, t: int, state: WNTRState, tank_id: Optional[str] = None) -> Dict[str, Any]:

        """

        将当前状态打平为一行记录，便于写入 CSV，格式类似 baseline：

          {"t": 0, "tank_level": 1.23, "PUMP1_status": "OPEN", ...}

        """

        tank_key = tank_id or self._default_tank

        row: Dict[str, Any] = {"t": int(t)}

        row["tank_level"] = state.tank_level.get(tank_key, None) if tank_key else None

        for pump in self._pump_names:

            row[f"{pump}_status"] = state.pump_status.get(pump, "UNKNOWN")

        return row



    def write_records_csv(self, records: List[Dict[str, Any]], out_path: str) -> None:

        """按照 baseline 形式写出 CSV：t, tank_level, <pump>_status..."""

        import csv

        os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

        pump_cols = [f"{p}_status" for p in self._pump_names]

        with open(out_path, "w", encoding="utf-8", newline="") as f:

            writer = csv.writer(f)

            writer.writerow(["t", "tank_level", *pump_cols])

            for row in records:

                writer.writerow([row.get("t", ""), row.get("tank_level", ""), *[row.get(col, "") for col in pump_cols]])





    def _observe(self) -> WNTRState:

        # 读取 tanks 的初始水位（若 WNTR 模型里可取）

        tank_level: Dict[str, float] = {}

        pump_status: Dict[str, str] = {}

        for tank_name in self._tank_names:

            tank = self.wn.get_node(tank_name)

            # WNTR tank 有 init_level 属性（可能为 None）

            lvl = getattr(tank, "init_level", None)

            tank_level[tank_name] = float(lvl) if lvl is not None else 0.0

        for pump_name in self._pump_names:

            try:

                pump = self.wn.get_link(pump_name)

                pump_status[pump_name] = self._status_to_str(getattr(pump, "initial_status", None))

            except Exception:

                pump_status[pump_name] = "UNKNOWN"

        return WNTRState(tank_level=tank_level, pump_status=pump_status)



    def _observe_from_results(self, results, target_t: int) -> WNTRState:

        tank_level: Dict[str, float] = {}

        pump_status: Dict[str, str] = {}

        head_df = results.node["head"] if hasattr(results, "node") and "head" in results.node else None

        for tank_name in self._tank_names:

            level_val = 0.0

            if head_df is not None and tank_name in head_df.columns:

                series = head_df[tank_name]

                idx = series.index.get_indexer([target_t], method="nearest")

                if idx.size > 0 and idx[0] != -1:

                    try:

                        head_val = float(series.iloc[idx[0]])

                        elev = float(getattr(self.wn.get_node(tank_name), "elevation", 0.0))

                        level_val = head_val - elev

                    except Exception:

                        level_val = 0.0

            tank_level[tank_name] = level_val


        status_df = results.link["status"] if hasattr(results, "link") and "status" in results.link else None

        for pump_name in self._pump_names:

            status_val_str = "UNKNOWN"

            if status_df is not None and pump_name in status_df.columns:

                series = status_df[pump_name]

                idx = series.index.get_indexer([target_t], method="nearest")

                if idx.size > 0 and idx[0] != -1:

                    status_val_str = self._status_to_str(series.iloc[idx[0]])

            else:

                try:

                    pump = self.wn.get_link(pump_name)

                    status_val_str = self._status_to_str(getattr(pump, "initial_status", None))

                except Exception:

                    status_val_str = "UNKNOWN"

            pump_status[pump_name] = status_val_str


        return WNTRState(tank_level=tank_level, pump_status=pump_status)



    @staticmethod

    def _status_to_str(val: Any) -> str:

        # LinkStatus 枚举或数值 -> OPEN/CLOSED/ACTIVE 字符串

        if isinstance(val, str):

            return val.upper()


        if isinstance(val, LinkStatus):

            if val == LinkStatus.Opened:

                return "OPEN"

            if val == LinkStatus.Closed:

                return "CLOSED"

            if val == LinkStatus.Active:

                return "ACTIVE"

            return str(val)


        try:

            ival = int(round(float(val)))

        except Exception:

            return "UNKNOWN" if val is None else str(val)


        if ival == int(LinkStatus.Opened):

            return "OPEN"

        if ival == int(LinkStatus.Closed):

            return "CLOSED"

        if ival == int(LinkStatus.Active):

            return "ACTIVE"

        return str(val)
