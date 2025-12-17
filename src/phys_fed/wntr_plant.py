from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
import os
import csv
import tempfile
import wntr
# 引入底层 Toolkit 接口
from wntr.epanet.toolkit import ENepanet

# 定义一些常用的 EPANET 常量，避免魔术数字
EN_ELEVATION = 0
EN_HEAD      = 11
EN_PRESSURE  = 12
EN_STATUS    = 11  # Link status
EN_PUMP      = 2   # Link type PUMP
EN_OPEN      = 1
EN_CLOSED    = 0

@dataclass
class WNTRState:
    tank_level: Dict[str, float]
    pump_status: Dict[str, str] = field(default_factory=dict)

class WNTRPlant:
    def __init__(self, inp_path: str, sensors: Dict[str, Any], actuators: Dict[str, Any]):
        if not os.path.exists(inp_path):
            raise FileNotFoundError(f"WNTR inp not found: {inp_path}")

        self.inp_path = inp_path
        self.sensors = sensors
        self.actuators = actuators
        
        # 1. 仍然加载一次高层模型，仅用于获取元数据（如 tank 名字，初始设置等），方便解析
        # 这样不用手写解析器来找哪个节点是 Tank
        self.wn = wntr.network.WaterNetworkModel(inp_path)
        
        # 解析 Tank 和 Pump 的名称列表
        self._tank_names = []
        for entry in self.sensors.get("tank_level", []):
            name = entry.get("tank") or entry.get("id") or entry.get("name") if isinstance(entry, dict) else entry
            if name: self._tank_names.append(str(name))

        self._pump_names = []
        for entry in self.actuators.get("pumps", []):
            name = entry.get("pump") or entry.get("id") or entry.get("name") if isinstance(entry, dict) else entry
            if name: self._pump_names.append(str(name))

        self._default_tank = self._tank_names[0] if self._tank_names else None
        
        # 2. 初始化底层 EPANET Toolkit
        self.en = ENepanet()
        self.idx_map = {"nodes": {}, "links": {}} # 缓存 ID -> Index 的映射
        self.elevations = {} # 缓存节点标高，用于计算水位
        
        # 记录当前仿真时间
        self.sim_time = 0.0
        
        # 加载文件到内存 (注意：需要生成临时 output 文件)
        self._temp_dir = tempfile.TemporaryDirectory()
        base_name = os.path.basename(inp_path)
        self.rpt_file = os.path.join(self._temp_dir.name, base_name.replace(".inp", ".rpt"))
        self.bin_file = os.path.join(self._temp_dir.name, base_name.replace(".inp", ".bin"))

        
    def reset(self) -> WNTRState:
        """重置仿真环境"""
        # 如果之前打开过，先关闭
        try:
            self.en.ENcloseH()
            self.en.ENclose()
        except Exception:
            pass

        # 1. 打开 INP 文件
        self.en.ENopen(self.inp_path, self.rpt_file, self.bin_file)
        
        # 2. 构建 ID 到 Index 的映射 (EPANET Toolkit 操作需要 Index)
        #    同时缓存 Tank 的 Elevation
        for t_name in self._tank_names:
            idx = self.en.ENgetnodeindex(t_name)
            self.idx_map["nodes"][t_name] = idx
            # 获取标高：ENgetnodevalue 返回 (value, error_code) 这里的封装可能直接返回 value
            # WNTR 1.0+ 的 ENepanet 通常直接返回 value
            self.elevations[t_name] = self.en.ENgetnodevalue(idx, EN_ELEVATION)
            
        for p_name in self._pump_names:
            idx = self.en.ENgetlinkindex(p_name)
            self.idx_map["links"][p_name] = idx

        # 3. 初始化水力模拟
        self.en.ENopenH()
        self.en.ENinitH(0) # 0 = 不保存 flag
        self.sim_time = 0.0

        # 4. 获取初始状态
        # 必须先 runH 一次来计算 t=0 的状态
        self.en.ENrunH()
        return self._observe()

    def step(self, dt: float, cmd: Dict[str, Any]) -> WNTRState:
        """
        增量推进 dt 秒
        """
        # 1. 应用控制命令 (设置泵状态)
        pumps_cmd = (cmd.get("pumps") or {}) if isinstance(cmd, dict) else {}
        for p_name, state in pumps_cmd.items():
            if p_name not in self._pump_names: continue
            
            idx = self.idx_map["links"][p_name]
            s_str = str(state).upper()
            
            val = None
            if s_str in ("OPEN", "1", "ON", "TRUE"):
                val = EN_OPEN
            elif s_str in ("CLOSED", "0", "OFF", "FALSE"):
                val = EN_CLOSED
            
            if val is not None:
                # 设置链路状态
                self.en.ENsetlinkvalue(idx, EN_STATUS, val)

        # 2. 时间推进循环
        # EPANET 的 ENnextH 可能会以小于 dt 的步长前进（如果发生水力事件，如Tank满了）
        # 所以我们需要循环推进，直到累积时间达到 dt
        
        target_time = self.sim_time + dt
        
        while self.sim_time < target_time:
            # 计算当前时刻的水力方程
            self.en.ENrunH()
            
            # 告诉 EPANET 我们想前进多久 (step_size)
            # 注意：这步通常是隐式的，ENnextH 会自动计算下一个水力事件时间
            # 但为了强制步长，我们可以在这里不做特殊处理，直接接受 EPANET 的步长
            # 或者反复调用 ENrunH 直到满足时间。
            
            # 简单做法：调用 ENnextH，它会返回实际推进了多少秒 (t_step)
            t_step = self.en.ENnextH()
            
            self.sim_time += t_step
            
            # 如果 t_step 为 0，说明仿真结束或无法继续
            if t_step <= 0:
                break
                
            # 如果推进过头了（很少见，除非 step 很小），不用特殊处理，
            # 下一次 step 会基于当前的物理状态继续

        # 3. 观测当前状态
        # 此时 EPANET 内部指针已经停在 self.sim_time
        # 注意：ENnextH 已经把时间推到了下一步，所以需要再 Run 一次确保状态对应当前时间吗？
        # 在 Toolkit 流程中：runH 计算当前，nextH 推进到下一刻。
        # 如果我们刚做完 nextH，当前所有的 getnodevalue 实际上是上一刻的值还是这一刻的？
        # 标准流程：ENrunH -> Get Results -> ENnextH.
        # 所以我们需要在 return 前再做一次 ENrunH 吗？通常不需要，除非要获取最新计算值。
        # 为了保险起见，做一次计算更新节点状态：
        self.en.ENrunH()
        
        return self._observe()

    def _observe(self) -> WNTRState:
        tank_level = {}
        pump_status = {}
        
        # 获取 Tank 水位
        for name in self._tank_names:
            idx = self.idx_map["nodes"][name]
            # 获取总水头 (Total Head)
            head = self.en.ENgetnodevalue(idx, EN_HEAD)
            elev = self.elevations[name]
            # Level = Head - Elevation
            tank_level[name] = head - elev
            
        # 获取 Pump 状态
        for name in self._pump_names:
            idx = self.idx_map["links"][name]
            # EN_STATUS 返回 0 (Closed) 或 1 (Open)
            val = self.en.ENgetlinkvalue(idx, EN_STATUS)
            status_str = "CLOSED"
            if int(val) == 1:
                status_str = "OPEN"
            pump_status[name] = status_str
            
        return WNTRState(tank_level=tank_level, pump_status=pump_status)

    def close(self):
        """清理资源"""
        try:
            self.en.ENcloseH()
            self.en.ENclose()
        except Exception:
            pass
        # 清理临时目录
        if hasattr(self, '_temp_dir'):
            self._temp_dir.cleanup()

    def __del__(self):
        self.close()

    # 保持辅助函数兼容性
    def make_record(self, t: int, state: WNTRState, tank_id: Optional[str] = None) -> Dict[str, Any]:
        tank_key = tank_id or self._default_tank
        row: Dict[str, Any] = {"t": int(t)}
        row["tank_level"] = state.tank_level.get(tank_key, None) if tank_key else None
        for pump in self._pump_names:
            row[f"{pump}_status"] = state.pump_status.get(pump, "UNKNOWN")
        return row

    def write_records_csv(self, records: List[Dict[str, Any]], out_path: str) -> None:
        import csv
        os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
        pump_cols = [f"{p}_status" for p in self._pump_names]
        with open(out_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["t", "tank_level", *pump_cols])
            for row in records:
                writer.writerow([row.get("t", ""), row.get("tank_level", ""), *[row.get(col, "") for col in pump_cols]])