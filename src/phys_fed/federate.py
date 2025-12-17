from __future__ import annotations
import json
from typing import Any, Dict
import csv
import os

import helics as h
import yaml

from common.schema import SensorSnapshot, ActuatorCommand
from phys_fed.wntr_plant import WNTRPlant


def run_phys_federate(config_path: str) -> None:
    cfg = yaml.safe_load(open(config_path, "r", encoding="utf-8"))
    sim_cfg = cfg.get("sim", {})
    helics_cfg = cfg.get("helics", {})
    dt = float(sim_cfg.get("dt_phys_s"))
    t_end = float(sim_cfg.get("t_end_s"))
    broker_port = helics_cfg.get("broker", {}).get("port", 23404)

    plant = WNTRPlant(
        inp_path=str(sim_cfg.get("inp_path")),
        sensors=cfg.get("sensors", {}),
        actuators=cfg.get("actuators", {}),
    )

    sensors_cfg = cfg.get("sensors", {}).get("tank_level", [])
    sensor_entry = sensors_cfg[0] if sensors_cfg else {}
    tank_id = sensor_entry.get("tank") if isinstance(sensor_entry, dict) else str(sensor_entry) if sensor_entry else "TANK"
    topic_sensors = sensor_entry.get("topic") if isinstance(sensor_entry, dict) else None
    if not topic_sensors:
        topic_sensors = f"phys/sensors/{tank_id}"

    actuators_cfg = cfg.get("actuators", {}).get("pumps", [])

    fedinfo = h.helicsCreateFederateInfo()
    h.helicsFederateInfoSetCoreInitString(fedinfo, f"--federates=1 --broker_address=tcp://127.0.0.1:{broker_port}")
    h.helicsFederateInfoSetCoreTypeFromString(fedinfo, "zmq")
    h.helicsFederateInfoSetTimeProperty(fedinfo, h.helics_property_time_delta, dt)

    fed = h.helicsCreateValueFederate("phys_fed", fedinfo)

    pub = h.helicsFederateRegisterGlobalPublication(fed, topic_sensors, h.HELICS_DATA_TYPE_STRING, "")


    # 订阅每个泵的命令 topic，并合并命令
    subs: Dict[str, h.HelicsInput] = {}
    for pump in actuators_cfg:
        topic = pump.get("topic") if isinstance(pump, dict) else None
        pump_name = pump.get("pump") if isinstance(pump, dict) else str(pump)
        if not topic or not pump_name:
            continue
        subs[pump_name] = h.helicsFederateRegisterSubscription(fed, topic, "")

    h.helicsFederateEnterExecutingMode(fed)
    os.makedirs("output", exist_ok=True)

    records = []

    # 初始状态发布
    state0 = plant.reset()
    t = 0.0
    pending_cmd = ActuatorCommand(pumps={})
    step_count = 0
    
    # 【新增】记录上一次的物理时间
    t_prev = 0.0
   

    while t < t_end:
        # 1) 请求到下一时刻
        t_next = t + dt # 这里的 dt 是你的最大步长（心跳）
        granted = h.helicsFederateRequestTime(fed, t_next)
        t = float(granted)

        # 2) 读取是否有新命令（若无，则沿用上次）
        for pump_name, sub in subs.items():
            if h.helicsInputIsUpdated(sub):
                raw = h.helicsInputGetString(sub)
                try:
                    incoming = ActuatorCommand.from_dict(json.loads(raw))
                    for k, v in incoming.pumps.items():
                        pending_cmd.pumps[k] = v
                except Exception:
                    pass

        # 3) 推进物理一步
        actual_dt = t - t_prev
        # 只有当时间真的向前推进了才 step (避免重复计算)
        if actual_dt > 1e-6:
            # 使用实际的差值来推演物理模型，保证时间严格对齐
            state = plant.step(dt=actual_dt, cmd=pending_cmd.to_dict())
            
            # 更新 t_prev
            t_prev = t

            # 4) 发布 & 记录 (保持不变)
            # 只有发生了物理推演才记录数据，避免重复记录相同时间点
            snap = SensorSnapshot(tank_level=state.tank_level)
            h.helicsPublicationPublishString(pub, json.dumps(snap.to_dict(), ensure_ascii=False))
            records.append(plant.make_record(t=int(t), state=state, tank_id=tank_id))
            
            if step_count % 10 == 0:
                level_val = state.tank_level.get(tank_id, None)
                print(f"[phys_fed] t={t:.2f}, tank_level[{tank_id}]={level_val}")
            
            step_count += 1
       
    print("[phys_fed] finished")

    out_path = os.path.join("output", "helics_phys_tank.csv")
    plant.write_records_csv(records, out_path)
    print(f"[phys_fed] wrote {out_path}")

    h.helicsFederateDisconnect(fed)
    h.helicsFederateFree(fed)
