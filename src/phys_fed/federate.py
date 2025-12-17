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

    snap0 = SensorSnapshot(tank_level=state0.tank_level)

    h.helicsPublicationPublishString(pub, json.dumps(snap0.to_dict(), ensure_ascii=False))

    records.append(plant.make_record(t=0, state=state0, tank_id=tank_id))



    t = 0.0

    pending_cmd = ActuatorCommand(pumps={})

    step_count = 0



    while t < t_end:
        # 1) 请求到下一时刻
        t_next = t + dt
        granted = h.helicsFederateRequestTime(fed, t_next)
        t = float(granted)

        # 2) 读取是否有新命令（若无，则沿用）
        for pump_name, sub in subs.items():
            if not h.helicsInputIsUpdated(sub):
                continue
            raw = h.helicsInputGetString(sub)
            try:
                incoming = ActuatorCommand.from_dict(json.loads(raw))
                for k, v in incoming.pumps.items():
                    pending_cmd.pumps[k] = v
            except Exception:
                # 保持上一条有效命令
                pass

        # 3) 推进物理一步
        state = plant.step(dt=dt, cmd=pending_cmd.to_dict())

        # 4) 发布传感器快照
        snap = SensorSnapshot(tank_level=state.tank_level)
        h.helicsPublicationPublishString(pub, json.dumps(snap.to_dict(), ensure_ascii=False))
        records.append(plant.make_record(t=int(t), state=state, tank_id=tank_id))

        if step_count % 10 == 0:
            level_val = state.tank_level.get(tank_id, None)
            print(f"[phys_fed] t={t}, tank_level[{tank_id}]={level_val}")
        step_count += 1

    # 结束前让 HELICS 有机会把数据发完
    h.helicsFederateRequestTime(fed, h.HELICS_TIME_MAXTIME)
    h.helicsFederateDisconnect(fed)
    print("[phys_fed] finished")

    out_path = os.path.join("output", "helics_phys_tank.csv")
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["t", "tank_level"])
        for row in records:
            writer.writerow([row["t"], row["tank_level"]])
    print(f"[phys_fed] wrote {out_path}")

    h.helicsFederateFree(fed)
    h.helicsCloseLibrary()
