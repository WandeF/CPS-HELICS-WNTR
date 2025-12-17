from __future__ import annotations
import json
from typing import Dict, Any, List

import helics as h
import yaml

from common.schema import SensorSnapshot, ActuatorCommand


def run_ctrl_federate(config_path: str) -> None:
    cfg = yaml.safe_load(open(config_path, "r", encoding="utf-8"))
    sim_cfg = cfg.get("sim", {})
    helics_cfg = cfg.get("helics", {})
    dt = float(sim_cfg.get("dt_phys_s"))
    t_end = float(sim_cfg.get("t_end_s"))
    broker_port = helics_cfg.get("broker", {}).get("port", 23404)

    sensors_cfg = cfg.get("sensors", {}).get("tank_level", [])
    sensor_entry = sensors_cfg[0] if sensors_cfg else {}
    tank_id = sensor_entry.get("tank") if isinstance(sensor_entry, dict) else str(sensor_entry) if sensor_entry else "TANK"
    sensor_topic = sensor_entry.get("topic") if isinstance(sensor_entry, dict) else None
    if not sensor_topic:
        sensor_topic = f"phys/sensors/{tank_id}"

    plc_cfgs: List[Dict[str, Any]] = []
    for plc in cfg.get("plcs", []):
        if plc.get("type") != "actuator_plc":
            continue
        publishes = plc.get("publishes", [])
        pub_entry = publishes[0] if publishes else {}
        topic = pub_entry.get("topic")
        schema = (pub_entry.get("schema") or {}).get("pump_cmd", {})
        pump_name = schema.get("pump") or plc.get("id")
        logic = plc.get("logic", {})
        if logic.get("kind") != "hysteresis_threshold":
            continue
        below = float(logic.get("below"))
        above = float(logic.get("above"))
        initial = str(logic.get("initial", "CLOSED"))
        open_val = str((logic.get("output") or {}).get("open_value", "OPEN"))
        closed_val = str((logic.get("output") or {}).get("closed_value", "CLOSED"))
        plc_cfgs.append(
            {
                "topic": topic,
                "pump": pump_name,
                "below": below,
                "above": above,
                "initial": initial,
                "open_val": open_val,
                "closed_val": closed_val,
            }
        )

    fedinfo = h.helicsCreateFederateInfo()
    h.helicsFederateInfoSetCoreInitString(fedinfo, f"--federates=1 --broker_address=tcp://127.0.0.1:{broker_port}")
    h.helicsFederateInfoSetCoreTypeFromString(fedinfo, "zmq")
    h.helicsFederateInfoSetTimeProperty(fedinfo, h.helics_property_time_delta, dt)

    fed = h.helicsCreateValueFederate("ctrl_fed", fedinfo)

    pubs: Dict[str, h.HelicsPublication] = {}
    for plc in plc_cfgs:
        if not plc["topic"]:
            continue
        pubs[plc["pump"]] = h.helicsFederateRegisterGlobalPublication(fed, plc["topic"], h.HELICS_DATA_TYPE_STRING, "")
    sub = h.helicsFederateRegisterSubscription(fed, sensor_topic, "")

    h.helicsFederateEnterExecutingMode(fed)

    t = 0.0
    last_snap = SensorSnapshot(tank_level={})
    last_state: Dict[str, str] = {plc["pump"]: plc["initial"] for plc in plc_cfgs}
    step_count = 0

    while t < t_end:
        t_next = t + dt
        granted = h.helicsFederateRequestTime(fed, t_next)
        t = float(granted)

        if h.helicsInputIsUpdated(sub):
            raw = h.helicsInputGetString(sub)
            try:
                last_snap = SensorSnapshot.from_dict(json.loads(raw))
            except Exception:
                pass

        level_val = last_snap.tank_level.get(tank_id)

        for plc in plc_cfgs:
            pump_name = plc["pump"]
            if level_val is None:
                state = last_state[pump_name]
            elif level_val < plc["below"]:
                state = plc["open_val"]
            elif level_val > plc["above"]:
                state = plc["closed_val"]
            else:
                state = last_state[pump_name]

            last_state[pump_name] = state
            cmd = ActuatorCommand(pumps={pump_name: state})
            pub = pubs.get(pump_name)
            if pub:
                h.helicsPublicationPublishString(pub, json.dumps(cmd.to_dict(), ensure_ascii=False))

        if step_count % 10 == 0:
            pump_states = ", ".join(f"{p}={s}" for p, s in last_state.items())
            print(f"[ctrl_fed] t={t}, tank_level[{tank_id}]={level_val}, {pump_states}")

        step_count += 1

    print("[ctrl_fed] finished")

    h.helicsFederateDisconnect(fed)
    h.helicsFederateFree(fed)
