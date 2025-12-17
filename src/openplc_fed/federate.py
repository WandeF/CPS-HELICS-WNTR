from __future__ import annotations
import json
import helics as h
import yaml

from common.schema import SensorSnapshot, ActuatorCommand


def run_openplc_federate(config_path: str) -> None:
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

    actuators_cfg = cfg.get("actuators", {}).get("pumps", [])
    pump_entry = actuators_cfg[0] if actuators_cfg else {}
    pump_name = pump_entry.get("pump") if isinstance(pump_entry, dict) else str(pump_entry) if pump_entry else "PUMP1"
    command_topic = pump_entry.get("topic") if isinstance(pump_entry, dict) else None
    if not command_topic:
        command_topic = f"ctrl/commands/{pump_name}"

    # 一个极简规则：水位低于阈值 -> 开泵
    threshold = 10.0

    fedinfo = h.helicsCreateFederateInfo()
    h.helicsFederateInfoSetCoreInitString(fedinfo, f"--federates=1 --broker_address=tcp://127.0.0.1:{broker_port}")
    h.helicsFederateInfoSetCoreTypeFromString(fedinfo, "zmq")
    h.helicsFederateInfoSetTimeProperty(fedinfo, h.helics_property_time_delta, dt)

    fed = h.helicsCreateValueFederate("openplc_fed", fedinfo)

    pub = h.helicsFederateRegisterPublication(fed, command_topic, h.HELICS_DATA_TYPE_STRING, "")
    sub = h.helicsFederateRegisterSubscription(fed, sensor_topic, "")

    h.helicsFederateEnterExecutingMode(fed)

    t = 0.0
    last_snap = SensorSnapshot(tank_level={})

    while t < t_end:
        if int(t) % int(dt * 10) == 0:
            print(f"[openplc_fed] t={t}")

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
        pumps_cmd = {}
        if level_val is not None and level_val < threshold:
            pumps_cmd[pump_name] = "OPEN"
        else:
            pumps_cmd[pump_name] = "CLOSED"

        cmd = ActuatorCommand(pumps=pumps_cmd)
        h.helicsPublicationPublishString(pub, json.dumps(cmd.to_dict(), ensure_ascii=False))

    print("[openplc_fed] finished")

    h.helicsFederateDisconnect(fed)
    h.helicsFederateFree(fed)
    #h.helicsCloseLibrary()
