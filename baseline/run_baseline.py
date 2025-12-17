import argparse
import csv
import os
import sys

import yaml

from wntr_baseline import run_wntr_baseline


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config, "r", encoding="utf-8"))
    sim_cfg = cfg.get("sim", {})
    inp_path = sim_cfg.get("inp_path")
    dt_s = sim_cfg.get("dt_phys_s")
    t_end_s = sim_cfg.get("t_end_s")

    if not inp_path or dt_s is None or t_end_s is None:
        raise SystemExit("config missing sim.inp_path / sim.dt_phys_s / sim.t_end_s")

    series, pump_names = run_wntr_baseline(str(inp_path), int(dt_s), int(t_end_s))
    pump_cols = [f"{name}_status" for name in pump_names]

    os.makedirs("output", exist_ok=True)
    out_path = os.path.join("output", "baseline_tank.csv")
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["t", "tank_level", *pump_cols])
        for row in series:
            writer.writerow([row.get("t", ""), row.get("tank_level", ""), *[row.get(col, "") for col in pump_cols]])

    print(f"baseline written to {out_path}")


if __name__ == "__main__":
    sys.exit(main())
