import argparse
import sys

sys.path.append("src")

from ctrl_fed.federate import run_ctrl_federate

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()
    run_ctrl_federate(args.config)
