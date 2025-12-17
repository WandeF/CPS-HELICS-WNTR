import argparse
import sys

# 让 python 能找到 src/
sys.path.append("src")

from phys_fed.federate import run_phys_federate

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()
    run_phys_federate(args.config)
