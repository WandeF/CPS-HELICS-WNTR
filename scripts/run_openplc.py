import argparse
import sys

sys.path.append("src")

from openplc_fed.federate import run_openplc_federate

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()
    run_openplc_federate(args.config)
