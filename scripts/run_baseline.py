import sys
import subprocess


def main():
    # Delegate to the baseline runner so scripts/ has a convenience entry point
    sys.exit(subprocess.call([sys.executable, "baseline/run_baseline.py", "--config", "config/minitown.yaml"]))


if __name__ == "__main__":
    main()
