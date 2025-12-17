import subprocess
import sys


def main():
    result = subprocess.run(
        [sys.executable, "scripts/run_all_local.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    print(result.stdout)
    if result.returncode != 0:
        raise SystemExit(f"smoke test failed with code {result.returncode}")
    if "finished" not in result.stdout:
        raise SystemExit("smoke test failed: missing 'finished' marker in output")


if __name__ == "__main__":
    main()
