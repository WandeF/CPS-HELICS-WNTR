import subprocess
import sys
import time


def main():
    # 1) 起 broker
    broker = subprocess.Popen(["helics_broker", "-f", "2", "--coretype=zmq", "--port=23404"])
    time.sleep(1.0)


    # 2) 起两个 federate
    phys = subprocess.Popen([sys.executable, "scripts/run_phys.py", "--config", "config/minitown.yaml"])
    ctrl = subprocess.Popen([sys.executable, "scripts/run_ctrl.py", "--config", "config/minitown.yaml"])

    # 3) 等待结束
    phys.wait()
    ctrl.wait()
    broker.terminate()


if __name__ == "__main__":
    main()
