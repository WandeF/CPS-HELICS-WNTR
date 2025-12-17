# CPS-HELICS-WNTR

Minimal co-simulation skeleton:
- WNTR physical federate (phys_fed)
- Dummy controller federate (ctrl_fed)
- HELICS coordinates time + data exchange

## Install
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Prepare

Put your EPANET `.inp` file at:

* `data/minitown.inp` (or change `config/minitown.yaml` -> `sim.inp_path`)

## Run (local, simplest)

```bash
python scripts/run_all_local.py
```

This starts `phys_fed` and `ctrl_fed` with `config/minitown.yaml`.

## Smoke test

```bash
python scripts/smoke_test.py
```
