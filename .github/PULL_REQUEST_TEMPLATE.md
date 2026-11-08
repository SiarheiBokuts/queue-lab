## What and why

## Checked

- [ ] `docker build --target backend-test .` and `--target web-test .` pass
- [ ] the tests of the worker languages I touched pass
- [ ] a new setting or metric is documented (README, Code page, openspec)
- [ ] no tuning values in worker code (they live in `backend/app/config.py`)