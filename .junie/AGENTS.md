# Agent Guidelines for sadist-be

## Environment and Commands
1. **Always use the virtual environment** at `./venv-test/`.
2. **Commands**:
   - Python: `./venv-test/bin/python`
   - Pytest: `./venv-test/bin/pytest`
   - NEVER use the bare `python` or `pytest` commands.
3. **Environment Variables**:
   - Always set `PYTHONPATH=app` when running scripts or tests from the root.
   - For tests, usually use `USE_MONGOMOCK=1`.
   
## Development Workflow
- Follow the existing code style in `app/`.
- Use `mongomoron` for DB operations as seen in `app/user.py`.

## Reference Commands
- Run all tests: `PYTHONPATH=app USE_MONGOMOCK=1 ./venv-test/bin/pytest`
- Run specific test: `PYTHONPATH=app USE_MONGOMOCK=1 ./venv-test/bin/pytest test/test_file.py`
