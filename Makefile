# -------------------------
# Config
# -------------------------
VENV=.venv
PYTHON=$(VENV)/bin/python
PIP=$(VENV)/bin/pip

API_HOST=127.0.0.1
API_PORT=8000

# -------------------------
# Help
# -------------------------
.PHONY: help
help:
	@echo ""
	@echo "Available targets:"
	@echo "  make venv        Create virtual environment"
	@echo "  make install     Install dependencies"
	@echo "  make api         Run FastAPI backend"
	@echo "  make ui          Run Streamlit UI"
	@echo "  make worker      Run background worker"
	@echo "  make test        Run pytest"
	@echo "  make clean       Remove caches"
	@echo ""

# -------------------------
# Environment
# -------------------------
.PHONY: venv
venv:
	python -m venv $(VENV)

.PHONY: install
install:
	$(PIP) install -r requirements.txt

# -------------------------
# Run services
# -------------------------
.PHONY: api
api:
	$(PYTHON) -m uvicorn app.main:app --reload --host $(API_HOST) --port $(API_PORT)

.PHONY: ui
ui:
	$(VENV)/bin/streamlit run ui/streamlit_app.py

.PHONY: worker
worker:
	$(PYTHON) -m worker.worker

# -------------------------
# Testing
# -------------------------
.PHONY: test
test:
	$(PYTHON) -m pytest -q

# -------------------------
# Cleanup
# -------------------------
.PHONY: clean
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
