VENV ?= .venv

.PHONY: install dbt-debug dbt-build dashboard export brief validate

install:
	python3 -m venv $(VENV)
	. $(VENV)/bin/activate && pip install -r requirements.txt

dbt-debug:
	. $(VENV)/bin/activate && dbt debug

dbt-build:
	. $(VENV)/bin/activate && dbt seed && dbt run --vars '{"lookback_days": 30}' && dbt test

dashboard:
	. $(VENV)/bin/activate && streamlit run streamlit_app.py

export:
	. $(VENV)/bin/activate && python3 scripts/run_bigquery_exports.py --project "$$GCP_PROJECT_ID" --dataset "$$BIGQUERY_DATASET" --output-dir artifacts

brief:
	. $(VENV)/bin/activate && python3 scripts/build_llm_brief.py --input-dir artifacts --output-dir artifacts

validate:
	python3 -m compileall scripts streamlit_app.py
	python3 scripts/validate_repo.py
