# Databricks Resume Screening Pipeline — Project Context

## What this project does
Ingests resumes and company job descriptions from Google Drive, processes them through a medallion architecture (Staging → Bronze → Silver) on Databricks using Lakeflow (DLT) pipelines. The goal is to use AI to extract structured data from unstructured documents for resume-JD matching.

---

## Architecture

```
Google Drive
  ├── Resume_Data_PDF/       (16 PDF resumes)
  └── Company_JD_s/          (5 DOCX job descriptions)
          ↓
    ingestion notebook
          ↓
UC Volume: resume_batch3.staging.source_files/
          ↓
    Lakeflow Pipeline (Batch3-Staging-Bronze)
          ↓
Bronze Layer (resume_batch3.bronze)
  ├── resume_data            (16 rows, SCD1)
  └── company_jd             (5 rows, SCD1)
          ↓
Silver Layer (resume_batch3.silver)  ← NEXT STEP
  ├── resume_data            (AI-extracted: Name, Title, Experience, Skills)
  └── company_jd_data        (AI-extracted: Title, Experience, Skills)
```

---

## Catalog / Workspace
- **Databricks workspace:** `https://dbc-d8ec3837-9bcb.cloud.databricks.com`
- **Unity Catalog:** `resume_batch3`
- **Schemas:** `staging`, `bronze`, `silver`, `gold`
- **UC Volume:** `/Volumes/resume_batch3/staging/source_files/`
- **Pipeline name:** `Batch3-Staging-Bronze`
- **Pipeline config parameter:** `catalog_name = resume_batch3`

---

## Repository Structure

```
databrickstraining/
├── PROJECT_CONTEXT.md                        ← this file
├── One-Time-Setup.ipynb.ipynb                ← creates catalog, schemas, volume
├── ingestion to staging DB.ipynb             ← Google Drive → UC Volume (incremental)
├── Delta Table-1.ipynb                       ← practice notebook
├── transformations/
│   ├── staging_to_bronze.py                  ← Bronze Lakeflow pipeline (DONE ✅)
│   ├── silver_resume.py                      ← Silver Lakeflow pipeline (CREATED, not run yet)
│   └── silver_company_jd.py                  ← Silver Lakeflow pipeline (CREATED, not run yet)
├── utilities/
│   └── utils.py                              ← Spark UDFs: parse_file_content, extract_email, extract_phone, extract_skills
└── Practice/
    └── DatabricksSetup.ipynb                 ← local Databricks Connect setup guide
```

---

## Key Files Explained

### `staging_to_bronze.py`
- Lakeflow pipeline (`from pyspark import pipelines as dp`)
- Reads binary files from UC Volume using Autoloader (`cloudFiles`)
- Parses PDF/DOCX using `utils.parse_file_content` UDF (PyPDF2 / python-docx)
- Extracts email, phone, skills using regex UDFs
- Writes to `bronze.resume_data` and `bronze.company_jd` via SCD1 `create_auto_cdc_flow`

### `silver_resume.py`
- Reads from `bronze.resume_data`
- Uses `ai_query('databricks-meta-llama-3-3-70b-instruct', ...)` to extract JSON
- Schema: `Name, Title, Experience (year range), Skills (comma-separated)`
- Writes to `silver.resume_data` via SCD1 `apply_changes`

### `silver_company_jd.py`
- Reads from `bronze.company_jd`
- Uses `ai_query('databricks-meta-llama-3-3-70b-instruct', ...)` to extract JSON
- Schema: `Title, Experience (year range), Skills (comma-separated)`
- Writes to `silver.company_jd_data` via SCD1 `apply_changes`

### `utilities/utils.py`
Spark UDFs imported as `from utilities import utils as u`:
- `u.parse_file_content(content, path)` — parses PDF/DOCX binary → text
- `u.extract_email(text)` — regex email extraction
- `u.extract_phone(text)` — regex phone extraction (handles PDF spacing artifacts)
- `u.extract_skills(text)` — matches against SKILLS_LIST

---

## Lakeflow Pipeline Notes
- API: `from pyspark import pipelines as dp` (newer Lakeflow), not old `import dlt`
- `spark` is implicit — injected by Databricks runtime, do not define it
- `catalog_name` must be set as a pipeline config parameter in Settings
- Libraries required: `PyPDF2`, `python-docx` (add in pipeline Settings → Libraries)
- `@dp.view` — intermediate view, not persisted (shows N/A in Tables tab)
- `dp.create_streaming_table` — creates the Delta table
- `dp.create_auto_cdc_flow` / `dp.apply_changes` — SCD1 upsert into the table

---

## Known Issues / Status

| Issue | Status |
|---|---|
| `catalog_name` SQL config not found | Fixed — added to pipeline config in UI |
| `PARSE_ERROR: No module named 'PyPDF2'` | Fixed — added PyPDF2 + python-docx as pipeline libraries |
| Phone/email null in bronze table | Partially fixed — phone regex updated in utils.py; need to sync utils.py to Databricks workspace and Full Refresh |
| Silver pipeline not run yet | silver_resume.py and silver_company_jd.py created, need to add to pipeline and run |

---

## Pending / Next Steps
1. **Sync updated `utils.py`** to Databricks workspace (fix phone regex) and Full Refresh bronze pipeline
2. **Add silver files** (`silver_resume.py`, `silver_company_jd.py`) to the Lakeflow pipeline
3. **Run silver pipeline** — verify AI extraction output (Name, Title, Experience, Skills)
4. **Gold layer** — join `silver.resume_data` with `silver.company_jd_data` to score resume-JD matches
