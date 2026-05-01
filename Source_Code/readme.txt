# Workflow Automation and Reporting System

This project provides a web-based platform to automate audit checks between a "GOLD" dataset and either "Salto" or "Sesam" datasets. It generates structured reports (Excel) containing pass/fail audit results, plus a summary sheet for easy review.

## Key Features

- Upload GOLD + Salto/Sesam datasets via a browser UI
- Run automated checks (matching your existing Python audit logic)
- Download a result file with audit outcomes and metadata
- Summary sheet includes total record count and pass/fail metrics
- Designed to support linking into Power BI via the generated Excel output

---

## Getting Started

### 1) Create & activate a virtual environment

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 2) Install dependencies

```powershell
pip install -r workflow_automation_app\requirements.txt
```

### 3) Run the app

```powershell
python workflow_automation_app\app.py
```

Then open http://127.0.0.1:5000 in a browser.

---

## How to use

1. Open the app in a browser (no login required).
2. Choose the workflow type (Salto or Sesam).
3. Upload the Excel/CSV file for GOLD.
4. Upload the corresponding Salto or Sesam file.
5. Click **Run Audit**.
6. Download the generated Excel report and use it for documentation or as a data source for Power BI.

The app also includes:
- A **Dashboard** view with summary tiles and a workflow chart.
- A **History** view showing past audit runs with download links.
- An **API endpoint** to export report data as JSON.
- A nightly scheduled run that re-executes the most recent successful audit (if input files are still present).

---

## Exporting Source Code to PDF

If you need to submit the source code as a PDF, you can:

1. Open the files in a code editor (e.g., VS Code).
2. Use the editor's **Print** or **Export to PDF** feature.
3. Alternatively, use `pandoc` to convert markdown files:

```powershell
pip install pandoc
pandoc README.md -o source_code.pdf
```

---

## Next enhancements (optional)

- Add a dashboard view inside the web UI showing metrics and charts.
- Add role-based access / authentication.
- Store history of uploads and audit runs in a database (SQLite).
- Generate a Power BI-friendly JSON export alongside the Excel report.

---

## Copyright & Acknowledgements

This tool is a custom implementation developed by the project owner (Khushi) based on the existing audit workflow.

