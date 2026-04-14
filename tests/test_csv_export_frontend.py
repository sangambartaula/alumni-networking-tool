import re
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent

EXPECTED_EXPORT_FIELDS = [
    "first",
    "last",
    "linkedin_url",
    "school",
    "degree",
    "major",
    "school_start",
    "grad_year",
    "school2",
    "degree2",
    "major2",
    "school3",
    "degree3",
    "major3",
    "discipline",
    "location",
    "working_while_studying",
    "title",
    "company",
    "job_employment_type",
    "job_start",
    "job_end",
    "exp_2_title",
    "exp_2_company",
    "exp_2_dates",
    "exp_2_employment_type",
    "exp_3_title",
    "exp_3_company",
    "exp_3_dates",
    "exp_3_employment_type",
    "seniority_level",
]


def test_csv_export_field_order_matches_required_schema():
    app_js = (PROJECT_ROOT / "frontend" / "public" / "app.js").read_text(encoding="utf-8")
    match = re.search(
        r"const ALUMNI_EXPORT_FIELDS = Object\.freeze\(\[(.*?)\]\);",
        app_js,
        re.DOTALL,
    )

    assert match, "ALUMNI_EXPORT_FIELDS constant not found"
    fields = re.findall(r"'([^']+)'", match.group(1))
    assert fields == EXPECTED_EXPORT_FIELDS


def test_csv_export_modal_contract_is_present():
    alumni_html = (PROJECT_ROOT / "frontend" / "public" / "alumni.html").read_text(encoding="utf-8")
    app_js = (PROJECT_ROOT / "frontend" / "public" / "app.js").read_text(encoding="utf-8")

    for element_id in [
        "exportCsvBtn",
        "csvExportModal",
        "csvFieldList",
        "csvSelectAll",
        "csvClearAll",
        "csvExportValidation",
        "downloadCsvExport",
    ]:
        assert f'id="{element_id}"' in alumni_html
        assert f"'{element_id}'" in app_js

    assert "Select at least one field before exporting." in app_js
    assert "Alumni are still loading." in app_js
