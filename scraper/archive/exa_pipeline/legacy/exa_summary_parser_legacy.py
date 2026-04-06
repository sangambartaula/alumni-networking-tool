import ast
from pathlib import Path


PROJECT_ROOT = Path(__file__).parent.parent
EXA_PATH = PROJECT_ROOT / "exa.py"


def _load_parser_namespace():
    source = EXA_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    keep = {
        "safe_text",
        "as_list",
        "_parse_explicit_entry",
        "parse_summary_output",
        "split_delimited_line",
        "parse_delimited_entries",
        "extract_education_entries",
        "extract_experience_entries",
        "_is_unt_school_name",
        "_to_exa_date_ordinal",
        "_is_valid_date_range",
        "_has_any_real_education",
        "_output_mentions_unt_experience",
        "_output_has_student_signal",
        "_should_try_education_recovery",
        "_merge_recovered_education",
        "_summary_output_is_valid",
    }
    module = ast.Module(
        body=[node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in keep],
        type_ignores=[],
    )
    ast.fix_missing_locations(module)
    def _fake_parse_groq_date(value):
        text = str(value or "").strip()
        if text.lower() == "present":
            return {"year": 9999, "month": 12}
        if text.isdigit() and len(text) == 4:
            return {"year": int(text), "month": 0}
        return None

    namespace = {"re": __import__("re"), "parse_groq_date": _fake_parse_groq_date}
    exec(compile(module, str(EXA_PATH), "exec"), namespace)
    return namespace


def test_parse_summary_output_uses_explicit_keys_and_defaults_missing_slots():
    namespace = _load_parser_namespace()
    parse_summary_output = namespace["parse_summary_output"]

    summary = (
        "FIRST: Jane || LAST: Doe || LOC: Denton, TX || HEAD: Engineer || "
        "EDU1: University of North Texas|BS|Computer Science|2016|2020 || "
        "EXP2: Developer|Acme|2021|Present"
    )

    parsed = parse_summary_output(summary)

    assert parsed["first"] == "Jane"
    assert parsed["last"] == "Doe"
    assert parsed["location"] == "Denton, TX"
    assert parsed["headline"] == "Engineer"
    assert parsed["education"][0] == "University of North Texas;;BS;;Computer Science;;2016;;2020"
    assert parsed["education"][1] == "N/A;;N/A;;N/A;;N/A;;N/A"
    assert parsed["education"][2] == "N/A;;N/A;;N/A;;N/A;;N/A"
    assert parsed["experience"][0] == "N/A;;N/A;;N/A;;N/A"
    assert parsed["experience"][1] == "Developer;;Acme;;2021;;Present"
    assert parsed["experience"][2] == "N/A;;N/A;;N/A;;N/A"


def test_summary_validator_requires_unt_as_primary_education_and_complete_job_pairs():
    namespace = _load_parser_namespace()
    parse_summary_output = namespace["parse_summary_output"]
    is_valid = namespace["_summary_output_is_valid"]

    valid_summary = (
        "FIRST: Jane || LAST: Doe || LOC: Denton, TX || HEAD: Engineer || "
        "EDU1: University of North Texas|BS|Computer Science|2016|2020 || "
        "EXP1: Developer|Acme|2021|Present"
    )
    valid_output = parse_summary_output(valid_summary)
    ok, reason = is_valid(valid_output)
    assert ok is True
    assert reason == ""

    non_unt_primary = (
        "FIRST: Jane || LAST: Doe || LOC: Denton, TX || HEAD: Engineer || "
        "EDU1: Texas A&M University|BS|Computer Science|2016|2020 || "
        "EDU2: University of North Texas|MS|Computer Science|2021|2023 || "
        "EXP1: Developer|Acme|2021|Present"
    )
    non_unt_output = parse_summary_output(non_unt_primary)
    ok, reason = is_valid(non_unt_output)
    assert ok is True
    assert reason == ""

    no_education = (
        "FIRST: Jane || LAST: Doe || LOC: Denton, TX || HEAD: Engineer || "
        "EDU1: N/A || EDU2: N/A || EDU3: N/A || "
        "EXP1: Developer|Acme|2021|Present"
    )
    no_edu_output = parse_summary_output(no_education)
    ok, reason = is_valid(no_edu_output)
    assert ok is False
    assert "missing usable education" in reason

    missing_title = (
        "FIRST: Jane || LAST: Doe || LOC: Denton, TX || HEAD: Engineer || "
        "EDU1: University of North Texas|BS|Computer Science|2016|2020 || "
        "EXP1: N/A|Acme|2021|Present"
    )
    missing_title_output = parse_summary_output(missing_title)
    ok, reason = is_valid(missing_title_output)
    assert ok is False
    assert "missing title/company pair" in reason

    invalid_dates = (
        "FIRST: Jane || LAST: Doe || LOC: Denton, TX || HEAD: Engineer || "
        "EDU1: University of North Texas|BS|Computer Science|2016|2020 || "
        "EXP1: Developer|Acme|2024|2020"
    )
    invalid_dates_output = parse_summary_output(invalid_dates)
    ok, reason = is_valid(invalid_dates_output)
    assert ok is False
    assert "invalid date range" in reason


def test_recovery_trigger_and_merge_for_missing_education():
    namespace = _load_parser_namespace()
    parse_summary_output = namespace["parse_summary_output"]
    should_recover = namespace["_should_try_education_recovery"]
    merge_education = namespace["_merge_recovered_education"]
    extract_education = namespace["extract_education_entries"]

    missing_edu_but_unt_exp = (
        "FIRST: Saba || LAST: Jazi || LOC: United States || HEAD: Exploring Machine Learning || "
        "EDU1: N/A || EDU2: N/A || EDU3: N/A || "
        "EXP1: Graduate Research And Teaching Assistant|University of North Texas|Jan 2020|Present"
    )
    output_missing = parse_summary_output(missing_edu_but_unt_exp)
    assert should_recover(output_missing) is True

    recovered = parse_summary_output(
        "FIRST: Saba || LAST: Jazi || LOC: United States || HEAD: Exploring Machine Learning || "
        "EDU1: University of North Texas|PhD|Computer Science|2020|N/A || EDU2: N/A || EDU3: N/A || "
        "EXP1: Graduate Research And Teaching Assistant|University of North Texas|Jan 2020|Present"
    )
    merged = merge_education(output_missing, recovered)
    merged_edu = extract_education(merged)
    assert merged_edu[0]["school"] == "University of North Texas"
