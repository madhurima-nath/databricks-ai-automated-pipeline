"""
SAS to PySpark / Databricks SQL Converter
==========================================
Converts legacy SAS code to modern equivalents used in Databricks pipelines.

Two-stage approach:
  1. Rule-based: handles well-known SAS patterns deterministically (no API call needed).
  2. LLM-enhanced: passes remaining or complex code to Claude for translation.

Target outputs: "pyspark" (DataFrame API), "databricks_sql", "yaml" (dbt-style).

Reusable across projects — no dependency on the financial pipeline.
"""

import re
import os
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Tuple


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ConversionResult:
    original: str
    target: str                       # "pyspark" | "databricks_sql" | "yaml"
    output: str
    method: str                       # "rule_based" | "llm" | "hybrid"
    notes: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Compiled patterns (ordered most-specific first)
# ---------------------------------------------------------------------------

_PROC_SORT = re.compile(
    r"PROC\s+SORT\s+DATA\s*=\s*(\w+)\s*(?:OUT\s*=\s*(\w+))?\s*;?\s*BY\s+([\w\s]+?)\s*;\s*RUN\s*;",
    re.IGNORECASE | re.DOTALL,
)
_PROC_MEANS = re.compile(
    r"PROC\s+MEANS\s+DATA\s*=\s*(\w+).*?;(.*?)RUN\s*;",
    re.IGNORECASE | re.DOTALL,
)
_PROC_SQL_BLOCK = re.compile(
    r"PROC\s+SQL\s*;?\s*((?:CREATE|SELECT|INSERT|DELETE|UPDATE).*?)\s*QUIT\s*;",
    re.IGNORECASE | re.DOTALL,
)
_DATA_STEP = re.compile(
    r"DATA\s+(\w+)\s*;\s*SET\s+(\w+)\s*;(.*?)RUN\s*;",
    re.IGNORECASE | re.DOTALL,
)
_VAR_STMT     = re.compile(r"\bVAR\s+([\w\s]+?)\s*;",    re.IGNORECASE)
_CLASS_STMT   = re.compile(r"\bCLASS\s+([\w\s]+?)\s*;",  re.IGNORECASE)
_KEEP_STMT    = re.compile(r"\bKEEP\s+([\w\s]+?)\s*;",   re.IGNORECASE)
_DROP_STMT    = re.compile(r"\bDROP\s+([\w\s]+?)\s*;",   re.IGNORECASE)
_RENAME_STMT  = re.compile(r"\bRENAME\s+((?:\w+\s*=\s*\w+\s*)+);", re.IGNORECASE)
_WHERE_STMT   = re.compile(r"\bWHERE\s+(.+?)\s*;",       re.IGNORECASE)
_IF_THEN_ELSE = re.compile(
    r"IF\s+(.+?)\s+THEN\s+(\w+)\s*=\s*(.+?)(?:\s*;\s*ELSE\s+(\w+)\s*=\s*(.+?))?\s*;",
    re.IGNORECASE,
)
_SAS_DATE_TODAY = re.compile(r"\bTODAY\(\)",                           re.IGNORECASE)
_SAS_DATE_MDY   = re.compile(r"\bMDY\((\d+),\s*(\d+),\s*(\d+)\)",     re.IGNORECASE)
_SAS_RETAIN     = re.compile(r"\bRETAIN\b",                             re.IGNORECASE)
_SAS_INTCK      = re.compile(r"\bINTCK\('(\w+)',\s*([\w()]+),\s*([\w()]+)\)", re.IGNORECASE)
_SAS_MACRO_VAR  = re.compile(r"&(\w+)",                                 re.IGNORECASE)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _mdy_to_spark(m: re.Match) -> str:
    """MDY(month, day, year) → F.to_date(F.lit('YYYY-MM-DD'))"""
    month = m.group(1).zfill(2)
    day   = m.group(2).zfill(2)
    year  = m.group(3)
    return f"F.to_date(F.lit('{year}-{month}-{day}'))"


def _intck_warning(m: re.Match) -> str:
    """INTCK cannot be naively mapped to datediff — insert a warning comment."""
    interval = m.group(1)
    from_dt  = m.group(2)
    to_dt    = m.group(3)
    return (
        f"# WARNING: INTCK('{interval}', {from_dt}, {to_dt}) — "
        f"SAS counts interval boundaries, not elapsed time. "
        f"Use F.datediff({to_dt}, {from_dt}) for days, or "
        f"F.months_between({to_dt}, {from_dt}) for months. Review carefully."
    )


def _clean_where(condition: str) -> str:
    """Convert SAS WHERE condition to Python-compatible filter string."""
    condition = condition.replace("^=", "!=")
    # SAS = inside WHERE is equality (not assignment)
    # Only replace = that are not already !=, >=, <=
    condition = re.sub(r"(?<![!<>])=(?!=)", "==", condition)
    return condition


# ---------------------------------------------------------------------------
# Rule-based converters
# ---------------------------------------------------------------------------

def _to_pyspark(sas_code: str) -> Tuple[str, List[str], List[str]]:
    """Rule-based conversion to PySpark. Returns (code, notes, warnings)."""
    notes: List[str] = []
    warnings: List[str] = []
    code = sas_code.strip()

    # Pre-check: macro variables may appear anywhere (SET clause, WHERE, etc.)
    # Detect before pattern matching so the warning survives even if no rule matches.
    if _SAS_MACRO_VAR.search(sas_code):
        warnings.append(
            "SAS macro variables (&var) found. Convert to Python variables "
            "or function parameters before running."
        )

    # --- PROC SORT ---
    m = _PROC_SORT.search(code)
    if m:
        src     = m.group(1)
        out     = m.group(2) or src
        by_cols = [c.strip() for c in m.group(3).split() if c.strip()]
        col_str = ", ".join(f'"{c}"' for c in by_cols)
        notes.append("PROC SORT → .orderBy()")
        return (
            f"# PROC SORT DATA={src} BY {' '.join(by_cols)}\n"
            f"{out}_df = {src}_df.orderBy({col_str})"
        ), notes, warnings

    # --- PROC MEANS ---
    m = _PROC_MEANS.search(code)
    if m:
        src   = m.group(1)
        body  = m.group(2)
        vm    = _VAR_STMT.search(body)
        cm    = _CLASS_STMT.search(body)
        var   = vm.group(1).split() if vm else []
        cls   = [c.strip() for c in cm.group(1).split() if c.strip()] if cm else []
        lines = [f"# PROC MEANS DATA={src}"]
        if cls and var:
            grp  = ", ".join(f'"{c}"' for c in cls)
            aggs = ", ".join(
                f'F.mean("{v}").alias("mean_{v}"), F.stddev("{v}").alias("std_{v}")'
                for v in var
            )
            lines.append(f"result_df = {src}_df.groupBy({grp}).agg({aggs})")
        else:
            lines.append(f"result_df = {src}_df.describe()")
        notes.append("PROC MEANS → .groupBy().agg() or .describe()")
        return "\n".join(lines), notes, warnings

    # --- PROC SQL ---
    m = _PROC_SQL_BLOCK.search(code)
    if m:
        sql = m.group(1).strip()
        sql = re.sub(r"CREATE\s+TABLE\s+(\w+)\s+AS", r"CREATE OR REPLACE TABLE \1 AS", sql, flags=re.IGNORECASE)
        notes.append("PROC SQL → spark.sql()")
        warnings.append("Check for SAS-only functions: MONOTONIC(), CALCULATED, INTO :macro_var.")
        return (
            f"result_df = spark.sql(\"\"\"\n{sql}\n\"\"\")"
        ), notes, warnings

    # --- DATA step ---
    m = _DATA_STEP.search(code)
    if m:
        out_ds = m.group(1)
        src_ds = m.group(2)
        body   = m.group(3).strip()
        lines  = [f"# DATA {out_ds}; SET {src_ds};", f"{out_ds}_df = {src_ds}_df"]

        km = _KEEP_STMT.search(body)
        if km:
            cols = [f'"{c.strip()}"' for c in km.group(1).split() if c.strip()]
            lines.append(f"    .select({', '.join(cols)})")
            notes.append("KEEP → .select()")

        dm = _DROP_STMT.search(body)
        if dm:
            cols = [f'"{c.strip()}"' for c in dm.group(1).split() if c.strip()]
            lines.append(f"    .drop({', '.join(cols)})")
            notes.append("DROP → .drop()")

        rm = _RENAME_STMT.search(body)
        if rm:
            for old, new in re.findall(r"(\w+)\s*=\s*(\w+)", rm.group(1)):
                lines.append(f"    .withColumnRenamed('{old}', '{new}')")
            notes.append("RENAME → .withColumnRenamed()")

        wm = _WHERE_STMT.search(body)
        if wm:
            cond = _clean_where(wm.group(1))
            lines.append(f"    .filter({cond!r})")
            notes.append("WHERE → .filter()")
            warnings.append("Review filter condition — check string quoting and operator mapping.")

        im = _IF_THEN_ELSE.search(body)
        if im:
            cond     = _clean_where(im.group(1))
            col_name = im.group(2)
            then_val = im.group(3).strip()
            else_val = im.group(5).strip() if im.group(5) else None
            if else_val:
                lines.append(f"    .withColumn('{col_name}', F.when({cond}, {then_val}).otherwise({else_val}))")
            else:
                lines.append(f"    .withColumn('{col_name}', F.when({cond}, {then_val}))")
            notes.append("IF-THEN-ELSE → F.when().otherwise()")

        # INTCK check must run BEFORE date substitutions, because TODAY() inside INTCK
        # would otherwise be rewritten to F.current_date() and the regex would stop matching.
        if _SAS_INTCK.search(body):
            body = _SAS_INTCK.sub(_intck_warning, body)
            warnings.append(
                "INTCK computes interval boundaries, not elapsed time. "
                "Use F.datediff() for days or F.months_between() for months — review carefully."
            )

        # Date function substitutions — applied to body so we can emit withColumn calls
        has_date_fns = bool(_SAS_DATE_TODAY.search(body) or _SAS_DATE_MDY.search(body))
        body = _SAS_DATE_TODAY.sub("F.current_date()", body)
        body = _SAS_DATE_MDY.sub(_mdy_to_spark, body)
        if has_date_fns:
            notes.append("SAS date literals → Spark date functions")

        # Emit any remaining col = <spark_expr> assignments as .withColumn() calls
        _spark_assign = re.compile(r"(\w+)\s*=\s*(F\.[A-Za-z_]+\([^;]*\))\s*;?")
        for am in _spark_assign.finditer(body):
            col_name, expr = am.group(1), am.group(2)
            lines.append(f"    .withColumn('{col_name}', {expr})")

        # RETAIN warning
        if _SAS_RETAIN.search(body):
            warnings.append(
                "RETAIN carries forward row-level state in SAS. "
                "In PySpark use: F.last(col, ignorenulls=True).over("
                "Window.orderBy('date').rowsBetween(Window.unboundedPreceding, 0))"
            )

        notes.append("DATA step → DataFrame chain")
        return "\n".join(lines), notes, warnings

    return "", notes, warnings


def _to_databricks_sql(sas_code: str) -> Tuple[str, List[str], List[str]]:
    """Rule-based conversion to Databricks SQL (near 1:1 for PROC SQL)."""
    notes: List[str] = []
    warnings: List[str] = []
    m = _PROC_SQL_BLOCK.search(sas_code)
    if m:
        sql = m.group(1).strip()
        sql = re.sub(
            r"CREATE\s+TABLE\s+(\w+)\s+AS",
            r"CREATE OR REPLACE TABLE \1 AS",
            sql, flags=re.IGNORECASE
        )
        notes.append("PROC SQL → Databricks SQL")
        warnings.append("Review: MONOTONIC(), CALCULATED, INTO :macro_var are SAS-only.")
        return sql, notes, warnings
    return "", notes, warnings


def _to_yaml(sas_code: str) -> Tuple[str, List[str], List[str]]:
    """Stub: rule-based YAML is complex — always delegates to LLM."""
    return "", [], []


# ---------------------------------------------------------------------------
# LLM conversion
# ---------------------------------------------------------------------------

def _llm_convert(sas_code: str, target: str, api_key: str) -> Tuple[str, List[str]]:
    """Use Claude claude-haiku-4-5 to convert code the rules couldn't handle."""
    try:
        import anthropic
    except ImportError:
        return (
            "# anthropic package not installed. Run: pip install anthropic\n",
            ["LLM skipped — anthropic not installed"]
        )

    if not api_key:
        return (
            "# ANTHROPIC_API_KEY not set in .env\n",
            ["LLM skipped — API key missing"]
        )

    target_desc = {
        "pyspark":       "PySpark DataFrame API code (pyspark.sql.functions imported as F)",
        "databricks_sql":"Databricks SQL compatible with Delta Lake",
        "yaml":          "dbt-style YAML model definition",
    }.get(target, "PySpark")

    prompt = f"""You are a data engineering expert migrating legacy SAS code to modern Databricks pipelines.

Convert the SAS code below to {target_desc}.

Requirements:
- Preserve all logic exactly — do not simplify or skip steps
- For SAS RETAIN → use Window functions with unboundedPreceding frame
- For SAS MERGE → use .join() and note key-variable differences in a comment
- For SAS macro variables (&var) → convert to Python variables
- Add a short comment above each converted block
- Mark anything that cannot be converted cleanly with # WARNING

SAS code:
```sas
{sas_code}
```

Return only the converted code with inline comments. No prose outside the code."""

    client = anthropic.Anthropic(api_key=api_key)
    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=2048,
        messages=[{"role": "user", "content": prompt}]
    )
    raw = msg.content[0].text.strip()
    raw = re.sub(r"^```\w*\n?", "", raw)
    raw = re.sub(r"\n?```$", "", raw)
    return raw, ["Converted via Claude claude-haiku-4-5"]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def convert(
    sas_code: str,
    target: str = "pyspark",
    api_key: Optional[str] = None,
) -> ConversionResult:
    """
    Convert SAS code to PySpark, Databricks SQL, or dbt YAML.

    Parameters
    ----------
    sas_code : str
        Source SAS code.
    target : str
        "pyspark" | "databricks_sql" | "yaml"
    api_key : str, optional
        Anthropic API key. Falls back to ANTHROPIC_API_KEY env var.

    Returns
    -------
    ConversionResult
    """
    if api_key is None:
        api_key = os.getenv("ANTHROPIC_API_KEY", "")

    target = target.lower().strip()

    dispatch = {
        "pyspark":       _to_pyspark,
        "databricks_sql":_to_databricks_sql,
        "yaml":          _to_yaml,
    }
    rule_fn = dispatch.get(target, _to_pyspark)
    output, notes, warnings = rule_fn(sas_code)

    if output:
        method = "rule_based"
    else:
        output, llm_notes = _llm_convert(sas_code, target, api_key)
        notes.extend(llm_notes)
        method = "llm"

    return ConversionResult(
        original=sas_code,
        target=target,
        output=output,
        method=method,
        notes=notes,
        warnings=warnings,
    )
