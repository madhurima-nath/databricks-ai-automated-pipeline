# From SAS to PySpark: Migrating Legacy Financial Analytics Code to a Modern Lakehouse

*Draft for Medium — target publication: ~2000 words*

---

## Introduction

SAS is still the dominant analytics platform at European banks, insurers, and asset managers.
It has been for decades. Many institutions have years of production code in SAS — data steps,
PROC SQL pipelines, PROC MEANS summaries — and they are now migrating to cloud-based platforms
like Databricks.

Migration is not just a lift-and-shift. SAS and PySpark have fundamentally different execution
models. A SAS DATA step processes data row by row; a Spark DataFrame operation processes it
in parallel across a distributed cluster. Some SAS constructs map cleanly; others require a
rethink.

This post walks through the two-stage converter I built for this project: a deterministic
rule engine for common patterns, backed by an LLM (Claude) for anything the rules can't handle.

---

## Why a two-stage approach

The first instinct when building a code converter is to use an LLM for everything. It handles
edge cases, understands intent, and produces readable output.

The problem is unpredictability. The same PROC SORT input might produce slightly different
PySpark on two runs. If you're converting hundreds of SAS scripts as part of a migration
project, you want deterministic output for the cases you understand well, and LLM assistance
only for the genuinely complex cases.

The rule-based engine is also faster and cheaper: it handles ~80% of typical production SAS
in milliseconds with no API call.

```python
from src.converter.sas_to_pyspark import convert

result = convert("""
PROC SORT DATA=customers;
    BY region last_name;
RUN;
""", target="pyspark")

print(result.output)
# customers_df = customers_df.orderBy("region", "last_name")

print(result.method)
# rule_based
```

---

## What the rule engine covers

The converter handles the SAS constructs most commonly found in financial analytics work:

**PROC SORT** maps directly to `.orderBy()`. The `OUT=` option maps to a new DataFrame variable.

**PROC MEANS with CLASS and VAR** maps to `.groupBy().agg()` with `F.mean()` and `F.stddev()`.
Without a CLASS statement, it falls back to `.describe()`.

**PROC SQL** is the most straightforward: the SQL body is extracted and passed directly to
`spark.sql()`. The only rewrite needed is `CREATE TABLE AS` → `CREATE OR REPLACE TABLE AS`
(Databricks syntax). The converter adds a warning to review SAS-only functions like `MONOTONIC()`
and `CALCULATED` that have no Spark equivalent.

**DATA steps** are the most varied. The converter handles:
- `KEEP` → `.select()`
- `DROP` → `.drop()`
- `RENAME` → `.withColumnRenamed()`
- `WHERE` → `.filter()` with operator mapping (`=` → `==`, `^=` → `!=`)
- `IF-THEN-ELSE` → `F.when().otherwise()`
- `TODAY()` → `F.current_date()`
- `MDY(month, day, year)` → `F.to_date(F.lit('YYYY-MM-DD'))`

---

## The constructs that need warnings

Some SAS patterns cannot be translated without human review.

**RETAIN** carries a value forward from one row to the next in a DATA step. In PySpark there
are no row-by-row mutations; the equivalent is a window function with an unbounded preceding
frame. The converter emits the pattern in a warning rather than guessing:

```
⚠️ RETAIN carries forward row-level state in SAS.
   In PySpark use: F.last(col, ignorenulls=True).over(
       Window.orderBy('date').rowsBetween(Window.unboundedPreceding, 0))
```

**INTCK** counts interval boundaries, not elapsed time. `INTCK('YEAR', '2020-01-01', '2020-12-31')`
returns 0 in SAS because no year boundary is crossed — a behaviour that surprises most people.
`F.datediff()` in Spark counts actual days, so a 1:1 substitution would silently produce
different numbers. The converter flags every INTCK call:

```
⚠️ INTCK('YEAR', hire_date, TODAY()) — SAS counts interval boundaries, not elapsed time.
   Use F.datediff(to_dt, from_dt) for days, or F.months_between(to_dt, from_dt) for months.
```

**Macro variables** (`&table_name`, `&report_year`) are a SAS-specific templating mechanism.
They must be converted to Python function parameters or f-string interpolation before the code
can run. The converter detects them across the entire input (not just the DATA step body) and
warns immediately.

---

## LLM fallback with Claude

When the rule engine produces no output — either because the pattern is unrecognised or because
the target is YAML — the converter falls back to Claude claude-haiku-4-5:

```python
def _llm_convert(sas_code: str, target: str, api_key: str):
    prompt = f"""You are a data engineering expert migrating legacy SAS code to modern Databricks pipelines.

Convert the SAS code below to {target_desc}.

Requirements:
- Preserve all logic exactly — do not simplify or skip steps
- For SAS RETAIN → use Window functions with unboundedPreceding frame
- For SAS MERGE → use .join() and note key-variable differences in a comment
- For SAS macro variables (&var) → convert to Python variables
- Mark anything that cannot be converted cleanly with # WARNING

SAS code:
```sas
{sas_code}
```

Return only the converted code with inline comments. No prose outside the code."""
    ...
```

The prompt instructs the model on the specific patterns that need special handling, so it
doesn't guess at RETAIN or INTCK. It also strips markdown fences from the response before
returning, which matters because the output is used directly in UI or written to a file.

---

## A subtle bug: ordering matters in the rule engine

One bug found during testing is worth describing because it's a good example of how regex-based
converters can fail silently.

The converter substitutes `TODAY()` with `F.current_date()` before checking for `INTCK`.
But `INTCK('YEAR', hire_date, TODAY())` becomes `INTCK('YEAR', hire_date, F.current_date())`
after the substitution — and the INTCK regex `[\w()]+` doesn't match the dot in `F.current_date()`.
The INTCK check silently stops matching, so the warning never fires.

The fix is to run the INTCK check *before* the date substitutions:

```python
# INTCK check must run BEFORE date substitutions
if _SAS_INTCK.search(body):
    body = _SAS_INTCK.sub(_intck_warning, body)
    warnings.append("INTCK computes interval boundaries, not elapsed time...")

# Date substitutions happen after
body = _SAS_DATE_TODAY.sub("F.current_date()", body)
body = _SAS_DATE_MDY.sub(_mdy_to_spark, body)
```

This was caught by the test case `test_intck_warning`, which uses `INTCK('YEAR', hire_date, TODAY())`
— exactly the combination that triggers the bug. The test is still in the suite and still passes.

---

## Using the converter in practice

The converter is available as a Python function and as a page in the Streamlit dashboard.

**Python API:**
```python
result = convert(sas_code, target="pyspark", api_key=None)
print(result.output)    # converted code
print(result.notes)     # what each construct mapped to
print(result.warnings)  # things that need human review
print(result.method)    # "rule_based" or "llm"
```

**Dashboard:** open the SAS → PySpark Converter page, paste your code, select the target
format, and click Convert. Warnings are highlighted in amber. Notes show the mapping for
each construct.

---

## What this looks like in a migration project

In practice, a SAS-to-Spark migration at a financial institution involves:

1. Cataloguing all SAS scripts (often thousands of files across multiple environments)
2. Classifying them by complexity: PROC SORT and simple DATA steps are mechanical; complex
   RETAIN-heavy ETL and SAS macro libraries need more work
3. Running the deterministic converter on the simple cases to get a first draft
4. Using the LLM fallback or manual rewrite for the complex cases
5. Validating output against the original: same row counts, same aggregations, same join keys

The converter in this project handles step 3 and produces explicit warnings for step 4.
It does not replace a data engineer — it gives one a starting point that is faster than
writing from scratch, and it makes the risky constructs (RETAIN, INTCK, macro variables) visible.

---

## Key takeaways

- Rule-based conversion is predictable and fast. Use it for the ~80% of SAS patterns that
  map cleanly to PySpark.
- LLM fallback handles the remaining cases, but give it explicit instructions about the
  constructs that require special handling (RETAIN → Window, INTCK → review).
- Some SAS patterns genuinely cannot be auto-converted: RETAIN, INTCK, and macro variables
  all require human judgment. Surfacing them as warnings is more useful than guessing.
- Test with real SAS patterns, including the edge cases: `INTCK` with `TODAY()` as an argument,
  macro variables in the `SET` clause, and `CREATE TABLE AS SELECT` in PROC SQL.

---

*Code: github.com/madhurima-nath/databricks-ai-automated-pipeline*
