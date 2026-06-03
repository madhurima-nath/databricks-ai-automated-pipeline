"""
Tests for the SAS → PySpark / Databricks SQL converter.
Run locally with: pytest tests/test_sas_converter.py -v
No Spark or Databricks connection required.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from converter.sas_to_pyspark import convert, ConversionResult


# ---------------------------------------------------------------------------
# PROC SORT
# ---------------------------------------------------------------------------

class TestProcSort:
    def test_basic_sort(self):
        sas = "PROC SORT DATA=customers; BY last_name; RUN;"
        result = convert(sas, target="pyspark")
        assert result.method == "rule_based"
        assert "orderBy" in result.output
        assert '"last_name"' in result.output
        assert "customers_df" in result.output

    def test_sort_with_out(self):
        sas = "PROC SORT DATA=orders OUT=orders_sorted; BY order_date customer_id; RUN;"
        result = convert(sas, target="pyspark")
        assert "orders_sorted_df" in result.output
        assert '"order_date"' in result.output
        assert '"customer_id"' in result.output

    def test_sort_note_recorded(self):
        sas = "PROC SORT DATA=sales; BY amount; RUN;"
        result = convert(sas, target="pyspark")
        assert any("PROC SORT" in n for n in result.notes)


# ---------------------------------------------------------------------------
# PROC MEANS
# ---------------------------------------------------------------------------

class TestProcMeans:
    def test_means_with_class_and_var(self):
        sas = """
        PROC MEANS DATA=sales;
            CLASS region;
            VAR revenue units;
        RUN;
        """
        result = convert(sas, target="pyspark")
        assert result.method == "rule_based"
        assert "groupBy" in result.output
        assert "mean" in result.output.lower()
        assert '"region"' in result.output

    def test_means_no_class(self):
        sas = "PROC MEANS DATA=inventory; VAR price; RUN;"
        result = convert(sas, target="pyspark")
        assert "describe" in result.output


# ---------------------------------------------------------------------------
# PROC SQL
# ---------------------------------------------------------------------------

class TestProcSQL:
    def test_select_statement(self):
        sas = """
        PROC SQL;
            SELECT customer_id, SUM(amount) AS total
            FROM transactions
            GROUP BY customer_id;
        QUIT;
        """
        result = convert(sas, target="pyspark")
        assert result.method == "rule_based"
        assert "spark.sql" in result.output
        assert "SELECT" in result.output

    def test_create_table_rewritten(self):
        sas = """
        PROC SQL;
            CREATE TABLE summary AS
            SELECT region, SUM(revenue) AS total FROM sales GROUP BY region;
        QUIT;
        """
        result = convert(sas, target="pyspark")
        assert "CREATE OR REPLACE TABLE" in result.output

    def test_databricks_sql_target(self):
        sas = """
        PROC SQL;
            SELECT date, close FROM prices WHERE close > 4000;
        QUIT;
        """
        result = convert(sas, target="databricks_sql")
        assert result.method == "rule_based"
        assert "SELECT" in result.output
        assert "spark.sql" not in result.output
        assert any("PROC SQL" in n for n in result.notes)

    def test_sql_warnings_present(self):
        sas = "PROC SQL; SELECT MONOTONIC() FROM data; QUIT;"
        result = convert(sas, target="pyspark")
        assert any("MONOTONIC" in w for w in result.warnings)


# ---------------------------------------------------------------------------
# DATA step
# ---------------------------------------------------------------------------

class TestDataStep:
    def test_keep_statement(self):
        sas = """
        DATA output;
            SET input;
            KEEP id name amount;
        RUN;
        """
        result = convert(sas, target="pyspark")
        assert result.method == "rule_based"
        assert ".select(" in result.output
        assert '"id"' in result.output

    def test_drop_statement(self):
        sas = """
        DATA clean;
            SET raw;
            DROP internal_flag temp_var;
        RUN;
        """
        result = convert(sas, target="pyspark")
        assert ".drop(" in result.output
        assert '"internal_flag"' in result.output

    def test_rename_statement(self):
        sas = """
        DATA renamed;
            SET source;
            RENAME cust_id=customer_id rev=revenue;
        RUN;
        """
        result = convert(sas, target="pyspark")
        assert "withColumnRenamed" in result.output
        assert "'cust_id'" in result.output
        assert "'customer_id'" in result.output

    def test_if_then_else(self):
        sas = """
        DATA categorised;
            SET transactions;
            IF amount > 1000 THEN tier = 'high';
            ELSE tier = 'standard';
        RUN;
        """
        result = convert(sas, target="pyspark")
        assert "F.when" in result.output
        assert "otherwise" in result.output

    def test_if_then_no_else(self):
        sas = """
        DATA flagged;
            SET orders;
            IF quantity > 100 THEN large_order = 1;
        RUN;
        """
        result = convert(sas, target="pyspark")
        assert "F.when" in result.output

    def test_where_clause(self):
        sas = """
        DATA active;
            SET customers;
            WHERE status = 'active';
        RUN;
        """
        result = convert(sas, target="pyspark")
        assert ".filter(" in result.output
        # SAS = should become == in filter
        assert "==" in result.output

    def test_today_function(self):
        sas = """
        DATA dated;
            SET records;
            run_date = TODAY();
        RUN;
        """
        result = convert(sas, target="pyspark")
        assert "F.current_date()" in result.output
        assert any("date" in n.lower() for n in result.notes)

    def test_mdy_function(self):
        sas = """
        DATA dated;
            SET records;
            start = MDY(1, 15, 2020);
        RUN;
        """
        result = convert(sas, target="pyspark")
        assert "F.to_date" in result.output
        assert "2020-01-15" in result.output

    def test_retain_warning(self):
        sas = """
        DATA running_total;
            SET daily_sales;
            RETAIN cumulative 0;
            cumulative = cumulative + revenue;
        RUN;
        """
        result = convert(sas, target="pyspark")
        assert any("RETAIN" in w for w in result.warnings)
        assert any("Window" in w for w in result.warnings)

    def test_intck_warning(self):
        sas = """
        DATA age_calc;
            SET employees;
            tenure = INTCK('YEAR', hire_date, TODAY());
        RUN;
        """
        result = convert(sas, target="pyspark")
        assert any("INTCK" in w or "WARNING" in result.output for w in result.warnings + [result.output])

    def test_macro_variable_warning(self):
        sas = """
        DATA filtered;
            SET &source_table;
            WHERE year = &report_year;
        RUN;
        """
        result = convert(sas, target="pyspark")
        assert any("macro" in w.lower() for w in result.warnings)


# ---------------------------------------------------------------------------
# ConversionResult structure
# ---------------------------------------------------------------------------

class TestConversionResult:
    def test_result_fields_present(self):
        sas = "PROC SORT DATA=x; BY id; RUN;"
        result = convert(sas)
        assert isinstance(result, ConversionResult)
        assert result.original == sas
        assert result.target == "pyspark"
        assert isinstance(result.output, str)
        assert isinstance(result.notes, list)
        assert isinstance(result.warnings, list)
        assert result.method in ("rule_based", "llm", "hybrid")

    def test_unrecognised_code_goes_to_llm_path(self):
        sas = "%MACRO complex_analysis; /* deeply nested SAS macro */ %MEND;"
        result = convert(sas, target="pyspark", api_key="")
        # No API key → LLM skipped gracefully, output is a comment
        assert result.method == "llm"
        assert "ANTHROPIC_API_KEY" in result.output or "anthropic" in result.output.lower()

    def test_default_target_is_pyspark(self):
        sas = "PROC SORT DATA=x; BY id; RUN;"
        result = convert(sas)
        assert result.target == "pyspark"
