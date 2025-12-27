import pytest
import warnings
import pandas as pd
from backend.engine import proc_print, proc_means, proc_freq, proc_reg, run_data_step
from statsmodels.tools.sm_exceptions import ValueWarning

warnings.filterwarnings("ignore", category=ValueWarning)


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "name": ["Alice", "Bob", "Carol", "Bob"],
        "age": [25, 35, 41, 35],
        "gender": ["F", "M", "F", "M"],
        "income": [48000, 52000, 68000, 52000]
    })

# ----- PROC PRINT -----

def test_proc_print_var(sample_df):
    plan = {"type": "proc_print", "var": ["name", "age"]}
    result = proc_print(sample_df, plan, output_format="json")
    assert result["columns"] == ["name", "age"]
    assert all("income" not in row for row in result["preview"])

def test_proc_print_obs(sample_df):
    plan = {"type": "proc_print", "obs": 2}
    result = proc_print(sample_df, plan, output_format="json")
    assert len(result["preview"]) == 2

def test_proc_print_var_and_obs(sample_df):
    plan = {"type": "proc_print", "var": ["name"], "obs": 1}
    result = proc_print(sample_df, plan, output_format="json")
    assert result["columns"] == ["name"]
    assert len(result["preview"]) == 1

def test_proc_print_html(sample_df):
    plan = {"type": "proc_print", "var": ["name", "age"], "obs": 2}
    result = proc_print(sample_df, plan, output_format="html")
    assert "<table" in result["html"]

# ----- PROC MEANS -----

def test_proc_means_json(sample_df):
    result = proc_means(sample_df, output_format="json")
    stats = result["statistics"]
    assert "age" in stats
    assert "mean" in stats["age"]

def test_proc_means_html(sample_df):
    result = proc_means(sample_df, output_format="html")
    assert "<table" in result["html"]

# ----- PROC FREQ -----

def test_proc_freq_oneway_json(sample_df):
    plan = {"type": "proc_freq", "tables": ["gender"]}
    result = proc_freq(sample_df, plan, output_format="json")
    assert result["frequencies"]["gender"]["M"] == 2

def test_proc_freq_twoway_json(sample_df):
    plan = {"type": "proc_freq", "tables": ["gender", "age"]}
    result = proc_freq(sample_df, plan, output_format="json")
    # SAS-style row-oriented dict: gender -> age -> count
    assert result["crosstab"]["M"][35] == 2
    assert result["crosstab"]["F"][25] == 1

def test_proc_freq_html(sample_df):
    plan = {"type": "proc_freq", "tables": ["gender", "age"]}
    result = proc_freq(sample_df, plan, output_format="html")
    assert "<table" in result["html"]

# ----- PROC REG -----

def test_proc_reg_json_summary(sample_df):
    plan = {"type": "proc_reg", "dependent": "income", "independent": ["age"]}
    result = proc_reg(sample_df, plan, output_format="json")
    summary = result["summary"]
    assert "coefficients" in summary
    assert "pvalues" in summary
    assert "rsquared" in summary

def test_proc_reg_html_summary(sample_df):
    plan = {"type": "proc_reg", "dependent": "income", "independent": ["age"]}
    result = proc_reg(sample_df, plan, output_format="html")
    assert "<table" in result["html"]

def test_proc_reg_chart_output(sample_df):
    plan = {
        "type": "proc_reg",
        "dependent": "income",
        "independent": ["age"],
        "plot": {"y": "income", "x": "age"}
    }
    result = proc_reg(sample_df, plan, output_format="json", chart=True)
    assert "chart_png_base64" in result
    assert isinstance(result["chart_png_base64"], str)
    assert len(result["chart_png_base64"]) > 100

# ----- PROC REG negative cases -----

def test_proc_reg_missing_dependent(sample_df):
    plan = {"type": "proc_reg", "dependent": "salary", "independent": ["age"]}
    result = proc_reg(sample_df, plan, output_format="json")
    assert result["message"] == "PROC REG error"
    assert "Dependent variable" in result["error"]

def test_proc_reg_missing_independent(sample_df):
    plan = {"type": "proc_reg", "dependent": "income", "independent": ["height"]}
    result = proc_reg(sample_df, plan, output_format="json")
    assert result["message"] == "PROC REG error"
    assert "Independent variable" in result["error"]

def test_proc_reg_non_numeric_independent(sample_df):
    # 'gender' is non-numeric in this fixture
    plan = {"type": "proc_reg", "dependent": "income", "independent": ["gender"]}
    result = proc_reg(sample_df, plan, output_format="json")
    # Should still execute but drop non-numeric predictors
    assert result["message"] == "PROC REG executed"
    summary = result["summary"]
    # Only intercept (const) should remain in coefficients
    assert list(summary["coefficients"].keys()) == ["const"]

def test_proc_reg_multiple_independents(sample_df):
    plan = {"type": "proc_reg", "dependent": "income", "independent": ["age", "income"]}
    result = proc_reg(sample_df, plan, output_format="json")
    summary = result["summary"]
    # Should include const, age, and income in coefficients
    assert "age" in summary["coefficients"]
    assert "income" in summary["coefficients"]
    assert "const" in summary["coefficients"]

def test_proc_freq_nonexistent_column(sample_df):
    plan = {"type": "proc_freq", "tables": ["unknown_col"]}
    result = proc_freq(sample_df, plan, output_format="json")
    # Frequencies dict should exist but be empty for that column
    assert "unknown_col" in result["frequencies"]
    assert result["frequencies"]["unknown_col"] == {}

def test_proc_freq_too_many_columns(sample_df):
    plan = {"type": "proc_freq", "tables": ["gender", "age", "income"]}
    result = proc_freq(sample_df, plan, output_format="json")
    assert "error" in result
    assert "Only 1 or 2 columns supported" in result["error"]

def test_data_step_missing_path():
    plan = {"type": "data_step"}  # no path
    result = run_data_step(plan, output_format="json")
    assert result["message"] == "DATA step error"
    assert "No dataset path" in result["error"]

def test_data_step_bad_path():
    plan = {"type": "data_step", "path": "nonexistent.csv"}
    result = run_data_step(plan, output_format="json")
    assert result["message"] == "DATA step error"
    assert "Failed to read CSV" in result["error"]
