import pandas as pd
import pytest
from backend.engine import proc_print, proc_means, proc_freq

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
