import io
import base64
from typing import Dict, List, Tuple, Optional

import pandas as pd

# Optional dependencies for Phase 4
import statsmodels.api as sm
import matplotlib.pyplot as plt

# ----- DATA step clause functions -----

def apply_where(df: pd.DataFrame, cond: Dict[str, str]) -> pd.DataFrame:
    col, op, val = cond["column"], cond["op"], cond["value"]
    if col not in df.columns:
        return df

    # Attempt numeric comparison when possible
    try:
        val_num = float(val)
        is_numeric = True
    except ValueError:
        val_num = val
        is_numeric = False

    series = df[col]
    if op == ">":
        df = df[series > (val_num if is_numeric else val)]
    elif op == "<":
        df = df[series < (val_num if is_numeric else val)]
    elif op == "=":
        df = df[series == (val_num if is_numeric else val)]
    elif op == ">=":
        df = df[series >= (val_num if is_numeric else val)]
    elif op == "<=":
        df = df[series <= (val_num if is_numeric else val)]
    elif op == "!=":
        df = df[series != (val_num if is_numeric else val)]
    return df

def apply_keep(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    return df[[c for c in cols if c in df.columns]]

def apply_drop(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    return df.drop(columns=cols, errors="ignore")

def apply_rename(df: pd.DataFrame, pairs: List[Tuple[str, str]]) -> pd.DataFrame:
    rename_map = {old: new for old, new in pairs}
    return df.rename(columns=rename_map)

def apply_clauses(df: pd.DataFrame, plan: Dict) -> pd.DataFrame:
    if "where" in plan:
        df = apply_where(df, plan["where"])
    if "keep" in plan:
        df = apply_keep(df, plan["keep"])
    if "drop" in plan:
        df = apply_drop(df, plan["drop"])
    if "rename" in plan:
        df = apply_rename(df, plan["rename"])
    return df

# ----- PROC PRINT -----

def proc_print(df: pd.DataFrame, plan: Dict, output_format: str = "json") -> Dict:
    df_out = df

    # Apply VAR (select columns)
    if "var" in plan:
        cols = [c for c in plan["var"] if c in df_out.columns]
        df_out = df_out[cols]

    # Apply OBS (limit rows), default 50 if not specified
    limit = plan.get("obs", 50)
    df_out = df_out.head(limit)

    if output_format == "html":
        return {
            "message": "PROC PRINT executed",
            "html": df_out.to_html(index=False),
            "shape": list(df.shape),
        }
    else:
        return {
            "message": "PROC PRINT executed",
            "columns": list(df_out.columns),
            "preview": df_out.to_dict(orient="records"),
            "shape": list(df.shape),
        }

# ----- PROC MEANS -----

def proc_means(df: pd.DataFrame, output_format: str = "json") -> Dict:
    num_df = df.select_dtypes(include=["number"])
    stats = num_df.agg(["mean", "min", "max", "std"])
    if output_format == "html":
        return {
            "message": "PROC MEANS executed",
            "html": stats.to_html(),
        }
    else:
        return {
            "message": "PROC MEANS executed",
            "statistics": stats.to_dict(),
        }

# ----- PROC FREQ -----

def proc_freq(df: pd.DataFrame, plan: Dict, output_format: str = "json") -> Dict:
    # TABLES col; or TABLES colA*colB;
    if "tables" in plan:
        cols = plan["tables"]
        if len(cols) == 1:
            col = cols[0]
            if col not in df.columns:
                return {"message": "PROC FREQ executed", "frequencies": {col: {}}}
            freq = df[col].value_counts(dropna=False).to_dict()
            if output_format == "html":
                return {
                    "message": "PROC FREQ executed",
                    "html": pd.Series(freq, name=col).to_frame(name="count").to_html(),
                }
            else:
                return {"message": "PROC FREQ executed", "frequencies": {col: freq}}
        elif len(cols) == 2:
            a, b = cols
            if a not in df.columns or b not in df.columns:
                return {"message": "PROC FREQ executed", "crosstab": {}}
            ctab = pd.crosstab(df[a], df[b], dropna=False)
            if output_format == "html":
                return {"message": "PROC FREQ executed", "html": ctab.to_html()}
            else:
                # SAS‑style: row‑oriented dict (first var = rows)
                return {"message": "PROC FREQ executed", "crosstab": ctab.to_dict(orient="index")}
        else:
            return {"message": "PROC FREQ executed", "error": "Only 1 or 2 columns supported in TABLES"}
    else:
        # Default: frequencies for all columns
        freq = {col: df[col].value_counts(dropna=False).to_dict() for col in df.columns}
        if output_format == "html":
            html_tables = {}
            for col, counts in freq.items():
                html_tables[col] = pd.Series(counts, name=col).to_frame(name="count").to_html()
            return {"message": "PROC FREQ executed", "html_tables": html_tables}
        else:
            return {"message": "PROC FREQ executed", "frequencies": freq}

# ----- PROC REG (OLS) -----

def proc_reg(df: pd.DataFrame, plan: Dict, output_format: str = "json", chart: bool = False) -> Dict:
    dep = plan["dependent"]
    indep_cols = plan["independent"]

    # Basic input validation
    if dep not in df.columns:
        return {"message": "PROC REG error", "error": f"Dependent variable '{dep}' not found"}
    for c in indep_cols:
        if c not in df.columns:
            return {"message": "PROC REG error", "error": f"Independent variable '{c}' not found"}

    y = df[dep]
    X = df[indep_cols]

    # Exclude non-numeric columns for OLS; one-hot encoding is Phase 5 territory
    numeric_cols = X.select_dtypes(include=["number"]).columns.tolist()
    X = X[numeric_cols]
    X = sm.add_constant(X, has_constant="add")

    model = sm.OLS(y, X, missing="drop").fit()

    summary = {
        "coefficients": model.params.to_dict(),
        "pvalues": model.pvalues.to_dict(),
        "rsquared": model.rsquared,
        "nobs": int(model.nobs),
    }

    result: Dict = {"message": "PROC REG executed"}

    if output_format == "html":
        result["html"] = model.summary().as_html()
    else:
        result["summary"] = summary

    # Optional chart generation via parser PLOT y*x or explicit chart=True flag
    plot_spec = plan.get("plot")
    if chart or plot_spec:
        # If plot was provided in script, use it; otherwise, pick first numeric predictor
        if plot_spec:
            y_name = plot_spec["y"]
            x_name = plot_spec["x"]
        else:
            # Choose first numeric independent variable
            if len(numeric_cols) == 0:
                return result
            x_name = numeric_cols[0]
            y_name = dep

        # Build scatter + regression line for x_name vs dep
        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax.scatter(df[x_name], df[y_name], label="Data", alpha=0.7)

        # Fit simple OLS for the chosen single predictor
        X_single = sm.add_constant(df[x_name], has_constant="add")
        model_single = sm.OLS(df[y_name], X_single, missing="drop").fit()
        x_sorted = df[x_name].sort_values()
        X_pred = sm.add_constant(x_sorted, has_constant="add")
        y_pred = model_single.predict(X_pred)

        ax.plot(x_sorted, y_pred, color="red", label="Regression line")
        ax.set_xlabel(x_name)
        ax.set_ylabel(y_name)
        ax.legend()

        buf = io.BytesIO()
        fig.tight_layout()
        fig.savefig(buf, format="png")
        plt.close(fig)
        buf.seek(0)
        result["chart_png_base64"] = base64.b64encode(buf.read()).decode("utf-8")

    return result

# ----- Dispatcher -----

def run_proc(plan: Dict, df: pd.DataFrame, output_format: str = "json", limit: Optional[int] = 50) -> Dict:
    if plan["type"] == "proc_print":
        return proc_print(df, plan, output_format=output_format)
    elif plan["type"] == "proc_means":
        return proc_means(df, output_format=output_format)
    elif plan["type"] == "proc_freq":
        return proc_freq(df, plan, output_format=output_format)
    elif plan["type"] == "proc_reg":
        # If HTML requested, return HTML summary; chart generation controlled by plan.plot or external flag
        return proc_reg(df, plan, output_format=output_format, chart=bool(plan.get("plot")))
    else:
        raise ValueError(f"Unknown PROC type: {plan['type']}")
