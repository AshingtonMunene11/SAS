import io
import base64
from typing import Dict, List, Tuple, Optional

import pandas as pd
import statsmodels.api as sm
import matplotlib.pyplot as plt

# ----- DATA step clause functions -----

def apply_where(df: pd.DataFrame, cond: Dict[str, str]) -> pd.DataFrame:
    col, op, val = cond["column"], cond["op"], cond["value"]
    if col not in df.columns:
        return df
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

# ----- DATA step executor -----

def run_data_step(plan: Dict, output_format: str = "json") -> Dict:
    # Handle both direct and nested path
    path = plan.get("path") or (plan.get("set") or {}).get("path")
    if not path:
        return {"message": "DATA step error", "error": "No dataset path provided"}

    try:
        df = pd.read_csv(path)
    except Exception as e:
        return {"message": "DATA step error", "error": f"Failed to read CSV '{path}': {e}"}

    df = apply_clauses(df, plan)

    if output_format == "html":
        return {
            "message": "DATA step executed",
            "html": df.head(10).to_html(index=False),
            "columns": list(df.columns),
            "shape": list(df.shape),
        }
    else:
        return {
            "message": "DATA step executed",
            "columns": list(df.columns),
            "preview": df.head(10).to_dict(orient="records"),
            "shape": list(df.shape),
        }


# ----- PROC PRINT -----

def proc_print(df: pd.DataFrame, plan: Dict, output_format: str = "json") -> Dict:
    df_out = df
    if "var" in plan:
        cols = [c for c in plan["var"] if c in df_out.columns]
        df_out = df_out[cols]
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
        return {"message": "PROC MEANS executed", "html": stats.to_html()}
    else:
        return {"message": "PROC MEANS executed", "statistics": stats.to_dict()}

# ----- PROC FREQ -----

def proc_freq(df: pd.DataFrame, plan: Dict, output_format: str = "json") -> Dict:
    if "tables" in plan:
        cols = plan["tables"]
        if len(cols) == 1:
            col = cols[0]
            if col not in df.columns:
                return {"message": "PROC FREQ executed", "frequencies": {col: {}}}
            freq = df[col].value_counts(dropna=False).to_dict()
            if output_format == "html":
                return {"message": "PROC FREQ executed",
                        "html": pd.Series(freq, name=col).to_frame(name="count").to_html()}
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
                return {"message": "PROC FREQ executed", "crosstab": ctab.to_dict(orient="index")}
        else:
            return {"message": "PROC FREQ executed", "error": "Only 1 or 2 columns supported in TABLES"}
    else:
        freq = {col: df[col].value_counts(dropna=False).to_dict() for col in df.columns}
        if output_format == "html":
            html_tables = {col: pd.Series(counts, name=col).to_frame(name="count").to_html()
                           for col, counts in freq.items()}
            return {"message": "PROC FREQ executed", "html_tables": html_tables}
        else:
            return {"message": "PROC FREQ executed", "frequencies": freq}

# ----- PROC REG -----

def proc_reg(df: pd.DataFrame, plan: Dict, output_format: str = "json", chart: bool = False) -> Dict:
    dep = plan["dependent"]
    indep_cols = plan["independent"]
    if dep not in df.columns:
        return {"message": "PROC REG error", "error": f"Dependent variable '{dep}' not found"}
    for c in indep_cols:
        if c not in df.columns:
            return {"message": "PROC REG error", "error": f"Independent variable '{c}' not found"}
    y = df[dep]
    X = df[indep_cols]
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
    plot_spec = plan.get("plot")
    if chart or plot_spec:
        if plot_spec:
            y_name = plot_spec["y"]
            x_name = plot_spec["x"]
        else:
            if len(numeric_cols) == 0:
                return result
            x_name = numeric_cols[0]
            y_name = dep
        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax.scatter(df[x_name], df[y_name], label="Data", alpha=0.7)
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

def run_proc(plan: Dict,
             df: Optional[pd.DataFrame] = None,
             output_format: str = "json",
             limit: Optional[int] = 50) -> Dict:
    if plan["type"] == "data_step":
        return run_data_step(plan, output_format=output_format)
    elif plan["type"] == "proc_print":
        return proc_print(df, plan, output_format=output_format)
    elif plan["type"] == "proc_means":
        return proc_means(df, output_format=output_format)
    elif plan["type"] == "proc_freq":
        return proc_freq(df, plan, output_format=output_format)
    elif plan["type"] == "proc_reg":
        return proc_reg(df, plan,
                        output_format=output_format,
                        chart=bool(plan.get("plot")))
    else:
        raise ValueError(f"Unknown PROC type: {plan['type']}")
