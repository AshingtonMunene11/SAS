import pandas as pd
from typing import Dict, List, Tuple, Optional

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

# ----- PROC functions -----

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

def run_proc(plan: Dict, df: pd.DataFrame, output_format: str = "json", limit: Optional[int] = 50) -> Dict:
    if plan["type"] == "proc_print":
        return proc_print(df, plan, output_format=output_format)
    elif plan["type"] == "proc_means":
        return proc_means(df, output_format=output_format)
    elif plan["type"] == "proc_freq":
        return proc_freq(df, plan, output_format=output_format)
    else:
        raise ValueError(f"Unknown PROC type: {plan['type']}")
