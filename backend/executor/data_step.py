from pathlib import Path
import pandas as pd

class Env:
    def __init__(self):
        self.datasets = {}

    def save(self, name: str, df: pd.DataFrame):
        self.datasets[name] = df

    def load_saved(self, name: str) -> pd.DataFrame:
        return self.datasets.get(name)

def load_dataset(path: str, sheet: str = None) -> pd.DataFrame:
    """
    Load a dataset from CSV or Excel.
    """
    ext = Path(path).suffix.lower()
    if ext == ".csv":
        return pd.read_csv(path)
    elif ext in (".xls", ".xlsx"):
        return pd.read_excel(path, sheet_name=sheet if sheet else 0)
    else:
        raise ValueError(f"Unsupported file type: {ext}")

def apply_clauses(df: pd.DataFrame, block: dict) -> pd.DataFrame:
    """
    Apply WHERE, KEEP, DROP, RENAME clauses to a dataframe.
    """
    if "where" in block:
        cond = block["where"]
        col, op, val = cond["column"], cond["op"], cond["value"]
        if op == ">":
            df = df[df[col] > float(val)]
        elif op == "<":
            df = df[df[col] < float(val)]
        elif op == "=":
            df = df[df[col] == val]
        elif op == ">=":
            df = df[df[col] >= float(val)]
        elif op == "<=":
            df = df[df[col] <= float(val)]
        elif op == "!=":
            df = df[df[col] != val]

    if "keep" in block:
        df = df[block["keep"]]

    if "drop" in block:
        df = df.drop(columns=block["drop"])

    if "rename" in block:
        rename_map = {old: new for old, new in block["rename"]}
        df = df.rename(columns=rename_map)

    return df

def run_data_step(plan, output_format="json", limit=20):
    try:
        path = plan.get("path")
        if not path:
            return {"message": "DATA step error", "error": "No dataset path"}
    except FileNotFoundError:
        return {"message": "DATA step error", "error": "Failed to read CSV"}
    except Exception as e:
        return {"message": "DATA step error", "error": str(e)}
