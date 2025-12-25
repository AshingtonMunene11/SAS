from pathlib import Path
import pandas as pd

class Env:
    """
    Simple environment to hold datasets by name.
    """
    def __init__(self):
        self.datasets = {}

    def save(self, name: str, df: pd.DataFrame):
        self.datasets[name] = df

    def load_saved(self, name: str) -> pd.DataFrame:
        return self.datasets.get(name)

def load_dataset(path: str) -> pd.DataFrame:
    """
    Load a dataset from CSV or Excel.
    """
    ext = Path(path).suffix.lower()
    if ext == ".csv":
        return pd.read_csv(path)
    elif ext in (".xls", ".xlsx"):
        return pd.read_excel(path)  # requires openpyxl
    else:
        raise ValueError(f"Unsupported file type: {ext}")
