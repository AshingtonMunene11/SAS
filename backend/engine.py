import pandas as pd

def apply_clauses(df: pd.DataFrame, plan: dict) -> pd.DataFrame:
    # WHERE clause
    if "where" in plan:
        cond = plan["where"]
        col, op, val = cond["column"], cond["op"], cond["value"]
        if op == ">":
            df = df[df[col] > int(val)]
        elif op == "<":
            df = df[df[col] < int(val)]
        elif op == "=":
            df = df[df[col] == val]
        elif op == ">=":
            df = df[df[col] >= int(val)]
        elif op == "<=":
            df = df[df[col] <= int(val)]
        elif op == "!=":
            df = df[df[col] != val]

    # KEEP clause
    if "keep" in plan:
        df = df[plan["keep"]]

    # DROP clause
    if "drop" in plan:
        df = df.drop(columns=plan["drop"], errors="ignore")

    # RENAME clause
    if "rename" in plan:
        for old, new in plan["rename"]:
            if old in df.columns:
                df = df.rename(columns={old: new})

    return df
