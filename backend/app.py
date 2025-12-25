from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
from .parser.parser import parse_script
from .engine import apply_clauses

app = FastAPI()

class ScriptRequest(BaseModel):
    code: str

@app.post("/run-script")
def run_script(req: ScriptRequest):
    plan = parse_script(req.code)[0]
    df = pd.read_csv(plan["set"]["path"])
    df = apply_clauses(df, plan)

    preview = df.head(5).to_dict(orient="records")
    return {
        "message": "DATA step executed",
        "columns": list(df.columns),
        "shape": list(df.shape),
        "preview": preview,
    }
