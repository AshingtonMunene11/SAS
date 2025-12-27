from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import logging

from .parser.parser import parse_script
from .engine import apply_clauses, run_proc

app = FastAPI()

import os
os.makedirs("logs", exist_ok=True)

class ScriptRequest(BaseModel):
    code: str
    output_format: str = "json"  
    limit: int = 50              

@app.post("/run-script")
def run_script(req: ScriptRequest):
    try:
        blocks = parse_script(req.code)
    except Exception as e:
        logging.error(f"Parse error: {e}")
        raise HTTPException(status_code=400, detail=f"Parse error: {e}")

    df = None
    last_path = None
    results = []

    for plan in blocks:
        if plan.get("type") == "data_step":
            path = plan["set"]["path"]
            last_path = path
            try:
                df = pd.read_csv(path)
            except Exception as e:
                logging.error(f"Failed to read CSV '{path}': {e}")
                raise HTTPException(status_code=400, detail=f"Failed to read CSV '{path}': {e}")

            df = apply_clauses(df, plan)

            preview = df.head(5).to_dict(orient="records")
            results.append({
                "message": f"DATA step executed for {plan['name']}",
                "columns": list(df.columns),
                "shape": list(df.shape),
                "preview": preview,
            })

        elif plan.get("type", "").startswith("proc_"):
            if df is None:
                if last_path is not None:
                    try:
                        df = pd.read_csv(last_path)
                    except Exception as e:
                        logging.error(f"Failed to reload last dataset '{last_path}': {e}")
                        raise HTTPException(status_code=400, detail=f"Failed to reload last dataset '{last_path}': {e}")
                else:
                    raise HTTPException(status_code=400, detail="No dataset loaded before PROC")

            proc_output = run_proc(plan, df, output_format=req.output_format, limit=req.limit)
            results.append(proc_output)

        else:
            raise HTTPException(status_code=400, detail=f"Unknown plan type: {plan}")

    return {
        "steps": len(blocks),
        "results": results,
    }
