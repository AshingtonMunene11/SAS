from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import logging
import os

from parser.parser import parse_script
from engine import run_proc
from executor.data_step import run_data_step

app = FastAPI()

# Ensure logs directory exists
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
            try:
                proc_output = run_data_step(plan, output_format=req.output_format, limit=req.limit)
            except Exception as e:
                logging.error(f"DATA step error: {e}")
                raise HTTPException(status_code=400, detail=f"DATA step error: {e}")
            results.append(proc_output)
            # remember last dataset path for subsequent PROCs
            last_path = plan.get("path") or (plan.get("set") or {}).get("path")

        elif plan.get("type", "").startswith("proc_"):
            if df is None:
                if last_path is not None:
                    try:
                        # reload dataset for PROC steps
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

    # Flatten response if only one block
    if len(results) == 1:
        return results[0]
    else:
        return {
            "steps": len(blocks),
            "results": results,
        }
