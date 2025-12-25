from fastapi import FastAPI
from pydantic import BaseModel
from backend.parser.parser import parse_script
from backend.engine import execute

app = FastAPI()

class ScriptIn(BaseModel):
    code: str

@app.get("/")
def root():
    return {"message": "SAS Clone backend is running"}

@app.post("/run-script")
def run_script(payload: ScriptIn):
    plan = parse_script(payload.code)
    df, env = execute(plan)
    return {
        "message": "DATA step executed",
        "columns": list(df.columns),
        "preview": df.head(5).to_dict(orient="records"),
        "shape": list(df.shape)
    }
