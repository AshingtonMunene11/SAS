import re
from lark import Lark, Transformer, v_args
from pathlib import Path

# Load grammar from the same directory as this file
grammar_text = Path(__file__).with_name("grammar.lark").read_text()
parser = Lark(grammar_text, start="start")

@v_args(inline=True)
class ToPlan(Transformer):
    # ----- DATA step -----

    def data_step(self, name, set_stmt, *clauses):
        block = {"type": "data_step", "name": str(name)}
        if isinstance(set_stmt, dict):
            block.update(set_stmt)
        for clause in clauses:
            if clause:
                block.update(clause)
        return block

    def set_stmt(self, path, sheet=None):
        return {"path": str(path), "sheet": str(sheet) if sheet else None}

    def where_stmt(self, cond):
        return {"where": cond}

    def condition(self, name, op, value):
        return {"column": str(name), "op": str(op), "value": str(value)}

    def keep_stmt(self, *cols):
        return {"keep": [str(c) for c in cols]}

    def drop_stmt(self, *cols):
        return {"drop": [str(c) for c in cols]}

    def rename_pair(self, old, new):
        return (str(old), str(new))

    def rename_stmt(self, *pairs):
        return {"rename": list(pairs)}

    # ----- PROC PRINT -----

    def var_stmt(self, *cols):
        return {"var": [str(c) for c in cols]}

    def obs_stmt(self, num):
        return {"obs": int(num)}

    def proc_print(self, *clauses):
        block = {"type": "proc_print"}
        for clause in clauses:
            if clause:
                block.update(clause)
        return block

    # ----- PROC MEANS -----

    def proc_means(self):
        return {"type": "proc_means"}

    # ----- PROC FREQ -----

    def proc_freq(self, tables_stmt=None):
        block = {"type": "proc_freq"}
        if tables_stmt:
            block.update(tables_stmt)
        return block

    def tables_stmt(self, expr):
        return {"tables": expr}

    def table_expr(self, *names):
        return [str(n) for n in names]

    # ----- PROC REG -----

    def proc_reg(self, model_stmt, plot_stmt=None):
        block = {"type": "proc_reg"}
        block.update(model_stmt)
        if plot_stmt:
            block.update(plot_stmt)
        return block

    def model_stmt(self, dep, first_indep, *others):
        return {
            "dependent": str(dep),
            "independent": [str(first_indep)] + [str(o) for o in others]
        }

    def plot_stmt(self, y, x):
        return {"plot": {"y": str(y), "x": str(x)}}

    # ----- Tokens -----

    def NAME(self, token):
        return token.value

    def PATH(self, token):
        return token.value

def parse_script(text: str):
    tree = parser.parse(text)
    transformer = ToPlan()
    plan = transformer.transform(tree)

    # Always return a list of blocks
    if hasattr(plan, "children"):
        return plan.children
    elif isinstance(plan, list):
        return plan
    else:
        return [plan]

def parse_set_statement(statement: str):
    """
    Parse SET statements like:
    SET path="file.xlsx" (sheet=Sheet1);
    """
    match = re.match(r'SET\s+path="([^"]+)"(?:\s*\(sheet=([^)]+)\))?', statement.strip())
    if match:
        path = match.group(1)
        sheet = match.group(2) if match.group(2) else None
        return {"path": path, "sheet": sheet}
    return None
