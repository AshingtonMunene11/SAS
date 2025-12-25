from lark import Lark, Transformer, v_args
from pathlib import Path

# Load grammar
grammar_text = Path(__file__).with_name("grammar.lark").read_text()
parser = Lark(grammar_text, start="start")

@v_args(inline=True)
class ToPlan(Transformer):
    def data_step(self, name, set_stmt, *clauses):
        block = {"type": "data_step", "name": str(name), "set": set_stmt}
        for clause in clauses:
            if clause:  # only add if not None
                block.update(clause)
        return block

    def set_stmt(self, path):
        return {"path": str(path)}

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

    def NAME(self, token):
        return token.value

    def PATH(self, token):
        return token.value

def parse_script(text: str):
    tree = parser.parse(text)
    transformer = ToPlan()
    plan = transformer.transform(tree)

    if hasattr(plan, "children"):
        return plan.children
    elif isinstance(plan, list):
        return plan
    else:
        return [plan]
