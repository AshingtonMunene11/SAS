from lark import Lark, Transformer, v_args
from pathlib import Path

grammar_text = Path(__file__).with_name("grammar.lark").read_text()
parser = Lark(grammar_text, start="start")

@v_args(inline=True)
class ToPlan(Transformer):
    def data_step(self, name, set_stmt):
        return {"type": "data_step", "name": str(name), "set": set_stmt}

    def set_stmt(self, path):
        return {"path": str(path)}

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
