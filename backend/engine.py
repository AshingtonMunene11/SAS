from backend.executor.data_step import Env, load_dataset

def execute(plan):
    """
    Execute a parsed plan (list of blocks).
    Currently supports DATA step with SET.
    """
    env = Env()
    last_df = None

    for block in plan:
        if block["type"] == "data_step":
            ds_name = block["name"]
            path = block["set"]["path"]
            df = load_dataset(path)
            env.save(ds_name, df)
            last_df = df
        else:
            raise ValueError(f"Unsupported block type: {block['type']}")

    return last_df, env
