from flask import request


def parse_multi_value_param(param_name):
    values = []
    for raw in request.args.getlist(param_name):
        if raw is None:
            continue
        cleaned = str(raw).strip()
        if cleaned:
            values.append(cleaned)
    return values


def parse_int_list_param(param_name, strict=False):
    values = []
    for raw in request.args.getlist(param_name):
        if raw is None:
            continue
        for piece in str(raw).split(","):
            cleaned = piece.strip()
            if not cleaned:
                continue
            try:
                values.append(int(cleaned))
            except Exception:
                if strict:
                    raise ValueError(f"Invalid integer value for {param_name}: {cleaned}")
    return values
