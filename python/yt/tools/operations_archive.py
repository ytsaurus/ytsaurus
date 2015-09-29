def get_filter_factors(op, attr):
    brief_spec = attr.get("brief_spec", {})
    return " ".join([
        op,
        attr.get("key", ""),
        attr.get("authenticated_user", ""),
        attr.get("state", ""),
        attr.get("operation_type", ""),
        attr.get("pool", ""),
        brief_spec.get("title", ""),
        str(brief_spec.get('input_table_paths', [''])[0]),
        str(brief_spec.get('input_table_paths', [''])[0])
    ]).lower()