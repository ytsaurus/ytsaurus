import re

FORBIDDEN_PATH_PREFIXES = frozenset({"//sys"})

FORBIDDEN_KEYWORDS = frozenset({"truncate"})

WRITE_KEYWORDS = frozenset({
    "insert",
    "replace",
    "upsert",
    "create",
    "drop",
    "delete",
    "update",
})


def check_system_paths(text):
    for prefix in FORBIDDEN_PATH_PREFIXES:
        if re.search(re.escape(prefix) + r"\b", text, re.IGNORECASE):
            raise ValueError("Access to system path '{}' is forbidden.".format(prefix))


def check_truncate(query):
    for keyword in FORBIDDEN_KEYWORDS:
        if re.search(r"\b" + re.escape(keyword) + r"\b", query, re.IGNORECASE):
            raise ValueError(
                "Query contains forbidden modifier '{}'. "
                "TRUNCATE is not allowed in any mode as it irreversibly destroys data.".format(keyword)
            )


def check_write_keywords(query, rw_mode):
    if rw_mode:
        return
    for keyword in WRITE_KEYWORDS:
        if re.search(r"\b" + re.escape(keyword) + r"\b", query, re.IGNORECASE):
            raise ValueError(
                "Query contains write operation '{}'. "
                "Write queries are only available with --rw-mode flag. "
                "WARNING: enabling --rw-mode allows data mutations.".format(keyword)
            )
