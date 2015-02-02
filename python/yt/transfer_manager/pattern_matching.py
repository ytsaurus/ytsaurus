import yt.wrapper as yt

def _is_placeholder(token):
    return token.startswith("{") and token.endswith("}")

def _tokenize(pattern):
    result = []
    current_index = 0
    while current_index < len(pattern):
        new_index = None
        start_index = pattern.find("{", current_index)
        found = False
        if start_index == -1:
            new_index = len(pattern)
        else:
            end_index = pattern.find("}", start_index)
            if end_index == -1:
                new_index = len(pattern)
            else:
                new_index = end_index + 1
                found = True
        if found:
            if current_index < start_index:
                result.append(pattern[current_index:start_index])
            result.append(pattern[start_index:new_index])
        else:
            result.append(pattern[current_index:new_index])

        current_index = new_index

    return tuple(result)

def _match(path, source_tokens, destination_tokens, result):
    destination_path_parts = list(destination_tokens)
    current_index = 0
    for i in xrange(len(source_tokens)):
        token = source_tokens[i]
        if _is_placeholder(token):
            if i + 1 < len(source_tokens):
                index = path.find(source_tokens[i + 1])
                if index == -1:
                    return
                value = path[current_index:index]
            else:
                value = path[current_index:]
            if "/" in value:
                return
            destination_path_parts[destination_path_parts.index(token)] = value
            current_index += len(value)
        else:
            if path[current_index:current_index + len(token)] != token:
                return
            current_index += len(token)
    result.append((path, "".join(destination_path_parts)))

def _get_prefix(tokens, cluster_type):
    token = tokens[0]
    if _is_placeholder(token):
        raise yt.YtError("Pattern can not start with placeholder")
    if cluster_type == "yt":
        last_slash = token.rfind("/")
        if last_slash != -1:
            return token[:last_slash]
        else:
            return token
    return token


def match_copy_pattern(client, source_pattern, destination_pattern):
    source_tokens = _tokenize(source_pattern)
    for i in xrange(len(source_tokens) - 1):
        if _is_placeholder(source_tokens[i]) and _is_placeholder(source_tokens[i + 1]):
            raise yt.YtError("Source pattern cannot contain consequtive placeholders")

    destination_tokens = _tokenize(destination_pattern)
    if filter(_is_placeholder, source_tokens) != filter(_is_placeholder, destination_tokens):
        raise yt.YtError("Source pattern {0} do not match destination pattern {1}".format(source_pattern, destination_pattern))

    prefix = _get_prefix(source_tokens, client._type)
    result = []
    if client._type == "yt":
        for table in client.search(prefix, node_type="table"):
            _match(table, source_tokens, destination_tokens, result)
    elif client._type == "yamr":
        for table_info in client.list(prefix):
            _match(table_info["name"], source_tokens, destination_tokens, result)
    else:
        raise yt.YtError("Incorrect client type " + client._type)

    return result

