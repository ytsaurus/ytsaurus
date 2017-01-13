from yt.packages.six.moves import xrange, filter as ifilter

import yt.wrapper as yt

import re

def _is_placeholder(token):
    return token.startswith("{") and token.endswith("}")

def _tokenize(pattern):
    parts = re.split(r"(\{\w*\}|\{\*\})", pattern)
    return tuple(ifilter(lambda p: p != "", parts))

def _match(path, source_tokens, destination_tokens, result):
    destination_path_parts = list(destination_tokens)
    current_index = 0
    for i in xrange(len(source_tokens)):
        token = source_tokens[i]
        if _is_placeholder(token):
            if i + 1 < len(source_tokens):
                index = path.find(source_tokens[i + 1], current_index)
                if index == -1:
                    return
                value = path[current_index:index]
            else:
                value = path[current_index:]
            if "/" in value:
                if token != "{*}":
                    return
            destination_path_parts[destination_path_parts.index(token)] = value
            current_index += len(value)
        else:
            if path[current_index:current_index + len(token)] != token:
                return
            current_index += len(token)
    if current_index != len(path):
        return
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

def _is_directory(client, path):
    if client._type == "yt":
        return client.get_type(path) == "map_node"
    else:
        raise yt.YtError("Failed to check if {} is a directory, unsupported client: {}".format(path, client._type))

def match_copy_pattern(client, source_pattern, destination_pattern, include_files=False):
    source_tokens = _tokenize(source_pattern)
    for i in xrange(len(source_tokens) - 1):
        if _is_placeholder(source_tokens[i]) and _is_placeholder(source_tokens[i + 1]):
            raise yt.YtError("Source pattern cannot contain consequtive placeholders")

    destination_tokens = _tokenize(destination_pattern)
    if list(ifilter(_is_placeholder, source_tokens)) != list(ifilter(_is_placeholder, destination_tokens)):
        raise yt.YtError("Source pattern {0} do not match destination pattern {1}".format(source_pattern, destination_pattern))

    star_placeholder_count = source_tokens.count("{*}")
    if star_placeholder_count > 1 or (star_placeholder_count == 1 and source_tokens[-1] != "{*}"):
        raise yt.YtError("Special placeholder {*} can be used only once and at the end of a pattern")

    if len(source_tokens) == 1 and _is_directory(client, source_pattern):
        source_tokens = (source_tokens[0].rstrip("/") + "/", "{*}")
        destination_tokens = (destination_tokens[0].rstrip("/") + "/", "{*}")

    prefix = _get_prefix(source_tokens, client._type)
    result = []
    if client._type == "yt":
        if not client.exists(prefix):
            raise yt.YtError("Prefix of source pattern does not exist")
        node_type = ["table"]
        if include_files:
            node_type.append("file")
        for node in client.search(prefix, node_type=node_type):
            _match(node, source_tokens, destination_tokens, result)
    else:
        raise yt.YtError("Listing tables for client {} is not supported".format(client._type))

    return result

