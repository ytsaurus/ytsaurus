try:
    from yt.packages.argcomplete import autocomplete  # noqa
except ImportError:
    from argcomplete import autocomplete  # noqa

import yt.wrapper as yt

if not yt.config["argcomplete_verbose"]:
    warn = lambda *args, **kwargs: None  # noqa


def complete_map_node(path):
    path = str(path).rsplit("/", 1)[0]
    content = yt.list(path, max_size=10**5, attributes=["type"])
    # Add attribute suggestion.
    suggestions = [path + "/@"]
    for item in content:
        full_path = path + "/" + str(item)
        type = item.attributes.get("type", "")
        if type == "map_node" or type.endswith("_map"):
            suggestions += [full_path + "/"]
        else:
            suggestions += [full_path]
    return suggestions


def complete_attributes(path):
    path, attribute = str(path).rsplit("@", 1)
    add_slash_after_attribute_path = False
    if "/" in attribute:
        attribute_path, _ = attribute.rsplit("/", 1)
        add_slash_after_attribute_path = True
    else:
        attribute_path = ""

    # Find all attributes that are siblings of a current attribute.
    attribute_list = yt.list(path + "@" + attribute_path)
    suggestions = []
    for sub_attribute in attribute_list:
        suggestions.append(
            path + "@" + attribute_path + ("/" if add_slash_after_attribute_path else "") + sub_attribute)

    # Try to find all children attributes of a current attribute
    # by treating it as a map attribute.
    children_attribute_list = []
    try:
        children_attribute_list = yt.list(path + "@" + attribute)
    except yt.YtError as err:
        if not err.is_resolve_error():
            raise
    for child_attribute in children_attribute_list:
        suggestions.append(path + "@" + attribute + ("/" if attribute != "" else "") + child_attribute)

    return suggestions


def complete_ypath(prefix, parsed_args, **kwargs):
    if parsed_args.proxy:
        yt.config["proxy"]["url"] = parsed_args.proxy
    if prefix in ["", "/"]:
        return ["//"]
    try:
        path = yt.TablePath(prefix)
        if "/@" in str(path):
            return complete_attributes(path)
        else:
            return complete_map_node(path)
    except Exception as err:
        warn("Caught following exception during completion:\n" + str(err))
