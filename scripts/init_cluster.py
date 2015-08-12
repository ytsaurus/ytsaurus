import yt.logger as logger
import yt.wrapper as yt

def create(type, name):
    try:
        yt.create(type, attributes={"name": name})
    except yt.YtResponseError as err:
        if yt.YtResponseError._contains_code(err, 501):
            logger.warning("'%s' already exists", name)
        else:
            raise

def add_member(subject, group):
    try:
        yt.add_member(subject, group)
    except yt.YtResponseError as err:
        if "is already present in group" in err.message:
            logger.warning(err.message)
        else:
            raise

def add_acl(path, new_acl):
    current_acls = set(yt.get(path + "/@acl"))
    if new_acl not in current_acls:
        yt.set(path + "/@acl/end", new_acl)
    

if __name__ == "__main__":
    for user in ["odin", "cron", "nightly_tester"]:
        create("user", user)
    for group in ["devs", "admins"]:
        create("group", group)
    add_member("cron", "superusers")
    add_member("devs", "admins")

    for dir in ["//sys", "//tmp", "//sys/tokens"]:
        yt.set(dir + "/@opaque", "true")

    add_acl("/", {"action": "allow", "subjects": ["admins", "cron"], "permissions": ["write", "remove"]})
    add_acl("/", {"action": "allow", "subjects": ["admins"], "permissions": ["administer"]})
    add_acl("//sys", {"action": "allow", "subjects": ["everyone"], "permissions": ["read"]})
    add_acl("//sys", {"action": "allow", "subjects": ["admins"], "permissions": ["write", "remove", "administer"]})
    yt.set("//sys/@inherit_acl", "false")

    add_acl("//sys/accounts/sys", {"action": "allow", "subjects": ["root", "admins", "cron"], "permissions": ["use"]})

    add_acl("//sys/tokens", {"action": "allow", "subjects": ["admins"], "permissions": ["read", "write", "remove"]})
    yt.set("//sys/tokens/@inherit_acl", "false")

    if not yt.exists("//home"):
        yt.create("map_node", "//home",
                  attributes={
                      "opaque": "true",
                      "account": "tmp"})

    for schema in ["user", "group"]:
        yt.set("//sys/schemas/%s/@acl" % schema,
            [
                {"action": "allow", "subjects": ["everyone"], "permissions": ["read"]},
                {"action": "allow", "subjects": ["admins"], "permissions": ["write", "remove", "create"]}
            ])
    
    yt.set("//sys/schemas/account/@acl",
        [
            {"action": "allow", "subjects": ["everyone"], "permissions": ["read"]},
            {"action": "allow", "subjects": ["admins"], "permissions": ["write", "remove", "create", "administer", "use"]}
        ])

    for schema in ["chunk", "erasure_chunk", "chunk_list"]:
        yt.set("//sys/schemas/account/@acl",
            [
                {"action": "allow", "subjects": ["users"], "permissions": ["read", "write", "remove", "create"]},
            ])
    
    yt.set("//sys/schemas/lock/@acl",
        [
            {"action": "allow", "subjects": ["everyone"], "permissions": ["read"]},
        ])
    
    yt.set("//sys/schemas/transaction/@acl",
        [
            {"action": "allow", "subjects": ["everyone"], "permissions": ["read"]},
            {"action": "allow", "subjects": ["users"], "permissions": ["write", "create"]}
        ])
