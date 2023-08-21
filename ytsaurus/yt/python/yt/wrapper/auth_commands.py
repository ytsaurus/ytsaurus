from .driver import make_request

from hashlib import sha256


def encode_sha256(password):
    return sha256(password.encode("utf-8")).hexdigest()


def set_user_password(user, new_password, current_password=None,
                      client=None):
    """Updates user password."""
    params = {"user": user, "new_password_sha256": encode_sha256(new_password)}
    if current_password:
        params["current_password_sha256"] = encode_sha256(current_password)
    return make_request(
        "set_user_password",
        params=params,
        client=client)


def issue_token(user, password=None,
                client=None):
    """Issues a new token for user."""
    params = {"user": user}
    if password:
        params["password_sha256"] = encode_sha256(password)
    return make_request(
        "issue_token",
        params=params,
        client=client)


def revoke_token(user, password=None, token=None, token_sha256=None,
                 client=None):
    """Revokes user token."""
    params = {"user": user}
    if not token_sha256:
        token_sha256 = encode_sha256(token)
    params["token_sha256"] = token_sha256
    if password:
        params["password_sha256"] = encode_sha256(password)
    return make_request(
        "revoke_token",
        params=params,
        client=client)


def list_user_tokens(user, password=None,
                     client=None):
    "Lists sha256-encoded user tokens."
    params = {"user": user}
    if password:
        params["password_sha256"] = encode_sha256(password)
    return make_request(
        "list_user_tokens",
        params=params,
        client=client)
