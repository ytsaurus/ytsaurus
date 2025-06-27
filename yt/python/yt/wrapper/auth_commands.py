from .config import get_config, get_option, set_option
from .driver import make_request, make_formatted_request
from .errors import YtError
from .http_helpers import get_user_name

from hashlib import sha256
from typing import TypedDict, Optional


def validate_password_strength(password):
    """
    Validate that password conforms to complexity requirements. In future
    should be expanded with more requirements.
    """
    return len(password) >= 12 and len(password) <= 128


def encode_sha256(password):
    return sha256(password.encode("utf-8")).hexdigest()


def set_user_password(user, new_password, current_password=None,
                      client=None):
    """Updates user password."""
    if get_config(client)["enable_password_strength_validation"] is True and \
            not validate_password_strength(new_password):
        raise ValueError("The password length must be between 12 and 128 characters")
    params = {"user": user, "new_password_sha256": encode_sha256(new_password)}
    if current_password is not None:
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
    return make_formatted_request("issue_token", params, format=None, client=client)


def revoke_token(user, password=None, token=None, token_sha256=None,
                 client=None):
    """Revokes user token."""
    params = {"user": user}
    if not token_sha256 and not token:
        raise ValueError("Either token or token_sha256 must be provided")
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


class DictCurrentUser(TypedDict):
    user: str


def get_current_user(client=None) -> Optional[DictCurrentUser]:
    """Get current user info"""
    # TODO: simplify after complet release `get_current_user`
    current_user = get_option("_current_user", client)
    if current_user:
        return current_user

    try:
        # try new method
        current_user = make_formatted_request(
            "get_current_user",
            params={},
            format=None,
            client=client)
    except YtError as ex:
        if ex.code == 1 and "is not supported" in ex.message:
            # fallback
            try:
                current_user = {"user": get_user_name(client=client)}
            except YtError:
                return None

    if current_user:
        set_option("_current_user", current_user, client)
        return current_user
    else:
        return None
