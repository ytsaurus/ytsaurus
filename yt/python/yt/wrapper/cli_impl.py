from .common import YtError
from .auth_commands import encode_sha256, get_current_user

import yt.wrapper as yt
import yt.logger as logger

from getpass import getpass


def _set_attribute(path, name, value, recursive, client=None):
    """ Sets attribute at given path
    """
    if recursive:
        batch_client = yt.create_batch_client(client=client)

        processed_nodes = []
        for obj in yt.search(path, attributes=[name], enable_batch_mode=True, client=client):
            if name not in obj.attributes or obj.attributes[name] != value:
                set_result = batch_client.set("{}&/@{}".format(str(obj), name), value)
                processed_nodes.append((obj, set_result))

        batch_client.commit_batch()

        for obj, set_result in processed_nodes:
            if set_result.get_error() is not None:
                error = yt.YtResponseError(set_result.get_error())
                if not error.is_resolve_error():
                    logger.warning("Cannot set attribute on path '%s' (error: %s)", str(obj), repr(error))
            else:
                logger.info("Attribute '%s' is set to '%s' on path '%s'", name, value, str(obj))
    else:
        yt.set_attribute(path, name, value)


def _remove_attribute(path, name, recursive, client=None):
    """ Removes attribute at given path
    """
    if recursive:
        batch_client = yt.create_batch_client(client=client)

        processed_nodes = []
        for obj in yt.search(path, attributes=[name], enable_batch_mode=True, client=client):
            if name in obj.attributes:
                remove_result = batch_client.remove("{}&/@{}".format(str(obj), name))
                processed_nodes.append((obj, remove_result))

        batch_client.commit_batch()

        for obj, remove_result in processed_nodes:
            if remove_result.get_error() is not None:
                error = yt.YtResponseError(remove_result.get_error())
                if not error.is_resolve_error():
                    logger.warning("Cannot remove attribute on path '%s' (error: %s)", str(obj), repr(error))
            else:
                logger.info("Attribute '%s' is removed on path '%s'", name, str(obj))
    else:
        yt.remove_attribute(path, name)


def _validate_authentication_command_permissions(user, client=None):
    """Checks if the authenticated user (represented by client) has permission to run authentication commands on user,
    and whether typing a password is required.

    :returns: "passwordless" if no password is required, "password" if password is required, raises YtError if the
    authenticated user is not allowed to run authentication commands on user.
    """
    # Follows TClient::ValidateAuthenticationCommandPermissions from native client.
    current_user = get_current_user(client=client)
    current_user_login = current_user["user"]
    if yt.check_permission(current_user_login, "administer", "//sys/users/" + user)["action"] == "allow":
        logger.debug("Allowing user %s to run passwordless authentication command on %s by present "
                     "administer permission", current_user_login, user)
        return "passwordless"
    if current_user_login == user:
        logger.debug("Password authentication required for user %s to run authentication command on themselves",
                     current_user_login)
        return "password"
    raise YtError('Action can be performed either by user "{}" themselves, or by a user having "administer" permission '
                  'on user \"{}\"'.format(user, user))


def _maybe_request_password(user, client=None):
    """Requests password if needed."""
    if _validate_authentication_command_permissions(user, client=client) == "passwordless":
        return None
    return getpass("Current password for {}: ".format(user))


def _set_user_password_interactive(user, client=None):
    """Updates user password, reading it and optionally a current password
    interactively and securely from console."""

    current_password = _maybe_request_password(user, client=client)
    new_password = getpass("New password: ")
    new_password_retyped = getpass("Retype new password: ")
    if new_password != new_password_retyped:
        raise YtError("Passwords do not match")
    return yt.set_user_password(user, new_password, current_password=current_password,
                                client=client)


def _issue_token_interactive(user, client=None):
    """Issues a new token for user, reading password interactively and securely from console."""
    password = _maybe_request_password(user, client=client)
    return yt.issue_token(user, password=password, client=client)


def _revoke_token_interactive(user, token_sha256=None, client=None):
    """Revokes user token, reading password and maybe token value interactively and securely from console."""
    password = _maybe_request_password(user, client=client)
    if not token_sha256:
        token = getpass("Token: ")
        token_sha256 = encode_sha256(token)
    return yt.revoke_token(user, password=password, token_sha256=token_sha256, client=client)


def _list_user_tokens_interactive(user, client=None):
    """Lists sha256-encoded user tokens, reading password interactively and securely from console."""
    password = _maybe_request_password(user, client=client)
    return yt.list_user_tokens(user, password=password, client=client)


def _whoami(client=None) -> str:
    """Invokes whoami command at YT API and returns login."""
    current_user = get_current_user(client=client)
    return current_user.get("user")
