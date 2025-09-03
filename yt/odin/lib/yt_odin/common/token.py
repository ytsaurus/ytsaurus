import os


def get_token(token: str, token_env_variable: str):
    return token if not token_env_variable else os.environ.get(token_env_variable)
