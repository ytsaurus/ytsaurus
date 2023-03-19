# Authentication and Passwords

To authenticate users in {{product-name}}, a password can be set for the user, that can be used by the user to gain access to the system. The user password can be changed by the system administrator or by the user themself. To change the password, one can use password update page or the `set-user-password` command in the {{product-name}} CLI.

After the user is created, no password is set for them, so the first password is set by the administrator. For example,

```bash
yt create user --attr '{name=oleg}'
yt set-user-password oleg --new-password cone
```

administrator created the user `oleg` and sets the password `cone` for them.

User can change password to `cube` using the command `set-user-password`.

```bash
yt set-user-password oleg --current-password cone --new-password cube
```

Note that unlike the administrator, the user is required to enter their current password in order to set the new password. The administrator does not need to enter a password either when setting the password for the first time or when changing it later.

# Tokens Management

For using {{product-name}} though CLI or API user should use tokens. For tokens management in CLI command `issue-token`, `revoke-token` and `list-user-tokens` are used.

Command `issue-token` is used to issue the new token for the user. Unlike a password, user can have multiple active tokens for a smooth process of replacing one with another.

```bash
yt issue-token oleg --password cone
"2c5956daecdff8dd45d2561a8679acf5"
```

Token `2c5956daecdff8dd45d2561a8679acf5` was issues for the user `oleg`. As with the `set-user-password` command, the should should specify a password, but the administrator is not required to do so.

List of the active user tokens can be obtained via `list-user-tokens` command.

```bash
yt list-user-tokens oleg --password cone
["87a5d9406ccf6a42cca510d86e43b20e2943aa7ade7e9129f4f4f947e1b02574"]
```

Note, that {{product-name}} does not store plaintext tokens, so command `list-user-tokens` returns not tokens but sha256-encoded tokens. For example,

```bash
echo -n '2c5956daecdff8dd45d2561a8679acf5' | sha256sum
87a5d9406ccf6a42cca510d86e43b20e2943aa7ade7e9129f4f4f947e1b02574  -
```

To revoke user's token command `revoke-token` can be used. This command accepts either token itself or its sha256 hash, so all tokens of a user can be revoked using result of `list-user-tokens`.

```bash
yt revoke-token oleg --token-sha256 87a5d9406ccf6a42cca510d86e43b20e2943aa7ade7e9129f4f4f947e1b02574  --password cube
yt revoke-token oleg --token 2c5956daecdff8dd45d2561a8679acf5  --password cube
yt list-user-tokens oleg --password cube
[]
```

{% notice warning "Warning" %}

Although token management requires the user's password, changing a user's password does not revoke their tokens, allowing tokens to be changed smoothly when the password is changed. In case the user's password is compromised, it is worth not only changing the password, but also revoking all tokens.

{% endnote %}
