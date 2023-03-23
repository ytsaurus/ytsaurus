# Authentication and Passwords

Every user in {{product-name}} has a password. The password can be changed by the system administrator or by the user themselves. To change the password, one can use password update page or the `set-user-password` command in the {{product-name}} CLI.

Initially, when the user is just created, no password is set, and it should be set separately by the administrator. For example,

```bash
yt create user --attr '{name=oleg}'
yt set-user-password oleg --new-password cone
```

administrator created the user `oleg` and sets the password `cone` for them.

Then, the users can update his password using the `set-user-password` command:

```bash
yt set-user-password oleg --current-password cone --new-password cube
```

Note that the user is required to enter the current password in order to change it. The administrator does not need to enter users' password when setting it for the first time or updating it later.

# Auth tokens

To use {{product-name}} though CLI or API, the user should provide an auth token. CLI commands `issue-token`, `revoke-token` and `list-user-tokens` can be used to manage tokens.

Command `issue-token` is used to issue a new token for the user. Unlike a password, user can have multiple active tokens.

```bash
yt issue-token oleg --password cone
"2c5956daecdff8dd45d2561a8679acf5"
```

Token `2c5956daecdff8dd45d2561a8679acf5` was issued for the user `oleg`. As with the `set-user-password` command, the should should specify a password when issuing a token, but the administrator is not required to do so.

List of active tokens for a user can be obtained via the `list-user-tokens` command.

```bash
yt list-user-tokens oleg --password cone
["87a5d9406ccf6a42cca510d86e43b20e2943aa7ade7e9129f4f4f947e1b02574"]
```

Note, that {{product-name}} does not store tokens in plain text. Instead of the actual token, the command `list-user-tokens` prints a SHA256 of each token.

```bash
echo -n '2c5956daecdff8dd45d2561a8679acf5' | sha256sum
87a5d9406ccf6a42cca510d86e43b20e2943aa7ade7e9129f4f4f947e1b02574  -
```

To revoke user's token command `revoke-token` can be used. This command accepts either token itself or its sha256 hash. Any tokens of a user can be revoked by taking a result of `list-user-tokens` and applying the `revoke-token` command.

```bash
yt revoke-token oleg --token-sha256 87a5d9406ccf6a42cca510d86e43b20e2943aa7ade7e9129f4f4f947e1b02574  --password cube
yt revoke-token oleg --token 2c5956daecdff8dd45d2561a8679acf5  --password cube
yt list-user-tokens oleg --password cube
[]
```

{% notice warning "Warning" %}

Although token management requires the user's password, changing a user's password does not revoke their tokens. Therefore, tokens are managed independently from the password. In case the user's password is compromised, it is worth not only changing the password, but also revoking all tokens.

{% endnote %}
