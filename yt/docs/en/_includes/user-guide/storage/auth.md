# Authentication and passwords

To authenticate users, {{product-name}} allows setting user passwords for accessing the system. A password can be changed either by the system administrator or by the user in question. To manage a user's password, go to the password reset page or run the `set-user-password` command in the {{product-name}} CLI.

New users don't have a password, so their first password must be set by the administrator. Let's consider the following example:

```bash
$ yt create user --attr '{name=alex}'
$ yt set-user-password alex
New password: <interactive typing>
Retype new password: <interactive typing>
```

The administrator creates a new user named `alex` and sets their password to `cone`. The password is prompted interactively and entered securely (without echo to the terminal), and must be retyped for confirmation.

The user can then run the `set-user-password` command to change their password to `cube`.

```bash
$ yt set-user-password alex
Current password for alex: <interactive typing>
New password: <interactive typing>
Retype new password: <interactive typing>
```

Note that unlike the administrator, the user is required to enter their current password in order to change it. The administrator doesn't need to enter the current password, neither when setting the user password for the first time nor when changing it later. All passwords are prompted interactively for security.

## Token management

Users need tokens to interact with {{product-name}} via the CLI or the API. To facilitate token management, the CLI supports the `issue-token`, `revoke-token`, and `list-user-tokens` commands.

The `issue-token` command issues a new token to the user. Unlike with passwords, a single user can have multiple active tokens. This allows for seamless replacement of one token with another.

```bash
$ yt issue-token alex
Current password for alex: <interactive typing>
ytct-2c59-56daecdff8dd45d2561a8679acf5
```

User `alex` was issued token `ytct-2c59-56daecdff8dd45d2561a8679acf5`. Similar to the `set-user-password` command, the user must enter their password (prompted interactively), while the administrator isn't required to do so.

Use the `list-user-tokens` command to see the information about user's active tokens. Note that {{product-name}} doesn't store user tokens. In particular, the `list-user-tokens` command returns the SHA-256 hashes of tokens rather than the tokens themselves. For instance,

```bash
$ yt list-user-tokens alex
Current password for alex: <interactive typing>
["87a5d9406ccf6a42cca510d86e43b20e2943aa7ade7e9129f4f4f947e1b02574"]

$ echo -n 'ytct-2c59-56daecdff8dd45d2561a8679acf5' | sha256sum
87a5d9406ccf6a42cca510d86e43b20e2943aa7ade7e9129f4f4f947e1b02574  -
```

With the `revoke-token` command, you can revoke the user's token. To revoke a token, you can specify its SHA-256 hash using the `--token-sha256` flag, or if the flag is omitted, the token value will be prompted interactively. The password is always prompted interactively. Using `--token-sha256` allows using the output of the `list-user-tokens` command to revoke specific tokens.

```bash
$ yt revoke-token alex --token-sha256 87a5d9406ccf6a42cca510d86e43b20e2943aa7ade7e9129f4f4f947e1b02574
Current password for alex: <interactive typing>
$ yt revoke-token alex
Current password for alex: <interactive typing>
Token to revoke: <interactive typing>
$ yt list-user-tokens alex
Current password for alex: <interactive typing>
[]
```

{% note warning "Note" %}

Although the user password is required to manage a user's tokens, changing it doesn't automatically revoke the tokens. This means you can replace tokens gradually if the password is changed. If a user's password is compromised, it's highly recommended that you revoke all of their tokens once the new password has been set.

{% endnote %}
