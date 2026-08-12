# Security in {{product-name}} Flow {#security}

## Which user the Vanilla operation runs as {#identity}

Controllers and workers must access {{product-name}} and other services under a dedicated service account, not under the person who started the Vanilla operation runner. Maintain this account the same way you maintain the pipeline itself: rotate its secrets regularly, grant only the roles it needs, and use different accounts for production and pre-production.

{% if audience == "internal" %}

In the internal infrastructure, this account is a robot or a TVM application; the controllers and workers access {{product-name}}, Logbroker, and Monitoring under it.

{% endif %}

## Secrets {#secrets}

Authentication secrets, including `YT_TOKEN`, must **never** end up in source code, in configs in the repository, or in images.

To pass secrets to the jobs (for example, {% if audience == "internal" %}`TVM_SECRET` for reaching Logbroker, or{% endif %} your own token), list the names of the corresponding environment variables in `secret_env`:

```yson
"vanilla" = {
    "enable" = %true;
    "pool" = "<your-pool>";
    "worker" = {"count" = 5};
    "secret_env" = ["MY_TOKEN"];
};
```

The values are taken from the launch environment (of whoever runs the command), passed in the `secure_vault` field of the spec the operation starts with, and are available inside the job as regular environment variables. `secure_vault` doesn’t make it into the stored operation spec: {{product-name}} strips it before writing, so the secret values aren’t visible afterwards either in Cypress or through the operation attributes. You don’t need to specify `YT_TOKEN` — it’s delivered automatically.

`node_config` goes into the Vanilla operation spec as a file and is uploaded to the file cache, so it isn’t a secret channel: don’t pass `YT_TOKEN` or other secrets through it.{% if audience == "internal" %} Pass `TVM_SECRET` through `secret_env`: the runner reads the value from its own environment and puts it into the operation’s `secure_vault`.{% endif %}

## Authentication {#auth}

The Vanilla operation runs under the user who executed the command — that same user must have access to {{product-name}}. `YT_TOKEN` is not passed to the controllers and workers directly: the launcher delivers it through the operation’s `secure_vault` (see [Secrets](#secrets)), so you don’t need to configure authentication for them separately, as you would for a long-running deployment.

An [access token](../../../user-guide/storage/auth.md) for {{product-name}} is required to launch: the launcher takes it from the `YT_TOKEN` variable, then from `YT_SECURE_VAULT_YT_TOKEN` ({{product-name}} sets it through `secure_vault` when the launcher runs inside a job), then from the file that `YT_TOKEN_PATH` points to, and finally from `~/.yt/token`. If the token isn’t in any of these places, the launch fails with an error.{% if audience == "internal" %} TVM is not used at launch: the launcher specifically needs an OAuth token.{% endif %}

For regular launches, run the command under a dedicated service account rather than under a person; the requirements for the account are listed in [Which user the Vanilla operation runs as](#identity).

Deployment modes that run controllers and workers as long-running processes outside a Vanilla operation configure authentication for them explicitly — see [Authentication](../../../flow/release/auth.md).

## Minimum required permissions {#permissions}

The pipeline account needs only the permissions without which the pipeline can’t work:

- a role on the {{product-name}} cluster with permissions for the pipeline directory and the paths of all tables it accesses;
- the permission to start operations in the required pool{% if audience == "internal" %};{% else %}.{% endif %}

{% if audience == "internal" %}

- read and write access to the relevant Logbroker topics, if the Logbroker connector is used;
- access to the Monitoring project the pipeline writes its metrics to.

{% endif %}

Don’t grant broad permissions, such as `root` on the cluster: if they are compromised, they increase the attack surface.

## See also

- [Initial deployment](../../../flow/release/launch-vanilla.md)
- [Updates and releases](../../../flow/release/releases.md)
