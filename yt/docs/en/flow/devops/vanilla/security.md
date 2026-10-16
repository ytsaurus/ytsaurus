# Security in {{product-name}} Flow

Review the run identity, secret handling, authentication, and minimum permissions before deploying a pipeline.

<span id="identity"></span>

## Which user the Vanilla operation runs as {#security}

Run the Vanilla operation launcher under a dedicated service account: the operation and its controller and worker jobs inherit the launcher's identity. Maintain this account the same way you maintain the pipeline itself: rotate its secrets regularly, grant only the roles it needs, and use different accounts for production and pre-production.

{% if audience == "internal" %}

Internally, use a robot or TVM application for access to {{product-name}} and any required Logbroker or Monitoring resources.

{% endif %}

## Secrets {#secrets}

Authentication secrets, including `YT_TOKEN`, must **never** end up in source code, in configs in the repository, or in images.

To pass secrets to the jobs (for example, your own token), list the names of the corresponding environment variables in `secret_env`:

```yson
"vanilla" = {
    "enable" = %true;
    "pool" = "<your-pool>";
    "worker" = {"count" = 5};
    "secret_env" = ["MY_TOKEN"];
};
```

The values are taken from the launch environment (of whoever runs the command), passed in the `secure_vault` field of the spec the operation starts with, and are available inside the job as regular environment variables. `secure_vault` doesn’t make it into the stored operation spec: {{product-name}} strips it before writing, so the secret values aren’t visible afterwards either in Cypress or through the operation attributes. You don’t need to specify `YT_TOKEN` — it’s delivered automatically.

`node_config` goes into the Vanilla operation spec as a file and is uploaded to the file cache, so it isn’t a secret channel: don’t pass `YT_TOKEN` or other secrets through it.

{% if audience == "internal" %}

For Logbroker access, pass `TVM_SECRET` through `secret_env` so the launcher reads it from its environment and delivers it through `secure_vault`. The Vanilla launcher itself still needs an OAuth token for {{product-name}}; TVM does not replace that launch credential.

{% endif %}

## Authentication {#auth}

The launcher must have access to {{product-name}} under the dedicated service account described above. `YT_TOKEN` is not passed to the controllers and workers directly: the launcher delivers it through the operation’s `secure_vault` (see [Secrets](#secrets)), so you don’t need to configure authentication for them separately, as you would for a long-running deployment.

An [access token](../../../user-guide/storage/auth.md) for {{product-name}} is required to launch: the launcher takes it from the `YT_TOKEN` variable, then from `YT_SECURE_VAULT_YT_TOKEN` ({{product-name}} sets it through `secure_vault` when the launcher runs inside a job), then from the file that `YT_TOKEN_PATH` points to, and finally from `~/.yt/token`. If the token isn’t in any of these places, the launch fails with an error.

For regular launches, run the command under a dedicated service account rather than under a person; the requirements for the account are listed in [Which user the Vanilla operation runs as](#identity).

Deployment modes that run controllers and workers as long-running processes outside a Vanilla operation require explicit authentication configuration; this page covers Vanilla operations.

{% if audience == "internal" %}

### TVM for separately deployed processes {#authentication-tvm}

Create a [TVM application](https://docs.yandex-team.ru/tvm/pages/getting_started) for the service and grant it access to the required {{product-name}}, Logbroker, and Monitoring resources. Start each separately deployed controller and worker with its `TVM_ID` and `TVM_SECRET` in the process environment. For a Vanilla job, put `TVM_SECRET` in `secret_env` instead; the Vanilla launcher still needs an OAuth token to start the operation.

{% endif %}

### Access token {#authentication}

Use the [cluster authentication guide](../../../user-guide/storage/auth.md) to obtain a token for the service account. The account needs a role on the cluster and access to the pipeline directory; a token alone does not grant permissions.

#### OAuth token {#authentication-oauth}

Set `YT_TOKEN` in the launcher environment, or use the documented `YT_TOKEN_PATH` or `~/.yt/token` sources. Keep the value out of the repository and images.

For separately deployed controllers and workers, outside this Vanilla procedure, create a dedicated service account, issue its token using the [authentication guide](../../../user-guide/storage/auth.md), and set `YT_USER` and `YT_TOKEN` in each process's environment. Grant that account access to the pipeline directory, input and output tables, and operation pool as needed. Rotate the token without storing it in the repository or image.

## Minimum required permissions {#permissions}

The pipeline account needs only the permissions without which the pipeline can’t work:

- a role on the {{product-name}} cluster with permissions for the pipeline directory and the paths of all tables it accesses;
- the permission to start operations in the required pool.

{% if audience == "internal" %}

- read and write access to the Logbroker topics the pipeline uses, when applicable;
- access to its Monitoring project.

{% endif %}

Don’t grant broad permissions, such as `root` on the cluster: if they are compromised, they increase the attack surface.

## See also

- [Initial deployment](initial-deploy.md)
- [Updates and releases](releases.md)
