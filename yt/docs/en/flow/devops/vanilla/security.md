# Security in {{product-name}} Flow


Review the run identity, secret handling, authentication, and minimum permissions before deploying a pipeline.

<span id="security"></span>

## Which user the Vanilla operation runs as {#identity}

Run the Vanilla operation launcher under a dedicated service account: the operation and its controller and worker jobs inherit the launcher's identity. Maintain this account the same way you maintain the pipeline itself: rotate its secrets regularly, grant only the roles it needs, and use different accounts for production and pre-production.

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

## Authentication {#auth}

The launcher must have access to {{product-name}} under the dedicated service account described above. `YT_TOKEN` is not passed to the controllers and workers directly: the launcher delivers it through the operation’s `secure_vault` (see [Secrets](#secrets)), so you don’t need to configure authentication for them separately, as you would for a long-running deployment.

An [access token](../../../user-guide/storage/auth.md) for {{product-name}} is required to launch: the launcher takes it from the `YT_TOKEN` variable, then from `YT_SECURE_VAULT_YT_TOKEN` ({{product-name}} sets it through `secure_vault` when the launcher runs inside a job), then from the file that `YT_TOKEN_PATH` points to, and finally from `~/.yt/token`. If the token isn’t in any of these places, the launch fails with an error.

For regular launches, run the command under a dedicated service account rather than under a person; the requirements for the account are listed in [Which user the Vanilla operation runs as](#identity).

Deployment modes that run controllers and workers as long-running processes outside a Vanilla operation require explicit authentication configuration; this page covers Vanilla operations.

### Access token {#authentication}

Use the [cluster authentication guide](../../../user-guide/storage/auth.md) to obtain a token for the service account. The account needs a role on the cluster and access to the pipeline directory; a token alone does not grant permissions.

#### OAuth token {#authentication-oauth}

Set `YT_TOKEN` in the launcher environment, or use the documented `YT_TOKEN_PATH` or `~/.yt/token` sources. Keep the value out of the repository and images.

For separately deployed controllers and workers, outside this Vanilla procedure, create a dedicated service account, issue its token using the [authentication guide](../../../user-guide/storage/auth.md), and set `YT_USER` and `YT_TOKEN` in each process's environment. Grant that account access to the pipeline directory, input and output tables, and operation pool as needed. Rotate the token without storing it in the repository or image.

## Self-signed controller TLS certificate {#controller-self-signed-tls}

When neither `cert_chain` nor `private_key` is configured in `bus_server` and encryption is not disabled, Flow automatically generates a self-signed controller certificate. The certificate and its SHA-256 fingerprint are published as `certificate_pem` and `certificate_sha256` in the leader record of the `<pipeline_path>/flow_control` table. After a controller restart, the new incarnation gets a new certificate and key pair. The private key stays in process memory and is not published in the table or the Orchid configuration.

The default `bus_server.encryption_mode = "optional"` allows both TLS and unencrypted connections. Setting only `encryption_mode = "required"` makes the controller require TLS while still using an automatically generated certificate. This setting requires encryption but does not configure the client's trust in the certificate.

The direct Flow client verifies the controller against its published certificate. To require certificate verification through an RPC or HTTP proxy, set `encryption_mode = "required"` on at least the controller or the proxy and set `verification_mode = "ca"` or `"full"` on the proxy. Use a [custom certificate](#controller-tls) signed by a CA trusted by the proxy.

## Custom controller TLS certificate {#controller-tls}

To accept RPC connections with your own TLS certificate, configure both `cert_chain` and `private_key` in `bus_server`. A complete pair replaces the [automatically generated self-signed certificate](#controller-self-signed-tls). An incomplete pair causes startup to fail when encryption is enabled.

The launcher normally sends commands through a {{product-name}} RPC proxy, while the UI and `yt flow` with an HTTP backend use an HTTP proxy. Both proxies connect directly to the controller over RPC. If the proxy verifies the controller certificate against a CA, use a certificate signed by a CA trusted by the proxy. TLS on the client-to-proxy connection is configured separately from TLS on the proxy-to-controller connection.

Pass the PEM certificate chain and the matching private key through environment variables. Add the following fragment to your existing `vanilla` settings:

```yson
"secret_env" = ["FLOW_TLS_CERTIFICATE", "FLOW_TLS_PRIVATE_KEY"];
"node_config" = {
    "bus_server" = {
        "encryption_mode" = "required";
        "cert_chain" = {"environment_variable" = "FLOW_TLS_CERTIFICATE"};
        "private_key" = {"environment_variable" = "FLOW_TLS_PRIVATE_KEY"};
    };
};
```

Removing `cert_chain` and `private_key` from the example while keeping `encryption_mode = "required"` makes the controller require TLS with a [self-signed certificate](#controller-self-signed-tls).

The variables must contain the PEM itself, rather than file paths. See [Secrets](#secrets) for delivery through `secret_env`.

You can use `file_name` instead of `environment_variable`, for example `cert_chain = {file_name = "/run/secrets/controller-chain.pem"}` and `private_key = {file_name = "/run/secrets/controller-key.pem"}`. The files must be accessible inside the process that accepts TLS connections. If both fields are configured, a defined environment variable takes precedence; the file is read when the variable is absent. The same `bus_server` configuration works for a separately deployed controller process.

Keep the private key in a secret file or environment variable. Do not use `private_key.value`: the node configuration is exposed through Orchid, and `node_config` is also stored in the file cache. The `file_name` and `environment_variable` references keep only the path and variable name in the configuration.

For a custom certificate, the controller does not publish `certificate_pem` in pipeline metadata. Without a published certificate, the direct Flow client connects without verifying the controller certificate. With `ca` or `full` verification, the proxy must trust the CA that issued the certificate; include intermediate certificates in the PEM chain. With `full` verification, the certificate SAN must match the address verified by the client. Flow advertises an RPC address with an IP. A certificate for a DNS name alone is insufficient unless the client is configured to verify that name using `peer_alternative_host_name`.

## Minimum required permissions {#permissions}

The pipeline account needs only the permissions without which the pipeline can’t work:

- a role on the {{product-name}} cluster with permissions for the pipeline directory and the paths of all tables it accesses;
- the permission to start operations in the required pool.

Don’t grant broad permissions, such as `root` on the cluster: if they are compromised, they increase the attack surface.

## Reading operation logs and artifacts {#operation-acl}

Operation `read` covers job stderr and other [operation artifacts](../../../user-guide/storage/access-control.md#managing-operations), including job input and live previews. Reading sandbox logs through job shell also requires `manage`. See [Logs](diagnostics/logs.md). Pipeline control and source-table permissions are separate.

### Operation ACL policy {#operation-acl-policy}

For normal runner launches with `vanilla.enable = %true`, `vanilla.acl` is optional:

- Omitted or `#` leaves the operation ACL unspecified and preserves scheduler defaults.
- `[]` submits an explicit empty ACL.
- A nonempty list supplies the complete explicit policy with native operation ACL semantics. Supported permissions are `read`, `manage`, and `administer`.

Flow does not copy pipeline ACLs to the operation. The direct `StartFlowVanillaOperation` API omits `acl` and uses scheduler defaults.

Add this field to the existing `vanilla` block. Replace `<runtime_group>` with an existing group on the runtime cluster. Include all intended readers in the explicit list.

```yson
acl = [{action = allow; subjects = ["<runtime_group>"]; permissions = [read];}];
```

### Validation and cluster boundaries {#operation-acl-validation}

Explicit ACL subjects must exist on the runtime cluster (`vanilla.runtime_cluster`, or the pipeline cluster by default). Group memberships and subject tags must match the intended policy there.

Normal launch validates an explicitly configured ACL and checks its subjects before uploads, prior-operation shutdown, or manifest writes. An empty ACL needs no subject checks. For `Cannot use vanilla operation ACL on runtime cluster`, fix the reported identities or cluster access and retry. A denial for the launching user is not itself a validation failure.

### Verify a reader's access {#operation-acl-verification}

After launch, use the intended reader's credentials on the operation cluster. Substitute the immutable operation ID from its UI page and the login returned by `whoami`.

```bash
yt --proxy <operation_cluster> whoami
yt --proxy <operation_cluster> check-operation-permission \
    --operation <operation_id> --user <login> --permission read --format json
```

Expect `action` to be `allow`. Otherwise, check matching denies, runtime group membership, and tags. If allowed but stderr is unavailable, check [log availability](diagnostics/logs.md#finished-jobs).

### ACL lifetime and recovery {#operation-acl-lifetime}

The saved operation spec preserves the configured ACL, including an explicit empty list or an absent ACL. Configuration changes and binary upgrades do not update running operations. A normal relaunch applies the current configuration. `reanimate_vanilla_operation` reuses the saved ACL without normal-launch preflight. Restore missing runtime subjects and [verify access after recovery](releases.md#reanimate-acl). Use a normal launch to change policy.

## See also

- [Initial deployment](initial-deploy.md)
- [Updates and releases](releases.md)
