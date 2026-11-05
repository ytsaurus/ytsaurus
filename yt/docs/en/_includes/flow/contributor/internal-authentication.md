# Internal authentication in {{product-name}} Flow

This page explains how Flow components prove their identity to each other. There are three independent trust boundaries, and each uses its own mechanism:

1. [Proxy → controller authentication](#proxy-to-controller) — external requests that reach the controller via the {{product-name}} RPC proxy.
2. [Authentication within pipeline nodes](#inside-pipeline) — communications between nodes of the same pipeline (controller ↔ workers).
3. [Direct runner → controller authentication](#client-to-controller) — commands that the pipeline runner sends to the controller itself, bypassing the RPC proxy.

The source of truth is the code: the [`IPipelineAuthenticator`]({{source-root}}/yt/yt/flow/library/cpp/common/authenticator.h) interface and its implementation in [`authenticator.cpp`]({{source-root}}/yt/yt/flow/library/cpp/common/authenticator.cpp).

## Proxy → controller authentication {#proxy-to-controller}

### Goal {#proxy-goal}

The Flow controller exposes an internal RPC endpoint (`FlowExecute`). All legitimate traffic to it is proxied through the RPC proxy, which has already authenticated the user and verified their permissions for the pipeline. The signing mechanism gives the controller cryptographic proof that the request:

- Came from the RPC proxy of its own {{product-name}} cluster (and not from a spoofed source).
- Passed authorization checks on the proxy side.
- Is addressed to this specific controller and this specific pipeline.

This protects against **source spoofing** / **direct endpoint access** / **SSRF**: a client that can reach the controller’s RPC port directly won’t be able to bypass the proxy and affect the pipeline without a valid signature.

### Scheme {#proxy-scheme}

The proxy signs the structured request metadata and attaches it to the RPC request.

- **Request metadata** — `TControllerRequestMetadata`, serialized into binary YSON. Fields:
  - `Method` — a fixed tag `FlowExecute`. It ties the signature to this type of request so you can’t reuse it against another signed {{product-name}} operation.
  - `PipelineObjectId` — the Cypress identifier of the targeted pipeline. This is the main binding: it ties a intercepted signature to a single pipeline, so you can’t reuse it against another.
  - `ControllerAddress` — the address of the leader controller that the proxy resolved. This field alone provides no security guarantees — it doesn’t protect against anything. It’s included because it’s meaningful for proxy ↔ controller interaction, and the controller can optionally use it as a sanity check.

- **Signature** — issued with the cluster’s asymmetric signing key ({{product-name}}, `ISignatureGenerator`). The bytes `header || serialized metadata` are signed. The header (`TSignatureHeader`) carries the issuer, key ID, signature ID, and validity window (`ValidAfter` / `ExpiresAt`), so each signature is short-lived.
- **Transport** — the signature travels in the request’s `TCustomMetadataExt` under the key `ControllerRequestMetadataSignatureKey`. The serialized metadata isn’t transmitted separately: it’s inside the signature’s payload.

Validation (on the controller side): verify the signature against the cluster’s trusted public keys, ensure it hasn’t expired, re-parse `TControllerRequestMetadata` from the signature’s payload, and confirm that `Method` and `PipelineObjectId` match the request being served (`ControllerAddress` can be optionally checked as a sanity check).

Authentication is one-time: each Flow control request is authenticated anew. There’s no long-lived connection or handshake that you can reuse, so the proxy generates a fresh signature for each request, and the controller verifies it for each request.

The main binding is `PipelineObjectId`: a user who manages the pods of pipeline A might intercept the proxy’s signature to leader A, but they can’t reuse it against another pipeline B — its controller will reject the metadata whose identifier doesn’t match its own. This prevents an intercepted signature from bypassing the pipeline-specific permission checks that the proxy performs.

The controller knows its own `ControllerAddress`: it’s the address that the controller publishes in Cypress (`leader_controller_address`) and that the proxy resolves from there. The address can change in dynamic environments, but the controller accepts only the address it currently advertises; if the published address changes on the fly, the proxy will re-resolve it, and from Flow’s perspective, this is a new leader.

{% note info %}

A new leader controller checks that the cluster can connect to it through the RPC proxy: it sends a flow command to itself at the published address (leadership confirmation) and gives up leadership if the command does not arrive. The check is skipped in two cases:

- Automatically, if the cluster requires TLS to connect to the controller and the controller bus server has no TLS certificate and key. This is because Flow does not currently support running in {{product-name}} with encryption enabled.
- If the `YT_FLOW_SKIP_LEADER_PROXY_CONFIRMATION=1` environment variable is set — a testing workaround for a cluster that cannot connect to the controller at all. Set the variable in the controller process environment; for a vanilla operation, set it in the runner environment and list it in [`secret_env`](../../../flow/devops/vanilla/security.md#secrets).

In both cases the pipeline processes data, but user flow commands (`yt flow`, SDK clients) and the UI do not work.

{% endnote %}

### Threat model {#proxy-threat-model}

**Mitigated threats:**

- **Source spoofing / direct endpoint access / SSRF.** Without the cluster’s private key, an attacker can’t forge a signature, so a request that reaches the controller directly (bypassing the proxy) is rejected.
- **Reuse across pipelines.** `PipelineObjectId` ties an intercepted signature to its pipeline. This is the main protection (see above).
- **Reuse across methods.** The `Method` tag prevents you from reusing a Flow signature against another signed {{product-name}} operation, and vice versa.
- **Long-lived reuse.** The validity window from the header limits the time during which an intercepted signature remains usable.

`ControllerAddress` is intentionally **not** in this list: it provides no security guarantees and is present only for proxy ↔ controller interaction and optional sanity check.

**Accepted threats / out of scope:**

- **Interception on the proxy → controller channel.** A request signature does not encrypt the channel. An observer of this traffic can obtain the user token; use a protected network path for this connection.
- **Compromise of the cluster signing key.** Out of scope; handled by the {{product-name}} signature infrastructure (key rotation, distribution of trusted public keys).

## Authentication within pipeline nodes {#inside-pipeline}

### Goal {#inside-goal}

Nodes of the same pipeline communicate with each other over RPC: workers send `Handshake` and `Heartbeat` to the controller, the controller pushes `PushMessages` to workers, and auxiliary services (admin, orchid) also run. All these calls must be authenticated so that an outsider who reaches a node’s RPC port can’t impersonate a pipeline component.

Unlike the proxy → controller boundary, all pipeline nodes are launched together by a single trusted entity and with the same credentials ({{product-name}} OAuth token{% if audience == "internal" %} or TVM{% endif %}). That’s why a simpler symmetric scheme based on a shared secret — HMAC tickets — is used here.

### Scheme {#inside-scheme}

Implementation — the `THmacTicketAuth` class in [`authenticator.cpp`]({{source-root}}/yt/yt/flow/library/cpp/common/authenticator.cpp).

- **Shared secret.** All pipeline nodes derive the same static secret from the launch credentials:
  - For OAuth — `<pipeline_path>,<OAuth_token>`{% if audience == "internal" %};
  - For TVM — `<pipeline_path>,<client-self-secret TVM>`{% endif %}.

  The secret isn’t transmitted over the network anywhere. Because both sides derive it identically, the HMAC is symmetric; an outsider without the token{% if audience == "internal" %} (or TVM secret){% endif %} can’t forge a ticket. Including the pipeline path in the secret binds the tickets to a specific pipeline, so you can’t reuse a ticket against another.

- **Ticket.** The ticket format is `ytflow_hmac_v0:<seconds>:<sha256-hex>`, where hex is `SHA256(prefix + seconds + static_secret)`. `<seconds>` is the ticket’s issuance time.
- **Transport.** The ticket is placed in the RPC request’s `TCustomMetadataExt` under the key `ytflow-hmac`. The channel wrapper `THmacTicketInjectingChannel` (factory — `CreateSelfCredentialsInjectingChannelFactory`) injects it, so the calling code doesn’t need to worry about authentication.
- **Lifetime.** The ticket lives for 10 minutes and is reissued midway through its lifetime (every 5 minutes).

Validation (on the receiving service side, `THmacTicketAuthenticator`): retrieve the ticket from the request metadata, verify that its issuance time is no more than the lifetime (10 minutes) from the current time, recalculate the HMAC using its own secret, and compare. If successful, the request is authenticated as the `root` user with realm `ytflow_hmac`. Services enable this check via `CreateSelfRpcAuthenticator`.

As with proxy → controller, authentication is one-time: a fresh ticket is included in each request and verified for each request. This mechanism has no separate configuration — it always works as soon as the pipeline has an OAuth token{% if audience == "internal" %} or TVM{% endif %}.

### Threat model {#inside-threat-model}

**Mitigated threats:**

- **Ticket forgery by an outsider.** Without the static secret (that is, without the pipeline’s token{% if audience == "internal" %} or TVM secret{% endif %}), you can’t compute a valid HMAC.
- **Reuse across pipelines.** The pipeline path is part of the secret, so a ticket from one pipeline won’t pass validation in another.
- **Long-lived reuse.** The 10-minute window limits the time during which an intercepted ticket remains usable.

**Accepted threats / out of scope:**

- **Man-in-the-middle within the pipeline.** Similar to the proxy → controller boundary: an attacker who observes traffic between nodes can already see and reuse the credentials themselves, so HMAC tickets don’t add any protection against them.

## Direct runner → controller authentication {#client-to-controller}

A runner in the [direct mode](../../../flow/tools/cli.md#direct-controller-commands) skips the RPC proxy: it reads the leader controller address from the pipeline's `flow_control` table (which requires `read` on the pipeline, like the proxy path) and sends the command to the controller itself. Nobody has checked the caller before the request reaches the controller, so the controller performs both the authentication and the authorization.

### Scheme {#client-scheme}

- **Credentials.** The runner sends the same {{product-name}} credentials it uses with the proxy: the token{% if audience == "internal" %} (or a service or user ticket){% endif %}. No new secret or signature is involved.
- **Authentication.** The runner marks a direct request with a `ytflow-direct` entry in `TCustomMetadataExt`, and the controller tells these requests apart from the ones forwarded by the proxy by that mark. {% if audience == "internal" %}Credentials alone do not tell: on a cluster with TVM the proxy forwards requests with a service ticket of its own. {% endif %}On such a request the controller builds a client of the pipeline's cluster with the credentials the request carries and calls `get_current_user` through the RPC proxy. The proxy validates the token with whatever authentication the cluster uses (Cypress tokens, OAuth, IAM) and returns the owner; the request then runs on behalf of that user. When the runner also sends a user name, the proxy rejects the request unless the name matches the token owner. A marked request {% if audience == "internal" %}with neither a token nor a ticket{% else %}without a token{% endif %} is rejected.
- **Authorization.** The controller then calls `check_permission` with its own client for that user on the pipeline node: `read` for queries and `write` for mutations. Without the permission the command is rejected. Requests forwarded by the proxy skip this step: the proxy has already made the same check. Only the `flow_execute` command accepts a direct request: the other controller methods, which do not know the required permission, reject a request marked as direct.
- **A request without the mark.** It does not count as direct and goes to the proxy signature check; the direct mode does not change that path. A controller with `require_proxy_signature = %true` rejects such a request, there being no signature. A controller with `require_proxy_signature = %false` runs it unauthenticated, as it did before the direct mode, so the checks of the direct mode protect only together with `require_proxy_signature = %true`.

### Threat model {#client-threat-model}

**Mitigated threats:**

- **A request marked as direct without credentials or with a forged token.** The cluster refuses to name the owner, so the request is rejected before any command runs.
- **Privilege escalation.** The permission check uses the ACL of the pipeline node, the same one the proxy path relies on.
- **User name spoofing.** A user name sent along with the token must match the token owner; otherwise the cluster rejects the request.

**Accepted threats / out of scope:**

- **The token is visible to the controller.** The pipeline process runs under the credentials of the pipeline owner and receives the caller's token; a malicious pipeline owner could reuse it — by offering a user their own pipeline, for one, and collecting the tokens of everyone who holds `read` on it. Direct mode is meant for the user's own or trusted pipelines.

  This can be closed by never sending the caller's credentials to the controller. Instead, the proxy could hand the client a signature for one specific request and a short lifetime (a few minutes), made with the same asymmetric keys the controller already verifies on the requests forwarded by the proxy. The client would then reach the controller with that signature, and the controller would trust it as it trusts a forwarded request. The scheme is not implemented here.
- **A request without the mark under `require_proxy_signature = %false`.** The controller runs it unauthenticated; that is the behaviour of the proxy path, and the direct mode does not close it. Open the controller's port only together with `require_proxy_signature = %true`.
- **A cluster without authentication.** The cluster takes the user name from the request as is, so the permission check protects no more than on the proxy path of the same cluster.
- **Man-in-the-middle on the runner → controller channel.** As for the other boundaries: the channel is plain TCP unless the controller's `bus_server` is configured with TLS, so whoever observes the traffic sees the token.
