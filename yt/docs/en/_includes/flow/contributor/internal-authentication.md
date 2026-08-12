# Internal authentication in {{product-name}} Flow

This page explains how Flow components prove their identity to each other. There are two independent trust boundaries, and each uses its own mechanism:

1. [Proxy → controller authentication](#proxy-to-controller) — external requests that reach the controller via the {{product-name}} RPC proxy.
2. [Authentication within pipeline nodes](#inside-pipeline) — communications between nodes of the same pipeline (controller ↔ workers).

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

### Threat model {#proxy-threat-model}

**Mitigated threats:**

- **Source spoofing / direct endpoint access / SSRF.** Without the cluster’s private key, an attacker can’t forge a signature, so a request that reaches the controller directly (bypassing the proxy) is rejected.
- **Reuse across pipelines.** `PipelineObjectId` ties an intercepted signature to its pipeline. This is the main protection (see above).
- **Reuse across methods.** The `Method` tag prevents you from reusing a Flow signature against another signed {{product-name}} operation, and vice versa.
- **Long-lived reuse.** The validity window from the header limits the time during which an intercepted signature remains usable.

`ControllerAddress` is intentionally **not** in this list: it provides no security guarantees and is present only for proxy ↔ controller interaction and optional sanity check.

**Accepted threats / out of scope:**

- **Man-in-the-middle on the proxy → controller channel.** We don’t protect against this. Inside a data center, it’s not an issue at all; across data centers, it’s currently acceptable and can be strengthened later (SECREVIEW-8749). An attacker who can observe this traffic can already read the user’s OAuth token and impersonate them directly, so the signature adds nothing against such an attacker. This also covers reuse of an intercepted signature (you need to observe the traffic to intercept it).
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