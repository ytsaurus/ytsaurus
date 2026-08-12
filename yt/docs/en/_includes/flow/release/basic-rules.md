# Basic rules for deploying a Pipeline in {{product-name}} Flow

This section is the entry point to deploying and operating a Flow pipeline: it lists the available launch methods and the pages that cover each part of the deployment surface. The general rules for deploying releases and changing the {{product-name}} configuration, which do not depend on the launch method, are collected under YT sync rules below.

## How to launch {#launch-flow}

You can start the controllers and workers in the following ways:

* [Launch in a Vanilla operation](../../../flow/release/launch-vanilla.md) — the controllers and workers start within a single {{product-name}} vanilla operation; no separate long-running deployment is needed. This is the simplest method.{% if audience == "internal" %}
* **Launch via Infractl** — a long-running deployment of controllers and workers in YP. Recommended for production. This method is described in the internal Russian documentation only.{% endif %}

## Deployment and operations {#deployment-pages}

Once the pipeline runs, the rest of the deployment surface is covered by these pages:

* [Pipeline operations](../../../flow/release/pipeline-operations.md) — start, stop, and pause a running pipeline.
* [Releases](../../../flow/release/releases.md) — roll out a new version, reanimate an aborted operation, and deploy a hotfix.
* [Security](../../../flow/release/security.md) — the account the operation runs as, secret delivery, and the minimum required permissions.
* [Logs](../../../flow/release/logs.md) — where the process logs are written and how to read them from a job.
* [YT sync rules](../../../flow/release/yt-sync-rules.md) — the general rules for deploying releases and for changing the table configuration in {{product-name}}.
* [Authentication](../../../flow/release/auth.md) — how the controllers and workers authenticate in {{product-name}}.

## See also

* [Launch in a Vanilla operation](../../../flow/release/launch-vanilla.md)
* [Spec and DynamicSpec](../../../flow/concepts/spec.md)
* [Pipeline CLI](../../../flow/release/cli.md)
* [Protection against zombie processes](../../../flow/release/flow-core-target.md)
