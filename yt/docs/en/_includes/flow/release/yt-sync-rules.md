# YT synchronization rules in {{product-name}} Flow

This section describes the general rules for deploying releases and changing the configuration in {{product-name}}, regardless of the launch method.

## How to launch {#launch-flow}

You can start the controllers and workers in the following ways:

* [Launch in a Vanilla operation](../../../flow/release/launch-vanilla.md) — the controllers and workers start within a single {{product-name}} vanilla operation; no separate long-running deployment is needed. This is the simplest method.

{% if audience == "internal" %}

* **Launch via Infractl** — a long-running deployment of controllers and workers in YP. Recommended for production. This method is described in the internal Russian documentation only.

{% endif %}

## Changing the table configuration in {{product-name}} {#yt-sync}
When you launch a pipeline for the first time or update it, you often need to change the configuration of objects in {{product-name}} — for example, add new tables, delete old ones, or modify the schema of existing tables. You can see how to do this in the [wait_click_join example](../../../flow/cpp/examples/wait_click_join.md#yt-sync).{% if audience == "internal" %} The most convenient way to perform these operations is [YtSync]({{yt-sync-docs}}/); see its own documentation for details.{% endif %}

Updating the configuration of objects in {{product-name}}, especially running [migrations](../../../flow/concepts/glossary.md#migration), is a potentially risky operation. Therefore, it’s the Flow user’s responsibility to run it. The rules you need to follow are described below.

## General rules for deploying pipeline releases and changing the configuration in {{product-name}} {#release-and-configure-basic-rules}

Regardless of how you manage objects in {{product-name}}, follow these rules when you deploy a [pipeline release](../../../flow/concepts/glossary.md#release-pipeline):

- Include a diff check in your release pipeline (in the procedure for deploying a new pipeline release){% if audience == "internal" %} (the [dump-diff scenario in YtSync]({{yt-sync-docs}}/getting_started#dostupnye-scenarii#dump-diff-scenario)){% endif %} between the current state of the objects (their existence, schema, and attributes) in {{product-name}} and the state required for the new pipeline version to work{% if audience == "internal" %} (this state is described in `YtSync`){% endif %}.

    Since the new release might, for example, use new tables, the Flow pipeline won’t be able to start if those tables aren’t created yet (it will fail with errors), which will lead to unwanted downtime. Performing a mandatory diff check before you update the static spec and the pipeline’s executable files can greatly reduce the likelihood of such errors.

    {% note warning "Attention" %}

    When you deploy a new release, the schemas of Flow’s internal tables might change. Stop the pipeline and run the [migration](../../../flow/concepts/glossary.md#migration) before you update it.

    {% endnote %}

- Perform all [migrations](../../../flow/concepts/glossary.md#migration) only while the pipeline is fully stopped.

    This reduces the risk of migration errors, especially those tied to a release, and makes it easier to investigate an incident.

    In a small number of cases, you can break this rule to reduce downtime:

    - You can create new user tables before you update the pipeline. Sometimes you can also create new columns, but only if you’re certain nothing will break as a result.
    - You can delete old user tables after you update the pipeline, after checking that no one is using them. You can also delete old unused columns this way, but only if you’re certain nothing will break as a result.

- Don’t deploy pipeline releases or perform table operations during maintenance work on the {{product-name}} clusters.

- Try to keep the table settings consistent across different [environments](../../../flow/concepts/glossary.md#environment) (production/pre-production/testing), or at least avoid unnecessary differences.
