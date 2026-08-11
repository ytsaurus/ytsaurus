# Basic rules for deploying a Pipeline in {{product-name}} Flow

This section describes the general rules for deploying releases and changing the configuration in {{product-name}}, regardless of the launch method.

## How to launch {#launch-flow}

You can start the controllers and workers in the following ways:

* [Launch in a Vanilla operation](../../../flow/release/launch-vanilla.md) — the controllers and workers start within a single {{product-name}} vanilla operation; no separate long-running deployment is needed. This is the simplest method.{% if audience == "internal" %}
* [Launch via Infractl](../../../flow/release/launch-infractl.md) — a long-running deployment of controllers and workers in YP. Recommended for production.{% endif %}

## Changing the table configuration in {{product-name}} {#yt-sync}

When you launch a pipeline for the first time or update it, you often need to change the configuration of objects in {{product-name}} — for example, add new tables, delete old ones, or modify the schema of existing tables. You can see how to do this in the [wait_click_join example](../../../flow/cpp/examples/wait_click_join.md#yt-sync).{% if audience == "internal" %} It’s most convenient to perform these operations using [YtSync]({{yt-sync-docs}}/); you can read more about it in its documentation.{% endif %}

Updating the configuration of objects in {{product-name}}, especially running [migrations](../../../flow/concepts/glossary.md#migration), is a potentially risky operation. Therefore, it’s the Flow user’s responsibility to run it. The rules you need to follow are described below.

## General rules for deploying pipeline releases and changing the configuration in {{product-name}} {#release-and-configure-basic-rules}

Regardless of how you manage objects in {{product-name}}, follow these rules when you deploy a [pipeline release](../../../flow/concepts/glossary.md#release-pipeline):

* Include a diff check in your release pipeline (in the procedure for deploying a new pipeline release) {% if audience == "internal" %}([the dump-diff scenario in YtSync]({{yt-sync-docs}}/getting_started#dostupnye-scenarii#dump-diff-scenario)){% endif %} between the current state of the objects (their existence, schema, and attributes) in {{product-name}} and the state required for the new pipeline version to work {% if audience == "internal" %}(this state is described in `YtSync`){% endif %}.

    Since the new release might, for example, use new tables, the Flow pipeline won’t be able to start if those tables aren’t created yet (it will fail with errors), which will lead to unwanted downtime. Performing a mandatory diff check before you update the static spec and the pipeline’s executable files can greatly reduce the likelihood of such errors.

    {% note warning "Attention" %}

    When you deploy a new release, the schemas of Flow’s internal tables might change. In this case, make sure to run the [migration](../../../flow/concepts/glossary.md#migration) while the pipeline is fully stopped (be sure to do this, and only when the pipeline is fully stopped).

    {% endnote %}

* Perform all system [migrations](../../../flow/concepts/glossary.md#migration) while the pipeline is fully stopped.

    This rule significantly improves the safety of migrations, especially those tied to a release. It also makes it easier to troubleshoot an incident if something goes wrong.

    In a small number of cases, you can break this rule to reduce downtime:

    * You can create new user tables before you update the pipeline. Sometimes you can also create new columns, but only if you’re certain nothing will break as a result.
    * You can delete old user tables after you update the pipeline, after checking that no one is using them. You can also delete old unused columns this way, but only if you’re certain nothing will break as a result.

* Don’t deploy pipeline releases or perform table operations during maintenance work on the {{product-name}} clusters.

* Try to keep the table settings consistent across different [environments](../../../flow/concepts/glossary.md#environment) (production/pre-production/testing), or at least avoid unnecessary differences.

## Configuring authentication {#authentication}

{% if audience == "internal" %}There are two authentication methods for interacting with {{product-name}}: TVM and OAUTH.

TVM is a more universal method: you only need to configure it once, and then you can simply grant permissions. It will be used for authentication in {{product-name}}, Logbroker, Monitoring (tracing), and so on.{% else %}Authentication for interacting with {{product-name}} is performed using an OAUTH token.{% endif %}

{% note warning "Attention" %}

Make sure that the user (robot){% if audience == "internal" %} or TVM application{% endif %} has at least one role on the YT cluster (with at least access to the pipeline directory). Otherwise, you’ll get an error indicating that the user{% if audience == "internal" %}/TVM application{% endif %} is missing.

{% endnote %}


{% if audience == "internal" %}

### TVM {#authentication-tvm}

1. Create a TVM application for your service ([instructions](https://docs.yandex-team.ru/tvm/pages/getting_started)) if you don’t have one yet.
2. Start the controllers and workers with the `TVM_ID` and `TVM_SECRET` environment variables, and set them to the corresponding values.

{% endif %}

### OAUTH {#authentication-oauth}

1. Create a robot.
2. Issue an {{product-name}} token for the robot.
3. Start the controllers and workers with the `YT_USER` and `YT_TOKEN` environment variables, and set them to the robot’s login and its {{product-name}} token.

## See also

* [Launch in a Vanilla operation](../../../flow/release/launch-vanilla.md){% if audience == "internal" %}
* [Launch via Infractl](../../../flow/release/launch-infractl.md){% endif %}
* [Spec and DynamicSpec](../../../flow/concepts/spec.md)
* [Pipeline CLI](../../../flow/release/cli.md){% if audience == "internal" %}
* [Monitoring](../../../flow/release/monitoring.md){% endif %}
* [Protection against zombie processes](../../../flow/release/flow-core-target.md)