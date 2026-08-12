# Authentication in {{product-name}} Flow {#authentication}

{% if audience == "internal" %}There are two authentication methods for interacting with {{product-name}}: TVM and OAuth.

TVM is a more universal method: you only need to configure it once, and then you can simply grant permissions. It will be used for authentication in {{product-name}}, Logbroker, Monitoring (tracing), and so on.{% else %}Authentication for interacting with {{product-name}} is performed using an OAuth token.{% endif %}

{% note warning "Attention" %}

Make sure that the user (robot){% if audience == "internal" %} or TVM application{% endif %} has at least one role on the YT cluster (with at least access to the pipeline directory). Otherwise, you’ll get an error indicating that the user{% if audience == "internal" %}/TVM application{% endif %} is missing.

{% endnote %}


{% if audience == "internal" %}

## TVM {#authentication-tvm}

1. Create a TVM application for your service ([instructions](https://docs.yandex-team.ru/tvm/pages/getting_started)) if you don’t have one yet.
2. Start the controllers and workers with the `TVM_ID` and `TVM_SECRET` environment variables, and set them to the corresponding values.

{% endif %}

## OAuth {#authentication-oauth}

1. Create a robot.
2. Issue an {{product-name}} token for the robot.
3. Start the controllers and workers with the `YT_USER` and `YT_TOKEN` environment variables, and set them to the robot’s login and its {{product-name}} token.
