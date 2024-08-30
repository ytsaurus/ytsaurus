## Strawberry

Is released as a docker image.

**Current release:** {{strawberry-version}} (`ytsaurus/strawberry:{{strawberry-version}}`)

**All releases:**

{% cut "**0.0.12**" %}

CHYT:

- Make `enable_geodata` default value configurable and set to false by default (PR: [#667](https://github.com/ytsaurus/ytsaurus/pull/667)). Thanks [@thenno](https://github.com/thenno) for the PR!
- Configure system log tables exporter during the clique start.

Livy:

-  Add SPYT Livy support to the controller.

{% endcut %}

{% cut "**0.0.11**" %}

`ytsaurus/strawberry:0.0.11`

- Improve strawberry cluster initializer to set up JupYT.

{% endcut %}

{% cut "**0.0.10**" %}

`ytsaurus/strawberry:0.0.10`

- Support cookie credentials in strawberry.

{% endcut %}

{% cut "**0.0.9**" %}

`ytsaurus/strawberry:0.0.9`

{% endcut %}

{% cut "**0.0.8**" %}

`ytsaurus/strawberry:0.0.8`

- Support builin log rotation for CHYT controller.
- Improve strawberry API for UI needs.

{% endcut %}