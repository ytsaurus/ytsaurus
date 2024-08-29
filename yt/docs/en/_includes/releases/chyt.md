## CHYT

Is published as a docker image.

**Current release:** {{chyt-version}} (`ytsaurus/chyt:{{chyt-version}}-relwithdebinfo`)

**All releases:**

{% cut "**2.14.0**" %}

`ytsaurus/chyt:2.14.0-relwithdebinfo`

- Support SQL UDFs.
- Support reading dynamic and static tables via concat-functions.

{% endcut %}

{% cut "**2.13.0**" %}

`ytsaurus/chyt:2.13.0-relwithdebinfo`

- Update ClickHouse code version to the latest LTS release (22.8 -> 23.8).
- Support for reading and writing ordered dynamic tables.
- Move dumping query registry debug information to a separate thread.
- Configure temporary data storage.

{% endcut %}

{% cut "**2.12.4**" %}

`ytsaurus/chyt:2.12.4-relwithdebinfo`

{% endcut %}