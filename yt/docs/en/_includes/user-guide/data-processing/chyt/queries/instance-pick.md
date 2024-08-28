# Selecting a coordinator

A heavy proxy receives a user query and delegates it to one of the instances, which becomes the "coordinator" instance. The user can influence this choice: for example, for more efficient cache use.

Below are different strategies for picking the coordinator, listed in descending order of priority (if you attempt to include more than one strategy, the one with the higher priority will be selected):

1) **Selecting a specific instance.**

    You need to add `job-cookie` of the instance to the clique alias using the `@` character. For an instance with `job-cookie=3`, for example, the query will look like this: `http://cluster.domain.com/chyt?chyt.clique_alias=my-clique@3`.

2) **Selecting an instance within a session.**

    Within a single ClickHouse session, the proxy will* select the same instance.

    {% note info "Note" %}

    *If no instances have been restarted in the last few minutes.

    {% endnote %}

    A session is defined by the `session_id` URL parameter (just like in the original ClickHouse). This results in the query `http://cluster.domain.com/chyt?chyt.clique_alias=my-clique&session_id=abracadabra`.

3) **StickyGroup strategy.**

    You can either specify the `QueryStickyGroupSize` parameter in the clique configuration or pass it along with the query in the `chyt.query_sticky_group_size` URL parameter.

    In this case, the proxy will delegate identical queries to a group of instances of the `QueryStickyGroupSize` size, which means that the same query will get into a random instance from that group.

    The choice of the `QueryStickyGroupSize` value can be based on the following reasoning: if the value is small, the cache in the selected group warms up quickly; if the value is large, the group is almost unaffected if one instance goes down, since most instances will preserve their cache.

4) **Selecting a random instance.**

    If there are no other strategies, the coordinator is selected equally likely among all available instances.
