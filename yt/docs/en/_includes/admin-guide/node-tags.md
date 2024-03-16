## Tagging cluster nodes { #node_tags }

Cluster nodes can be marked with special string literals called tags.

Certain tags, usually called system tags, are determined by the node itself based on its config and environment. The environment is set by the cluster administrators. Nodes can be assigned additional tags using the `user_tags` attribute on the node host in Cypress.

Tags can be used to filter a set of nodes using Boolean formulas.

A boolean formula is an expression that:
- Contain the operators `&` (logical AND), `|` (logical OR), `!` (logical NOT), or parentheses.
- Are formed by literals. A literal is a string of Latin characters and the special characters `_`, `/`, `-`, `.`, `:`.

The expression is evaluated for a specific node. The literal value is substituted with "true" if the node's tag list includes the literal and "false" if it doesn't. Next, the expression is evaluated using the standard rules of Boolean expressions. Empty expressions always return "false".

Expressions on tags are used in two scenarios:
- The `scheduling_tag_filter` option in the operation specification. This option specifies a filter for a subset of nodes allowed to execute jobs of the operation.
- Keys in the `//sys/cluster_nodes/@config` dynamic node config.

{% cut "Starting an operation with specified scheduling_tag_filter " %}

<small>Listing 1 — Setting a custom tag on the node</small>

```bash
$ yt set //sys/cluster_nodes/exe-0.my_cluster.net:9012/@user_tags '[test]'
$ yt set //sys/cluster_nodes/exe-1.my_cluster.net:9012/@user_tags '[test]'
```

 <small>Listing 2 — Launching an operation specifying the tag</small>

```bash
$ yt map --spec '{scheduling_tag_filter="test"}' ...
```

{% endcut %}
