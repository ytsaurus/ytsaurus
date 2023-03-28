# Annotations

This section describes the annotations of tables, directories, and other [Cypress](../../../user-guide/storage/cypress.md) nodes.

## Purpose of annotations { #rationale }

Annotations are a way to provide a table, directory, or any other Cypress node with a short description.

Formally, such a description can be random text up to 1 KB, consisting of non-control ASCII characters, but we recommend sticking to Markdown format.

{% note info "Note" %}

Annotations are stored in the memory of the {{product-name}} master servers. We recommend limiting the content of the annotations to one or two sentences and links to more detailed documentation.

{% endnote %}

## The annotation attribute { #annotation_attribute }

All Cypress nodes have the `annotation` attribute with the [YSON string](../../../user-guide/storage/yson.md) value. You can set the value, as for the other attributes:

```bash
yt set //home/project/path/table/@annotation '"This table contains valuable data."'
```

Removal is performed in the usual way:

```bash
yt remove //home/project/path/table/@annotation
```

{% note info "Note" %}

The `annotation` attribute is always present, but it is `null` by default. Removal resets the attribute to `null`, but formally it still exists.

Thus, checking the existence of the `annotation` attribute always returns a positive result and is therefore useless.  Instead, check that the attribute is `null`.

This behavior peculiarity is related to inheritance.

{% endnote %}

## Inheritance { #inheritance }

The annotation can accompany both a table and an entire subtree. To add a subtree annotation, set it to the appropriate directory. Any node recursively contained in it that does not have its own annotation will then display it in its `annotation` attribute.

In other words, the `annotation` attribute is inherited: it always displays the nearest non-zero annotation of the ancestor. The search starts with the node.

To understand which ancestor the displayed annotation belongs to, use the `annotation_path` attribute. It is read-only and contains the full path to the corresponding ancestor or to the node if it is annotated.

## Support in the web interface { #web_interface }

The web interface supports annotations and accepts the Markdown format — the [YFM](https://ydocs.tech/en/) dialect supported by the [@doc-tools/transform](https://www.npmjs.com/package/@doc-tools/transform) package.
Annotations are displayed in the **Navigation** section, on the **Annotation** tab of the selected node. To edit an annotation, click **Edit metadata** and go to the **Description** tab.
