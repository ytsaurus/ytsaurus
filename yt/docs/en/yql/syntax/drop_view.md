# DROP VIEW

`DROP VIEW` deletes an independent [view](select/view.md).

## Syntax

```yql
DROP VIEW [IF EXISTS] [cluster.]`//path/to/view`;
```

If `//path/to/view` is not a view, the command fails with an error.
An attempt to drop a non-existent view also fails with an error unless the `IF EXISTS` modifier is specified.

## Availability

`DROP VIEW` is available starting from language version [2025.05](../changelog/2025.05.md).

## Examples

```yql
DROP VIEW `//path/to/view1`; -- drops the view on the current cluster
DROP VIEW IF EXISTS cluster.`//path/to/view2`; -- explicit cluster specification, completes without error if `//path/to/view2` does not exist
```

## See also

* [CREATE VIEW](create_view.md)

