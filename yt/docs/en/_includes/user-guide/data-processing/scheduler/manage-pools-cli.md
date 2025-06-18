# Managing pools via the CLI

Creating a subpool:

```bash
yt create scheduler_pool --attributes='{pool_tree={{pool-tree}};name=project-subpool1;parent_name=project-root}'
```

In the attributes, you can additionally pass the pool attributes and they will be validated. If the validation fails, the object will not be created.

Example of a weight change for the created pool:

```bash
yt set //sys/pool_trees/{{pool-tree}}/project-root/project-subpool1/@weight 10
```

Initial setting of a pool guarantee provided that the parent has an unallocated guarantee:

```bash
yt set //sys/pool_trees/{{pool-tree}}/project-root/project-subpool1/@strong_guarantee_resources '{cpu=50}'
```

A specific parameter can be changed to alter a set guarantee:

```bash
yt set //sys/pool_trees/{{pool-tree}}/project-root/project-subpool1/@strong_guarantee_resources/cpu 100
```

Moving is performed in the standard way:

```bash
yt move //sys/pool_trees/{{pool-tree}}/project-root/project-subpool1 //sys/pool_trees/{{pool-tree}}/new-project/new-subpool
```
Renaming via moving is supported.
Validation occurs when moving.

Attributes are set in the standard way:

```bash
yt set //sys/pool_trees/{{pool-tree}}/project_pool/@max_operation_count 10
```
Validation occurs when setting attributes.

