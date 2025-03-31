# Recursive directory traversal in a cluster

An iterator for traversing the target cluster tree, with the ability to accumulate state, typically in the form of a list of paths to tables.
Custom functions are called for the descendants of each node to accumulate state and select attributes and nodes for further traversal.

Specified as the `WalkFolders` function in [FROM](from.md).

Returns a single `State` column with the same type as `InitialState`.

Required:

1. **Path**: Path to the original directory.

Optional:

2. **InitialState** (`Persistable`). The state must be of any serializable type (for example, `Callable` or `Resource` can't be used). By default, `ListCreate(String)` is used

Optional named:

3. **RootAttributes** (`String`): Semicolon-separated string listing the desired meta attributes (for example, `"schema;row_count"`). `""` by default.
4. **PreHandler**: Lambda function called for the list of descendants of the current directory after the List operation (before resolving the links). Accepts a list of nodes, the current state, and the current traversal depth, returning the next state.

   Signature: `(List<Struct<'Path':String, 'Type':String, 'Attributes':Yson>>, TypeOf(InitialState), Int32) -> TypeOf(InitialState)`
   TypeOf(InitialState) is the returned type of InitialState.

   Default implementation: `($nodes, $state, $level) -> ($state)`

5. **ResolveHandler**: Lambda function called after `PreHandler`. Accepts a list of descendant links of the current directory, the current state, a list of requested attributes for the ancestor directory, and the current traversal depth. Returns `Tuple<List<Tuple<String,String>>, TypeOf(InitialState)>`, a tuple with a list of links to traverse with the requested meta attributes, along with the next state. If a link is broken, WalkFolders ignores it: you don't need to check for this in the handler

   Signature: `(List<Struct<'Path':String, 'Type':String, 'Attributes':Yson>>, TypeOf(InitialState), List<String>, Int32) -> Tuple<List<Tuple<String,String>>, TypeOf(InitialState)>`

   Default implementation:

   ```yql
   -- resolve each link, requesting the same attributes that we requested from their ancestor
   ($nodes, $state, $rootAttrList, $level) -> {
           $linksToVisit = ListMap($nodes, ($node) -> (($node.Path, $rootAttrList)));
           return ($linksToVisit, $state);
       }
   ```

6. **DiveHandler**: Lambda function called after `ResolveHandler`. Accepts a list of descendant directories of the current directory, the current state, a list of requested attributes for the ancestor directory, and the current traversal depth. Returns `Tuple<List<Tuple<String,String>>, TypeOf(InitialState)>`, a tuple with a list of directories to traverse with the requested meta attributes after processing the current directory, along with the next state. The returned paths are queued for traversal.

   Signature: `(List<Struct<'Path':String, 'Type':String, 'Attributes':Yson>>, TypeOf(InitialState), List<String>, Int32) -> Tuple<List<Tuple<String,String>>, TypeOf(InitialState)>`

   Default implementation:

   ```yql
   -- traverse each subdirectory, requesting the same attributes that we requested from their ancestor
       ($nodes, $state, $rootAttrList, $level) -> {
           $nodesToDive = ListMap($nodes, ($node) -> (($node.Path, $rootAttrList)));
           return ($nodesToDive, $state);
       }
   ```

7. **PostHandler**: Lambda function called after `DiveHandler`. Accepts a list of descendants of the current directory after resolving the links, the current state, and the current traversal depth.

   Signature: `(List<Struct<'Path':String, 'Type':String, 'Attributes':Yson>>, TypeOf(InitialState), Int32) -> TypeOf(InitialState)`

   Default implementation: `($nodes, $state, $level) -> ($state)`

{% note warning %}

* **WalkFolders can create a significant load on the master.** Exercise caution when using WalkFolders with attributes containing large values (`schema` could be one of those) or when traversing a subtree with significant size or depth.

   Directory listing requests within a single WalkFolders call can be executed in parallel. When requesting attributes with large values, **reduce** the number of simultaneous requests by using the [`yt.BatchListFolderConcurrency`](../pragma.md#ytbatchlistfolderconcurrency) pragma.

* Handlers are executed via [EvaluateExpr](../../builtins/basic.md#evaluate_expr_atom). There is a limit on the number of YQL AST nodes. You can't use very large containers within State.

   This restriction can be circumvented by running multiple WalkFolders calls and combining the results. Or you can serialize the new state into a string without intermediate deserialization (for example, using JSON/Yson lines).

* Because of parallel directory listing calls, the traversal order of the nodes in the tree doesn't follow a depth-first search (DFS) pattern

* InitialState is used to infer handler types. It must be specified explicitly: for example, as `ListCreate(String)` rather than `[]`.

{% endnote %}

Recommendations for use:

* We recommend using [Yson UDF](../../udf/list/yson.md) to work with the Attributes column

* The listing result for each directory is cached so you can quickly traverse the same subtree again in another WalkFolders call within the same request, if needed

## Examples

Recursively collect the paths of all tables starting from `initial_folder`:

```yql
$postHandler = ($nodes, $state, $level) -> {
    $tables = ListFilter($nodes, ($x)->($x.Type = "table"));
    return ListExtend($state, ListExtract($tables, "Path"));
};

SELECT State FROM WalkFolders(`initial_folder`, $postHandler AS PostHandler);
```

Recursively find the last table created in `initial_folder`:

```yql
$extractTimestamp = ($node) -> {
    $creation_time_str = Yson::LookupString($node.Attributes, "creation_time");
    RETURN DateTime::MakeTimestamp(DateTime::ParseIso8601($creation_time_str));
};
$postHandler = ($nodes, $maxTimestamp, $_) -> {
    $tables = ListFilter($nodes, ($node) -> ($node.Type == "table"));
    RETURN ListFold(
        $tables, $maxTimestamp,
        ($table, $maxTimestamp) -> (max_of($extractTimestamp($table), $maxTimestamp))
    );
};
$initialTimestamp = CAST(0ul AS Timestamp);
SELECT
    *
FROM WalkFolders(`initial_folder`, $initialTimestamp, "creation_time" AS RootAttributes, $postHandler AS PostHandler);

```

Recursively collect the paths of all tables two levels deep from `initial_folder`

```yql
$diveHandler = ($nodes, $state, $attrList, $level) -> {
    $paths = ListExtract($nodes, "Path");
    $pathsWithReqAttrs = ListMap($paths, ($x) -> (($x, $attrList)));

    $nextToVisit = IF($level < 2, $pathsWithReqAttrs, []);
    return ($nextToVisit, $state);
};

$postHandler = ($nodes, $state, $level) -> {
    $tables = ListFilter($nodes, ($x)->($x.Type = "table"));
    return ListExtend($state, ListExtract($tables, "Path"));
};

SELECT State FROM WalkFolders(`initial_folder`,
    $diveHandler AS DiveHandler, $postHandler AS PostHandler);
```

Collect the paths from all nodes within `initial_folder` without traversing its subdirectories

```yql
$diveHandler = ($_, $state, $_, $_) -> {
    $nextToVisit = [];
    RETURN ($nextToVisit, $state);
};
$postHandler = ($nodes, $state, $_) -> {
    $tables = ListFilter($nodes, ($x) -> ($x.Type = "table"));
    RETURN ListExtend($state, ListExtract($tables, "Path"));
};
SELECT
    State
FROM WalkFolders(`initial_folder`, $diveHandler AS DiveHandler, $postHandler AS PostHandler);
```

Recursively collect the paths of all broken links (links with non-existent destination paths) from `initial_folder`.

```yql
$resolveHandler = ($list, $state, $attrList, $_) -> {
    $broken_links = ListFilter($list, ($link) -> (Yson::LookupBool($link.Attributes, "broken")));
    $broken_links_target_paths = ListNotNull(
        ListMap(
            $broken_links,
            ($link) -> (Yson::LookupString($link.Attributes, "target_path"))
        )
    );
    $nextState = ListExtend($state, $broken_links_target_paths);
    -- WalkFolders ignores broken links during resolution
    $paths = ListTake(ListExtract($list, "Path"), 1);
    $pathsWithReqAttrs = ListMap($paths, ($x) -> (($x, $attrList)));
    RETURN ($pathsWithReqAttrs, $nextState);
};

SELECT
    State
FROM WalkFolders(`initial_folder`, $resolveHandler AS ResolveHandler, "target_path" AS RootAttributes);
```

Recursively build a Yson that includes a `Type`, a `Path`, and an `Attributes` map containing node attributes (`creation_time` and a custom `foo` attribute) for each node from `initial_folder`.

```yql
-- If you need to accumulate a very large state in a single request, you can store it as a string to circumvent the limit on the number of nodes during the Evaluate call.
$saveNodesToYsonString = ($list, $stateStr, $_) -> {
    RETURN $stateStr || ListFold($list, "", ($node, $str) -> ($str || ToBytes(Yson::SerializeText(Yson::From($node))) || "\n"));
};
$serializedYsonNodes =
    SELECT
        State
    FROM WalkFolders("//home/yql", "", "creation_time;foo" AS RootAttributes, $saveNodesToYsonString AS PostHandler);

SELECT
    ListMap(String::SplitToList($serializedYsonNodes, "\n", true AS SkipEmpty), ($str) -> (Yson::Parse($str)));

```

Pagination of WalkFolders results. Skipping the first 200 paths, collect 100 paths from `initial_folder`:

```yql
$skip = 200ul;
$take = 100ul;

$diveHandler = ($nodes, $state, $reqAttrs, $_) -> {
    $paths = ListExtract($nodes, "Path");
    $pathsWithReqAttrs = ListMap($paths, ($x) -> (($x, $reqAttrs)));

    $_, $collectedPaths = $state;
    -- complete the traversal if we've reached the required number of nodes
    $nextToVisit =  IF(
        ListLength($collectedPaths) > $take,
        [],
        $pathsWithReqAttrs
    );
    return ($nextToVisit, $state);
};

$postHandler = ($nodes, $state, $_) -> {
    $visited, $collectedPaths = $state;
    $paths = ListExtract($nodes, "Path");
    $itemsToTake = IF(
        $visited < $skip,
        0,
        $take - ListLength($collectedPaths)
    );
    $visited = $visited + ListLength($paths);

    return ($visited, ListExtend($collectedPaths, ListTake($paths, $itemsToTake)));
};
$initialState = (0ul, ListCreate(String));

$walkFoldersRes = SELECT * FROM WalkFolders(`initial_folder`, $initialState, $diveHandler AS DiveHandler, $postHandler AS PostHandler);

$_, $paths = Unwrap($walkFoldersRes);
SELECT $paths;
```


