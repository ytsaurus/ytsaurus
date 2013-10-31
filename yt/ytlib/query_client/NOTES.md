  * TDataSplit should expose KeyColumns and Schema, so TDataSplit wraps TChunkSpec.
  * Run AST structure consistency checks from time to time. :)
  * Get rid of memory pool, leave GC root.
  * Introduce kinds to ast nodes.
  * Cache expression type and name in expr nodes; display them in graphviz;
  * Cache inferred key columns and schemas in operator nodes; display them in graphviz.

        //
        // 1) Traverse operator tree and split "heavy" nodes
        // (by estimating data size per subfragment).
        //
        // 2) Push certain operators (F, P, G) down to data.
        //
        // 3) Pin fragments to copartitioned data splits.
        //
        // 4) Rewrite fragments so they could be delegated
        // incorporating auxiliary Scan nodes into topmost fragment.
        //
        // 5) For every remote fragment take copartitioned executor
        // and save appropriate readers.
        //
        // 6) Schedule local fragment and attach it to the output writer.


