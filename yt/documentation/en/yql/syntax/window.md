{% include [x](_includes/window.md) %}

In terms of MapReduce, window functions are physically executed through Reduce, with `PARTITION BY` keys, which can mean lengthy execution for big sections, as well as a strict 200 GB per section limit for the main clusters {{product-name}}.

<!--[Example in tutorial](https://cluster-name.yql/Tutorial/yt_11_Window_functions)-->


