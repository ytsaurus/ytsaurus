{% include [x](_includes/window.md) %}

В терминах MapReduce оконные функции физически выполняются через Reduce по ключам `PARTITION BY`, что может означать длительное выполнение для разделов большого размера, а также жёсткий лимит в 200Гб на раздел для основных кластеров {{product-name}}.

<!--[Пример в tutorial](https://cluster-name.yql/Tutorial/yt_11_Window_functions)-->


