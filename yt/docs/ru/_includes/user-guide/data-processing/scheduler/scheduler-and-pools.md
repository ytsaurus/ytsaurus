# Планировщик и пулы

В данном разделе приведено описание планировщика (scheduler) {{product-name}}, вводятся понятия пула (pool), дерева пулов (pool_tree), рассматривается процесс планирования вычислений, приводится пример вычисления fair share ratio и описание работы вытеснения (preemption).

## Общие сведения

Планировщик  — часть системы, которая отвечает за распределение ресурсов между операциями и определение очередности, в которой операции будут выполняться. В системе {{product-name}} используется иерархический [fair_share](http://en.wikipedia.org/wiki/Fair-share_scheduling) планировщик. Это означает, что ресурсы динамически распределяются между пользователями системы в соответствии с их гарантиями и приоритетом.

Пул — контейнер для ресурсов CPU и RAM, которыми оперирует планировщик. Когда пользователи запускают задачи в пулах, планировщик распределяет ресурсы между пулами. С помощью дерева пулов можно построить иерархическую систему распределения ресурсов.

## Ресурсы

Планировщик оперирует следующими ресурсами:

- `cpu` — 1 CPU соответствует одному HyperThreading ядру на узле кластера. По умолчанию все пользовательские джобы заказывают при запуске одно ядро. Если джобы многопоточные, то можно увеличить количество заказанных ядер в настройках операции. В противном случае джобы процессов будут ограничены (throttling) таким образом, чтобы количество реально потребляемого процессорного времени соответствовало одному ядру;
- `memory` — ресурс для контроля потребления оперативной памяти на узле кластера в байтах;
- `user_slots` — ресурс для контроля числа джобов на узле кластера. Каждый джоб пользовательских операций всегда потребляют ровно один user_slot;
- `network` — ресурс для контроля потребления внутренней сети кластера операциями, активно использующими сеть. Например, сортировки. У единицы ресурса нет физического аналога, а автоматически выставляемые планировщиком значения для операций сортировки подобраны таким образом, чтобы джобы, запущенные на одном и том же узле кластера, не перегружали сетевой канал.

Для каждого пула и операции существует понятие доминантного ресурса. Доминантным ресурсом среди запрашиваемых ресурсов операции или пула считается тот вид ресурса, который имеет наибольшую долю относительно общего объёма соответствующего ресурса всего кластера.

Например: есть операция, для выполнения которой требуется 1 000 `cpu` и 2 TiB `memory`. При этом во всём кластере 50 000 `cpu` и 180 TiB `memory`. Тогда можно оценить, что 1 000 `cpu` составляет 0.02 (2%) от общего числа `cpu` в кластере, а 2 TiB составляет 0.011 (1.1%) от общего объёма `memory` кластера. Доля `cpu` больше доли `memory`, поэтому для конкретной операции доминантным ресурсом является `cpu`.

## Описание работы планировщика { #scheduler }

### Пулы и деревья пулов

Планировщик {{product-name}} хранит в памяти несколько деревьев пулов. Каждое дерево состоит из операций и вычислительных пулов. Узлами дерева являются вычислительные пулы, а операции — листьями. Каждая операция может принадлежать одному или нескольким деревьям.

Логически можно рассматривать каждое дерево как отдельный вычислительный кластер — каждому дереву принадлежит некоторое количество вычислительных узлов кластера, при этом узел кластера может принадлежать только одному дереву.

Деревья пулов хранятся в Кипарисе и настраиваются администратором системы {{product-name}}. Дерево пулов представляет из себя узел `scheduler_pool_tree` на первом уровне директории `//sys/pool_trees` в Кипарисе. Описание пулов для дерева `<tree_id>` представляет собой набор вложенных `scheduler_pool`, находящихся в узле `//sys/pool_trees/<tree_id>`. Характеристики пулов описываются в атрибутах соответствующих узлов.

Деревья, в которые попадет операция при запуске, определяются параметром `pool_trees` в настройках операции. Если данный параметр не указан, то будет использовано одно дерево по умолчанию `default_pool_tree`. Для каждого из перечисленных деревьев можно указать пул из этого дерева, в котором нужно запустить операцию (опция `scheduling_options_per_pool_tree`).

У каждого дерева пулов существует атрибут `default_parent_pool`, указывающий на пул по умолчанию. Данный пул используется следующим образом: если в настройках операции для дерева или для всех деревьев пул указан не был, в дереве будет создан временный эфемерный пул с именем пользователя в {{product-name}}, запустившего операцию. Пулы образуют иерархию `//sys/pool_trees/<tree_id>/<default_parent_pool>/<username>`, в конечном узле которой и будет запущена операция.

Пример настройки для запуска операции в двух деревьях с указанием пула в одном из них:

```
pool_trees = [{{pool-tree}}; cloud];
scheduling_options_per_pool_tree = {
    {{pool-tree}} = {
        pool = cool_pool;
    }
}
```

## Процесс планирования джоба

Каждая операция состоит из набора джобов. Джоб — это выполнение некоторой полезной работы над данными на одном из узлов кластера. Джоб является единицей планирования.

Планирование джобов происходит на каждом узле кластера отдельно:

1. Узел кластера присылает планировщику специальное сообщение (heartbeat), в котором указывает тип и объём свободных ресурсов, информацию о завершившихся и выполняющихся в данный момент джобах.
2. В ответ на сообщение планировщик принимает решение о запуске новых джобов и возможном прерывании уже запущенных. Планирование происходит в дереве, которому принадлежит узел кластера.
3. Все прерванные джобы в дальнейшем будут запущены заново.

Чтобы понять, какие джобы необходимо запустить, а какие прервать на конкретном узле кластера, планировщик анализирует дерево пулов. Каждый пул в дереве обладает рядом характеристик, которые позволяют планировщику понять, нуждается ли данное поддерево в запуске новых джобов, и, если нуждается, то насколько сильно. Согласно характеристикам пула, планировщик спускается по дереву сверху вниз, пока не достигнет листа. При достижении листа планировщик предлагает операции запустить джоб. Такие обходы планировщик делает до тех пор, пока есть незапланированные джобы и пока не будут исчерпаны ресурсы узла кластера.

Могут встречаться операции, которым планировщик выдал меньше ресурсов, чем обязан был гарантировать. Такие операции в системе называются [страдающими](../../../../user-guide/data-processing/scheduler/preemption.md) (starving). При обнаружении страдающей операции планировщик выбирает джоб с минимальным временем работы у нестрадающей операции на том же узле кластера и прерывает его.

{% note info "Примечание" %}

Рассматриваемые при планировании характеристики операций и пулов являются глобальными, не зависящими от конкретного узла кластера. Например, важной характеристикой является доля ресурсов кластера, которую уже занимают выполняющиеся джобы операции (`usage_ratio`).

{% endnote %}
