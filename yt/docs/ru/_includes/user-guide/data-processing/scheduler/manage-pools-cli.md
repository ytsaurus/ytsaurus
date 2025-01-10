## Управление пулами через CLI { #cli }

Создание подпула:

```bash
yt create scheduler_pool --attributes='{pool_tree={{pool-tree}};name=project-subpool1;parent_name=project-root}'
```

В `attributes` можно дополнительно передать атрибуты пула, они будут провалидированы. В случае неуспешной валидации объект создан не будет.

Пример изменения веса для созданного пула:

```bash
yt set //sys/pool_trees/{{pool-tree}}/project-root/project-subpool1/@weight 10
```

Первичное выставление гарантии пула при условии, что у родителя имеется нераспределенная гарантия:

```bash
yt set //sys/pool_trees/{{pool-tree}}/project-root/project-subpool1/@strong_guarantee_resources '{cpu=50}'
```

Для изменения уже выставленной гарантии можно изменить конкретный параметр:

```bash
yt set //sys/pool_trees/{{pool-tree}}/project-root/project-subpool1/@strong_guarantee_resources/cpu 100
```

Перемещение производится стандартным образом:

```bash
yt move //sys/pool_trees/{{pool-tree}}/project-root/project-subpool1 //sys/pool_trees/{{pool-tree}}/new-project/new-subpool
```
Переименование через перемещение поддерживается.
При перемещении происходит валидация.

Атрибуты выставляются стандартным образом:

```bash
yt set //sys/pool_trees/{{pool-tree}}/project_pool/@max_operation_count 10
```
При выставлении атрибутов происходит валидация.
