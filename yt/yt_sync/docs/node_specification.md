# Спецификация нод
__ВАЖНО__: необходимо использовать данную спецификацию только для нод, отличных от таблиц. Для таблиц следует использовать соответствующую [спецификацию](table_specification.md).

Спецификация нод в коде описана [здесь](https://a.yandex-team.ru/arcadia/yt/yt_sync/core/spec/node.py).

## Свойства группы нод
Описаны в классе `Node`:
- __`type: Node.Type | str`__ - тип ноды
- __`clusters: dict[str, ClusterNode]`__ - описание свойств ноды на конкретном кластере

### Типы нод
На текущий момент явно поддерживаются следующие типы:
- `FOLDER` - папка (известный также как `map_node`)
- `FILE` - файл
- `DOCUMENT` - документ
- `LINK` - символическая ссылка
- `ANY` - произвольный тип, нужен для обновления атрибутов и добавления в модель узла или кластера

__ВАЖНО__: тип `ANY` не предназначен для создания или удаления ноды, только для чтения и изменения ее атрибутов.

*Примечание*: другие типы нод также можно использовать, если указать их по
[названию](https://yt.yandex-team.ru/docs/user-guide/storage/objects).

## Свойства ноды на кластере
Описаны в классе `ClusterNode`:
* `main: bool`
  > Необходим для корректной настройки кластера для таблиц. Если кластер в спецификации ноды указан один, то `main` 
    можно не указывать. Для всех таблиц и нод в рамках одного запуска `YtSync` должен быть одинаковый `main` кластер. 
    Если кроме нод в конфигурации есть и таблицы, то можно для нод указывать `main = None`, тогда главный кластер 
    будет получен из конфигурации таблиц.
* __`path: str`__
  > Путь до ноды на данном кластере.
* `attributes: dict[str, Any]`
  > Набор атрибутов ноды.
* `propagated_attributes: set[str]`
  > Набор атрибутов ноды, которые будут унаследованы нодами и таблицами в её поддереве. Игнорируется для любых типов 
    нод, кроме `FOLDER` и `ANY`.
* `target_path: str`
  > Путь до ноды, на который указывает данная нода. Обязательное свойство для ссылок, причём сам путь либо уже должен 
    существовать, либо должен создаваться в том же сценарии. Игнорируется для всех остальных типов нод.

## Использование и примеры
Спецификация ноды передается в метод `YtSync.add_desired_node()`.

Переданная спецификация ноды валидируется на корректность.

Спецификацию можно передать в виде заполненного экземпляра класса `Node`, либо в виде `dict`, заполненного в
соответствии с полями классов спецификации, подобно [таблицам](table_specification.md).

### Как можно передать спецификацию
```python
from yt.yt_sync import ClusterNode
from yt.yt_sync import Node
from yt.yt_sync import Settings
from yt.yt_sync import YtSync

yt_sync = YtSync(settings=Settings(db_type=Settings.REPLICATED_DB, use_deprecated_spec_format=False, ...), ...)

# spec from dataclasses
yt_sync.add_desired_node(
    Node(
        type=Node.Type.FILE,
        clusters={
            "primary": ClusterNode(path="node_path", attributes={"my_attribute": "value"})
        }
    )
)

# spec from dict
yt_sync.add_desired_node(
    {
        "type": Node.Type.FILE,
        "clusters": {
            "primary": {"path": "node_path", "attributes": {"my_attribute": "value"}}
        }
    }
)
```

### Пример спецификации ссылок
Для ссылок также необходимо передать `target_path`.

```python
from yt.yt_sync import ClusterNode
from yt.yt_sync import Node
from yt.yt_sync import Settings
from yt.yt_sync import YtSync

yt_sync = YtSync(settings=Settings(db_type=Settings.REPLICATED_DB, use_deprecated_spec_format=False, ...), ...)

# spec from dataclasses
yt_sync.add_desired_node(
    Node(
        type=Node.Type.LINK,
        clusters={
            "primary": ClusterNode(
                path="node_path",
                attributes={"my_attribute": "value"},
                target_path="path_to_target",
            )
        }
    )
)
```

### Наследование атрибутов
При добавлении в модель папки (будь то через `Node.Type.FOLDER` или `Node.Type.ANY`) на все ноды и таблицы, лежащие в
её поддереве, распространяются атрибуты, указанные в `propagated_attributes`.

Особенности:
* если `propagated_attributes == None`, то наследуются все присутствующие атрибуты
* если `propagated_attributes == set()`, то ни один атрибут не наследуется
* при переопределении наследуются ближайшие значения атрибутов

```python
from yt.yt_sync import ClusterNode
from yt.yt_sync import Node
from yt.yt_sync import Settings
from yt.yt_sync import Types
from yt.yt_sync import YtSync

def add_node(
    yt_sync: YtSync,
    node_type: Node.Type,
    node_path: str,
    attributes: Types.Attributes,
    propagated_attributes: set[str] | None = None,
):
  yt_sync.add_desired_node(
    Node(
      type=node_type,
      clusters={
        "primary": ClusterNode(path=node_path, attributes=attributes, propagated_attributes=propagated_attributes)
      }
    )
  )

yt_sync = YtSync(
    settings=Settings(db_type=Settings.REPLICATED_DB, use_deprecated_spec_format=False, ...),
    scenario_name="ensure",
    ...
)

add_node(yt_sync, Node.Type.FOLDER, "//folder", {"attr1": "old_value"}, {"attr1"})
add_node(yt_sync, Node.Type.FILE, "//folder/file", None)

# '//folder/subfolder1' already exists
add_node(yt_sync, Node.Type.ANY, "//folder/subfolder1", {"attr1": "new_value", "attr2": "old_value"}, None)
add_node(yt_sync, Node.Type.DOCUMENT, "//folder/subfolder1/doc", {"attr1": None, "attr2": "new_value"})
yt_sync.add_desired_table(
    {
        "schema": [
            {"name": "key", "type": "string", "sort_order": "ascending"},
            {"name": "data", "type": "string"},
        ],
        "clusters": {
            "primary": {
                "main": True,
                "path": "//folder/subfolder1/table",
                "attributes": {
                    "dynamic": True,
                }
            }
        }
    }
)

add_node(yt_sync, Node.Type.FOLDER, "//folder/subfolder2", {"attr2": "value"}, {"attr1"})
add_node(yt_sync, Node.Type.DOCUMENT, "//folder/subfolder2/doc", {})

# '//folder/subfolder3' already exists
add_node(yt_sync, Node.Type.ANY, "//folder/subfolder3", {"attr2": "value"}, set())
add_node(yt_sync, Node.Type.DOCUMENT, "//folder/subfolder3/doc", {})

yt_sync.sync()

# Result (all attributes are shown in brackets):
#
# /
# - folder[attr1=old_value]
#   - file[attr1=old_value]
#   - subfolder1[attr1=new_value, attr2=old_value]
#     - doc[attr2=new_value]
#     - table[dynamic=True, attr1=new_value, attr2=old_value]
#   - subfolder2[attr1=old_value, attr2=value]
#     - doc[attr1=old_value]
#   - subfolder3[attr1=old_value, attr2=value]
#     - doc[]
```
