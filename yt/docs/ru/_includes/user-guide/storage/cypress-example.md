# Работа с деревом метаинформации

В данном разделе собраны наиболее популярные примеры по работе с [Кипарисом](../../../user-guide/storage/cypress.md) из командной строки (CLI), более подробное описание команд и их параметров можно найти в разделе [Команды](../../../api/commands.md).

## List { #list }

Данная команда выводит список всех ключей узла `<path>`, узел обязан иметь тип map_node.

### Описание команды list

```bash
$ yt list [-l] [--format FORMAT] [--attribute ATTRIBUTES]
[--max-size MAX_SIZE] [--absolute] <path>
```

Опция `-l` позволяет показывать дополнительные атрибуты (тип узла, аккаунт, объем занимаемого места, дата последнего изменения).

### Вызов команды list

```bash
$ yt list -l --max-size 3 //home/dev
string_node   dev            0 2018-12-22 09:30 some_key1
  map_node    dev         1248 2019-01-22 13:14 username1
  map_node    dev  69856453509 2019-02-26 09:05 username2
WARNING: list is incomplete, check --max-size option
```

Опция `--format` определяет формат выходных данных. Поддерживаются форматы `json` и `yson`. Подробнее можно прочитать в разделе [Форматы](../../../user-guide/storage/formats.md). Опцию `--format` нельзя указывать вместе с опцией `-l`.

### Вызов команды list с опцией format

```bash
$ yt list --format json //home/dev
["some_key1","username1","username2","some_key2","username3","some_key3"]
```

С помощью опции `--attribute` (которая может быть указана несколько раз) могут быть запрошены дополнительные атрибуты узлов, соответствующих перечисляемым ключам. К примеру, `--attribute account` добавит к каждому ключу аккаунт, которому принадлежит данный узел. Опцию `--attribute` можно использовать только совместно с опцией `--format`.

### Вызов команды list с опцией attribute

```bash
$ yt list --attribute account --attribute owner --format "<format=pretty>json" --max-size 3 //home/dev
{
    "$attributes": {
        "incomplete": true
    },
    "$value": [
        {
            "$attributes": {
                "account": "dev",
                "owner": "username1"
            },
            "$value": "some_key1"
        },
        {
            "$attributes": {
                "account": "dev",
                "owner": "username2"
            },
            "$value": "some_key2"
        },
        {
            "$attributes": {
                "account": "dev",
                "owner": "username3"
            },
            "$value": "some_key3"
        }
    ]
}
```

Опция `--max-size` позволяет ограничить количество возвращаемых элементов. Будут показаны произвольные элементы.

### Вызов команды list с опцией max-size

```bash
$ yt list --max-size 3 //home/dev
some_key1
username1
username2
WARNING: list is incomplete, check --max-size option
```

Опция `--absolute` покажет абсолютные пути.

### Вызов команды list с опцией absolute

```bash
$ yt list --absolute --max-size 3 //home/dev
//home/dev/some_key1
//home/dev/username1
//home/dev/username2
WARNING: list is incomplete, check --max-size option
```

## Create { #create }

Данная команда создает узел указанного типа `<type>` по пути `<path>`. Команда возвращает идентификатор созданного объекта. Поддерживаемые объекты перечислены в разделе [Объекты](../../../user-guide/storage/objects.md).

### Описание команды create

```bash
$ yt create [-r, --recursive] [-i,--ignore-existing] [-l,--lock-existing] [-f, --force] [--attributes ATTRIBUTES] <type> <path>
```

Опция `-r, --recursive` создаст все промежуточные узлы типа `map_node`.

### Вызов команды create с опцией recursive

```bash
$ yt create --recursive map_node //home/dev/test1/test2/test3
127c1-388f86-3fe012f-6f994b03
```

Опция `-i, --ignore-existing` не будет пересоздавать заново указанный узел, если он уже существует. Кроме того тип существующего узла и заказываемого должны совпадать, иначе выполнение запроса завершится ошибкой.

### Вызов команды create без опции ignore-existing

```bash
$ yt create map_node //home/dev/test1/test2/test3
Node //home/dev/test1/test2/test3 already exists
```
### Вызов команды create с опцией ignore-existing

```bash
$ yt create --ignore-existing map_node //home/dev/test1/test2/test3
127c1-388f86-3fe012f-6f994b03
```

Опцию `-l, --lock-existing` можно указывать вместе с `--ignore-existing`. Тогда на указанный узел будет взята [exclusive-блокировка](../../../user-guide/storage/transactions.md#locks), даже если он уже существует. (В случае конфликта блокировки команда завершится ошибкой.) Т.к. фактическое создание узла всегда неявно (автоматически) берет на него блокировку, опция позволяет сделать эффект команды при использовании `--ignore-existing` более предсказуемым.

Опция `-f, --force` позволяет принудительно создать объект. В случае, если указанный узел уже существует, он удаляется и на его месте создается новый. При этом существующий узел может быть любого типа. При пересоздании меняется id узла.

### Вызов команды create с опцией force

```bash
$ yt create --force map_node //home/dev/test1/test2/test3
127c1-4352be-3fe012f-6acab448
```

С помощью опции `--attributes` можно задать атрибуты создаваемого узла.

### Вызов команды create с опцией attributes

```bash
$ yt create --attributes "{test_attr=test1;}" map_node //home/dev/test1/test2/test3
127c2-1b78-3fe012f-288106fe
```
### Проверка результата команды create с опцией attributes
```bash
$ yt get //home/dev/test1/test2/test3/@test_attr
"test1"
```

## Remove { #remove }

Данная команда удаляет узел по указанному пути `<path>`. Если узел является составным и не пустым, то команда завершится с ошибкой.

### Описание команды remove

```bash
$ yt remove [-r, --recursive] [-f, --force] <path>
```

Опция `-r, --recursive` позволяет удалить все поддерево рекурсивно.

### Вызов команды remove без опции recursive

```bash
$ yt remove //home/dev/test1
Cannot remove non-empty composite node
```
### Вызов команды remove с опцией recursive
```bash
$ yt remove --recursive //home/dev/test1
```

Опция `-f, --force` позволяет игнорировать отсутствие указанного узла.

### Вызов команды remove без опции force

```bash
$ yt remove //home/dev/test1
Node //home/dev has no child with key "test1"
```
### Вызов команды remove с опцией force
```bash
$ yt remove --force //home/dev/test1
```

## Exists { #exists }

Данная команда проверяет существование переданного ей узла `<path>`.

### Описание команды exists

```bash
$ yt exists <path>
```
### Вызов команды exists над существующим узлом
```bash
$ yt exists //home/dev/test1/test2/test3
true
```
### Вызов команды exists над несуществующим узлом
```bash
$ yt exists //home/dev/test1/test2/test4
false
```

## Get { #get }

Данная команда выводит все поддерево Кипариса, находящееся по пути `<path>`.

### Описание команды get

```bash
$ yt get [--max-size MAX_SIZE] [--format FORMAT] [--attribute ATTRIBUTES] <path>
```

Путь `<path>` должен существовать.

### Вызов команды get над несуществующим узлом

```bash
$ yt get //home/dev/test1/test2/test4
Error resolving path //home/dev/test1/test2/test4
Node //home/dev/test1/test2 has no child with key "test4"
```
### Вызов команды get над существующим узлом
```bash
$ yt get //home/dev/test1/test2
{
    "test3" = {};
}
```
### Вызов команды get над атрибутом узла
```bash
$ yt get //home/dev/test1/test2/@account
"dev"
```

Опция `--max-size` позволяет ограничить количество узлов, которые будут выданы в случае виртуальных составных узлов.

{% note warning "Внимание" %}

Для обычных map-узлов в настоящий момент эта опция не имеет смысла.

{% endnote %}

### Вызов команды get без опции max-size
```bash
$ yt get //sys/transactions/@type
"transaction_map"
```
### Вызов команды get с опцией max-size
```bash
$ yt get --max-size 3 //sys/transactions
<
    "incomplete" = %true;
> {
    "12814-3b9ce8-3fe0001-3b10627" = #;
    "12817-1cc292-3fe0001-fcdca56f" = #;
    "1268e-42d32d-3fe0001-d0fc5c4f" = #;
}
```

Опция `--format` определяет формат выходных данных. Поддерживаются форматы `json` и `yson`. Подробнее можно прочитать в разделе [Форматы](../../../user-guide/storage/formats.md).

### Вызов команды get с опцией format

```bash
$ yt get --format "<format=pretty>json" //home/dev/test1
{
    "test2": {
        "test3": {

        }
    }
}
```

Объекты специальных типов, а также **непрозрачные** узлы (объекты с атрибутом `opaque` равным `%true` ) выводятся как entity. Атрибуты, связанные с узлами поддерева, по умолчанию не выводятся. Но их можно заказать для вывода с помощью опции `--attribute`, аналогично команде `list`.

### Вызов команды get с опцией attribute

```bash
$ yt get --attribute opaque //home/dev/test1/test2
<
    "opaque" = %false;
> {
    "test3" = <
        "opaque" = %false;
    > {};

```


Некоторые узлы могут вернуться как непрозрачные (у таких узлов не будет никаких дополнительных атрибутов), в случае если обходимое поддерево оказывается слишком большим.

## Set { #set }

Команда `set` присваивает указанное значение `value` в формате [YSON](../../../user-guide/storage/yson.md) по указанному пути `<path>`.

### Описание команды set

```bash
$ yt set [--format FORMAT] [-r, --recursive] [-f, --force]<path> <value>
```
### Вызов команды set
```bash
$ yt set //home/dev/test1/@some_attr test
```
### Проверка вызова команды set
```bash
$ yt get //home/dev/test1/@some_attr
"test"
```

Опция `--format` определяет формат значения `value`. Поддерживаются форматы `json` и `yson`. Подробнее можно прочитать в разделе [Форматы](../../../user-guide/storage/formats.md).

### Вызов команды set с опцией format

```bash
$ yt set --format json //home/dev/test1/test2/test4 '{"some_test_key":"some_test_value"}'
```
### Вызов команды set без опции format
```bash
$ yt get //home/dev/test1/test2/test4
{
    "some_test_key" = "some_test_value";
}
```

Опция `-r, --recursive` позволяет создать все несуществующие промежуточные узлы пути.

### Вызов команды set без опции recursive

```bash
$ yt set //home/dev/test1/test2/test3 some_test_value
Node //home/dev has no child with key "test1"
```

### Вызов команды set с опцией recursive
```bash
$ yt set --recursive //home/dev/test1/test2/test3 some_test_value
```

Опция `-f, --force` позволяет менять любые узлы Кипариса, а не только атрибуты и документы.

## Copy, Move { #copy_move }

Данная команда копирует/перемещает узел из пути `<source_path>` в `<destination_path>`. При копировании файлов или таблиц чанки физически не копируются, все изменения происходят на уровне метаинформации.

{% note warning "Внимание" %}

При перемещении сначала происходит копирование, а потом удаление исходного узла, таким образом идентификаторы всех объектов в поддереве изменятся.

{% endnote %}

### Описание команды copy/move

```bash
$ yt copy/move [--preserve-account | --no-preserve-account]
               [--preserve-expiration-time] [--preserve-expiration-timeout]
               [--preserve-creation-time]
               [-r, --recursive] [-f, --force]
               <source_path> <destination_path>
```

Опция `--preserve-account` позволяет сохранить аккаунт копируемого узла, вместо того, чтобы использовать аккаунт родительского каталога узла назначения.

Опция `--preserve-expiration-time` позволяет сохранить назначенное [время удаления](../../../user-guide/storage/cypress.md) копируемого узла.

Опция `--preserve-expiration-timeout` позволяет сохранить назначенный [интервал удаления](../../../user-guide/storage/cypress.md) копируемого узла.

Опция `--preserve-creation-time` позволяет сохранить [время создания](../../../user-guide/storage/cypress.md) копируемого узла. **Только для копирования.**

### Проверка аккаунта

```bash
$ yt get //home/dev/test1/test_file/@account
"dev"
```
### Вызов команды copy
```bash
$ yt copy //home/dev/test1/test_file //home/dev/test1/test2/test_file_new

yt get //home/dev/test1/test2/test_file_new/@account
"tmp"
```
### Вызов команды copy с опцией preserve-account
```bash
$ yt copy --preserve-account //home/dev/test1/test_file //home/dev/test1/test2/test_file_new_p

yt get //home/dev/test1/test2/test_file_new_p/@account
"dev"
```

Опция `-r, --recursive` регулирует поведение в случае, если не существуют промежуточные узлы `<destination_path>` . В случае, если опция включена, данные узлы будут рекурсивно созданы, иначе команда выдаст ошибку.

### Вызов команды copy без опции recursive

```bash
$ yt copy //home/dev/test1/test_file //home/dev/test1/test2/test3/test_file_new
Node //home/dev/test1/test2 has no child with key "test3"
```
### Вызов команды copy с опцией recursive
```bash
$ yt copy --recursive //home/dev/test1/test_file //home/dev/test1/test2/test3/test_file_new
```

Опция `-f, --force` позволяет выполнить команду, даже если узел назначения существует. Существующий узел при этом удаляется.

### Вызов команды copy без опции force

```bash
$ yt copy //home/dev/test1/test_file //home/dev/test1/test2/test_file_new
Node //home/dev/test1/test2/test_file_new already exists
```
### Вызов команды copy с опцией force
```bash
$ yt copy --force //home/dev/test1/test_file //home/dev/test1/test2/test_file_new
```

## Link { #link }

Данная команда создает [символическую ссылку](../../../user-guide/storage/links.md) по пути `<link_path>` на объект, находящийся по пути `<target_path>`.

### Описание команды link

```bash
$ yt link [-r, --recursive]
          [-i, --ignore-existing] [-l, --lock-existing]
          [-f, --force]
          <target_path> <link_path>
```

Опция `-r, --recursive` регулирует поведение в случае, если не существуют промежуточные узлы `<link_path>` . В случае, если опция включена, данные узлы будут рекурсивно созданы, иначе команда выдаст ошибку.

### Вызов команды link без опции recursive

```bash
$ yt link //home/dev/test1/test_file //home/dev/test1/test2/link_test_file
Node //home/dev/test1 has no child with key "test2"
```
### Вызов команды link с опцией recursive
```bash
$ yt link --recursive  //home/dev/test1/test_file //home/dev/test1/test2/link_test_file
```

Опция `-i, --ignore-existing` не будет пересоздавать заново указанную ссылку, если она уже существует.

### Вызов команды link без опции ignore-existing

```bash
$ yt link //home/dev/test1/test_file //home/dev/test1/test2/link_test_file
Node //home/dev/test1/test2/link_test_file already exists
```
### Вызов команды link с опцией ignore-existing
```bash
$ yt link --ignore-existing //home/dev/test1/test_file //home/dev/test1/test2/link_test_file
```

Опция `-l, --lock-existing` гарантирует, что на ссылку будет установлена [exclusive-блокировка](../../../user-guide/storage/transactions.md#locks), даже если ссылка уже существует. (Имеет смысл только вместе с `--ignore-existing`; в случае невозможности установить блокировку команда завершится ошибкой).

Опция `-f, --force` позволяет выполнить команду, даже если узел назначения существует. Существующий узел при этом удаляется.

## Concatenate { #concatenate }

Команда `concatenate` объединяет данные из узлов `<source_paths>` в узел `<destination_path>`. Все узлы обязаны существовать и также иметь одинаковый тип – быть либо таблицами, либо файлами.

{% note warning "Внимание" %}

Объединение данных происходит на уровне метаинформации, т. е. только на мастер-сервере.

{% endnote %}

### Описание команды concatenate
```bash
$ yt concatenate --src <source_paths> --dst <destination_path>
```

{% note warning "Внимание" %}

Время работы данного запроса прямо пропорционально количеству чанков, из которых (суммарно) состоят входные узлы.  При этом во многих клиентских библиотеках существует ограничение на максимальное время работы команд, обычно это десятки секунд. Поэтому использовать concatenate стоит только в случае, если вы уверены, что по выходным путям мало чанков, порядка 10 000. В остальных случаях следует использовать операцию [Merge](../../../user-guide/data-processing/operations/merge.md). Она не будет работать эффективнее (и на таблицах в десятки миллионы чанков может занимать многие минуты), но для клиента отслеживание ее работы будет осуществляться через периодический опрос статуса, что фактически снимает все ограничения по времени выполнения.

{% endnote %}
