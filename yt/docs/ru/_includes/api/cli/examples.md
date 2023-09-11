# Примеры

## Запуск простейшей операции

Для краткости в примерах ниже опущен параметр ```--proxy <cluster_name>``` или предполагается, что установлена переменная окружения ```YT_PROXY```.

```bash
# Создаем директорию, в которой будут храниться выходные таблицы
yt create map_node //tmp/yt_cli_examples

# Запускаем grep по данным из логов. Необходимо обязательно указать формат, в котором данные поступят на вход операции.
yt map 'grep "domain.com" || exit 0' --src //home/logs/search/2017-Q3 --dst //tmp/yt_cli_examples/grep_result_table --format dsv

....

# Смотрим, сколько получилось строк в выходной таблице
yt get //tmp/yt_cli_examples/grep_result_table/@row_count

# Смотрим на первые 10 строк. Указание формата обязательно.
yt read '//tmp/yt_cli_examples/grep_result_table[:#10]' --format dsv

# Считываем 6 и 7 строчки (нумерация с нуля) — в квадратных скобках задаётся полуинтервал, включающий левую границу и не включающий правую
yt read '//tmp/yt_cli_examples/grep_result_table[#5:#7]' --format dsv

# Поколоночное чтение:
yt read '//tmp/yt_cli_examples/grep_result_table{column1,column2}' --format dsv

# **Только для сортированной таблицы**
# Смотрим диапазон ключей в таблице c "key_begin" по "key_end" (полуинтервал, включающий левую границу и не включающий правую).
yt read '//tmp/yt_cli_examples/grep_result_table["key_begin":"key_end"]' --format dsv
```

## Команды get, set и remove

```bash
yt get //home/@count
36
yt set //home/@custom_attribute '{x=y}'
yt get //home/@custom_attribute
{
    "x" = "y"
}
yt remove //home/@custom_attribute
```

## Команды find и list

```bash
yt find / --name "dev"
//home/dev
//tmp/images/dev
//tmp/music/dev
...
yt list //home
abc_feedback
abt
abt-dev
abt-viewers
acid
...
yt list //home -l
map_node  abc_feedback 0.0B 2017-02-28 09:40 abc_feedback
map_node  abt          0.0B 2019-05-28 08:37 abt
map_node  abt-dev      0.0B 2019-04-28 20:23 abt-dev
map_node  abt-viewers  0.0B 2019-03-26 12:55 abt-viewers
map_node  dev          0.0B 2016-10-21 11:34 acid
...
```

## Команды read и write

```bash
echo "x=10" | yt write //tmp/yt_cli_examples/table --format dsv
yt read //tmp/yt_cli_examples/table --format dsv
x=10
```

```bash
echo -e "x=10\ty=20" | yt write //tmp/yt_cli_examples/table --format dsv
yt read //tmp/yt_cli_examples/table --format dsv
x=10   y=20
```

Опция `--format` является обязательной и регулирует [формат](../../../user-guide/storage/formats.md), в котором будут прочитаны данные.

Используя `  --format "<format=text>yson" --control-attributes '{enable_row_index=%true}'` можно узнать номер первой строки в заказанном диапазоне.

## Команды upload и download

```bash
cat binary | yt upload //tmp/yt_cli_examples/binary --executable
yt download //tmp/yt_cli_examples/binary > binary_copy
diff binary binary_copy
```

## Запуск операций

```bash
yt map "./binary" --src //tmp/yt_cli_examples/table --dst //tmp/yt_cli_examples/output --format dsv --local-file binary
2013-03-28 16:23:35.797 ( 0 min): operation 1535429-8d795980-9f7f5a9f-44bec919 initializing
...

# Сортируем по колонке x (по возрастанию)
yt sort --src //tmp/yt_cli_examples/table --dst //tmp/yt_cli_examples/output --sort-by "x"
...

# Сортируем по колонке x (по убыванию, доступно только на кластерах с мастерами версии 21.2+)
yt sort --src //tmp/yt_cli_examples/table --dst //tmp/yt_cli_examples/output --sort-by "{name=x; sort_order=descending;}"
...

# Сортируем по колонке x по убыванию, в случае равенства - по колонке y по возрастанию (сортировка по убыванию доступна только на кластерах с мастерами версии 21.2+)
yt sort --src //tmp/yt_cli_examples/table --dst //tmp/yt_cli_examples/output --sort-by "{name=x; sort_order=descending;}" --sort-by "y"
...

yt map-reduce --mapper cat --reducer cat --src //tmp/yt_cli_examples/table --dst //tmp/yt_cli_examples/output --format dsv --reduce-by "x"
...

yt map "cat" --spec "{pool=example_pool; weight=10.0}" --src //tmp/yt_cli_examples/table --dst //tmp/yt_cli_examples/output --format yson
...

yt vanilla --tasks '{task={job_count=10; command="sleep 60"; cpu_limit=2};}' --spec '{resource_limits={user_slots=2}}'
...
```