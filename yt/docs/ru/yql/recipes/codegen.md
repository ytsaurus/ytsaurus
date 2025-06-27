# Рецепты по использованию кодогенерации

## Введение
При работе с таблицами, в которых содержится большое количество колонок, бывает утомительно перечислять их снова и снова в разных частях SQL запроса, в том случае, когда невозможно использовать выборку всех колонок через `*` и, опционально, `WITHOUT`. Также иногда выбор колонок может основываться кроме имени еще и на их типе данных. Для того, чтобы не генерировать текст SQL запроса можно воспользоваться механизмами [интроспекции типов](../builtins/types.md) и [кодогенерации](../builtins/codegen.md). При этом сама генерация кода проводится до начала выполнения основного запроса и полученные программы полностью эквивалентны написанным вручную.

При использовании кодогенерации важно учитывать то, что из-за того, что она выполняется до начала основного запроса, в процессе кодогенерации можно использовать только типы колонок таблиц, но не их значения. Также без ограничений можно использовать константы и выражения, не зависящие от колонок таблиц.

Далее мы рассмотрим типовые сценарии и способы их решения.

## Сценарии

### Декларировать тип UDF с помощью вычислений

```yql
$tuple = AsTuple(1, "foo", AsList(-1.2, 3.0));

$tupleType = TypeOf($tuple);
$reverseTupleType = EvaluateType(TupleTypeHandle(
    ListReverse(TupleTypeComponents(TypeHandle($tupleType)))));

$script = @@
def reverse_any_tuple(t):
   return tuple(reversed(list(t)))
@@;


$udfType = CallableType(0, $reverseTupleType, $tupleType);
$udf = {{python-lib-qt}}::reverse_any_tuple($udfType, $script);

SELECT $udf($tuple);
```

Ссылки на документацию:


* [EvaluateType](../builtins/types.md#evaluatetype)


### Фильтрация колонок по списку имен

```yql
USE {{production-cluster}};

$makeFieldsSelector = ($fields) -> {
    RETURN EvaluateCode(LambdaCode(($row) -> {
        $items = ListMap($fields, ($f) -> {
            $atom = AtomCode($f);
            RETURN ListCode($atom, FuncCode("Member", $row, $atom));
        });

        RETURN FuncCode("AsStruct", $items);
    }))
};

$selectFields1 = $makeFieldsSelector(AsList("age", "name", "region"));
$selectFields2 = $makeFieldsSelector(AsList("age", "name"));

INSERT INTO @tmp1
SELECT * FROM (
    SELECT $selectFields1(TableRow()) FROM `home/yql/tutorial/users`
    WHERE region != 225
) FLATTEN COLUMNS;

INSERT INTO @tmp2
SELECT * FROM (
    SELECT $selectFields2(TableRow()) FROM `home/yql/tutorial/users`
    WHERE region = 225
) FLATTEN COLUMNS;

COMMIT;

SELECT * FROM @tmp1;
SELECT * FROM @tmp2;
```

Ссылки на документацию:


* [EvaluateCode](../builtins/codegen.md#evaluatecode)
{% if audience == "internal" %}
* [Member]({{yql.s-expressions-link}}/functions#member)
* [AsStruct]({{yql.s-expressions-link}}/functions#asstruct)
{% endif %}
* [ListMap](../builtins/list.md#listmap)
* [Lambda функции](../syntax/expressions.md#lambda)
* [TableRow](../builtins/basic.md#tablerow)


### Поиск во всех строковых колонках

```yql
USE {{production-cluster}};

$containsAnyColumns = ($strValue, $needle) -> {
    -- Лямбда функция - предикат, которая будет применяться к значению в каждой колонки
    $func = ($where) -> {
        RETURN String::Contains($where, $needle);
    };

    $code = EvaluateCode(LambdaCode(($strCode) -> {
        -- Выбираем список полей структуры
        $members = StructTypeComponents(TypeHandle(TypeOf($strValue)));

        $filteredMembers = ListFilter($members, ($x) -> {
             $type = $x.Type;
             -- Убираем опциональный тип верхнего уровня
             $cleanType = If (TypeKind($type) == "Optional", OptionalItemType($type), $type);
             -- Оставляем только колонки с типом String или String?
             RETURN TypeKind($cleanType) == "Data" AND
                DataTypeComponents($cleanType)[0] == "String";
        });

        -- Генерируем выражение вида $func(column1) OR $func(column2)...
        RETURN Yql::Fold($filteredMembers, ReprCode(false), ($item, $state) -> {
            $member = FuncCode("Member", $strCode, AtomCode($item.Name));
            $apply = FuncCode("Apply", QuoteCode($func), $member);
            RETURN FuncCode("Or", $state, $apply);
        });
    }));

    RETURN $code($strValue);
};

SELECT * FROM `home/yql/tutorial/users`
WHERE $containsAnyColumns(TableRow(), "comment");
```

Ссылки на документацию:


* [EvaluateCode](../builtins/codegen.md#evaluatecode)
* [ListFilter](../builtins/list.md#listmap)
{% if audience == "internal" %}
* [Member]({{yql.s-expressions-link}}/functions#member)
* [Apply]({{yql.s-expressions-link}}/functions#apply)
* [Fold]({{yql.s-expressions-link}}/functions#fold)
{% endif %}
* [Lambda функции](../syntax/expressions.md#lambda)
* [TableRow](../builtins/basic.md#tablerow)

### Слияние структур слева направо

```yql

-- Удаляет из типа данных Optional верхнего уровня
$removeOptional = ($type) -> {
    RETURN IF(TypeKind($type) == "Optional", OptionalItemType($type), $type);
};

$combineMembersLeft = ($str1, $str2) -> {
    $code = EvaluateCode(LambdaCode(($str1code, $str2code) -> {
        -- Получаем списки полей обоих структур
        $leftMembers = StructTypeComponents($removeOptional(TypeHandle(TypeOf($str1))));
        $rightMembers = StructTypeComponents($removeOptional(TypeHandle(TypeOf($str2))));

        -- Генерируем получение полей из левой структуры
        $members1 = ListMap($leftMembers, ($x) -> {
            $atom = AtomCode($x.Name);
            RETURN ListCode($atom, FuncCode("Member", $str1code, $atom));
        });

        -- Собираем в множество имена полей левой структуры для удобного поиска
        $leftNames = ToSet(ListExtract($leftMembers, "Name"));

        -- Генерируем получение полей из правой структуры, если их еще не было в левой
        $members2 = ListFlatMap($rightMembers, ($x) -> {
            $atom = AtomCode($x.Name);
            $member = ListCode($atom, FuncCode("Member", $str2code, $atom));
            RETURN IF(DictContains($leftNames, $x.Name), NULL, $member);
        });

        -- Формируем финальную структуру
        RETURN FuncCode("AsStruct", ListExtend($members1, $members2));
    }));

    RETURN $code($str1, $str2);
};

SELECT $combineMembersLeft(
    Just(AsStruct(1 as a, 2 as b)),
    AsStruct(3 as c, 4 as b)
);
-- (a:1, b:2, c:3)
```

Ссылки на документацию:


* [EvaluateCode](../builtins/codegen.md#evaluatecode)
* [ListMap](../builtins/list.md#listmap)
{% if audience == "internal" %}
* [Member]({{yql.s-expressions-link}}/functions#member)
* [AsStruct]({{yql.s-expressions-link}}/functions#asstruct)
{% endif %}
* [Lambda функции](../syntax/expressions.md#lambda)

### Прочитать колонку по имени, записанном в другой колонке

```yql
$input = AsList(
    AsStruct(1 as key, "value1" as index, "foo" as value1),
    AsStruct(2 as key, "value2" as index, "bar" as value2),
    AsStruct(3 as key, "value3" as index)
);

-- Построить лямбду, которая для входной строки таблицы возвращает
-- две колонки - ключ, копируемый без изменений, и значение,
-- которое извлекается из колонки, чье имя записано в индексной колонке.
$makeDynamicAccessor = ($keyColumn, $indexColumn, $valueColumn) -> {
    RETURN ($row) -> {
        RETURN EvaluateCode(LambdaCode(($rowCode) -> {
            -- Строим список всех полей кроме ключевого и индексного
            $names = ListExtract(StructTypeComponents(TypeHandle(TypeOf($row))), "Name");
            $names = ListFilter($names, ($x) -> {
                    return $x != $keyColumn and $x != $indexColumn
            });

            -- Строим словарь из имени колонки в variant
            $dict = EvaluateCode(FuncCode("AsDict",
                ListMap($names,
                    ($x) -> {
                        RETURN ListCode(ReprCode($x),
                            FuncCode("Variant", FuncCode("Void"), AtomCode($x),
                            FuncCode("VariantType", FuncCode("StructType",
                                ListMap($names, ($x) -> {
                                    RETURN ListCode(AtomCode($x), FuncCode("VoidType")) })))))
                    })));

            -- Значение индексной колонки
            $indexValue = FuncCode("Member", $rowCode, AtomCode($indexColumn));

            -- Ключевую колонку берем без изменений
            $keyMember = ListCode(AtomCode($keyColumn),
                FuncCode("Member", $rowCode, AtomCode($keyColumn)));

            -- Выполняем поиск в словаре
            $enumLookup = FuncCode("Lookup", ReprCode($dict), $indexValue);

            -- Обрабатываем найденное в словаре значение
            $value = FuncCode("FlatMap", $enumLookup, LambdaCode(($x) -> {
                -- Строим список обработчиков для каждого имени колонки
                $handlers = ListFlatMap($names, ($name) -> {
                    RETURN AsList(AtomCode($name),
                        LambdaCode(($u) -> {
                            RETURN FuncCode("Member", $rowCode, AtomCode($name));
                        }));
                });

                -- Применяем визитор к найденному значению
                RETURN FuncCode("Visit", $x, $handlers);
            }));

            $valueMember = ListCode(AtomCode($valueColumn), $value);

            -- Собираем выходную структуру из ключа и значения
            RETURN FuncCode("AsStruct", AsList($keyMember, $valueMember));
        }))($row);
    };
};

SELECT * FROM (
    SELECT $makeDynamicAccessor("key","index","value")(TableRow())
    FROM AS_TABLE($input)
) FLATTEN COLUMNS
```

Ссылки на документацию:


* [EvaluateCode](../builtins/codegen.md#evaluatecode)
* [ListMap](../builtins/list.md#listmap)
{% if audience == "internal" %}
* [Member]({{yql.s-expressions-link}}/functions#member)
* [AsStruct]({{yql.s-expressions-link}}/functions#asstruct)
* [Visit]({{yql.s-expressions-link}}/functions#visit)
{% endif %}
* [Lambda функции](../syntax/expressions.md#lambda)

### Посчитать сумму по элементам кортежа

```yql
$t = AsTuple(1,2,3);

$sumTuple = ($tupleValue) -> {
    $code = EvaluateCode(LambdaCode(($tupleCode) -> {
        -- Вычисляем размер кортежа
        $count = ListLength(TupleTypeComponents(TypeHandle(TypeOf($tupleValue))));
        -- Генерируем сумму
        $pair = Yql::Fold(ListFromRange(0ul, $count), AsTuple(ReprCode(0), 0), ($item, $state) -> {
            -- $state это пара из кода для суммы и текущего индекса
            RETURN AsTuple(
                FuncCode("+", $state.0, FuncCode("Nth", $tupleCode, AtomCode(
                    CAST($state.1 as string)))),
                $state.1 + 1
            )
        });

        RETURN $pair.0;
    }));

    RETURN $code($tupleValue);
};

SELECT $sumTuple($t);
```

Ссылки на документацию:


* [EvaluateCode](../builtins/codegen.md#evaluatecode)
{% if audience == "internal" %}
* [Nth]({{yql.s-expressions-link}}/functions#nth)
* [Fold]({{yql.s-expressions-link}}/functions#fold)
{% endif %}
* [Lambda функции](../syntax/expressions.md#lambda)

### Агрегация над всеми числовыми колонками

Выбрать все числовые колонки в таблице, применить к ним несколько агрегационных функций, и записать в выходные колонки, добавив к имени название агрегационной функции.

```yql
USE {{production-cluster}};

-- Удаляет из типа данных Optional верхнего уровня
$removeOptional = ($type) -> {
    RETURN IF(TypeKind($type) == "Optional", OptionalItemType($type), $type);
};

-- Фильтруем только колонки с числами и кладем в структуру nums
$filterOnlyNumericColumns = ($strValue) -> {
    $code = EvaluateCode(LambdaCode(($str) -> {
        $members = StructTypeComponents(TypeHandle(TypeOf($strValue)));
        $filteredMembers = ListFilter($members, ($x) -> {
            $type = $x.Type;
            $cleanType = $removeOptional($type);
            RETURN TypeKind($cleanType) == "Data" AND
                DataTypeComponents($cleanType)[0] REGEXP "Int[0-9]+|Uint[0-9]+|Float|Double"
        });

        $list = ListMap($filteredMembers, ($x) -> {
            RETURN ListCode(AtomCode($x.Name), FuncCode("Member", $str, AtomCode($x.Name)));
        });

        RETURN FuncCode("AsStruct",$list);
    }));

    RETURN $code($strValue);
};

$nums = (SELECT $filterOnlyNumericColumns(TableRow()) as nums FROM `home/yql/tutorial/users`);

SELECT * FROM $nums;

-- Применяем заданную агрегационную функцию одновременно ко всей структуре nums.
-- Результаты агрегаций - вложенные структуры для колонок count/min/...
$agg = (SELECT
    MULTI_AGGREGATE_BY(nums, AggregationFactory("count")) as count,
    MULTI_AGGREGATE_BY(nums, AggregationFactory("min")) as min,
    MULTI_AGGREGATE_BY(nums, AggregationFactory("max")) as max,
    MULTI_AGGREGATE_BY(nums, AggregationFactory("avg")) as avg,
    MULTI_AGGREGATE_BY(nums, AggregationFactory("percentile", 0.9)) as p90
FROM $nums);

SELECT * FROM $agg;

-- Развернем структуры так, чтобы колонки были вида originalcolumn_aggrfunc.
-- Просто FLATTEN COLUMNS тут не справится, т.к. есть конфликты - в count/min и т.п.
-- вложенные поля одинаковые.

$rotateStruct = ($value) -> {
    $code = EvaluateCode(LambdaCode(($strValue) -> {
        -- Получаем колонки-структуры верхнего уровня
        $topMembers = StructTypeComponents(TypeHandle(TypeOf($value)));
        $list = ListFlatMap($topMembers, ($x) -> {
            $topMember = FuncCode("Member", $strValue, AtomCode($x.Name));
            -- Обходим поля в структурах
            $nestedMembers = StructTypeComponents($removeOptional($x.Type));
            RETURN ListMap($nestedMembers, ($y) -> {
                RETURN ListCode(AtomCode($y.Name || "_" || $x.Name),
                    FuncCode("Member", $topMember, AtomCode($y.Name)));
            })
        });

        RETURN FuncCode("AsStruct", $list);
    }));

    RETURN $code($value);
};

$rotate = (SELECT $rotateStruct(TableRow()) FROM $agg);

SELECT * FROM $rotate;
```

Ссылки на документацию:


* [EvaluateCode](../builtins/codegen.md#evaluatecode)
* [ListFilter](../builtins/list.md#listfilter)
* [ListMap](../builtins/list.md#listmap)
{% if audience == "internal" %}
* [Member]({{yql.s-expressions-link}}/functions#member)
* [AsStruct]({{yql.s-expressions-link}}/functions#asstruct)
{% endif %}
* [Lambda функции](../syntax/expressions.md#lambda)
* [MULTI_AGGREGATE_BY](../builtins/aggregation.md#multiaggregateby)
* [AggregationFactory](../builtins/basic.md#aggregationfactory)

### Транспонировать таблицу (PIVOT)

```yql
-- Пример данных
$input = AsList(
    AsStruct("2012" as year, 1 as key, 3.0 as value),
    AsStruct("2013" as year, 1 as key, 4.0 as value),
    AsStruct("2013" as year, 2 as key, 5.0 as value),
    AsStruct("2014" as year, 1 as key, 6.0 as value),
    AsStruct("2014" as year, 1 as key, 7.0 as value)
);

$makePivot = ($nameColumn, $valueColumn, $nameList) -> {
    RETURN EvaluateCode(LambdaCode(($row) -> {
        $name = FuncCode("Member", $row, AtomCode($nameColumn));
        $value = FuncCode("Member", $row, AtomCode($valueColumn));
        $row = FuncCode("RemoveMember", $row, AtomCode($nameColumn));
        $row = FuncCode("RemoveMember", $row, AtomCode($valueColumn));

        $ensureOptional = ($x) -> {
            RETURN YQL::MatchType($x, AsAtom("Optional"), () -> { RETURN $x }, () -> { RETURN Just($x) });
        };

        $structItems = ListMap($nameList, ($item) -> {
            $adjustedValue = FuncCode("Apply", QuoteCode($ensureOptional), $value);
            $ifValue = FuncCode("FlatOptionalIf",
                FuncCode("Coalesce", FuncCode("==", $name, ReprCode($item)), ReprCode(false)),
                $adjustedValue);
            RETURN ListCode(AtomCode($item), $ifValue);
        });

        $struct = FuncCode("AsStruct", $structItems);
        RETURN FuncCode("AddMember", $row, AtomCode($valueColumn), $struct);
    }));
};

-- Строим PIVOT-операцию:
-- 1. Значение поля `value` меняется на структуру, в которой созданы колонки, указанные в последнем списке.
-- 2. Согласно значению в поле `year` заполняется та или иная ячейка в выходной структуре `value`.
-- 3. После этого поле `year` удаляется.

$pivot = $makePivot("year", "value", AsList("2012", "2013", "2014"));

-- Исходные данные
SELECT * FROM AS_TABLE($input);

$x = (SELECT * FROM (
    SELECT $pivot(TableRow()) FROM AS_TABLE($input)
) FLATTEN COLUMNS);

-- Результат
SELECT * FROM (
    SELECT
        AsStruct(key as key),
        MULTI_AGGREGATE_BY(value, AggregationFactory("sum"))
    FROM $x GROUP BY key
) FLATTEN COLUMNS;
```

Ссылки на документацию:


* [EvaluateCode](../builtins/codegen.md#evaluatecode)
* [ListMap](../builtins/list.md#listmap)
{% if audience == "internal" %}
* [Member]({{yql.s-expressions-link}}/functions#member)
* [AddMember]({{yql.s-expressions-link}}/functions#addmember)
* [RemoveMember]({{yql.s-expressions-link}}/functions#removemember)
* [AsStruct]({{yql.s-expressions-link}}/functions#asstruct)
{% endif %}
* [Lambda функции](../syntax/expressions.md#lambda)
* [MULTI_AGGREGATE_BY](../builtins/aggregation.md#multiaggregateby)
* [AggregationFactory](../builtins/basic.md#aggregationfactory)

### Объединить шаблоны подзапросов

```yql
$combineQueries = ($query, $list) -> {
    RETURN EvaluateCode(LambdaCode(($world) -> {
        -- Неявным параметром шаблона подзапроса является аргумент world, через который передаются
        -- зависимости, например, видимые PRAGMA или операции COMMIT в точке использования шаблона подзапроса.
        $queries = ListMap($list, ($arg) -> {
                -- Передаем world дальше первым аргументом шаблона подзапроса
                RETURN FuncCode("Apply", QuoteCode($query), $world, ReprCode($arg))
            });

        -- Объединяем все результаты в общий список, при этом требуется совпадение типов.
        -- Для более слабого объединения можно воспользоваться функцией UnionAll.
        RETURN FuncCode("Extend", $queries);
    }));
};

DEFINE SUBQUERY $sub($n) AS
   SELECT $n;
END DEFINE;

-- Построить шаблон запроса, который получается если объединить
-- результаты подстановки шаблона подзапроса $sub для каждого значения от 0 до 9 включительно.
$fullQuery = $combineQueries($sub, ListFromRange(0, 10));
SELECT * FROM $fullQuery();
```

Ссылки на документацию:


* [EvaluateCode](../builtins/codegen.md#evaluatecode)
* [ListMap](../builtins/list.md#listmap)
{% if audience == "internal" %}
* [Apply]({{yql.s-expressions-link}}/functions#apply)
* [Extend]({{yql.s-expressions-link}}/functions#extend)
{% endif %}
* [Lambda функции](../syntax/expressions.md#lambda)
* [Шаблоны подзапросов](../syntax/subquery.md)

### Построить шаблон подзапроса с сортировкой по списку колонок

```yql
USE {{production-cluster}};

$sorted = ($world, $input, $orderByColumns, $asc) -> {
    $n = ListLength($orderByColumns);

    $keySelector = LambdaCode(($row) -> {
        $items = ListMap($orderByColumns,
            ($x) -> {
                RETURN FuncCode("Member", $row, AtomCode($x));
            });
        RETURN ListCode($items);
    });

    $sort = EvaluateCode(LambdaCode(($x) -> {
        return FuncCode("Sort",
            $x,
            ListCode(ListReplicate(ReprCode($asc), $n)),
            $keySelector)
    }));

    RETURN $sort($input($world));
};

DEFINE SUBQUERY $source() AS
    PROCESS `home/yql/tutorial/users`;
END DEFINE;

PROCESS $sorted($source, AsList("name","age"), true);
PROCESS $sorted($source, AsList("name"), true);
PROCESS $sorted($source, ListCreate(TypeOf("")), true);
```

Ссылки на документацию:


* [EvaluateCode](../builtins/codegen.md#evaluatecode)
* [ListMap](../builtins/list.md#listmap)
{% if audience == "internal" %}
* [Member]({{yql.s-expressions-link}}/functions#member)
* [Sort]({{yql.s-expressions-link}}/functions#sort)
{% endif %}
* [Lambda функции](../syntax/expressions.md#lambda)
* [Шаблоны подзапросов](../syntax/subquery.md)
