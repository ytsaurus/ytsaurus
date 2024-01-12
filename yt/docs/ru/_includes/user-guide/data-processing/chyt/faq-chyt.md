# FAQ

#### **Q: Почему в CHYT есть какие-то клики, тогда как в обычном ClickHouse ничего похожего нет? Что вообще такое клика?**

**A:** Про это есть отдельная статья [Концепции](../../../../user-guide/data-processing/chyt/general.md).

------

#### **Q: Получаю одну из ошибок `DB::NetException: Connection refused`, `DB::Exception: Attempt to read after eof: while receiving packet from`. Что это значит?**

**A:** Обычно такое означает, что какой-то инстанс ушел по тем или иным причинам. Можно посмотреть в [UI операции](../../../../user-guide/data-processing/chyt/cliques/ui.md#jobs) на счетчики числа aborted/failed джобов. Если есть (свежие) aborted-джобы по причине preemption, то это значит, что клике не хватает ресурсов. Если есть (свежие) failed джобы, стоит обратиться к администратору.

------

#### **Q: Что значит ошибка `Subquery exceeds data weight limit: XXX > YYY`?**

**A:** смотрите опцию `max_data_weight_per_subquery` в статье по [конфигурации](../../../../user-guide/data-processing/chyt/reference/configuration.md#configuration_example).

------

#### **Q: Как сохранять данные в таблицу?**

**A:** Есть функции **INSERT INTO** и **CREATE TABLE**, подробнее про них можно прочитать в разделе [Работа с таблицами {{product-name}}](../../../../user-guide/data-processing/chyt/yt-tables.md#save).

------

#### **Q: Как загрузить геословари в собственной клике?**

**A:** При старте любой клики можно указать опцию `--cypress-geodata-path`, которая позволяет указать путь к геословарям в Кипарисе. Подробнее про эту опцию можно прочитать в статье [Как попробовать](../../../../user-guide/data-processing/chyt/try-chyt.md).

------

#### **Q: А CHYT умеет обрабатывать даты в формате `TzDatetime`?**

**A:**  CHYT умеет обрабатывать даты в формате `TzDatetime` ровно в той же мере, в какой обычный ClickHouse (все те же функции доступны). Хранить данные придется в виде строк или чисел и конвертировать при чтении и записи. Пример извлечения даты:

```sql
toDate(reinterpretAsInt64(reverse(unhex(substring(hex(payment_dt), 1, 8)))))
```
------

#### **Q: Как переложить таблицу на SSD?** { #how-to-set-ssd }

**A:** Для начала необходимо убедиться, что в вашем аккаунте в {{product-name}} квота в [медиуме](../../../../user-guide/storage/media.md) **ssd_blobs**. Для этого можно перейти на {% if audience == "public" %}страницу **Аккаунтов**{% else %}[страницу аккаунтов](https://yt.yandex-team.ru/hahn/accounts/general?medium=ssd_blobs){% endif %}, переключить тип медиума на **ssd_blobs** и ввести название своего аккаунта. Если квоты в медиуме `ssd_blobs` нет, то ее стоит запросить у администратора.

После получения квоты на медиуме **ssd_blobs** необходимо изменить значение атрибута `primary_medium`, данные будут в фоне переложены на соответствующий медиум.

Для статических таблиц можно форсировать перекладывание с помощью операции [Merge](../../../../user-guide/data-processing/operations/merge.md).

```bash
yt set //home/dev/test_table/@primary_medium ssd_blobs
yt merge --mode auto --spec '{"force_transform"=true;}' --src //home/dev/test_table --dst //home/dev/test_table
```

Если таблица динамическая, для изменения медиума нужно предварительно отмонтировать таблицу,
установить атрибут, а затем примонтировать обратно:

```bash
yt unmount-table //home/dev/test_table --sync
yt set //home/dev/test_table/@primary_medium ssd_blobs
yt mount-table //home/dev/test_table --sync
```

Дополнительно ускорить перекладывание можно с помощью [forced_compaction](../../../../user-guide/dynamic-tables/overview.md#attributes), однако использование этого метода создает большую нагрузку на кластер и сильно не рекомендуется.


Чтобы проверить, что таблица действительно изменила медиум, можно воспользоваться командой:

```bash
yt get //home/dev/test_table/@resource_usage

{
    "tablet_count" = 0;
    "disk_space_per_medium" = {
        "ssd_blobs" = 930;
    };
    "tablet_static_memory" = 0;
    "disk_space" = 930;
    "node_count" = 1;
    "chunk_count" = 1;
}
```

------

#### **Q: Поддерживается ли конструкция `SAMPLE` языка ClickHouse?**

**A:** CHYT поддерживает конструкцию `Sample`. Отличие заключается в том, что CHYT игнорирует команду `OFFSET ...`, таким образом нельзя получить выборку из другой части отобранных данных.

  Пример:

  ```SQL
  SELECT count(*) FROM "//tmp/sample_table" SAMPLE 0.05;

  SELECT count(*) FROM "//tmp/sample_table" SAMPLE 1/20;

  SELECT count(*) FROM "//tmp/sample_table" SAMPLE 100500;
  ```

------

#### **Q: Как получить имя таблицы в запросе?**

**A:** Можно воспользоваться виртуальными колонками `$table_name` и `$table_path`. Подробнее про виртуальные колонки можно прочитать в разделе [Работа с таблицами {{product-name}}](../../../../user-guide/data-processing/chyt/yt-tables.md#virtual_columns).
