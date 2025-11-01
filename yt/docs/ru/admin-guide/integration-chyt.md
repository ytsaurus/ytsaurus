# Интеграция CHYT с внешними системами
Данная инструкция показывает, как можно интегрировать CHYT с внешними системами. CHYT поддерживает интерфейсы взаимодействия с внешними системами по аналогии с ClickHouse по HTTP/JDBC.

## Подключение Trino к CHYT
1. [Откройте порты](../user-guide/data-processing/chyt/try-chyt.md#chyt_port) ClickHouse в конфигурации http-proxy.
1. Настройте Database-as-Directory.
   В спеклете выбранной клики в блоке YT config укажите путь до директории, которая будет представлена как база данных в терминах ClickHouse:
   ```json
   {
       ...
       "database_directories":
       {
           "dbo": "//home/my_db_folder"
       }
   }
   ```
   `dbo` &mdash; пример имени базы данных, которая будет содержать таблицы из директории `//home/my_db_folder`.

   После указания этой настройки запросы в клику будут выглядеть следующим образом:
   ```sql
   SELECT * FROM dbo.table_name;
   ```
   Это будет эквивалентно:
   ```sql
   SELECT * FROM `//home/my_db_folder/table_name`
   ```

1. Настройте Trino
   Подключение Trino к CHYT осуществляется через встроенный коннектор к ClickHouse. Создайте файл с конфигурацией нашего подключения `/etc/catalog/ch.properties`:
   ```text
   connector.name=clickhouse
   connection-url=jdbc:clickhouse://my_proxy_url:8123/YT?chyt.clique_alias=my_clique_name
   connection-user=my_clique_name
   connection-password=my_token
   ```
   где `my_proxy_url` &mdash; адрес прокси, `my_clique_name` &mdash; имя клики, `my_token` &mdash; токен доступа к {{product-name}}.

После этого в Trino будет доступен каталог `ch`, таблицы которого находятся на кластере {{product-name}} и будут доступны через клику `my_clique_name`.
