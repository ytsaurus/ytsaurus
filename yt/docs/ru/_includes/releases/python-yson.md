## Python YSON bindings


Доступно в виде пакета в [PyPI](https://pypi.org/project/ytsaurus-yson/). История версий доступна на [PyPI](https://pypi.org/project/ytsaurus-yson/#history).




**Релизы:**

{% cut "**0.4.10**" %}

**Дата релиза:** 2025-03-05


**Страница релиза:** [0.4.10](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-yson/0.4.10)


**Пакет в PyPI:** [0.4.10](https://pypi.org/project/ytsaurus-yson/0.4.10/)


#### Новые возможности
   *  Поддержка `dump-parquet` в параллельном режиме
   *  Поддержка `read_table_structured` в параллельном режиме
   *  Поддержка повторов при загрузке parquet/orc
   *  Поддержка `dump-orc` в параллельном режиме
   *  Добавлена опция `min_batch_row_count` для dump parquet
  
#### Исправления
 * Исправлена ошибка `Unexpected end of stream` при загрузке parquet


{% endcut %}


{% cut "**0.4.9**" %}

**Дата релиза:** 2024-08-07


**Страница релиза:** [0.4.9](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-yson/0.4.9)


**Пакет в PyPI:** [0.4.9](https://pypi.org/project/ytsaurus-yson/0.4.9/)


Новые возможности:
  - Поддержка формата ORC
  - Доступ к thread local переменным через noinline функции
  - Поддержка Python 3.13 (отказ от устаревшей PyImport_ImportModuleNoBlock)

{% endcut %}


{% cut "**0.4.8**" %}

**Дата релиза:** 2024-04-24


**Страница релиза:** [0.4.8](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-yson/0.4.8)


**Пакет в PyPI:** [0.4.8](https://pypi.org/project/ytsaurus-yson/0.4.8/)


  * Добавлено создание таблицы при загрузке parquet
  * Уменьшен размер .so файла bindings


{% endcut %}


{% cut "**0.4.7**" %}

**Дата релиза:** 2024-03-09


**Страница релиза:** [0.4.7](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-yson/0.4.7)


**Пакет в PyPI:** [0.4.7](https://pypi.org/project/ytsaurus-yson/0.4.7/)


- Добавлена реализация `upload_parquet`
- Исправлен недопустимый доступ к памяти в YsonStringProxy

{% endcut %}