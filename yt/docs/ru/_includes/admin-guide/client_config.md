
Некоторые настройки {{product-name}} SDK специфичны конфигурации кластера. Для упрощения работы sdk получают их с кластера из предопределенного места в Кипарисе - `//sys/client_config/default`.


{% cut "**Создание клиентского конфига**" %}

Клиентский конфиг - это Документ, находящийся по пути `//sys/client_config/default`

```(bash)
yt create map_node //sys/client_config
yt create document //sys/client_config/default
```

Первичная настройка конфига (пример)
```(bash)
yt set //sys/client_config/default '{enable_proxy_discovery=%false}'
```

{% endcut %}


##### Описание полей конфига

NB: не все поля поддержаны во всех SDK

- `enable_proxy_discovery`, `bool`, по умолчанию включена. Использовать ли для "тяжелых" запросов (write/read/...) "тяжелые прокси". Актуально для небольших кластеров.
- `http_proxy_discovery_url`, `str`, устарело, по умолчанию - "hosts". Можно переопределить адрес получения проксей (к примеру добавить роль).
- `operation_link_template`, `str`. Шаблон для формирования ссылок на операции (имеет смысл если UI развернут на другом домене/пути)
- `query_link_template`, `str`. Аналогично предыдущему, но используется для формирования ссылок query tracker
- `strawberry_ctl_address`, `str`. Шаблон для адреса Strawberry контроллера
- `strawberry_cluster_name`,  `str`, по умолчанию None. Имя кластера в Strawberry контроллере, если оно отлично от proxy
- `max_replication_factor`, `int`, по умолчанию None (используются умолчания SDK). Максимальный replication_factor при загрузке таблиц/фалов. Имеет смысл для небольших кластеров.
- `python_enable_password_strength_validation`, `str`, по умолчанию False. Валидировать ли длинну пароля при `set_user_password`
- `python_pickling_ignore_system_modules`, `bool`, по умолчанию False. Управляет сбором зависимостей при запуске операций. Выключает сборк всех установленныех в "системный питон" пакетов.
- `python_pickling_dynamic_libraries_enable_auto_collection`, `bool`. Как и предыдущий пункт, влияет на сбор зависимостей. Выключает сбор бинарных библиотек у пакетов.
- `python_encrypt_pickle_files`, `bool`. Шифровать ли файлы со стейтом кода операции. Упрощает сбор зависимостей.
