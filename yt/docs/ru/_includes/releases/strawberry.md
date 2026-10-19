## Strawberry

Поставляется в виде docker-образа.

**Релизы:**

{% cut "**v0.0.17**" %}

**Дата релиза:** 2026-04-03

**Страница релиза:** [v0.0.17](https://github.com/ytsaurus/ytsaurus/releases/tag/yt/chyt/controller/v0.0.17)

**Docker-образ:** [ghcr.io/ytsaurus/strawberry:0.0.17](https://github.com/orgs/ytsaurus/packages/container/strawberry/775184666?tag=0.0.17)

{% cut "**v0.0.18**" %}

**Release date:** 2026-08-14


# Features

- Stop retrying operation restarts indefinitely when broken-state signal errors occur. After a configurable number of consecutive failures, mark the oplet as `Broken` ([902c7bc](https://github.com/ytsaurus/ytsaurus/commit/902c7bc08453873f7557e79ce2623a39b55130f4)).
- Check permissions for network projects ([ba5fad9](https://github.com/ytsaurus/ytsaurus/commit/ba5fad9864242ddb2dd58e0e74aa260d05fc1fa5)).
- Add a handler exposing agent metrics for active and failed oplets ([9844d63](https://github.com/ytsaurus/ytsaurus/commit/9844d630faf3d101f602995e788a0539116f1ec7)).
- Support the CHYT `dictionary_repository` configuration section and initialize the `storage_artifacts` directory ([d0c8921](https://github.com/ytsaurus/ytsaurus/commit/d0c892193a631108b48fdfeec72794ebb4beff51)).
- Expose the `chyt_running_version` attribute ([f794179](https://github.com/ytsaurus/ytsaurus/commit/f79417970e645ef1cedfc3faef9a53fff02f30fe)).
- Add oplet-pass duration histograms ([ec124b9](https://github.com/ytsaurus/ytsaurus/commit/ec124b9a4f4bd8d2b7776b9b66cdef5fb3899a88)).
- Add ODBC support for CHYT ([35e94c6](https://github.com/ytsaurus/ytsaurus/commit/35e94c6bc4296f8226bd0d60a35f5a538444cb82)).

# Fixes

- Resolve a deadlock between the operation collector and job checker ([83c0bf9](https://github.com/ytsaurus/ytsaurus/commit/83c0bf9d1529aec7caf70db902c6d1d204ce162f)).
- Avoid panicking when discovery-client initialization fails ([659a609](https://github.com/ytsaurus/ytsaurus/commit/659a609095b0d303c556386462dfd3fd391ac8a2)).
- Update the restart condition for CHYT version drift ([d58548e](https://github.com/ytsaurus/ytsaurus/commit/d58548e3b0cd09bf417c4f0c4cfbb052ed34bc85)).


{% endcut %}


{% cut "**v0.0.17**" %}

**Release date:** 2026-04-03


### General Updates

* Make discovery health check opt-in (Commit: 19a9ccd688af14ddf3cc7ab8cd3afdd195a02776)


{% endcut %}


{% cut "**v0.0.16**" %}

**Release date:** 2026-03-06


### General Updates

**Features:**

* Add CHYT option to automatically restart the clique if the version currently running the clique differs from the one indicated by the speclet (Commit: 36ba795)
* Add new config section to  specify the default speclet. This is necessary in order to be able to redefine the settings of oplets at one point in  the system. The per location configuration option is also supported. (Commit: c8aa165)
* Improved accuracy of CHYT clique health state (Commit: 1de3d55)
* The routine for checking completed jobs of the oplet operation for failures is presented. Added a monitoring handler to generate alerts about this. (Commit: 8a263b9)
* Present mechanism for excluding some parts from CHYT config specified in speclet. It is necessary to hide the redefinition of the service parts of the configuration. (Commit: 0b0510e)

**Fixes:**

* Fix logging for https cluster proxy urls. (Commit: 9d0b858)


{% endcut %}


{% cut "**v0.0.15**" %}

**Release date:** 2025-09-18


### General Updates

**Features:**

- Added mechanism for detecting changes in secrets and cluster connection (Commit: c1f711ed6c95b0370656f228759e89687a762402) @koct9i
- Added runtime specific info about oplets. Currently, it is supported only for the CHYT version. (Commit: d863aa541452997ab2d79e055cdf47db42fc9f9c) @Sonireg

{% endcut %}


{% cut "**v0.0.14**" %}

**Release date:** 2025-08-20


### General Updates

**Features:**

- Added Bearer token support to controller (Commit: ec97c80de07a3d1b0f42c5b1ca07c723f15130f0) @a-dyu

**Fixes:**

- Fixed pool validation. (Commit: 68b574792e50e355c59fbe1f65edb9a1feee4210) @dmi-feo
- Added family prefix to yt op alias to prevent alias collisions (Commit: 734958682f3a6a263c9d03574adc283267c40738) @dmi-feo

### CHYT

- Added options for mTLS (Commit: 642a88355b0972489ff4ff29b2cf7615c5b3f85f) @koct9i 
- Allowed to set default CHYT resources in strawberry config (Commit: e4ba1a582b48151eeb61714b06e417d4e3e06bf1) @dmi-feo
- Logging of http headers is enabled by default (Commit: b52dd26fb366eb388e124faa8eb950ab8c077226) @a-dyu
 
### Jupyter

- Allowed to run jupyt operations on behalf of user (Commit: c9b02c7f24b0cbf18c2bf35af01143240ca4a792) @dmi-feo

{% endcut %}


{% cut "**v0.0.13**" %}

**Release date:** 2025-04-08


### General Updates

**Features:**

- Improved support for Jupyter operations. Thanks to @dmi-feo and @thenno for the PRs!
- Added restart command to the controller. (Commit: 7eef7be610082c92ab7608fbebe58f64bf4db42d)
- Added location-based overriding of the strawberry config. (Commit: 53761978b238167d340a4c4f0ef8309a3555941d)
- Added the `pool_trees` speclet option. (Commit: 26a36552dcd47b4126e21b226532dd1b9c6c551a)
- Added a `filters` parameter to the `list` command. (Commit: 007a51cc8364fca85cb8c680b39265dffc76ceda)
- Added a config option to grant the `administer` right to the operation creator. (Commit: 40e9bff15e07d6128bd27b4e77963fe793312bd9)

**Fixes:**

- Fixed panic on initialization if the cluster is unavailable. (Commit: b567f1737aeadd3c1ee0eba0d7bc7b46ec66a789) 

### CHYT

- Added an option to disable the export of runtime data from CHYT operations and an explicit expiration timeout for exported data. (Commit: 58d91c249ee4aaf7d7be3070af58569f5f2ad1b9)
- Changed the default `read_from` option in AttributeCache. (Commit: 33de404dcce77593968fa45d548bf9ebceb3204e)

### SPYT

- Added Squashfs support for Livy via strawberry. (Commit: 15c08cd668eacdf57312dc9bcb01452faa82ce7d)
- Specified yt token for Livy operations. (Commit: c91680616715f65da1855669e082a914f9909973)

### Jupyter

- Added GPU support in Jupyter operations. (Commit: 935e0a5a7c2ffd4a45d3e4260f9aea9d4534a8c0)
- Added job scaler interface and used it to suspend inactive Jupyter operations. (Commit: e4b7df0c0213644f54af6411ed06b2bc34576059)

{% endcut %}


{% cut "**v0.0.12**" %}

* Проверка работоспособности discovery стала опциональной (коммит: 19a9ccd688af14ddf3cc7ab8cd3afdd195a02776)

{% endcut %}

{% cut "**v0.0.16**" %}

**Дата релиза:** 2026-03-06

**Страница релиза:** [v0.0.16](https://github.com/ytsaurus/ytsaurus/releases/tag/yt/chyt/controller/v0.0.16)

**Docker-образ:** [ghcr.io/ytsaurus/strawberry:0.0.16](https://github.com/orgs/ytsaurus/packages/container/strawberry/721097681?tag=0.0.16)

#### Общие обновления

**Новые возможности:**

* Добавлена опция CHYT для автоматического перезапуска клики, если версия, запущенная в данный момент, отличается от указанной в спектлете (коммит: 36ba795)
* Добавлена новая секция конфигурации для задания спектлета по умолчанию. Это необходимо для того, чтобы иметь возможность переопределять настройки оплетов в одной точке системы. Также поддерживается конфигурация для каждого расположения. (коммит: c8aa165)
* Повышена точность определения состояния здоровья клики CHYT (коммит: 1de3d55)
* Представлена процедура проверки завершённых джобов операции оплета на наличие сбоев. Добавлен мониторинговый обработчик для генерации алертов об этом. (коммит: 8a263b9)
* Представлен механизм исключения некоторых частей из конфигурации CHYT, указанной в спектлете. Это необходимо для скрытия переопределения служебных частей конфигурации. (коммит: 0b0510e)

**Исправления:**

* Исправлено журналирование для https-прокси URL кластера. (коммит: 9d0b858)

{% endcut %}

{% cut "**v0.0.15**" %}

**Дата релиза:** 2025-09-18

**Страница релиза:** [v0.0.15](https://github.com/ytsaurus/ytsaurus/releases/tag/yt/chyt/controller/v0.0.15)

**Docker-образ:** [ghcr.io/ytsaurus/strawberry:0.0.15](https://github.com/orgs/ytsaurus/packages/container/strawberry/524145257?tag=0.0.15)

#### Общие обновления

**Новые возможности:**

- Добавлен механизм обнаружения изменений в секретах и подключении к кластеру (коммит: c1f711ed6c95b0370656f228759e89687a762402) @koct9i
- Добавлена информация об оплетах, специфичная для среды выполнения. В настоящее время поддерживается только для версии CHYT. (коммит: d863aa541452997ab2d79e055cdf47db42fc9f9c) @Sonireg

{% endcut %}

{% cut "**v0.0.14**" %}

**Дата релиза:** 2025-08-20

**Страница релиза:** [v0.0.14](https://github.com/ytsaurus/ytsaurus/releases/tag/yt/chyt/controller/v0.0.14)

**Docker-образ:** [ghcr.io/ytsaurus/strawberry:0.0.14](https://github.com/orgs/ytsaurus/packages/container/strawberry/491844408?tag=0.0.14)

#### Общие обновления

**Новые возможности:**

- Добавлена поддержка Bearer-токенов в контроллер (коммит: ec97c80de07a3d1b0f42c5b1ca07c723f15130f0) @a-dyu

**Исправления:**

- Исправлена валидация пула. (коммит: 68b574792e50e355c59fbe1f65edb9a1feee4210) @dmi-feo
- Добавлен семейный префикс к алиасу операции YT для предотвращения коллизий алиасов (коммит: 734958682f3a6a263c9d03574adc283267c40738) @dmi-feo

#### CHYT

- Добавлены опции для mTLS (коммит: 642a88355b0972489ff4ff29b2cf7615c5b3f85f) @koct9i
- Добавлена возможность задавать ресурсы CHYT по умолчанию в конфигурации strawberry (коммит: e4ba1a582b48151eeb61714b06e417d4e3e06bf1) @dmi-feo
- Журналирование http-заголовков включено по умолчанию (коммит: b52dd26fb366eb388e124faa8eb950ab8c077226) @a-dyu

#### Jupyter

- Добавлена возможность запускать операции Jupyter от имени пользователя (коммит: c9b02c7f24b0cbf18c2bf35af01143240ca4a792) @dmi-feo

{% endcut %}

{% cut "**v0.0.13**" %}

**Дата релиза:** 2025-04-08

**Страница релиза:** [v0.0.13](https://github.com/ytsaurus/ytsaurus/releases/tag/yt/chyt/controller/v0.0.13)

**Docker-образ:** [ghcr.io/ytsaurus/strawberry:0.0.13](https://github.com/orgs/ytsaurus/packages/container/strawberry/391211630?tag=0.0.13)

#### Общие обновления

**Новые возможности:**

- Улучшена поддержка операций Jupyter. Спасибо @dmi-feo и @thenno за PR!
- Добавлена команда перезапуска в контроллер. (коммит: 7eef7be610082c92ab7608fbebe58f64bf4db42d)
- Добавлено переопределение конфигурации strawberry на основе расположения. (коммит: 53761978b238167d340a4c4f0ef8309a3555941d)
- Добавлена опция `pool_trees` в спектлет. (коммит: 26a36552dcd47b4126e21b226532dd1b9c6c551a)
- Добавлен параметр `filters` в команду `list`. (коммит: 007a51cc8364fca85cb8c680b39265dffc76ceda)
- Добавлена опция конфигурации для предоставления права `administer` создателю операции. (коммит: 40e9bff15e07d6128bd27b4e77963fe793312bd9)

**Исправления:**

- Исправлена паника при инициализации, если кластер недоступен. (коммит: b567f1737aeadd3c1ee0eba0d7bc7b46ec66a789)

#### CHYT

- Добавлена опция отключения экспорта данных среды выполнения из операций CHYT и явный таймаут истечения срока действия для экспортируемых данных. (коммит: 58d91c249ee4aaf7d7be3070af58569f5f2ad1b9)
- Изменено значение по умолчанию опции `read_from` в AttributeCache. (коммит: 33de404dcce77593968fa45d548bf9ebceb3204e)

#### SPYT

- Добавлена поддержка Squashfs для Livy через strawberry. (коммит: 15c08cd668eacdf57312dc9bcb01452faa82ce7d)
- Указан YT-токен для операций Livy. (коммит: c91680616715f65da1855669e082a914f9909973)

#### Jupyter

- Добавлена поддержка GPU в операциях Jupyter. (коммит: 935e0a5a7c2ffd4a45d3e4260f9aea9d4534a8c0)
- Добавлен интерфейс масштабирования джобов и использован для приостановки неактивных операций Jupyter. (коммит: e4b7df0c0213644f54af6411ed06b2bc34576059)

{% endcut %}

{% cut "**v0.0.12**" %}

**Дата релиза:** 2024-06-21

**Страница релиза:** [v0.0.12](https://github.com/ytsaurus/ytsaurus/releases/tag/yt/chyt/controller/v0.0.12)

**Docker-образ:** [ghcr.io/ytsaurus/strawberry:0.0.12](https://github.com/orgs/ytsaurus/packages/container/strawberry/233452797?tag=0.0.12)

**CHYT:**
- Значение по умолчанию опции `enable_geodata` теперь можно настроить; по умолчанию установлено значение `false` (PR: #667). Спасибо @thenno за PR!
- Настройка экспортера системных таблиц журналов при запуске клики

**Livy:**
- Добавлена поддержка SPYT Livy в контроллер

{% endcut %}
