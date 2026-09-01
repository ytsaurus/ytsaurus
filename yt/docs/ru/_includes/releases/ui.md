## UI


Поставляется в виде docker-образа.




**Релизы:**

{% cut "**3.17.1**" %}

**Дата релиза:** 2026-07-09


**Страница релиза:** [3.17.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v3.17.1)


**Docker-образ:** [ghcr.io/ytsaurus/ui:3.17.1](https://github.com/orgs/ytsaurus/packages/container/ui/1015719296?tag=3.17.1)


#### [3.17.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v3.17.0...ui-v3.17.1) (2026-07-09)


#### Исправления

* **Dockerfile:** не показывать приветственную страницу nginx [[#1694](https://github.com/ytsaurus/ytsaurus-ui/issues/1694)] ([5aa72ec](https://github.com/ytsaurus/ytsaurus-ui/commit/5aa72eceda1d7d187a1c440840ac9ae851976b40))
* **UI:** форматировать границы min/max в сообщениях об ошибках валидации NumberInput [YTFRONT-5452] ([50a0b07](https://github.com/ytsaurus/ytsaurus-ui/commit/50a0b0797f6a129c370efb48d68b6b9174fec7c8))

{% endcut %}


{% cut "**3.17.0**" %}

**Дата релиза:** 2026-07-02


**Страница релиза:** [3.17.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v3.17.0)


**Docker-образ:** [ghcr.io/ytsaurus/ui:3.17.0](https://github.com/orgs/ytsaurus/packages/container/ui/995139631?tag=3.17.0)


#### [3.17.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v3.16.0...ui-v3.17.0) (2026-07-02)


#### Новые возможности

* **Accounts:** предупреждение о ресурсах [YTFRONT-5886] ([3de1ce2](https://github.com/ytsaurus/ytsaurus-ui/commit/3de1ce2a1b0648633a73641b9be4835a62e9904b))


#### Исправления

* **Accounts:** исправлен текст ошибки при редактировании квоты [YTFRONT-5854] ([c8b226a](https://github.com/ytsaurus/ytsaurus-ui/commit/c8b226adb889af62158cc422096650e9f2ad5a40))
* **ACL/RowGroups:** сортировка групп по имени [YTFRONT-5758] ([7012f35](https://github.com/ytsaurus/ytsaurus-ui/commit/7012f354cbdb83f9080a2f753c687726ed649772))
* **App/Toaster:** улучшен цвет фона для тостов [YTFRONT-5856] ([f1c3219](https://github.com/ytsaurus/ytsaurus-ui/commit/f1c321962182e93c816124c0170a7f4587b6e054))
* **App:** удалён лишний импорт [YTFRONT-5870] ([1bb667f](https://github.com/ytsaurus/ytsaurus-ui/commit/1bb667f197939e3716e10e4d8a548c3f23c97234))
* **Navigation:** валидация пути для remote-copy [YTFRONT-5898] ([66a1471](https://github.com/ytsaurus/ytsaurus-ui/commit/66a1471387bb688d705046556c7ebecb046bf7a5))
* **Navigation:** типизированный вывод пользовательских атрибутов [YTFRONT-5880] ([a9092da](https://github.com/ytsaurus/ytsaurus-ui/commit/a9092daabd248b07d8773bfb6d2dbe13d218b170))
* **Operations/Incarnations:** копирование выделенного текста в разделе событий [YTFRONT-5740] ([64355b9](https://github.com/ytsaurus/ytsaurus-ui/commit/64355b94db51bc875ef7b4766f249261ba5e361f))
* **Operations:** неверные данные в модальном окне таймлайна [YTFRONT-5894] ([c7d6dd0](https://github.com/ytsaurus/ytsaurus-ui/commit/c7d6dd0c0dacae990544d129ff831fb28d168ff3))
* **PrometheusDashboards:** корректная обработка 'gbytes' [YTFRONT-5831] ([761d0d5](https://github.com/ytsaurus/ytsaurus-ui/commit/761d0d56c1a1505284008655119ff881d8847e6b))
* **Queries:** корректный цвет предупреждений внутри ошибок [YTFRONT-5264] ([9c12fca](https://github.com/ytsaurus/ytsaurus-ui/commit/9c12fca871baf3283a7e59613a2874f8e4a4ff8b))
* **Queries:** кириллическое имя в истории [YTFRONT-5881] ([287681d](https://github.com/ytsaurus/ytsaurus-ui/commit/287681d1594df05d73acca849cdd1c81c8dec17d))
* **Queries:** не отключать кнопку запуска до ответа контроллера [YTFRONT-5818] ([9c6304e](https://github.com/ytsaurus/ytsaurus-ui/commit/9c6304ea331357b80519bf836065c55bd903d9f2))
* **Queries:** не добавлять discovery path в spark connect [YTFRONT-5887] ([58c9f96](https://github.com/ytsaurus/ytsaurus-ui/commit/58c9f96cc60120ba1a948a6aab66e66bd0dce763))
* **Queries:** неверный aco по умолчанию [YTFRONT-5668] ([83dc3c0](https://github.com/ytsaurus/ytsaurus-ui/commit/83dc3c0975341dfe3828e17530e636a4af2571fc))
* **Scheduling/ACL:** сброс состояния acl для нового пути [YTFRONT-5848] ([a7be8dd](https://github.com/ytsaurus/ytsaurus-ui/commit/a7be8dd98db2705655df24aa353d5ff0cdfb0e02))
* **Scheduling/Attributes:** сохранение url-параметров pool и tree [YTFRONT-5867] ([058b8ef](https://github.com/ytsaurus/ytsaurus-ui/commit/058b8ef3e0f069f29dad34e22e99940313607066))
* **Scheduling:** исправлена опечатка [YTFRONT-5680] ([3416cc0](https://github.com/ytsaurus/ytsaurus-ui/commit/3416cc08b0f3e88e0211f4faaee69a72e2996d5e))
* **System:** ручной выбор контейнера [YTFRONT-5778] ([1f2423c](https://github.com/ytsaurus/ytsaurus-ui/commit/1f2423ce835a759fa5e473599f62f40008fde656))

{% endcut %}


{% cut "**3.16.0**" %}

**Дата релиза:** 2026-06-17


**Страница релиза:** [3.16.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v3.16.0)


**Docker-образ:** [ghcr.io/ytsaurus/ui:3.16.0](https://github.com/orgs/ytsaurus/packages/container/ui/953128211?tag=3.16.0)


#### [3.16.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v3.15.0...ui-v3.16.0) (2026-06-17)


#### Новые возможности

* **Components/node:** добавлена новая вкладка — таблица для exe-таблиц [YTFRONT-5037] ([640cea2](https://github.com/ytsaurus/ytsaurus-ui/commit/640cea24a519b9d097cbaa15b7c6121a4191ada1))


#### Исправления

* **Components/Operations:** редизайн диалога редактирования пулов и весов операции [YTFRONT-3779] ([2e5056d](https://github.com/ytsaurus/ytsaurus-ui/commit/2e5056d70cdec8699bfe4eb7fd0c196fe25aa083))
* **Navigation:** копирование выбранных путей [YTFRONT-5809] ([a0dd387](https://github.com/ytsaurus/ytsaurus-ui/commit/a0dd38759f3b02113ecb09c2061561ae90d9734a))

{% endcut %}


{% cut "**3.15.0**" %}

**Дата релиза:** 2026-06-11


**Страница релиза:** [3.15.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v3.15.0)


**Docker-образ:** [ghcr.io/ytsaurus/ui:3.15.0](https://github.com/orgs/ytsaurus/packages/container/ui/936669709?tag=3.15.0)


#### [3.15.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v3.14.0...ui-v3.15.0) (2026-06-10)


#### Новые возможности

* **DownloadManager:** добавлена вкладка CSV [YTFRONT-3983] ([4727540](https://github.com/ytsaurus/ytsaurus-ui/commit/47275403f529cc1ee4b2ce7d952d434052c623f9))
* **Navigation:** добавлен CurrentPathActions [YTFRONT-4037] ([d8acc22](https://github.com/ytsaurus/ytsaurus-ui/commit/d8acc229d2364eeb7b6d43a9ee9f5c44b252d28a))
* **Navigation/Attributes:** просмотр opaque-атрибутов [YTFRONT-2241] ([ce8596a](https://github.com/ytsaurus/ytsaurus-ui/commit/ce8596a24f1c2d4d11a82e28b7addce759b95f86))
* **Navigation/Table:** добавлен элемент мета-таблицы rls-access [YTFRONT-5782] ([13d7b3a](https://github.com/ytsaurus/ytsaurus-ui/commit/13d7b3a9d0ee95670faa1b71ca2611bcc73ac91f))
* **Queries/History:** добавлена проверка ctrl [YTFRONT-5793] ([bb5d027](https://github.com/ytsaurus/ytsaurus-ui/commit/bb5d027ef647c12877258b3bc740e0a732284462))


#### Исправления

* **Components/Suggest:** обработка 'Tab', 'Shift+Tab' [YTFRONT-5823] ([0508ae6](https://github.com/ytsaurus/ytsaurus-ui/commit/0508ae64ab1b108225c74f5e95eef659a3428fb3))
* **Flow:** исправлена опечатка [YTFRONT-5826] ([8ef5e88](https://github.com/ytsaurus/ytsaurus-ui/commit/8ef5e88ab1adf0310adc4ff86d018f07d3351c94))
* **Flow/Messages:** тело диалога прокручивается [YTFRONT-5780] ([84a810f](https://github.com/ytsaurus/ytsaurus-ui/commit/84a810fbc0ff9c3868d11aebf2d5dc75974bc316))
* **Operations:** исправлено отображение джобов операции [YTFRONT-5699] ([a9ffe4e](https://github.com/ytsaurus/ytsaurus-ui/commit/a9ffe4e6c4291fbda1aba46f796ddc16944e8f25))
* **Queries:** мелкое css-исправление [YTFRONT-5850] ([bf0e94c](https://github.com/ytsaurus/ytsaurus-ui/commit/bf0e94c82c78c379fdbe115bf327155c78e5f474))
* **SegmentedRadioGroupOrSelect:** добавлен новый компонент [YTFRONT-5790] ([eab0716](https://github.com/ytsaurus/ytsaurus-ui/commit/eab0716184cc2d1376a81d2b4d88ed7dab5fbcd1))
* **Scheduling:** разрешено любое имя пула в подсказках [YTFRONT-4228] ([9f115f4](https://github.com/ytsaurus/ytsaurus-ui/commit/9f115f4300520400797ceb1563e862a41fe50fff))
* **YTErrorBlock:** корректная обработка неожиданного формата ошибки [YTFRONT-5665] ([bb940ed](https://github.com/ytsaurus/ytsaurus-ui/commit/bb940eddfcb25e2045c631607b20bfc27368bab3))

{% endcut %}


{% cut "**3.13.0**" %}

**Дата релиза:** 2026-05-27


**Страница релиза:** [3.13.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v3.13.0)


**Docker-образ:** [ghcr.io/ytsaurus/ui:3.13.0](https://github.com/orgs/ytsaurus/packages/container/ui/896105506?tag=3.13.0)


#### [3.13.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v3.12.2...ui-v3.13.0) (2026-05-26)


#### Новые возможности

* **PrometheusDashboard:** использовать //sys/interface_monitoring [YTFRONT-5802] ([96ddd54](https://github.com/ytsaurus/ytsaurus-ui/commit/96ddd547b1f4a6d3022313768739daa9274c3fea))


#### Исправления

* **Dockerfile:** обновлён nginx/1.30.1 [YTFRONT-5787] ([9eb8033](https://github.com/ytsaurus/ytsaurus-ui/commit/9eb8033bf22628f5263986d1f0d273558d393530))
* **Queries/Timeline:** добавлен фон длительностей [YTFRONT-5811] ([7f7f10e](https://github.com/ytsaurus/ytsaurus-ui/commit/7f7f10e27bb6de505956e0f89f7469cf69ba7dc8))
* **Queries/Timeline:** неработающие ссылки [YTFRONT-5811] ([ed1f5a2](https://github.com/ytsaurus/ytsaurus-ui/commit/ed1f5a2e655ddb01b802e1982888bc60095f2cc6))
* **Queries/Timeline:** проблема с интервалом [YTFRONT-5811] ([68e4afa](https://github.com/ytsaurus/ytsaurus-ui/commit/68e4afa8fe50a3559e4f18026c582e7871cb3964))
* **UI:** тема типа данных [YTFRONT-5601] ([5f8bb0a](https://github.com/ytsaurus/ytsaurus-ui/commit/5f8bb0a3ecac9effc8547c20e2280aa7af1a5319))
* **Components:** сдвиг в кнопке буфера обмена [YTFRONT-5799] ([7838626](https://github.com/ytsaurus/ytsaurus-ui/commit/783862668ad2fdc1b583693be023e79839eb39c8))

{% endcut %}


{% cut "**3.12.1**" %}

**Дата релиза:** 2026-05-15


**Страница релиза:** [3.12.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v3.12.1)


**Docker-образ:** [ghcr.io/ytsaurus/ui:3.12.1](https://github.com/orgs/ytsaurus/packages/container/ui/867156894?tag=3.12.1)


#### [3.12.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v3.12.0...ui-v3.12.1) (2026-05-15)

#### Новые возможности

* **Queries/Navigation:** возможность редактировать путь навигации ([5221504](https://github.com/ytsaurus/ytsaurus-ui/commit/5221504cc5700fe32301ecc862e743f4d61b600f))
* **Queries:** сортировка результирующей таблицы [YTFRONT-5766] ([b91831c](https://github.com/ytsaurus/ytsaurus-ui/commit/b91831cbd4f0ec98196e416b1e17c1576d237938))
* **Components/Node** обновлена цветовая палитра [YTFRONT-4270] ([4782884](https://github.com/ytsaurus/ytsaurus-ui/commit/4782884c79880a6537945e6c9f5febead945a483))
* **Queries:** новые настройки spyt [YTFRONT-5378] ([a76ee3d](https://github.com/ytsaurus/ytsaurus-ui/commit/a76ee3da42282b072191e1e711b0d262c26535d3))
* **Queries/Search:** добавлен полнотекстовый поиск [YTFRONT-5697] ([806b52d](https://github.com/ytsaurus/ytsaurus-ui/commit/806b52d6d13af111b453188cbaebfd4a33f0fa2b))

#### Исправления

* **Navigation/ReplicatedTable:** исправлено 'автоматическое переключение режима таблицы' [YTFRONT-5784] ([44e6c66](https://github.com/ytsaurus/ytsaurus-ui/commit/44e6c662db6e623ec6b2c142fd904b1e633bb0a9))
* **ACL/ManageAcl:** добавлен параметр 'name' для updateAcl [YTFRONT-5762] ([7e636f0](https://github.com/ytsaurus/ytsaurus-ui/commit/7e636f05f8f5f8296c08668e1806cdf4e7ff1926))
* **Navigation/QueryTracker:** всегда показывать кнопку «Поделиться» [YTFRONT-5776] ([496267b](https://github.com/ytsaurus/ytsaurus-ui/commit/496267ba34aee07a54530967fe983e6bbad7b0c7))
* **Navigation/ReplicatedTable:** добавлена сортировка в реплицируемых таблицах и сохранение состояния в настройках [YTFRONT-5703] ([818040f](https://github.com/ytsaurus/ytsaurus-ui/commit/818040f0adcb1ef23f02cd2ee5d178167d52c623))
* **Navigation:** предпросмотр ячейки [YTFRONT-5751] ([92fc00f](https://github.com/ytsaurus/ytsaurus-ui/commit/92fc00fc5c976fd584b33b67e8c11cd532c1b362))
* **PathEditor:** открытие всплывающей подсказки при изменении ввода ([51fa332](https://github.com/ytsaurus/ytsaurus-ui/commit/51fa332f5f66b81b38cedb32d0c3f4ebdddb10fa))
* **Navigation/tablets:** исправление сортировки и отображения полей [YTFRONT-5611] ([6dce89d](https://github.com/ytsaurus/ytsaurus-ui/commit/6dce89d553ace9bd4f9a1d94b066de7b9a774044))
* **server/configure-app:** проверка clusterConfigPath,ytInterfaceSecret при запуске [YTFRONT-5764] ([05c6614](https://github.com/ytsaurus/ytsaurus-ui/commit/05c66146d17e731dcd37aabf4dbf5677df356fb7))
* **Components:** убрано написание фильтра ролей заглавными буквами [YTFRONT-5718] ([e794f63](https://github.com/ytsaurus/ytsaurus-ui/commit/e794f6324e1b00a6576faaff32216e8fd4720000))
* **Modal,SimpleModal:** не использовать устаревшие 'onClose', 'onOutsideClick' [YTFRONT-5695] ([a95f480](https://github.com/ytsaurus/ytsaurus-ui/commit/a95f480aafa998525cef9ef7596629df7b71249c))
* **Navigation/MapNode:** добавлено сообщение 'Выбрано слишком много элементов' [YTFRONT-5739] ([095feaf](https://github.com/ytsaurus/ytsaurus-ui/commit/095feaf75001b92a4e3fa378cadf39aeb953ad19))
* **Navigation/Queue:** исправлена опечатка [YTFRONT-5746] ([414c956](https://github.com/ytsaurus/ytsaurus-ui/commit/414c956ec1dade0efec69d154d9b78196b8580db))
* **Queries/Graph:** неверный кластер в ссылке [YTFRONT-5723] ([e56f1a5](https://github.com/ytsaurus/ytsaurus-ui/commit/e56f1a5cdef86d832c8db15cf45d939932c1aa89))
* **Jobs:** показывать все атрибуты джоба [YTFRONT-4414] ([492e1fa](https://github.com/ytsaurus/ytsaurus-ui/commit/492e1fa593f2b2718c84b5a86f74148fbf7213dc))


{% endcut %}

{% cut "**3.9.0**" %}

**Дата релиза:** 2026-04-16


**Страница релиза:** [3.9.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v3.9.0)


**Docker-образ:** [ghcr.io/ytsaurus/ui:3.9.0](https://github.com/orgs/ytsaurus/packages/container/ui/801427271?tag=3.9.0)


#### [3.9.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v3.8.0...ui-v3.9.0) (2026-04-16)


#### Новые возможности

* **Navigation/Description:** добавлена возможность редактировать внешнюю аннотацию [YTFRONT-5096] ([d3795b3](https://github.com/ytsaurus/ytsaurus-ui/commit/d3795b3fc1f717243c496ffb60dfb7066ec0e7c1))
* **Navigation/tablets:** в таблицы добавлены новые колонки [YTFRONT-5611] ([3bcb2c6](https://github.com/ytsaurus/ytsaurus-ui/commit/3bcb2c6f289a086f6a51fe340f733db60b5a52af))
* **Components/Nodes:** в информацию об узле добавлено поле register_time [YTFRONT-3578] ([732380b](https://github.com/ytsaurus/ytsaurus-ui/commit/732380bb0ac64d129b7806336d5d137262c18d96))
* **Navigation/ACL:** добавлено «Row groups permissions» [YTFRONT-5385] ([a32e730](https://github.com/ytsaurus/ytsaurus-ui/commit/a32e730016fb0718e9b60a7fae36540eaa8fc6ac))
* **Queries:** движок SPYT теперь отображается всегда [YTFRONT-5528] ([2a25e28](https://github.com/ytsaurus/ytsaurus-ui/commit/2a25e28eebc1d45bd34dbc1011a2fa031bb4ba81))
* **Queries:** отключена кнопка запуска [YTFRONT-5598] ([ea59b04](https://github.com/ytsaurus/ytsaurus-ui/commit/ea59b042f84073a2fa8e41617e7e5cb259408903))


#### Исправления

* **Navigation:** исправлена сортировка таблиц по датам [YTFRONT-5406] ([5549f84](https://github.com/ytsaurus/ytsaurus-ui/commit/5549f84db342cb4922eb5cddcc74f4719c3b75b8))
* **Operation/Details:** дополнительный статус отображается только для выполняющихся операций [YTFRONT-5709] ([1368881](https://github.com/ytsaurus/ytsaurus-ui/commit/136888105cb94500304e01db3cfbcdb993e6552d))
* **Queries/Graph:** длительность во всплывающем окне узла [YTFRONT-5674] ([f9f1663](https://github.com/ytsaurus/ytsaurus-ui/commit/f9f1663f8a9f683989e885c0db41ebc341c02320))
* **Queries/Timeline:** цвета в тёмной теме [YTFRONT-5716] ([a37fb1d](https://github.com/ytsaurus/ytsaurus-ui/commit/a37fb1d6e076e0272c52866566f28e934018f2a7))
* **Queries:** неверное количество джобов [YTFRONT-5708] ([fe82410](https://github.com/ytsaurus/ytsaurus-ui/commit/fe82410b526fd13f0d7e77a0c5b5b4ab6ad1980f))
* **Query/Suggestions:** переменная хранилища может быть не определена ([35edf38](https://github.com/ytsaurus/ytsaurus-ui/commit/35edf3812eb1d63762c16a3c405723f16eb92e1f))
* **ACL:** исправлена работа с пустыми субъектами [[#1268](https://github.com/ytsaurus/ytsaurus-ui/issues/1268)] ([69167dc](https://github.com/ytsaurus/ytsaurus-ui/commit/69167dcb07ab2f1c92e01f1a84a6f33a2af8fb68))
* **Flow/Graph:** использование cpu_usage,memory_usage вместо metrics.*_10m [YTFRONT-5644] ([abd0e50](https://github.com/ytsaurus/ytsaurus-ui/commit/abd0e50c6286e0ba6c2b7294caa4481adb942ab5))
* **Navigation/ACL:** в проверку добавлено разрешение 'full_read' [YTFRONT-5311] ([e3f464c](https://github.com/ytsaurus/ytsaurus-ui/commit/e3f464c676b2d5e1273f948cd331889845c88e7c))
* **Navigation/DownloadManager:** при скачивании excel число разделяется на разряды [YTFRONT-3613] ([cab8256](https://github.com/ytsaurus/ytsaurus-ui/commit/cab82562f6df02dd0504a4103f8a13653a5690ec))
* **Navigation/MapNodesTable:** отключён переход при клике по чекбоксу в таблице [YTFRONT-5493] ([a6befb1](https://github.com/ytsaurus/ytsaurus-ui/commit/a6befb1a60ec0434963b95156e1393c1fce2778f))
* **Navigation/Table:** для ридера добавлен параметр 'omit_inaccessible_rows: true' [YTFRONT-5297] ([fe99458](https://github.com/ytsaurus/ytsaurus-ui/commit/fe9945819de79ae798607ac567f1159d3a12477b))
* **Navigation:** изменение размера списка выбора колонок [YTFRONT-5527] ([cf0536a](https://github.com/ytsaurus/ytsaurus-ui/commit/cf0536acd3e4102c27d5ebe4db2cb8eecea334cb))
* **Nginx:** увеличен таймаут [YTFRONT-5607] ([ad2bfe7](https://github.com/ytsaurus/ytsaurus-ui/commit/ad2bfe71221c7d39e53885b5de72049ad3c5afdd))
* **Operation/Details:** улучшен статус операции [YTFRONT-5659] ([885a8f7](https://github.com/ytsaurus/ytsaurus-ui/commit/885a8f732ca02304d4717ff15f2965c2487d980e))
* **Operation/Details:** исправление мелких ошибок вёрстки [YTFRONT-5529] ([ae30800](https://github.com/ytsaurus/ytsaurus-ui/commit/ae30800f3bee7655554fc52e1e1db3ab7276afc0))
* **Operation/Jobs:** для 'vanilla' не отображается «Прогресс» [YTFRONT-5662] ([0240bb5](https://github.com/ytsaurus/ytsaurus-ui/commit/0240bb5dea4b21d0b1285ab824552278f1859cd5))
* **Operation:** прогресс в промежуточном состоянии [YTFRONT-5634] ([51a44b9](https://github.com/ytsaurus/ytsaurus-ui/commit/51a44b98b6c4d7f2331a9d7263feddd99c60eff0))
* **Scheduling/ACL:** добавлена возможность отключить наследование ACL [YTFRONT-5616] ([bbb1177](https://github.com/ytsaurus/ytsaurus-ui/commit/bbb1177d01dae1e43120fe147ed2e95288c2ed64))
* **Scheduling/Meta:** улучшен прогресс strong guarantees [YTFRONT-5660] ([ccfec33](https://github.com/ytsaurus/ytsaurus-ui/commit/ccfec336c5d0b848f506b96930772c42dc431ea6))
* **Scheduling/PoolEditor:** улучшено сообщение об отсутствии прав на запись [YTFRONT-5656] ([7744329](https://github.com/ytsaurus/ytsaurus-ui/commit/774432925b1c4003fc1cc5e143caab22060d1bb9))
* **System:** новый атрибут версии [YTFRONT-5566] ([e277e83](https://github.com/ytsaurus/ytsaurus-ui/commit/e277e83823767d6aac13b4b798fb11b51fa97224))
* **Tablet:** упрощено отображение идентификатора таблета в виде одной редактируемой ссылки [YTFRONT-5632] ([16ebc1f](https://github.com/ytsaurus/ytsaurus-ui/commit/16ebc1f157b039efa469f98f01ec84eac29cd10d))

{% endcut %}


{% cut "**3.7.0**" %}

**Дата релиза:** 2026-03-19


**Страница релиза:** [3.7.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v3.7.0)


**Docker-образ:** [ghcr.io/ytsaurus/ui:3.7.0](https://github.com/orgs/ytsaurus/packages/container/ui/746071989?tag=3.7.0)


#### [3.7.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v3.6.1...ui-v3.7.0) (2026-03-19)


#### Новые возможности

* **Queries:** несколько полных результатов [YTFRONT-5615] ([d1adf53](https://github.com/ytsaurus/ytsaurus-ui/commit/d1adf532b6d2bb7ab67f852c6a308fb4161d73c4))


#### Исправления

* **ACL/EditInheritance:** кнопка «Подтвердить» неактивна, пока поле не изменено [YTFRONT-5562] ([e9a3c20](https://github.com/ytsaurus/ytsaurus-ui/commit/e9a3c208caedb27f05fc09a6c25edaf3dc82996d))
* **Components/Proxies:** добавлена фильтрация вариантов «Роль» [YTFRONT-] ([54c2f78](https://github.com/ytsaurus/ytsaurus-ui/commit/54c2f783107d82b8bbc60a8f5bab73cf0cec1c92))
* **Flows:** для вычислений используется поле 'name' [YTFRONT-5604] ([a5c2bc3](https://github.com/ytsaurus/ytsaurus-ui/commit/a5c2bc390f22b55ebea1d3324498fdb4fe07c25e))
* **Navigation/ChaosReplicatedTable:** для переключения автоматической репликации используется команда 'alter_replication_card' [YTFRONT-5287] ([b911633](https://github.com/ytsaurus/ytsaurus-ui/commit/b91163362a0b8d28c0b5951bcf0b5a443f733b0b))
* **Monitoring:** исправлено автообновление дашборда [YTFRONT-5614] ([5414ee4](https://github.com/ytsaurus/ytsaurus-ui/commit/5414ee418dee9d456a25d1a86cab93b69dde1d98))
* **Navigation:** метаданные в пустых таблицах [YTFRONT-5638] ([4c15602](https://github.com/ytsaurus/ytsaurus-ui/commit/4c15602d12b4442f9b7e89a38666d8e3798ade5c))
* **Navigation/TopRow:** путь с '\/' не должен помечаться как символическая ссылка [YTFRONT-5182] ([b302c3b](https://github.com/ytsaurus/ytsaurus-ui/commit/b302c3b046c3c9e60c8210147c644c26f9bb00f3))
* **Operation/Details:** пути слоёв с атрибутами должны корректно отображаться [YTFRONT-5268] ([2e76f34](https://github.com/ytsaurus/ytsaurus-ui/commit/2e76f345296ce4772282da363fc38f1c7e0de0b3))
* **Queries/Progress:** исправлен URL операции [YTFRONT-5622] ([73e6b3d](https://github.com/ytsaurus/ytsaurus-ui/commit/73e6b3d5c37d830b86da26c5c2e9e92ce53e259f))
* **Scheduling:** для заголовков операций используется utf8.decode [YTFRONT-5597] ([dbb222c](https://github.com/ytsaurus/ytsaurus-ui/commit/dbb222cbdeba9a96f28470e19b7ab9adf84f34df))
* **System:** слово 'nvme' должно быть в ВЕРХНЕМ РЕГИСТРЕ [YTFRONT-5534] ([dac156c](https://github.com/ytsaurus/ytsaurus-ui/commit/dac156c67387192986dbc6d76785ae034783db4d))
* **System:** копирование адреса в буфер обмена [YTFRONT-5129] ([0182d77](https://github.com/ytsaurus/ytsaurus-ui/commit/0182d7774600e32a59e1c3ac8d55ab8d5142056c))


{% endcut %}


{% cut "**3.6.0**" %}

**Дата релиза:** 2026-02-18


**Страница релиза:** [3.6.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v3.6.0)


**Docker-образ:** [ghcr.io/ytsaurus/ui:3.6.0](https://github.com/orgs/ytsaurus/packages/container/ui/696857218?tag=3.6.0)


#### [3.6.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v3.5.1...ui-v3.6.0) (2026-02-18)


#### Новые возможности

* **Accounts/DetailedUsage:** запросы должны отправляться через nodejs BFF [YTFRONT-5366] ([b240437](https://github.com/ytsaurus/ytsaurus-ui/commit/b24043752ea0c2abd983de23574c11e72805449c))
* **Flow:** добавлены флоу в виде отдельной страницы [YTFRONT-5241] ([74ae966](https://github.com/ytsaurus/ytsaurus-ui/commit/74ae96625e06a16283934898792ab49ae17533ff))
* **Flow:** сообщения отображаются для всех вкладок [YTFRONT-5244] ([3552691](https://github.com/ytsaurus/ytsaurus-ui/commit/35526919d1834dbd04216e4f06ce7a663fb4dfbd))
* **Navigation/AccessLog:** запросы должны отправляться через nodejs BFF [YTFRONT-5300] ([15e6e39](https://github.com/ytsaurus/ytsaurus-ui/commit/15e6e3924f551d6e4eabf6cff4a9e7db7d1ab3fc))
* **Queries/Progress:** новый дизайн графа [YTFRONT-5468] ([1daca1f](https://github.com/ytsaurus/ytsaurus-ui/commit/1daca1fed95a6ae0445ba8bf88a46a4f3251a661))


#### Исправления

* **Flow/Computation:** использование highlight_cpu_usage, highlight_memory_usage [YTFRONT-5115] ([129c231](https://github.com/ytsaurus/ytsaurus-ui/commit/129c231daa03faf638384ef4bc180ada90d72b5d))
* **Flow/Messages:** добавлена поддержка 'markdown_text' [YTFRONT-5255] ([0f0a96f](https://github.com/ytsaurus/ytsaurus-ui/commit/0f0a96fc2de1045e35f03668051789b880ac58e1))
* **Flow/Messages:** первое сообщение разворачивается при messages.length == 1 [YTFRONT-5237] ([36de213](https://github.com/ytsaurus/ytsaurus-ui/commit/36de2130a929a794f0cb3feaf437fb6b98cdba3f))
* **Flow/Monitoring:** передача pipeline_path [YTFRONT-4488] ([f84673a](https://github.com/ytsaurus/ytsaurus-ui/commit/f84673a30d596c1fa8723e854c7a3a314631c046))
* **FlowMessages:** добавлена поддержка 'markdown_text' [YTFRONT-5255] ([757df50](https://github.com/ytsaurus/ytsaurus-ui/commit/757df50e8886df26ee1c83cd56f38074c7859297))
* **Navigation/Flow:** улучшены вычисления [YTFRONT-5115] ([3f413ec](https://github.com/ytsaurus/ytsaurus-ui/commit/3f413ec710c1f5545254c2ca679ac3d80b918dae))
* **Queries/Tutorials:** поддержка старых qt-туториалов [YTFRONT-5509] ([25de4f7](https://github.com/ytsaurus/ytsaurus-ui/commit/25de4f733bbb10fec4380579895a66a6d80af18c))
* **Queries:** версия YQL в новом запросе [YTFRONT-5522] ([ff5c03a](https://github.com/ytsaurus/ytsaurus-ui/commit/ff5c03aa4b8670f69e632dff5d11ac053acc2379))
* **Redirect:** исправлена проверка значения useBeta [YTFRONT-5543] ([199b0d3](https://github.com/ytsaurus/ytsaurus-ui/commit/199b0d3c0550bca9f1c7bd9e4033bed2587c5113))

{% endcut %}


{% cut "**3.5.1**" %}

**Дата релиза:** 2026-02-02


**Страница релиза:** [3.5.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v3.5.1)


**Docker-образ:** [ghcr.io/ytsaurus/ui:3.5.1](https://github.com/orgs/ytsaurus/packages/container/ui/667539257?tag=3.5.1)



#### [3.5.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v3.5.0...ui-v3.5.1) (2026-02-02)


#### Исправления

* улучшен docsBaseurl по умолчанию [YTSAURUSSUP-2262] ([d0bcc3e](https://github.com/ytsaurus/ytsaurus-ui/commit/d0bcc3e75ddb7ec9ac611854db6bf3c071b482b2))
* **Components/Node:** исправлена опечатка [YTFRONT-5511] ([cb3390f](https://github.com/ytsaurus/ytsaurus-ui/commit/cb3390fb61ecc6c571a3d0a843a298bc5bdd39e4))
* **Components/Versions:** для фильтра «Хост» не используется автозаполнение [YTSAURUSSUP-2262] ([306338c](https://github.com/ytsaurus/ytsaurus-ui/commit/306338c3f0cf536f19d8b8b018bbc8747625282a))
* **Dashboard:** новый дашборд включен по умолчанию [YTSAURUSSUP-2262] ([b09cec1](https://github.com/ytsaurus/ytsaurus-ui/commit/b09cec1fe138caef82685d475952d5eee8b0704e))
* **DownloadManager:** проблема с шифрованием [YTFRONT-5474] ([418a4b2](https://github.com/ytsaurus/ytsaurus-ui/commit/418a4b27554c69cb24cbdee86b370ac300dafe08))
* **Navigation/MapNode:** добавлена кнопка «Настройки» с опцией «Группировать узлы по типу» [YTSAURUSSUP-2262] ([0e5c599](https://github.com/ytsaurus/ytsaurus-ui/commit/0e5c599df27fc04e25cf8e930c4e8e913b9a7d7a))
* **oatuh:** исправлен maxAge для YT_OAUTH_REFRESH_TOEKEN_NAME [YTFRONT-5504] ([d289da8](https://github.com/ytsaurus/ytsaurus-ui/commit/d289da8396e5949389b2ece6d3ddb25ff1bc172e))
* **Operation/JobsTimeline:** увеличено количество джобов [YTFRONT-5484] ([6553fc6](https://github.com/ytsaurus/ytsaurus-ui/commit/6553fc6acecf9abb0e43fdf23685b09dd1be5613))
* **Scheduling:** улучшена сортировка для «Пул/Операция» [YTFRONT-5469] ([02d9800](https://github.com/ytsaurus/ytsaurus-ui/commit/02d9800fabddb50497f1dc4481c3d5be635fc11b))
* **Scheduling:** хлебные крошки подогнаны под видимую область [YTFRONT-5506] ([5f47b66](https://github.com/ytsaurus/ytsaurus-ui/commit/5f47b663bb8738ce6c860bf2c4240e0892c99a1b))
* **server:** не использовать cacheable-lookup [YTSAURUSSUP-2262] ([03f0751](https://github.com/ytsaurus/ytsaurus-ui/commit/03f07512b7a97fa924f3223b951fb4ab24180dd4))
* **styles:** устранены предупреждения об устаревании SASS ([448e6f2](https://github.com/ytsaurus/ytsaurus-ui/commit/448e6f2acb9d648b1f4b5c56b67efa539d045d77))


#### Новые возможности

* **Accounts:** добавлена интеграция с дашбордом Prometheus [YTFRONT-4388] ([a3c85e0](https://github.com/ytsaurus/ytsaurus-ui/commit/a3c85e024be4f4726f64377bf4e0c6ef65d2856d))
* **Bundles:** добавлена интеграция с дашбордом Prometheus [YTFRONT-4388] ([2ce6c39](https://github.com/ytsaurus/ytsaurus-ui/commit/2ce6c39409b46d265fc2c3eb1fcedce85734dfa9))
* **CHYT:** добавлена интеграция с дашбордом Prometheus [YTFRONT-4388] ([2019484](https://github.com/ytsaurus/ytsaurus-ui/commit/201948411996d07db5155c0f2ad30f7c5ac8446d))
* **Job:** добавлена интеграция с дашбордом Prometheus [YTFRONT-4388] ([d905243](https://github.com/ytsaurus/ytsaurus-ui/commit/d905243d4446fa7b79dd1f2cfb60f8e2fec80790))
* **Monitoring:** добавлена интеграция с дашбордами Prometheus [YTFRONT-4388] ([7a1ce2a](https://github.com/ytsaurus/ytsaurus-ui/commit/7a1ce2a6285ee33ec32fe6c74f689f1a3f8b33d4))
* **Navigation/Consumer:** добавлена интеграция с дашбордом Prometheus [YTFRONT-4388] ([873c644](https://github.com/ytsaurus/ytsaurus-ui/commit/873c644daa6d7f732ae92bef52d5ca4637e09951))
* **Navigation/Flow:** добавлена интеграция с Prometheus [YTFOTN-4388] ([eace5ae](https://github.com/ytsaurus/ytsaurus-ui/commit/eace5aecc96a9210036cf60b2bd20bfe6e5f10f3))
* **Navigation/Queue:** добавлена интеграция с дашбордом Prometheus [YTFRONT-4388] ([c10369d](https://github.com/ytsaurus/ytsaurus-ui/commit/c10369dd2a2a8d4ff26d7ce77661e3cd8cdcabb7))
* **Navigation/Metadata:** ссылка на операцию [YTFRONT-4994] ([33a1d26](https://github.com/ytsaurus/ytsaurus-ui/commit/33a1d26d095221c1fa47964a3ce8866fa831e1a6))
* **Navigation:** дублирующиеся имена [YTFRONT-5458] ([7bec172](https://github.com/ytsaurus/ytsaurus-ui/commit/7bec172450a2789a52602f4c8681351852c97b24))
* **Navigation:** длинный текст в ошибке [YTFRONT-5477] ([3341ef7](https://github.com/ytsaurus/ytsaurus-ui/commit/3341ef77390bffef156802e9be5f0469dcb53477))
* **Navigation:** поддержка Ctrl в хлебных крошках [YTFRONT-5465] ([8600951](https://github.com/ytsaurus/ytsaurus-ui/commit/8600951431332c8028bc3d642fc3b699375c264d))
* **Operation:** добавлена интеграция с дашбордом Prometheus [YTFRONT-4388] ([c615ccb](https://github.com/ytsaurus/ytsaurus-ui/commit/c615ccb37248c84c260bea5a99e8de7b647bed4f))
* **Operations:** ограничение формата [YTFRONT-5386] ([7e3940a](https://github.com/ytsaurus/ytsaurus-ui/commit/7e3940a5a7dd8d07537acf431e77f8c0ffe7b084))
* **Queries/Progress:** прежняя высота графа [YTFRONT-5500] ([3283db8](https://github.com/ytsaurus/ytsaurus-ui/commit/3283db81b52693df0f03439def60011931647c30))
* **Queries:** отображаются только поддерживаемые движки [YTFRONT-5502] ([a2d9e16](https://github.com/ytsaurus/ytsaurus-ui/commit/a2d9e16339131b3a8a933c3d055d311c1adbd560))
* **Plan:** переход на gravity timeline [YTFRONT-5215] ([57cb5f4](https://github.com/ytsaurus/ytsaurus-ui/commit/57cb5f4dfe5301c96d854b2a3ad316d487d9e239))
* **Scheduling:** неверное время ожидания [YTFRONT-5439] ([8959c61](https://github.com/ytsaurus/ytsaurus-ui/commit/8959c611822821ba49d7bcd855d90c5475ae0e78))
* **System:** добавлена интеграция с дашбордом Prometheus [YTFRONT-4388] ([66da56d](https://github.com/ytsaurus/ytsaurus-ui/commit/66da56d9d0d0ef864f9493eaa271386180f26c2e))
* **Users/EditModal:** альтернативная форма, если шифрование не поддерживается [YTFRONT-5474] ([6c9eee8](https://github.com/ytsaurus/ytsaurus-ui/commit/6c9eee83a677ffe9b26fb5fb08e39068070ec984))
* **UTF8:** информация о деталях узла графа [YTFRONT-5246] ([d83815d](https://github.com/ytsaurus/ytsaurus-ui/commit/d83815d16397988fd5c4dace4c26416e3bc3ff00))


{% endcut %}


{% cut "**3.4.1**" %}

**Дата релиза:** 2025-12-23


**Страница релиза:** [3.4.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v3.4.1)


**Docker-образ:** [ghcr.io/ytsaurus/ui:3.4.1](https://github.com/orgs/ytsaurus/packages/container/ui/621108018?tag=3.4.1)


#### [3.4.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v3.4.0...ui-v3.4.1) (2025-12-23)


#### Новые возможности

* **AiChat:** добавлены агенты по движку [YTFRONT-5381] ([32b26ff](https://github.com/ytsaurus/ytsaurus-ui/commit/32b26ff1e3cce2860f889a1c2a67743d4aabaabc))
* **interface-helpers:** добавлен format.NumberWithSuffix ([0080c7d](https://github.com/ytsaurus/ytsaurus-ui/commit/0080c7d5caa0a3345233e20eb0d8cdac10481a54))


#### Исправления

* **Components:** длинное имя роли [YTFRONT-5390] ([aad281b](https://github.com/ytsaurus/ytsaurus-ui/commit/aad281b17634285ebf9e22afac448a68a0515ec9))
* **Navigation/Breadcrumbs:** хлебные крошки должны обрезаться по ширине окна просмотра [YTFRONT-5421] ([d005496](https://github.com/ytsaurus/ytsaurus-ui/commit/d005496874a9dbe0b7b598bdbde80bb04a030d64))
* **Scheduling/Table:** исправление прокрутки [YTFRONT-5134] ([b4857f6](https://github.com/ytsaurus/ytsaurus-ui/commit/b4857f638c41b01edcf8fbce56901143ccf5f6c1))
* **Scheduling:** добавлен редирект для удалённой вкладки «Детали» [YTFRONT-5134] ([84282c1](https://github.com/ytsaurus/ytsaurus-ui/commit/84282c1e2e63707909f88caba1bb79b47f7cdb0e))
* **Scheduling:** возвращён блок «Статическая конфигурация» [YTFRONT-5423] ([5bfa023](https://github.com/ytsaurus/ytsaurus-ui/commit/5bfa02392181193f119418931a0a33255f3893c0))
* **Statistics:** улучшено форматирование значений для YTChartKitPie [YTFRONT-5304] ([a7ca6cf](https://github.com/ytsaurus/ytsaurus-ui/commit/a7ca6cfc5d93956cdbf33ba393f4f894622e6dcf))

{% endcut %}

{% cut "**3.3.1**" %}

**Дата релиза:** 2025-12-12


**Страница релиза:** [3.3.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v3.3.1)


**Docker-образ:** [ghcr.io/ytsaurus/ui:3.3.1](https://github.com/orgs/ytsaurus/packages/container/ui/609454535?tag=3.3.1)


#### [3.3.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v3.3.0...ui-v3.3.1) (2025-12-12)

#### ⚠ КРИТИЧЕСКИЕ ИЗМЕНЕНИЯ

* обновление @gravity-ui/uikit до версии 7 [YTFRONT-4917]

#### Новые возможности

* добавлена новая тема кластера 'electricviolet' [YTFRONT-5318] ([4d30f9e](https://github.com/ytsaurus/ytsaurus-ui/commit/4d30f9e15dec0dd2ec1ca73c7d033cd4b4157ecc))
* **Queries:** русская локализация [YTFRONT-5069] ([1aa195c](https://github.com/ytsaurus/ytsaurus-ui/commit/1aa195c9d832afa39e0c3e8cc9930197c7f680a5))
* **Scheduling:** переработана страница [YTFRONT-5134] ([e289ac4](https://github.com/ytsaurus/ytsaurus-ui/commit/e289ac4e28a754c0f9695327fdfe0a487df5c80d))
* **Queries/Tutorials:** добавлена пагинация в туториалы [YTFRONT-5344] ([29dcc79](https://github.com/ytsaurus/ytsaurus-ui/commit/29dcc7956e6fac3c6781b422b0d8d1d77e1c990c))
* **AiChat:** добавлен AI-чат [YTFRONT-5048] ([ca57375](https://github.com/ytsaurus/ytsaurus-ui/commit/ca57375da7103565df0f79ece2e9e22a057c8774))
* добавлена опция disableHeavyProxies в clusters-config [YTFRONT-5176] ([df70d88](https://github.com/ytsaurus/ytsaurus-ui/commit/df70d883cc053696523829ef509d7748e6688cb8))
* **Operation/Details/Specification:** добавлены cpu_limit/gpu_limit/memory_limit для задач [YTFRONT-5145] ([939fead](https://github.com/ytsaurus/ytsaurus-ui/commit/939fead0fdf534a8530f365e8220a1c1efb1e798))
* обновление @gravity-ui/uikit до версии 7 [YTFRONT-4917] ([29c4362](https://github.com/ytsaurus/ytsaurus-ui/commit/29c4362ed5ae9ce10d9b7a964784c804f6de3b0b))
* обновление react-redux 7->9 ([76ca6fe](https://github.com/ytsaurus/ytsaurus-ui/commit/76ca6fefccd64186c1bfb01baa8503ea8d2d8638))

#### Исправления

* **Settings:** вкладка разработчика скрыта от пользователей [YTFRONT-5393] ([8402b68](https://github.com/ytsaurus/ytsaurus-ui/commit/8402b68a2a7808f4302462e868c06082da55ee2e))
* **Maintenance:** верхняя строка должна быть видна после нажатия «Proceed to cluster anyway» [YTFRONT-5320] ([98d5ebe](https://github.com/ytsaurus/ytsaurus-ui/commit/98d5ebe5ce863c764a863a3fa9d8798c0f5f8d0e))
* **Navigation/Flow:** улучшен z-index для диалогов [YTFRONT-5401] ([61add6c](https://github.com/ytsaurus/ytsaurus-ui/commit/61add6c107607b393601ea819e13c63605dca28d))
* **Queries:** учёт стейджа в alterQuery [YTFRONT-5394] ([cfee11f](https://github.com/ytsaurus/ytsaurus-ui/commit/cfee11f4d1e80a63c47fcbf0da1df74c190f4407))
* **UIFactory:** добавлены поля 'hidden' для результатов UIFactory.getSchedulingExtraTabs(...) [YTFRONT-5271] ([f9c9c67](https://github.com/ytsaurus/ytsaurus-ui/commit/f9c9c670803a509507b513a329a6c13bbd01f085))
* **JobLogs:** использование конфигурации кластера [YTFRONT-5348] ([df75e2d](https://github.com/ytsaurus/ytsaurus-ui/commit/df75e2d35d619a31fb2151e0466f29f8dead1b15))
* **Queries/History:** неверный формат фильтра по дате [YTFRONT-5344] ([72eb7a2](https://github.com/ytsaurus/ytsaurus-ui/commit/72eb7a271b9725e07a80410a6a91c4dfa11d9cc7))
* **Queries/History:** проблема с обновлением списка [YTFRONT-5344] ([9a0450b](https://github.com/ytsaurus/ytsaurus-ui/commit/9a0450b5ee559b5551120a3d33d5a7e46bbd7709))
* **Queries/Tutorials:** проблема с URL при клике на элемент [YTFRONT-5344] ([f485257](https://github.com/ytsaurus/ytsaurus-ui/commit/f4852576b96320d90d6a2bfe12f997dce92f4753))
* **Scheduling:** возвращена фильтрация при выборе пул-дерева [YTFRONT-5363] ([67a7c4d](https://github.com/ytsaurus/ytsaurus-ui/commit/67a7c4d0165f2b3ebe6e3bb82258cbf34ab81f64))
* **Account/Editor:** запрет удаления аккаунтов с ненулевым использованием ресурсов [YTFRONT-5320] ([e2ca472](https://github.com/ytsaurus/ytsaurus-ui/commit/e2ca472c4c24acf20dad64c7ffe34a6b101ff9ec))
* **ACL:** переименование 'Edit ACL' в 'Add ACL' [YTFRONT-5314] ([14f7ba7](https://github.com/ytsaurus/ytsaurus-ui/commit/14f7ba7914bd6417d149d4e5924ba25cde743df5))
* **Navigation:** исправлена ссылка на операцию в метаданных [YTFRONT-5193] ([a09f5ae](https://github.com/ytsaurus/ytsaurus-ui/commit/a09f5aea16e9d1921727ab6f43f201bd3dbf5c3b))
* **Navigation:** редактирование бандла таблет-селлов для реплицированных таблиц [YTFRONT-5350] ([2757be5](https://github.com/ytsaurus/ytsaurus-ui/commit/2757be5902145931d15464d55b46a6b6453014c0))
* **Operation/Jobs:** фильтр по описанию [YTFRONT-5254] ([d14b6e1](https://github.com/ytsaurus/ytsaurus-ui/commit/d14b6e1490f93fe735b8891faa49ad5dfe3167ed))
* **Queries/Navigation:** потеря состояния при выполнении запроса [YTFRONT-5252] ([dfa972c](https://github.com/ytsaurus/ytsaurus-ui/commit/dfa972ca2d3107eaf0c39349a19ed7190590a1d6))
* **Queries/Navigation:** повторный рендеринг при запуске запроса [YTFRONT-5252] ([d2585bf](https://github.com/ytsaurus/ytsaurus-ui/commit/d2585bfe3166271fb4d0482372887d072cb549f7))
* **Queries/Tutorials:** неверные элементы в списке [YTFRONT-5302] ([dc4a980](https://github.com/ytsaurus/ytsaurus-ui/commit/dc4a980b41ba59eeef2f1b9eec0b36c118da5399))
* **Queries:** корректная загрузка версии YQL [YTFRONT-5349] ([419e729](https://github.com/ytsaurus/ytsaurus-ui/commit/419e72948285311922276399f628ea26cd0faae4))
* **Queries:** корректный тип настроек [YTFRONT-5351] ([376c943](https://github.com/ytsaurus/ytsaurus-ui/commit/376c9437b8180a921b40d8690f4aaaffa06e4870))
* **Dashboard2/WidgetSettings:** ключ должен зависеть от данных [YTFRONT-5154] ([d6a2fa9](https://github.com/ytsaurus/ytsaurus-ui/commit/d6a2fa991edcf11a4af761824d5d10ef80ba2ca3))
* **Dashboard2:** лимиты виджетов операций и запросов [YTFRONT-5154] ([662662d](https://github.com/ytsaurus/ytsaurus-ui/commit/662662d6f8552bb3e0ef5e0d45e252faab8e3911))
* **Dashboard2:** улучшенные ошибки для элементов виджетов Accounts/Pools/Services [YTFRONT-5154] ([d6d4b4b](https://github.com/ytsaurus/ytsaurus-ui/commit/d6d4b4bb6e949b78f69dffbe9645c92d063ba510))
* **Dashboards2:** кэширование при копировании настроек кластера из предыдущего кластера [YTFRONT-5154] ([d9fb327](https://github.com/ytsaurus/ytsaurus-ui/commit/d9fb327b6341d0bd4c7d4e5fafc3cacfb03a648c))
* **Navigation/Link:** ошибка рендеринга [YTFRONT-5293] ([f976a39](https://github.com/ytsaurus/ytsaurus-ui/commit/f976a39c3f3861bea10b6551e578cf06f1750bcb))
* **Queries/List:** ошибка фильтра по статусу [YTFRONT-5310] ([6a5c876](https://github.com/ytsaurus/ytsaurus-ui/commit/6a5c8765937e53a9edb32c3bae0cb7f297880ea1))

{% endcut %}


{% cut "**2.7.0**" %}

**Дата релиза:** 2025-11-11


**Страница релиза:** [2.7.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v2.7.0)


**Docker-образ:** [ghcr.io/ytsaurus/ui:2.7.0](https://github.com/orgs/ytsaurus/packages/container/ui/573170766?tag=2.7.0)


#### [2.7.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v2.6.0...ui-v2.7.0) (2025-11-11)


#### Новые возможности

* **Components/CypressProxies:** добавлена вкладка cypress proxies [YTFRONT-5036] ([4e91046](https://github.com/ytsaurus/ytsaurus-ui/commit/4e91046d5ab84ade0511b7c4426c9060728098e8))
* **Operation/Logs:** поддержка вкладок логов для операций и логов [YTFRONT-5191] ([b088313](https://github.com/ytsaurus/ytsaurus-ui/commit/b0883136a4fc649b0c478d6b8dee0dc0b38e26c4))
* **System/CypressProxies:** добавлена секция cypress proxies [YTFRONT-5036] ([249dba8](https://github.com/ytsaurus/ytsaurus-ui/commit/249dba86ab4d92bba40dbe762c8215cea401df18))
* **Queries:** публикация запроса с активной вкладкой [YTFRONT-5001] ([9ea4fcb](https://github.com/ytsaurus/ytsaurus-ui/commit/9ea4fcba2c443463a26504825fba6cc6c2ea771e))
* **Dashboard2:** интернационализация страницы Dashboard [YTFRONT-3400] ([fdef408](https://github.com/ytsaurus/ytsaurus-ui/commit/fdef4081b15178d9b5255730905c1bec5cc35f0c))
* **Operation/Timeline:** отображение инкарнаций [YTFRONT-4980] ([7f3209d](https://github.com/ytsaurus/ytsaurus-ui/commit/7f3209d98cadd8fd15fb6a16a2919c32a7c16f84))
* **Queries:** добавлены версии YQL [YTFRONT-5098] ([1762ef8](https://github.com/ytsaurus/ytsaurus-ui/commit/1762ef8234c7f84c602201f8ac62796a1e95ffb9))
* **Settings:** настройка стейджа доступна пользователям [YTFRONT-5261] ([3ad3eda](https://github.com/ytsaurus/ytsaurus-ui/commit/3ad3eda4c444c0ba67eae3e303966de3f337c903))
* **Operation/Jobs:** добавлен фильтр по monitoring descriptor [YTFRONT-5254] ([5b7bc64](https://github.com/ytsaurus/ytsaurus-ui/commit/5b7bc6456494bff6e0a5451123d8a17a07c80b43))
* **Operation/JobsMonitoring:** новый монитор джобов [YTFRONT-5053] ([c9d5051](https://github.com/ytsaurus/ytsaurus-ui/commit/c9d5051d755c681a96c1ee1c2e7c8b52efe164ce))


#### Исправления

* **Queries:** проблема с информацией о движках [YTFRONT-5286] ([5253699](https://github.com/ytsaurus/ytsaurus-ui/commit/525369988fbd3c60e08bda2b1d62e4318f94e6bb))
* **Operations/JobsMonitoring:** полный список дескрипторов [YTFRONT-5192] ([8a3c244](https://github.com/ytsaurus/ytsaurus-ui/commit/8a3c24477092f8a8af1f2b74d8c33cdfb10a53f2))
* **UTF8:** обрезанная кодировка ячейки [YTFRONT-5206] ([69f44f7](https://github.com/ytsaurus/ytsaurus-ui/commit/69f44f78f9c7d941d0ec13729ebfa03b88399903))
* **UTF8:** обрезанное модальное окно [YTFRONT-5202] ([a64ede7](https://github.com/ytsaurus/ytsaurus-ui/commit/a64ede7cc6ba11635e6a4c523e594583d1f09f29))
* **Navigation/Description:** использование '&' для подавления разрешения ссылок [YTFRONT-5232] ([7c72895](https://github.com/ytsaurus/ytsaurus-ui/commit/7c72895e0d947bfa68852bf77ef7d18c26b58616))
* **Navigation/NavigationError/RequestPermission:** разрешены запросы для 'portal_exit' [YTFRONT-5233] ([0e54e1a](https://github.com/ytsaurus/ytsaurus-ui/commit/0e54e1ac69f24f7e96624d3248d972a8ae6ce5aa))
* **Navigation/Table:** использование 'Allow raw string' при 'YQL V3 Types' [YTFRONT-5226] ([481aed0](https://github.com/ytsaurus/ytsaurus-ui/commit/481aed0c55a3cb8f018ebbb94df510fb10045691))
* **Operations/JobsMonitoring:** отображение превышения лимита [YTFRONT-5231] ([6d6a597](https://github.com/ytsaurus/ytsaurus-ui/commit/6d6a5972ebb9568ef576a0343276e7412213a722))
* **Queries/Chart:** дата на оси [YTFRONT-5105] ([52378f1](https://github.com/ytsaurus/ytsaurus-ui/commit/52378f1bc7f09c4591faefc92d1e00fb688d649d))
* **Queries:** вкладка туториалов [YTFRONT-5240] ([02ad5e2](https://github.com/ytsaurus/ytsaurus-ui/commit/02ad5e26ca88745b61bbb44b93f00fcab6ab1a95))
* **Queries:** неверный размер селектора клики [YTFRONT-5227] ([db610ec](https://github.com/ytsaurus/ytsaurus-ui/commit/db610ec8fcd8a22296a4cb864a11412e2465b7ac))
* **Queries:** график при переключении вкладок [YTFRONT-5263] ([0d649e4](https://github.com/ytsaurus/ytsaurus-ui/commit/0d649e48d555cada46be9d4b62d7d1530dc584c1))
* **Chyt/ACL:** отображение конкретного набора разрешений [YTFRONT-5275] ([1022307](https://github.com/ytsaurus/ytsaurus-ui/commit/1022307a0804eaa979c21c78067926f63b16e548))
* **ClustersMenu:** размещение группы GPU после 'Auxiliary MRs' [YTFRONT-5282] ([d7f387a](https://github.com/ytsaurus/ytsaurus-ui/commit/d7f387ab3f0ca93c09138c13e44b7be85b6d1674))
* **Navigation/Attributes:** кодек сжатия [YTFRONT-5200] ([1062551](https://github.com/ytsaurus/ytsaurus-ui/commit/1062551b150fcb2baf3923ac1c967f581ac34512))
* **Navigation/Description:** не закрывать режим редактирования при ошибке сохранения [YTFRONT-5273] ([a3b06cf](https://github.com/ytsaurus/ytsaurus-ui/commit/a3b06cf13d438aad436babdcbf88a19986fb9677))
* **Navigation/Table:** применение 'Allow raw strings' только для строковых типов [YTFRONT-5226] ([4358b69](https://github.com/ytsaurus/ytsaurus-ui/commit/4358b69a04ea901e5569e0e84a549569ab52a0b4))
* **Operation/Incarnations:** первая инкарнация развернута по умолчанию [YTFRONT-5278] ([e55f832](https://github.com/ytsaurus/ytsaurus-ui/commit/e55f8321b7c159aa6de7f9624f8a1b191e78d88c))
* **Queries:** проблема с обновлением списка запросов [YTFRONT-5260] ([1a8708f](https://github.com/ytsaurus/ytsaurus-ui/commit/1a8708fe6bc6f5025cf998ceefead8d37f6d6791))
* **UTF8:** проблема с кодировкой заголовка операции [YTFRONT-5269] ([0a7ebb2](https://github.com/ytsaurus/ytsaurus-ui/commit/0a7ebb2f43b39bba062986d4a16aff4cda4234bd))

{% endcut %}


{% cut "**2.1.0**" %}

**Дата релиза:** 2025-09-18


**Страница релиза:** [2.1.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v2.1.0)


**Docker-образ:** [ghcr.io/ytsaurus/ui:2.1.0](https://github.com/orgs/ytsaurus/packages/container/ui/519415157?tag=2.1.0)


#### [2.1.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v2.0.0...ui-v2.1.0) (2025-09-18)

#### ⚠ КРИТИЧЕСКИЕ ИЗМЕНЕНИЯ

* **interface-helpers:** перенос format.DateTime в ui/src/common/hammer/format [YTFRONT-5171]

#### Новые возможности

* **yql:** подсветка синтаксиса для новых ключевых слов YQL ([ab6ff5a](https://github.com/ytsaurus/ytsaurus-ui/commit/ab6ff5af05eb06512ce183ebd9cce5d214c03555))
* **Navigation/Flow:** добавлен флаг 'force' для редактирования статического спека [YTFRONT-5185] ([26a75b6](https://github.com/ytsaurus/ytsaurus-ui/commit/26a75b63fdab09fe416e7372629cb33a7acf7307))
* **OperationDetail/Incarnations:** добавлена вкладка инкарнаций [YTFRONT-5119] ([7f6264c](https://github.com/ytsaurus/ytsaurus-ui/commit/7f6264ca9e4fde6df1f37305910c88d90313cc2a))
* **Queries:** добавлена кнопка секретов [YTFRONT-5162] ([8b6a834](https://github.com/ytsaurus/ytsaurus-ui/commit/8b6a8342a8e2e1b3c7137ad5fc987e587ed270d5))
* **Navigation/ReplicatedTable:** добавлена колонка для content_type [YTFRONT-4964] ([c9b16f3](https://github.com/ytsaurus/ytsaurus-ui/commit/c9b16f3763f01b0b3a67bdf46b8e09ded849c08c))
* обновление @gravity-ui/chartkit до v5.22.4 [YTFRONT-5116] ([e2b0be8](https://github.com/ytsaurus/ytsaurus-ui/commit/e2b0be8a721a14777784cdb6df0be86af6b715b5))
* **Bundles:** удален балансировщик бандлов [YTFRONT-5160] ([7b5f141](https://github.com/ytsaurus/ytsaurus-ui/commit/7b5f141e9b7f3498049e2af55f8b3682c7560c77))
* **Navigation/Table:** встроенный предпросмотр для тегов `audio`, `image` [YTFRONT-5022] ([b84ed7d](https://github.com/ytsaurus/ytsaurus-ui/commit/b84ed7d9e3c96e6a1f55e43945c8e4e5d808e373))
* **Operations/Runtime:** добавлены абсолютные значения [YTFRONT-5120] ([a986bd4](https://github.com/ytsaurus/ytsaurus-ui/commit/a986bd4796c42983ec522ffeedfb935b217dfef8))
* **Operations/Runtime:** добавлены подсказки [YTFRONT-5120] ([a26b6e4](https://github.com/ytsaurus/ytsaurus-ui/commit/a26b6e41625c5963e3c604d8e76948657b77f4e8))
* **Queries/Result:** встроенный предпросмотр для тегов `image`, `audio` [YTFRONT-5022] ([0c400d7](https://github.com/ytsaurus/ytsaurus-ui/commit/0c400d73bd42396c758edf1c187604ea033a318e))
* **System:** добавлено десятичное значение для cell-tag [YTFRONT-4939] ([f3d53bb](https://github.com/ytsaurus/ytsaurus-ui/commit/f3d53bb94dc515ba6dafd3d0198fd299c34e2111))
* **Dashboard2/PoolsWidget:** добавлена настройка избранного/пользовательского списка [YTFRONT-3400] ([989c148](https://github.com/ytsaurus/ytsaurus-ui/commit/989c1480c82e05f3ca4decc46f1258457078c70f))
* **Dashboard2/ServicesWidget:** добавлена настройка избранного/пользовательского списка [YTFRONT-3400] ([127d2c9](https://github.com/ytsaurus/ytsaurus-ui/commit/127d2c9e80238a0c8dae5b6552f205ca03c939cc))
* **Dashboard2:** добавлено количество элементов в заголовок виджета [YTFRONT-3400] ([040d913](https://github.com/ytsaurus/ytsaurus-ui/commit/040d913f22525a919695bff2725053feb6ba3bf9))
* **Groups:** замена колонки idm на внешнюю систему [YTFRONT-5113] ([75d3473](https://github.com/ytsaurus/ytsaurus-ui/commit/75d347388cb2d5526ceb6df8df3916f5d8daef5f))
* **Monaco:** новые цвета для темной и светлой контрастной темы [YTFRONT-5060] ([a49bf9d](https://github.com/ytsaurus/ytsaurus-ui/commit/a49bf9d3318d36432da64cdd2a07dae5148784b5))
* **Navigation/CreateTable:** добавлен новый агрегатный тип [YTFRONT-5153] ([7e0ee63](https://github.com/ytsaurus/ytsaurus-ui/commit/7e0ee63ca4ae849e55d483709f229ad346c13aee))
* **Navigation/YqlWidget:** ссылка на операцию QT [YTFRONT-4994] ([8b9688c](https://github.com/ytsaurus/ytsaurus-ui/commit/8b9688c3e000716b1143cdd597b01d97944d9073))
* **Queries/List:** добавлены дополнительные фильтры в списке [YTFRONT-5058] ([fc5ad97](https://github.com/ytsaurus/ytsaurus-ui/commit/fc5ad976051beb0a6b7299e1f9cdda80ce4c7286))
* **Queries/List:** бесконечный список запросов [YTFRONT-5060] ([9a58558](https://github.com/ytsaurus/ytsaurus-ui/commit/9a5855808e5eb4292bf4b700df70158c44ad1a30))
* **Queries:** фильтрация движков по выбранному кластеру [YTFRONT-4852] ([7a0daaf](https://github.com/ytsaurus/ytsaurus-ui/commit/7a0daaf443d186a54c2f4cb8e7f24f9646ceb425))
* **Queries:** редизайн меню QT [YTFRONT-4852] ([04a5b2c](https://github.com/ytsaurus/ytsaurus-ui/commit/04a5b2c5387a5aec06cad5c90f9c4eadabc5af25))
* **Users:** замена колонки idm на внешнюю систему [YTFRONT-5113] ([dc25858](https://github.com/ytsaurus/ytsaurus-ui/commit/dc258587276e1c06e6917b95e827e569f1852e61))


#### Исправления

* незначительное исправление для публикации нового релиза ([e90a68b](https://github.com/ytsaurus/ytsaurus-ui/commit/e90a68bbb5f165acb4c9e9b0237a3978227c01f5))
* **Navigation/ACL:** исправлена опечатка [YTFRONT-5166] ([85f1c06](https://github.com/ytsaurus/ytsaurus-ui/commit/85f1c066d0f762ddc02bfafd46676792823e7635))
* **Operation/Details:** кнопка редактирования должна быть всегда видимой [YTFRONT-5164] ([658a55a](https://github.com/ytsaurus/ytsaurus-ui/commit/658a55a28fb5b1a04f43038395ebea62715f3e39))
* **Operation/Jobs:** запрет сворачивания колонки 'Id/Address' [YTFRONT-5171] ([0fb04a9](https://github.com/ytsaurus/ytsaurus-ui/commit/0fb04a912c4d4a255a9efe161aa94962d920a736))
* **UTF8:** кодировка схемы таблицы [YTFRONT-5161] ([ded51be](https://github.com/ytsaurus/ytsaurus-ui/commit/ded51be9fbe5c26ed78caa69d6bde785bcf09510))
* **Accounts/DetailedUsage:** использование всех полей строки для '/get-versioned-resource-usage' [YTFRONT-5187] ([a8e73d6](https://github.com/ytsaurus/ytsaurus-ui/commit/a8e73d6b57c8a939b84d83162b3a188c2b03b65f))
* **Acl:** добавлен параметр "vital" в вызов requestPermissions, так как он требуется для разрешения "Register queue consumer (vital)" ([c7e96a2](https://github.com/ytsaurus/ytsaurus-ui/commit/c7e96a2ec55af73616dc6e8aea906cf4293cf4ce))
* **ACL:** удален 'mode: keep-missing-fields' [YTFRONT-5039] ([ea90bfa](https://github.com/ytsaurus/ytsaurus-ui/commit/ea90bfa3a927f168f3bf6da7cc4f1af684a37d32))
* **Operation:** ошибка utf8 в описании [YTFRONT-4982] ([f60f965](https://github.com/ytsaurus/ytsaurus-ui/commit/f60f965ddc6fd4c8d4756cc3fd66b12849edc2e6))
* **Navigation/Table:** использование getInitialSettingsData из редьюсеров [YTFRONT-5137] ([a94a549](https://github.com/ytsaurus/ytsaurus-ui/commit/a94a54986e2b409adbdce0440d7f902a2dcf84ad))
* **Queries/Result:** приоритет вкладки результатов [YTFRONT-5122] ([6d83a4b](https://github.com/ytsaurus/ytsaurus-ui/commit/6d83a4be1b9cff4291312250a8c4817c1b041bc0))
* **Scheduling:** использование 'estimated_guarantee_resources' вместо 'promised_fair_share_resources' [YTFRONT-4015] ([4031d70](https://github.com/ytsaurus/ytsaurus-ui/commit/4031d70961dc27b9697ef5d7170d4b8b427afe22))
* **Accounts/DetailedUsage:** перенос навигационной ссылки из пути в кнопку [YTFRONT-4896] ([27ebab3](https://github.com/ytsaurus/ytsaurus-ui/commit/27ebab38598675470b198f9491d43b088f1df3b8))
* **Accounts:** опечатка в графике [YTFRONT-5151] ([3a1975c](https://github.com/ytsaurus/ytsaurus-ui/commit/3a1975c72a1999dcbeb59f22275ac86e96de4a8a))
* **ColumnSelector:** декодирование колонок с неанглийскими названиями [YTFRONT-4873] ([d9a552d](https://github.com/ytsaurus/ytsaurus-ui/commit/d9a552d7d7d801066d2a38abc6d665c86057f420))
* **Dashboard2/Accounts:** читаемые медиумы [YTFRONT-3400] ([4432bea](https://github.com/ytsaurus/ytsaurus-ui/commit/4432bea9fe9b44b913b65f41cd4ad8f19034b51d))
* **Dashboard2/Accounts:** объединение контролов колонок [YTFRONT-3400] ([d25f743](https://github.com/ytsaurus/ytsaurus-ui/commit/d25f743c3cc43d66bded37cb6cfeb83c0938dde4))
* **Dashboard2:** добавлены лимиты для запросов и операций [YTFRONT-3400] ([cd35267](https://github.com/ytsaurus/ytsaurus-ui/commit/cd35267539ec48251e27a72220960bf7ee510eee))
* **Modal:** закрытие модального окна при нажатии esc [YTFRONT-2533] ([72ddde9](https://github.com/ytsaurus/ytsaurus-ui/commit/72ddde9d60dbcce5c3a476896aa81b2e27b3480b))
* **Navigation:** исправлена отмена ([cfa5d83](https://github.com/ytsaurus/ytsaurus-ui/commit/cfa5d836e6acb247a54cc19842b15706e073489f))
* **Queries/List:** неверный порядок элементов при запуске запроса [YTFRONT-4770] ([f3a59a3](https://github.com/ytsaurus/ytsaurus-ui/commit/f3a59a3b907ef8e860d83d96fe5a1feec613cb82))
* **Queries/Navigation:** корректные значки элементов [YTFRONT-5099] ([7e64c6d](https://github.com/ytsaurus/ytsaurus-ui/commit/7e64c6da3096eb4efb8ddffbcb6a0febb81727d3))
* **Queries/Result:** приоритет вкладки результатов [YTFRONT-5122] ([153313f](https://github.com/ytsaurus/ytsaurus-ui/commit/153313f7ecfbb92aa07a2d6f8bf2ab50e63d664e))
* **Queries/Share:** изменена зависимость от запроса списка [YTFRONT-4770] ([bbb62f7](https://github.com/ytsaurus/ytsaurus-ui/commit/bbb62f79708b60ce3ff1745fdf5fee0623a18c32))

#### Рефакторинг кода

* **interface-helpers:** перенос format.DateTime в ui/src/common/hammer/format [YTFRONT-5171] ([53721f4](https://github.com/ytsaurus/ytsaurus-ui/commit/53721f4c0e9c81d41070d5ebc0240e5402be0427))

#### Зависимости

* Обновлены следующие зависимости workspace
  * devDependencies
    * @ytsaurus/interface-helpers обновлён с ^0.3.0 до ^1.0.0

{% endcut %}


{% cut "**1.98.0**" %}

**Дата релиза:** 2025-07-01


**Страница релиза:** [1.98.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.98.0)


**Docker-образ:** [ghcr.io/ytsaurus/ui:1.98.0](https://github.com/orgs/ytsaurus/packages/container/ui/451253868?tag=1.98.0)


#### [1.98.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.97.0...ui-v1.98.0) (2025-07-01)

#### Новые возможности

* **Accounts:** добавлена возможность указать базовый URL для использования аккаунтов [YTFRONT-4927] ([510587e](https://github.com/ytsaurus/ytsaurus-ui/commit/510587e7da13a6990e949316f1d4a74147e874ba))
* **Consumer:** добавлены кнопки регистрации для очередей и потребителей [YTFRONT-4869] ([f9d0a84](https://github.com/ytsaurus/ytsaurus-ui/commit/f9d0a843905362050fa80ddbdffeb4f5056d078f))
* **Dialog:** добавлен фильтр для элемента выбора сервисов [YTFRONT-5007] ([51aab9b](https://github.com/ytsaurus/ytsaurus-ui/commit/51aab9b249b07cedcf7039dbb13b9235c3036cd1))
* **Dialog/TimeDuration:** запрещены все символы, кроме цифр и латинских букв [YTFRONT-4995] ([380c4ab](https://github.com/ytsaurus/ytsaurus-ui/commit/380c4ab0ad8aa531c8acf3647d397301012d668c))
* **Dialog/AccountsMultiple:** добавлено удаление подписей элементов [YTFRONT-3400] ([44e34aa](https://github.com/ytsaurus/ytsaurus-ui/commit/44e34aa32bf1bf7222ab9f7fa1206cf978cc21d1))
* **Navigation:** добавлены значки для продюсеров и потребителей очередей [YTFRONT-4981] ([401465d](https://github.com/ytsaurus/ytsaurus-ui/commit/401465d2250ba6e893ac9b5bca20a09067869e35))
* **Navigation/Description:** добавлена опция внешнего описания [YTFRONT-4680] ([dbfe196](https://github.com/ytsaurus/ytsaurus-ui/commit/dbfe196174aad542e6616acf5677f92edb064644))
* **Navigation/Description:** отображение загруженного описания [YTFRONT-5015] ([76fc3d3](https://github.com/ytsaurus/ytsaurus-ui/commit/76fc3d358db8719f350d9969d40eb575bf2836c9))
* **Navigation/TabletErorrs:** сообщение об ошибке переделано в кнопку, первая ошибка разворачивается [YTFRONT-4740] ([865f759](https://github.com/ytsaurus/ytsaurus-ui/commit/865f7598ba0ce86e9b5e8e3cb30c21f76b2aa431))
* **Navigation/TabletErrors:** удаление бесполезных URL-параметров при размонтировании [YTFRONT-4740] ([f755d62](https://github.com/ytsaurus/ytsaurus-ui/commit/f755d62e6853b8a6764f3938bef348fc51bcde80))
* **Navigation/TabletErrors:** небольшие улучшения [YTFRONT-4740] ([e9b459b](https://github.com/ytsaurus/ytsaurus-ui/commit/e9b459bbdd146ba359d45f7f59a6a90b6b272fd1))
* **Navigation/MapNode:** поддержка загрузки файлов через интерфейс [[#1173](https://github.com/ytsaurus/ytsaurus-ui/issues/1173)] ([8acf29e](https://github.com/ytsaurus/ytsaurus-ui/commit/8acf29e863e427f247653706dd2d66119a91ca51))
* **Operation/Details:** атрибуты error.job_id должны отображаться в виде ссылки [YTFRONT-3916] ([fd38edf](https://github.com/ytsaurus/ytsaurus-ui/commit/fd38edf1e6b11407fbd81fef146d69e2ed8f9a97))
* **Operation/Jobs:** добавлена поддержка атрибута 'With interruption info' [YTFRONT-4810] ([02cf8ea](https://github.com/ytsaurus/ytsaurus-ui/commit/02cf8ea5c667c413f57579ce33e57c3134c75ab1))
* **Queries/Navigation:** добавлена кнопка открытия в новой вкладке [YTFRONT-4985] ([4bb9786](https://github.com/ytsaurus/ytsaurus-ui/commit/4bb978649a4a956913dd4d585c9a399ab95fdf9b))
* **Queries:** добавлена мультидиаграмма [YTFRONT-4999] ([d760704](https://github.com/ytsaurus/ytsaurus-ui/commit/d760704cdfa14914c019a339b872a30528a94044))
* **Queue/Exports:** перенос мс в подсказки [YTFRONT-4995] ([77010c4](https://github.com/ytsaurus/ytsaurus-ui/commit/77010c4c59d5361421b8845b72369eba66dc7d67))
* **Queries/Monaco:** прокрутка редактора к выбранной строке [YTFRONT-4915] ([9a747aa](https://github.com/ytsaurus/ytsaurus-ui/commit/9a747aae7241ad742988c54b1b645b4c3b93d1ba))
* **Scheduling:** прокрутка к представлению операции ref [YTFRONT-4941] ([237e207](https://github.com/ytsaurus/ytsaurus-ui/commit/237e207a04b68bf311f52d6ad3eb326ada718ff2))
* **Scheduling/Overview:** добавлена возможность настройки колонок [YTFRONT-4402] ([06d92bc](https://github.com/ytsaurus/ytsaurus-ui/commit/06d92bca76d61f26938b253b15f2a69e23a9ad18))
* **TabletError:** добавлена опция тестирования API [YTFRONT-4740] ([9bd661d](https://github.com/ytsaurus/ytsaurus-ui/commit/9bd661dbd3e5c6ddced004a27622914f4c055e10))
* **VCS:** запоминание последнего выбора пользователя [YTFRONT-4504] ([d253b09](https://github.com/ytsaurus/ytsaurus-ui/commit/d253b096391acb94700069dc09b6c8d00145cd3a))
* **UIFactory:** расширение UIFactory для внешних описаний [YTFRONT-4680] ([7201467](https://github.com/ytsaurus/ytsaurus-ui/commit/7201467d87cba61c0fd5ca172fe30032d16be5eb))
* **UIFactory:** создание analytics factory ([9e47bdb](https://github.com/ytsaurus/ytsaurus-ui/commit/9e47bdb9629b3603fce6ac0ec5a1f32107aa69b7))
* **UIFactory/Query:** добавлен чат [YTFRONT-4813] ([757de1f](https://github.com/ytsaurus/ytsaurus-ui/commit/757de1fbbe45a0381be13e70e1ba8efe0530f1be))

#### Исправления

* **Accounts:** использование recoursive_resource_usage для 'Aggregation' [YTFRONT-5024] ([a15f1a9](https://github.com/ytsaurus/ytsaurus-ui/commit/a15f1a906b6e0dc1ba6319998502f1aabb540d34))
* **BundleEditorDialog:** небольшие исправления [YTFRONT-4947] ([7a7432a](https://github.com/ytsaurus/ytsaurus-ui/commit/7a7432a4644e809a24a75f177a64dbd26506f81a))
* **CreateTableModal:** переименование 'Queue' =&gt; 'Queue table' [YTFRONT-4953] ([50b46b1](https://github.com/ytsaurus/ytsaurus-ui/commit/50b46b1b59327100a3b1bf986eb5b3260978ac5f))
* **Dashboard2:** редактирование конфигурации [YTFRONT-3400] ([1d3cea1](https://github.com/ytsaurus/ytsaurus-ui/commit/1d3cea14ed3d82858a143c28015a64ed11fddbff))
* **Dashboard:** автовысота, опечатки, лишний контрол в настройках операций, запасной вариант навигации [YTFRONT-3400] ([3a55fc1](https://github.com/ytsaurus/ytsaurus-ui/commit/3a55fc1bbb1a3229c6d48d6fdbc7a30dd4395b70))
* **Job:** хост должен быть кликабельным [YTFRONT-4958] ([fd55fa5](https://github.com/ytsaurus/ytsaurus-ui/commit/fd55fa507546711be886d1be9bfa65682bb94508))
* **Dialog/AccountsMultiple:** промежутки [YTFRONT-3400] ([0d2213f](https://github.com/ytsaurus/ytsaurus-ui/commit/0d2213f06d879fe5a15d533adfa589ad72f7b5dc))
* **Navigation/Consumers:** положение значка в кнопке [YTFRONT-4869] ([f59d4c5](https://github.com/ytsaurus/ytsaurus-ui/commit/f59d4c599a28c9e71a476db1bf7e8fd75c6e630d))
* **Navigation/Queue:** добавлено сообщение 'mapping in proggress' [YTFRONT-4954] ([0b14083](https://github.com/ytsaurus/ytsaurus-ui/commit/0b1408368e944b0cfb8bca71023d17c2a08e9bdf))
* **Navigation/Tablets:** клик по стрелке [YTFRONT-5030] ([044d0ae](https://github.com/ytsaurus/ytsaurus-ui/commit/044d0ae66d7fa10970ee46b9186cb770a4f83e33))
* **Navigation/AccessLog:** добавлен фильтр 'Recursive' в виде флажка [YTFRONT-5023] ([3e96cca](https://github.com/ytsaurus/ytsaurus-ui/commit/3e96cca6aac21c08008e2b9f281d0bdcbbf65ebe))
* **Navigation/Queue:** добавлено создание потребителя [YTFRONT-4869] ([69c23f9](https://github.com/ytsaurus/ytsaurus-ui/commit/69c23f9e4b1cba38cf2aea3cc803cdd64eb937b9))
* **Navigation/UploadFileManager:** сброс состояния после закрытия и сдвиг элемента меню ([b7ac155](https://github.com/ytsaurus/ytsaurus-ui/commit/b7ac155abd41fd6dfedbfebb4b7a8923aa22be7b))
* **Operations:** не показывать особый статус вместо 'suspended' [YTFRONT-5035] ([f305463](https://github.com/ytsaurus/ytsaurus-ui/commit/f305463f731c89afbd5e0b5ac5253303f1fb2940))
* **Operations:** таймлайн джобов [YTFRONT-4695] ([6f0296e](https://github.com/ytsaurus/ytsaurus-ui/commit/6f0296e1de33b764a2ae56f481ce33ffd23cba06))
* **Operation:** не отображать 'Data flow' для 'vanilla' [YTFRONT-4959] ([50c8a1f](https://github.com/ytsaurus/ytsaurus-ui/commit/50c8a1f6a01198fc652e91680b33d09c488a9d37))
* **Operation:** не показывать 'total job wall time'/'total cpu time spent' для 'vanilla' [YTFRONT-4960] ([4d9dec9](https://github.com/ytsaurus/ytsaurus-ui/commit/4d9dec9b02b87807aba8cebeff9c43756f8577c7))
* **Operations:** добавлена ссылка на систему анализа производительности [YTFRONT-4924] ([1bd2b69](https://github.com/ytsaurus/ytsaurus-ui/commit/1bd2b69ca71b8538504ce33b469d9c905e784410))
* **Operations:** изменение метаданных времени выполнения [YTFRONT-4940] ([db63474](https://github.com/ytsaurus/ytsaurus-ui/commit/db63474a5e160f2fb2207f9cb61c0c4d3c4f3cf7))
* **Operation/Details/Tasks:** добавлен фильтр 'Hide empty' в диалог "Aborted statistics" [YTFRONT-5012] ([4a55020](https://github.com/ytsaurus/ytsaurus-ui/commit/4a550206c57eab90ef5456f7b8e5e8e88639e9c8))
* **Operation/LivePreview:** исправление для 'n.map is not a function ...' [YTFRONT-5021] ([71fda06](https://github.com/ytsaurus/ytsaurus-ui/commit/71fda06115b2375d389ff33b847243ccb27b8de8))
* **Operation/Details:** установлен лимит по умолчанию для видимого количества env/layers [YTFRONT-4962] ([29a1217](https://github.com/ytsaurus/ytsaurus-ui/commit/29a12172d24a478aa943b52248c030a06f76ae23))
* **Operations/Detail:** всплывающая подсказка особого статуса [YTFRONT-4943] ([7405792](https://github.com/ytsaurus/ytsaurus-ui/commit/7405792875fe57cff6a4cfd30199f480f96262ba))
* **Operations/Detail:** добавлены особые статусы для gpu vanilla операций [YTFRONT-4943] ([a4a6a40](https://github.com/ytsaurus/ytsaurus-ui/commit/a4a6a40796058c658cedf200700788612763f81f))
* **Queries/Chart:** datetime YQL на диаграмме [YTFRONT-4937] ([e3d692b](https://github.com/ytsaurus/ytsaurus-ui/commit/e3d692b0036786aace2d3fc9d87eb9f0a38565ee))
* **Queue/Exports:** валидатор длительности экспорта [YTFRONT-4995] ([462525c](https://github.com/ytsaurus/ytsaurus-ui/commit/462525c7da3a959794f6feac7dc0724ba2efd44d))
* **Scheduling:** отображение 'Automatically calculated' только для пулов [YTFRONT-3812] ([a2b80d3](https://github.com/ytsaurus/ytsaurus-ui/commit/a2b80d3abb37c5565ffe0faf43143cb1d7d85dec))
* **Scheduling:** использование fifo_index в качестве колонки сортировки для FIFO-пула по умолчанию [YTFRONT-4942] ([422ffe7](https://github.com/ytsaurus/ytsaurus-ui/commit/422ffe7f50afdcbb169fe971a7502fd14f72281f))
* **Timeline:** ошибка from to [YTFRONT-5011] ([b58e037](https://github.com/ytsaurus/ytsaurus-ui/commit/b58e0376e88ff9af6d0afceb6debbec5a16a3ccf))


{% endcut %}


{% cut "**1.91.3**" %}

**Дата релиза:** 2025-05-21


**Страница релиза:** [1.91.3](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.91.3)


**Docker-образ:** [ghcr.io/ytsaurus/ui:1.91.3](https://github.com/orgs/ytsaurus/packages/container/ui/420661217?tag=1.91.3)


#### [1.91.3](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.91.2...ui-v1.91.3) (2025-05-21)

#### Новые возможности

* **Accounts:** преобразование строки пути в ссылку [YTFRONT-4896] ([a514bbe](https://github.com/ytsaurus/ytsaurus-ui/commit/a514bbec1287e561dac0bca4aa8c2c44d06bb85e))
* **Bundles:** добавлен лимит памяти для запросов [YTFRONT-4651] ([eca2997](https://github.com/ytsaurus/ytsaurus-ui/commit/eca29978c3962cf40a593bec32865390fee0dc37))
* **Bundles:** бит/с в Б/С [YTFRONT-4523] ([34f08aa](https://github.com/ytsaurus/ytsaurus-ui/commit/34f08aa3d5a8b93a79e007400eae64adcd010ac2))
* **Download:** копирование в буфер обмена [YTFRONT-4895] ([623201e](https://github.com/ytsaurus/ytsaurus-ui/commit/623201e8b19a4a8e1327bf96eb3455b51683b019))
* **Monaco:** vim-режим [YTFRONT-4807] ([00b5cb7](https://github.com/ytsaurus/ytsaurus-ui/commit/00b5cb701d3af062dfb6d4dcea40683bc3b98340))
* **Navigation/Queue:** добавлены возможности для экспорта очередей [YTFRONT-4482] ([58baccb](https://github.com/ytsaurus/ytsaurus-ui/commit/58baccb4374a0f6282d331654bf0e53ce825acd2))
* **Navigation/SymLinks:** добавлена кнопка target_path [YTFRONT-4224] ([8072c53](https://github.com/ytsaurus/ytsaurus-ui/commit/8072c53b4022fd84d97d8af2a347a6f3b0e0313d))
* **Operations/List:** применение транзакций к таблицам 'in'/'out' [YTFRONT-3134] ([db581dd](https://github.com/ytsaurus/ytsaurus-ui/commit/db581dd0274e3d099d07f368be575aeeec6a2167))
* **Queries:** сохранение выбора движка пользователем [YTFRONT-4816] ([6865bde](https://github.com/ytsaurus/ytsaurus-ui/commit/6865bdeaf77ff8fca429ec4df51eb4f86160145c))
* **Queries/Graph:** повышена наглядность количества задач [YTFRONT-4812] ([d393feb](https://github.com/ytsaurus/ytsaurus-ui/commit/d393feb2c5d62e43f7ff6bff135206d4896cbd25))
* **Queries:** открытие навигации по клику на путь [YTFRONT-4802] ([2630b4c](https://github.com/ytsaurus/ytsaurus-ui/commit/2630b4c29aadc9b5c1559cec69bd29a6bf0aa3a0))
* **Queries:** модальное окно подтверждения перенаправления [YTFRONT-4809] ([78da341](https://github.com/ytsaurus/ytsaurus-ui/commit/78da34127592eec1afb81f1c6d43e0e8a2bdd238))
* **Queries:** отображение статуса в селекторе клики CHYT [YTFRONT-4825] ([3c5065a](https://github.com/ytsaurus/ytsaurus-ui/commit/3c5065ac85050b9f23c501c264a27f4ec9c9732e))
* **Scheduling:** добавлен значок для пулов с effective_lightweight_operations_enabled [YTFRONT-4275] ([395c512](https://github.com/ytsaurus/ytsaurus-ui/commit/395c5122c06c0255fe69a0756e70f3ca3564d003))
* обновление @ytsaurus/javascript-wrapper ([a953b63](https://github.com/ytsaurus/ytsaurus-ui/commit/a953b6370921e691013b74a6909906a64d2201f4))


#### Исправления

* **Account:** нормализация временной метки [YTFRONT-4913] ([76b38e1](https://github.com/ytsaurus/ytsaurus-ui/commit/76b38e17eb8574668ea5d4c00ec5b5434482d850))
* **ACL:** некорректное значение 'Inherit ACL' [YTFRONT-4718] ([65d1f3e](https://github.com/ytsaurus/ytsaurus-ui/commit/65d1f3ecc97f7739390db398def3bdd93e22664f))
* **ChaosReplicatedTable:** использование `alter_table_replica` для управления `enable_replicated_table_tracker` [YTFRONT-4796] ([6983439](https://github.com/ytsaurus/ytsaurus-ui/commit/6983439c34cc6f9536824c3aa7f5125bfdfc3ab5))
* **DataTableYT:** атрибуты таблицы должны корректно прокручиваться при открытой боковой панели QT [YTFRONT-4900] ([9f64661](https://github.com/ytsaurus/ytsaurus-ui/commit/9f6466109d8935d2c231a4c858f5e477720dbe99))
* **Dialog:** обновление события при очистке [YTFRONT-4829] ([9d8ab15](https://github.com/ytsaurus/ytsaurus-ui/commit/9d8ab15fbbf33c3aacc2cb090e87501f91337b86))
* **Navigation:** изменение заголовка таблиц [YTFRONT-4822] ([ddfa239](https://github.com/ytsaurus/ytsaurus-ui/commit/ddfa239e6becbfc492f0f53c25535c1e1d9972ad))
* **Navigation/AccessLog:** отображение пути назначения [YTFRONT-4341] ([18ac142](https://github.com/ytsaurus/ytsaurus-ui/commit/18ac142f2b21902ff90dace3284e478a0704879e))
* **Navigation/DownloadManager:** добавлены подробности в сообщение об ошибке [YTFRONT-4790] ([25c35ad](https://github.com/ytsaurus/ytsaurus-ui/commit/25c35ad41998c185929ff4553494754363f525d4))
* **Navigation/DynamicTable:** не скрывать ключевые колонки в динамических таблицах [YTFRONT-4826] ([edc5133](https://github.com/ytsaurus/ytsaurus-ui/commit/edc51336f5978c4cea160e36bc6419646f3dde0b))
* **Navigation/MapNode:** добавлен значок упорядоченной таблицы ([f872cde](https://github.com/ytsaurus/ytsaurus-ui/commit/f872cdec0d8fe80f1cc1786aedd9b64d0d13e353))
* **Navigation/MapNode/TableSortModal:** кодировка колонок [YTFRONT-4873] ([6a4ab81](https://github.com/ytsaurus/ytsaurus-ui/commit/6a4ab8113e0e44ffdfbe9a34b89f64df4a98858b))
* **Navigation/Metadata:** vim-режим monaco [YTFRONT-4908] ([10d4f4b](https://github.com/ytsaurus/ytsaurus-ui/commit/10d4f4bc333a7ae50befbbd4ffc7318cdab160dc))
* **Navigaion/NavigationError:** n.map is not a function [YTFRONT-4858] ([ff5e6c6](https://github.com/ytsaurus/ytsaurus-ui/commit/ff5e6c6bb21e9ccbd78978ca9dc6d581282feee8))
* **Navigation/Table:** исправление смещения и постраничного вывода для размонтированных таблиц [YTFRONT-4021] ([f7b2b68](https://github.com/ytsaurus/ytsaurus-ui/commit/f7b2b68ff0b5e04c96f6dc4a9d45664bf6a7530e))
* **Navigation/Table/SidPanel:** не закрывать боковую панель при переходе в родительскую папку [YTFRONT-4729] ([b9afc28](https://github.com/ytsaurus/ytsaurus-ui/commit/b9afc28826bbef9126107ce1d109350def1f95bd))
* **Navigation/Table/CellPreview:** предпросмотр должен работать для колонок с косой чертой [YTFRONT-4797] ([c93e8bc](https://github.com/ytsaurus/ytsaurus-ui/commit/c93e8bc2a2e8657c138c107b658cea3f37e4f090))
* **Navigation/Table/SidePanel: ** избавление от дублирующихся полос прокрутки [YTFRONT-4840] ([7c81ad5](https://github.com/ytsaurus/ytsaurus-ui/commit/7c81ad5cdad68a73cba2e602bcdd952793ee4391))
* **Naviagation/Table/Download:** не использовать `dump_error_into_response: true` [YTFRONT-4856] ([6560aa7](https://github.com/ytsaurus/ytsaurus-ui/commit/6560aa7bba4d5eeb30de1300c541605033348713))
* **Nodes:** неверный цвет в теме с высокой контрастностью [YTFRONT-4610] ([e00b6c0](https://github.com/ytsaurus/ytsaurus-ui/commit/e00b6c04d97d3d2ca4f4ff3a32473fedb3a0b3b0))
* **Operations:** большое количество строк в джобах [YTFRONT-4854] ([be0bde3](https://github.com/ytsaurus/ytsaurus-ui/commit/be0bde3ad66cc37eb2da22904df0e3f1be8bf08e))
* **Operation/Details/Alerts:** исправление для информационных URL [YTFRONT-4800] ([c6961a0](https://github.com/ytsaurus/ytsaurus-ui/commit/c6961a043218717ac20269a0aa09052a094445a4))
* **Operation/Jobs:** исправление опечатки [YTFRONT-4798] ([31ea5a1](https://github.com/ytsaurus/ytsaurus-ui/commit/31ea5a137cbc2f7f4c8e46890e08a16d532a0042))
* **Operation/Statistics:** добавлена кнопка копирования значений [YTFRONT-4805] ([d1832fe](https://github.com/ytsaurus/ytsaurus-ui/commit/d1832fe864049ec3f350fa5178218ed1addde290))
* **OperationJobsTable:** закрытие модального окна ввода пути по esc ([80629ec](https://github.com/ytsaurus/ytsaurus-ui/commit/80629ecc2b40eb8d43cc2621d270d13b4425ed16))
* **Operations/List:** содержимое таблицы не должно подпрыгивать при закреплении панели инструментов [YTFRONT-4870] ([f227af2](https://github.com/ytsaurus/ytsaurus-ui/commit/f227af2f809c6948336bb4af00fb7eaf0e7cae99))
* **Operation/Jobs:** установлена минимальная ширина всплывающего окна фильтра 'Incarnation' [YTFRONT-4836] ([ee9fed0](https://github.com/ytsaurus/ytsaurus-ui/commit/ee9fed006d8d7c852d198f4357671aa1b91694a6))
* **PathEditor:** возврат обратного вызова onBlur ([dea479b](https://github.com/ytsaurus/ytsaurus-ui/commit/dea479be18178a9f6a891f75f1fc027fffa61c9a))
* **Queries:** разрешен пустой ACO для существующего запроса ([2ee0d26](https://github.com/ytsaurus/ytsaurus-ui/commit/2ee0d2661d2af2dca765b386ef6fbf33a780f6db))
* **Queries:** исправлена ссылка для скачивания Excel ([912dc1e](https://github.com/ytsaurus/ytsaurus-ui/commit/912dc1ef7a87db854c0c858facb02de554bbb9b4))
* **Queries:** прогресс CHYT [YTFRONT-4833] ([a535db2](https://github.com/ytsaurus/ytsaurus-ui/commit/a535db27cf855f20b70298f618bf3956128af24a))
* **Queries:** перенос кнопки редактирования вправо от строки [[#1033](https://github.com/ytsaurus/ytsaurus-ui/issues/1033)] ([af269c8](https://github.com/ytsaurus/ytsaurus-ui/commit/af269c805cef6656edd1e3865e5b4dabf7da707b))
* **Queries:** исправлено отображение статуса неактивной клики, когда она активна и исправна ([8d4f2b7](https://github.com/ytsaurus/ytsaurus-ui/commit/8d4f2b732c2712b7c9ec1dbbead117d794812c8b))
* **Queries:** запрос пути ([45964bd](https://github.com/ytsaurus/ytsaurus-ui/commit/45964bd3d45f224cac600b1dc9a9f7b76c9a1761))
* **Queries:** повышение скорости навигации [YTFRONT-4771] ([f60341d](https://github.com/ytsaurus/ytsaurus-ui/commit/f60341d48c8675760a717d0acbf0196977bd226e))
* **Queries:** исправление схемы [YTFRONT-4771] ([c008d19](https://github.com/ytsaurus/ytsaurus-ui/commit/c008d1976db7119c7d2f4bfb3034ae71b41987db))
* **Queries:** обновление черновика во время выполнения запроса [YTFRONT-4791] ([d4647da](https://github.com/ytsaurus/ytsaurus-ui/commit/d4647daa58e8c7e060da730369f7711498a03dc8))
* **Queries/History:** значок статуса [YTFRONT-4821] ([b5f29a9](https://github.com/ytsaurus/ytsaurus-ui/commit/b5f29a9193127b1f8cde393eb126e202a4cb4e0b))
* **Queries/Monaco:** обрезка подсказки пути [YTFRONT-4806] ([9fb5334](https://github.com/ytsaurus/ytsaurus-ui/commit/9fb5334338e9b097d6e8dc85a6fcb0d5cffece0d))
* **Queries:** исправлена навигация по URL-пути [YTFRONT-4802] ([fc5f0e5](https://github.com/ytsaurus/ytsaurus-ui/commit/fc5f0e5bc7b23d5eab5aff39e2fecd34199c0e58))
* **Queries:** исправлена подсказка запроса, теперь `` полностью заменяются на `//` [[#1032](https://github.com/ytsaurus/ytsaurus-ui/issues/1032)] ([d1d43de](https://github.com/ytsaurus/ytsaurus-ui/commit/d1d43de49a224035b23a01c244d40ce3cfa7d62d))
* **RequestPermissions:** поле 'Subjects' помечено как обязательное [YTFRONT-4745] ([ed3ec63](https://github.com/ytsaurus/ytsaurus-ui/commit/ed3ec633ee694d18157cc1388c0d955dc0ba8971))
* **Scheduling:** исправление опечатки (+новый значок) [YTFRONT-4275] ([919a26d](https://github.com/ytsaurus/ytsaurus-ui/commit/919a26d4c91d7f36443edbe30996fc29855eba02))
* **Scheduling/Details:** лимиты операций [YTFRONT-4820] ([4798733](https://github.com/ytsaurus/ytsaurus-ui/commit/4798733bd04be733bb1bb2d0bade339c517fa010))
* **System:** мастера на маленьких мониторах [YTFRONT-4830] ([6e46f6c](https://github.com/ytsaurus/ytsaurus-ui/commit/6e46f6cf8967b39d2e27afa74b0ed19ecc8fea4d))
* **Users/GroupSuggest:** видимость подсказки группы [YTFRONT-4737] ([2084021](https://github.com/ytsaurus/ytsaurus-ui/commit/2084021c65c153117331d80fc177fc8c167e0a7d))
* **UI/Layout:** использование 'maxContentWidth' для большинства страниц [YTFRONT-4149] ([259754a](https://github.com/ytsaurus/ytsaurus-ui/commit/259754ab35a7df49c606146da1269b34f49ef1ed))
* **UIFactory:** переработан метод `UIFactory.renderAppFooter` [YTFRONT-4149] ([b3dc9b2](https://github.com/ytsaurus/ytsaurus-ui/commit/b3dc9b259df791822f42f41b09bdf88c7ff9464f))
* **YTErrorBlock:** кнопка копирования должна использовать тот же формат текста [YTFRONT-3310] ([cd03ff8](https://github.com/ytsaurus/ytsaurus-ui/commit/cd03ff8e86ee32e3ea20029467eb318d218bae16))
* **YsonView:** прокрутка распарсенного значения [YTFRONT-4823] ([43e181d](https://github.com/ytsaurus/ytsaurus-ui/commit/43e181d5e19e2d8c3037f2db196a0ab37121ece5))
* **localmode:** синхронизация cluster_name [YTFRONT-4326] ([3bddca1](https://github.com/ytsaurus/ytsaurus-ui/commit/3bddca1a4403da895605b992181b6c976c0d6209))
* изменено значение по умолчанию UIFactory.onChytAliasSqlClick — теперь кнопка sql работает на странице chyt ([2ed97f0](https://github.com/ytsaurus/ytsaurus-ui/commit/2ed97f06a344eb9bfd8a2112e4c144180d0aaa5a))
* селекторы кластеров не отображаются, если доступен только один кластер ([01ce947](https://github.com/ytsaurus/ytsaurus-ui/commit/01ce947dfb86ada716ae3fb5acbc47356a77737f))
* типы batch api ([41a3d68](https://github.com/ytsaurus/ytsaurus-ui/commit/41a3d6823d596e913dffdf5e70e5de0f90a60e97))
* сообщения об ошибках создания и удаления очередей ([35bc106](https://github.com/ytsaurus/ytsaurus-ui/commit/35bc106219d7926eaf78aa93a8f5d1242c07ad67))



{% endcut %}


{% cut "**1.84.0**" %}

**Дата релиза:** 2025-03-12


**Страница релиза:** [1.84.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.84.0)


**Docker-образ:** [ghcr.io/ytsaurus/ui:1.84.0](https://github.com/orgs/ytsaurus/packages/container/ui/372794424?tag=1.84.0)


#### [1.84.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.83.0...ui-v1.84.0) (2025-03-12)


#### Новые возможности

* **Components/Versions:** отображение большего числа колонок типов узлов [YTFRONT-4406] ([bbd62fa](https://github.com/ytsaurus/ytsaurus-ui/commit/bbd62fa594642ca6888a78392cf27ac3dc672472))
* **Components/Versions:** отображение большего числа колонок типов узлов [YTFRONT-4406] ([bbd62fa](https://github.com/ytsaurus/ytsaurus-ui/commit/bbd62fa594642ca6888a78392cf27ac3dc672472))
* **Navigation/Table:** добавлена опция remount [YTFRONT-3593] ([a1ea452](https://github.com/ytsaurus/ytsaurus-ui/commit/a1ea452174491b7304787c5a10b9066b150a11a4))
* **Navigation/Table:** разрешён просмотр отмонтированных динамических таблиц [YTFRONT-4021] ([519a1b7](https://github.com/ytsaurus/ytsaurus-ui/commit/519a1b763cfb317ff4e4198d0396fdb0b6cbcd0e))
* **Navigation:** изменены метаданные использования ресурсов [YTFRONT-4764] ([5247a7f](https://github.com/ytsaurus/ytsaurus-ui/commit/5247a7f952200e3bebae41ee382b75c207ce8adc))
* **Operation/Jobs:** добавлен фильтр 'Incarnation' [YTFRONT-4684] ([fa3f2e1](https://github.com/ytsaurus/ytsaurus-ui/commit/fa3f2e10a92be34a59c2da9f1313f2af53cd6db1))
* **Operations/Details:** отображение путей слоёв [YTFRONT-4618] ([43f783d](https://github.com/ytsaurus/ytsaurus-ui/commit/43f783dae33dee87174d6a799d2bd64c3b0326ba))
* **Queries:** добавлена ссылка на полный результат [YTFRONT-4674] ([c32a867](https://github.com/ytsaurus/ytsaurus-ui/commit/c32a867b7ba1b4bbc4b4f90ca9c932a0f62550cb))

#### Исправления

* добавлен корректный отступ во всплывающем окне выбора даты ([96f19af](https://github.com/ytsaurus/ytsaurus-ui/commit/96f19af972c6e429edccb639989fd599e1b4567e))
* **Components:** добавлен отступ для пользовательских футеров [YTFRONT-4406] ([48b5e40](https://github.com/ytsaurus/ytsaurus-ui/commit/48b5e401b1aa7b982ecd1919f26f7a87330e26a6))
* **Components:** обработка данных списка узлов [YTFRONT-4765] ([e27323b](https://github.com/ytsaurus/ytsaurus-ui/commit/e27323baad47aae7c489502fa9fc0ecd9ae4e61e))
* **localmode/Queries/QueryClusterSelector:** использование кластера из get_query_tracker_info и //sys/[@cluster](https://github.com/cluster)_name [YTFRONT-4326] ([becf0ec](https://github.com/ytsaurus/ytsaurus-ui/commit/becf0ec4a35336fbfe60c00c5153ee056f4d16a1))
* **navigation:** исправлена опечатка в AccessLog ([fdecdbe](https://github.com/ytsaurus/ytsaurus-ui/commit/fdecdbe1c0082337d63b01d7d42640993b78da24))
* **OAuth:** замена заголовка Authorization на cookie access_token [[#958](https://github.com/ytsaurus/ytsaurus-ui/issues/958)] ([2a3d604](https://github.com/ytsaurus/ytsaurus-ui/commit/2a3d604a59992d218851d790b668176b5cbe2408))
* **Operations:** статистика utf8 [YTFRONT-4700] ([e344415](https://github.com/ytsaurus/ytsaurus-ui/commit/e344415863e601d8f44857f482d018232a344a4a))
* **Queries:** изменено выравнивание результата [YTFRONT-4736] ([95cd793](https://github.com/ytsaurus/ytsaurus-ui/commit/95cd793f073f2dad716581e0878000affb6c6084))
* **Queries:** теперь изменение ACO запроса не сбрасывает состояние графика и наоборот [[#1006](https://github.com/ytsaurus/ytsaurus-ui/issues/1006)] ([1a94c7f](https://github.com/ytsaurus/ytsaurus-ui/commit/1a94c7f8c7561d98bf01ebe3401fbb2c9a5ac94a))
* **Queries:** тело запроса не отображается в новом окне [[#266](https://github.com/ytsaurus/ytsaurus-ui/issues/266)] ([c80f1ed](https://github.com/ytsaurus/ytsaurus-ui/commit/c80f1ed4881021ffadbff054905376a2abe50256))
* **Queries:** исправлен экспорт результатов запросов через excel [[#1022](https://github.com/ytsaurus/ytsaurus-ui/issues/1022)] ([4de3573](https://github.com/ytsaurus/ytsaurus-ui/commit/4de357391c9554389ee1638b21d3d9ae73addbdc))
* **Queries:** исправлена вставка предложенного пути в редакторе запросов [[#1027](https://github.com/ytsaurus/ytsaurus-ui/issues/1027)] ([6515314](https://github.com/ytsaurus/ytsaurus-ui/commit/6515314ba74f60ea206c0ffccec232785b459368))
* **Queries:** теперь вся область элемента истории запросов кликабельна [[#1000](https://github.com/ytsaurus/ytsaurus-ui/issues/1000)] ([2ff5dec](https://github.com/ytsaurus/ytsaurus-ui/commit/2ff5dece061e8b585d7d2e4a2469254c22a5759a))


{% endcut %}


{% cut "**1.82.1**" %}

**Дата релиза:** 2025-02-15


**Страница релиза:** [1.82.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.82.1)


**Docker-образ:** [ghcr.io/ytsaurus/ui:1.82.1](https://github.com/orgs/ytsaurus/packages/container/ui/356327466?tag=1.82.1)


#### [1.82.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.82.0...ui-v1.82.1) (2025-02-15)


#### Исправления

* **Navigation/Table:** кнопки запросов должны быть всегда видимы [YTFRONT-4706] ([8d19235](https://github.com/ytsaurus/ytsaurus-ui/commit/8d19235558016e2d342b4973c84c6d0fa09c88f5))
* **Navigation:** добавлен отсутствующий атрибут и исправлена кнопка копирования ([f18fd4e](https://github.com/ytsaurus/ytsaurus-ui/commit/f18fd4e21f328a638241d03de8f5140b1dc48a77))

{% endcut %}


{% cut "**1.82.0**" %}

**Дата релиза:** 2025-02-14


**Страница релиза:** [1.82.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.82.0)


**Docker-образ:** [ghcr.io/ytsaurus/ui:1.82.0](https://github.com/orgs/ytsaurus/packages/container/ui/355778977?tag=1.82.0)


#### [1.82.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.81.0...ui-v1.82.0) (2025-02-14)


#### Новые возможности

* **CHYT/Tabs:** добавлена вкладка для ссылки на логи [YTFRONT-4675] ([fc2f4b9](https://github.com/ytsaurus/ytsaurus-ui/commit/fc2f4b9fa6135b0460921582635d5ae892caeb7c))
* **Navigation/NavigationError:** добавлена кнопка копирования и улучшены детали ошибки [YTFRONT-4049] ([fb70cba](https://github.com/ytsaurus/ytsaurus-ui/commit/fb70cba73da9891e84fb3fc0b072752f05b6e108))
* **Operations:** вытесненные джобы [YTFRONT-4641] ([472d41e](https://github.com/ytsaurus/ytsaurus-ui/commit/472d41e25e79e408ed09ffb1192e38b0baa0b894))
* **Settings:** изменён элемент настроек подсказок [YTFRONT-4703] ([4c1776c](https://github.com/ytsaurus/ytsaurus-ui/commit/4c1776cefc8348d2eaba97be83f05b397ff52818))
* **CopyObjectModal:** добавлено рекурсивное создание папок при копировании [YTFRONT-3041] ([670c084](https://github.com/ytsaurus/ytsaurus-ui/commit/670c084f758d30477ca0cc9915070e06aacbcd62))
* **ManageTokens:** опциональный пароль для выпуска токена ([e696da8](https://github.com/ytsaurus/ytsaurus-ui/commit/e696da8540821afc5f471c2c3210c75bb9deba58))
* **Navigation:** добавлены страницы для ошибок 500 и 901 [YTFRONT-4049] ([8b20786](https://github.com/ytsaurus/ytsaurus-ui/commit/8b20786304fcdc2cf3210756483f48d7bc92d836))
* **System/Masters:** добавлена возможность «Сменить лидера» для «Вторичных мастеров» и «Провайдера меток времени» [YTFRONT-4214] ([20762b3](https://github.com/ytsaurus/ytsaurus-ui/commit/20762b3091b4d9994404b79d7f405302ae8b1cb5))
* **Navigation/Document:** кнопка `YQL query` доступна, если _yql_type == "view" [YTFRONT-4463] ([0421120](https://github.com/ytsaurus/ytsaurus-ui/commit/0421120c1cd1e9e7c60a500973cf2dde0645372d))
* **Job/Statistics:** используется operation_statistics_descriptions из supported_features [YTFRONT-3522] ([abf49e5](https://github.com/ytsaurus/ytsaurus-ui/commit/abf49e5af07010f7253a01e211e8a19f3a131e3a))
* **Node:** отображается версия узла [YTFRONT-4555] ([0e460d2](https://github.com/ytsaurus/ytsaurus-ui/commit/0e460d29bf528a34ea48f4ffef4bccb24dde32f1))
* **DownloadManager:** добавлена настройка для имени файла [YTFRONT-3564] ([285d075](https://github.com/ytsaurus/ytsaurus-ui/commit/285d075f06a478b096628bf69ada5aa355492eef))
* **Queries:** новый график прогресса [YTFRONT-4112] ([24c142e](https://github.com/ytsaurus/ytsaurus-ui/commit/24c142e1bd8fdff6cb948fd1a72c838d6d216db9))
* **Navigation/CreateTableModal:** добавлена опция создания очереди [YTFRONT-4658] ([df445ed](https://github.com/ytsaurus/ytsaurus-ui/commit/df445ed485b5e8983a204fbcb7965d2a7fe15763))
* теперь UIFactory.getNavigationExtraTabs позволяет запрашивать дополнительные атрибуты для узла навигации ([36a2d12](https://github.com/ytsaurus/ytsaurus-ui/commit/36a2d128fd36b9cdcc6e347c20166a3816ea59f8))
* **Queries:** встроенные подсказки [YTFRONT-4612] ([11d4b59](https://github.com/ytsaurus/ytsaurus-ui/commit/11d4b596c051310671a8bc805a55612af59e8f49))
* **Navigation/ContentViewer:** добавлена возможность просмотра chaos_cells [YTFRONT-3653] ([299de09](https://github.com/ytsaurus/ytsaurus-ui/commit/299de0962a3d2c4e2c00b110d39fa6d267331f6b))
* **Queries:** chart kit [YTFRONT-4506] ([5ad677d](https://github.com/ytsaurus/ytsaurus-ui/commit/5ad677df47f4f878268cf450ceeda1906d4c226e))


#### Исправления

* **Navigation:** флажок xlsx типов выключен по умолчанию [YTFRONT-4699] ([233e945](https://github.com/ytsaurus/ytsaurus-ui/commit/233e945fdd820c2906e7ea3868414dc50e9b5893))
* предотвращение срабатывания горячих клавиш страницы при открытом YTDialog [[#768](https://github.com/ytsaurus/ytsaurus-ui/issues/768)] ([1e6d097](https://github.com/ytsaurus/ytsaurus-ui/commit/1e6d097bb1074c8a0b07c4e4a0679afe80380d53))
* **ExperimentalPages:** ожидание allowedExperimentalPages перед редиректом startPage ([527ad72](https://github.com/ytsaurus/ytsaurus-ui/commit/527ad7269cb14a34009ad82408ebec41785b06e1))
* **CellPreview:** отображение кнопки предпросмотра при отключенных типах yql v3 [[#928](https://github.com/ytsaurus/ytsaurus-ui/issues/928)] ([0e6fd69](https://github.com/ytsaurus/ytsaurus-ui/commit/0e6fd690a44a79eb4245017a7defc3950cb2e27c))
* **CopyObjectModal:** изменён текст флажка [YTFRONT-3041] ([ce6a02f](https://github.com/ytsaurus/ytsaurus-ui/commit/ce6a02fd6383371ccc51c844454de0fed605e3a2))
* **Users/Groups:** упрощён компонент CommaSeparatedListWithRestCounter, теперь пользователь всегда может видеть всех участников группы и все группы конкретного пользователя [[#704](https://github.com/ytsaurus/ytsaurus-ui/issues/704)] ([453d8d7](https://github.com/ytsaurus/ytsaurus-ui/commit/453d8d7f00fad7d2cf054a3b85fb43687c68e002))
* **ACL:** мелкие исправления для SubjectsControl [YTFRONT-4465] ([92f521b](https://github.com/ytsaurus/ytsaurus-ui/commit/92f521b773df34bff1c8fcf292a2232df4d61669))
* **ACL:** UI должен отображать ошибку, когда у пользователя нет прав на установку ACL [[#938](https://github.com/ytsaurus/ytsaurus-ui/issues/938)] ([93722c7](https://github.com/ytsaurus/ytsaurus-ui/commit/93722c7fc76d344ce9e1f710316d8302c6ebe7fe))
* **ManageTokens:** управление токенами не работает для http [[#953](https://github.com/ytsaurus/ytsaurus-ui/issues/953)] ([03fdc67](https://github.com/ytsaurus/ytsaurus-ui/commit/03fdc67ac0ad31bb52cae1332d1b7af469f5f241))
* **Navigation:** проверка существования файла по имени [YTFRONT-4638] ([2edc61b](https://github.com/ytsaurus/ytsaurus-ui/commit/2edc61b2634e6df8dd529848e34fcee80e5e729a))
* теперь редактор атрибутов всегда использует упорядоченное слияние, что предотвращает перемешивание строк таблицы ([8097144](https://github.com/ytsaurus/ytsaurus-ui/commit/809714489139becd16ad1ebf75806d50054eae9e))
* **Queries:** узлы графа снова кликабельны [YTFRONT-4682] ([ddb125c](https://github.com/ytsaurus/ytsaurus-ui/commit/ddb125cdfeff24fc34447bfd6e83087a8974e0dd))
* **Components/Node/MemoryPopup:** не отображать строки со значением '0B' [YTFRONT-4625] ([00268e3](https://github.com/ytsaurus/ytsaurus-ui/commit/00268e33e40d486fd05eca3277867b1a4beec232))
* **Components/Node/SloutResources:** исправлен расчёт 'Slot Resources' [YTFRONT-4631] ([4c898c0](https://github.com/ytsaurus/ytsaurus-ui/commit/4c898c0706fea5872810d41384282aac67fe5ce8))
* **Jobs:** исправлен фильтр по состоянию джоба, теперь он корректно применяется из URL [[#775](https://github.com/ytsaurus/ytsaurus-ui/issues/775)] ([e4f96b7](https://github.com/ytsaurus/ytsaurus-ui/commit/e4f96b749fd22bbb7824196e38221637f6b300a3))
* **MultipleActions:** фильтрация пустых секций [YTFRONT-4627] ([8986360](https://github.com/ytsaurus/ytsaurus-ui/commit/8986360756b9755a03f8dbefa503f402b6cc9668))
* **Navigation/PathEditor:** использование последнего фрагмента для подсказки [YTFRONT-4032] ([f451603](https://github.com/ytsaurus/ytsaurus-ui/commit/f451603614e4f659b7eb75c12df5bf5bcbf5ae68))
* **Navigation/Tabs:** оптимизация запросов атрибутов [YTFRONT-3182] ([ef9100c](https://github.com/ytsaurus/ytsaurus-ui/commit/ef9100cb0e751abb88821511c16be7f4e711f312))
* **Operaion/Jobs:** убран лишний фильтр 'DataSource' [YTFRONT-4629] ([77c2ace](https://github.com/ytsaurus/ytsaurus-ui/commit/77c2ace10ce2653adc81b6d8847d1cc68a44d574))
* **Operation/Events:** мелкое исправление стиля для прогресса [YTFRONT-4631] ([c3b1225](https://github.com/ytsaurus/ytsaurus-ui/commit/c3b1225974efe18c586ee74ae23853696003afa5))
* **OperationDetail/Tasks:** мелкое исправление для колонки Aborted [YTFRONT-4632] ([73f32c9](https://github.com/ytsaurus/ytsaurus-ui/commit/73f32c95a024477991f6b16e8056c73068ff0557))
* **Scheduling:** замена устаревшего атрибута [YTFRONT-4652] ([71d0534](https://github.com/ytsaurus/ytsaurus-ui/commit/71d05349bac045ee4850bb5b3ba631db3e9f0f7b))
* **System:** мелкое css-исправление для '[nonvoting]' [YTFRONT-4477] ([ec76ecc](https://github.com/ytsaurus/ytsaurus-ui/commit/ec76ecceca83d8abadc0bf2a9256a41ced466977))
* **DeleteObjectModal:** отображение разных текстов при попытке пользователя безвозвратно удалить объекты [[#937](https://github.com/ytsaurus/ytsaurus-ui/issues/937)] ([e067dba](https://github.com/ytsaurus/ytsaurus-ui/commit/e067dba80c45f8cac265694cbb31132f0f7ea7f9))
* **Navigation:** добавлен отсутствующий атрибут для вкладки flow [YTFRONT-4665] ([cb2d40b](https://github.com/ytsaurus/ytsaurus-ui/commit/cb2d40b6168139342f5fa623febf87465e70a656))
* **ClusterMenu:** исправление для Settings/Queries ([2de6bd7](https://github.com/ytsaurus/ytsaurus-ui/commit/2de6bd78b1fdafaf559c1426dad35a88dc329b48))
* **Navigation/MapNode:** параметр фильтра не работает из url [YTFRONT-4481] ([b4d9ab2](https://github.com/ytsaurus/ytsaurus-ui/commit/b4d9ab29be683eb525942737c210c62a55674776))
* **Operation/JobsMonitor:** улучшено условие видимости [YTFRONT-4600] ([12184fb](https://github.com/ytsaurus/ytsaurus-ui/commit/12184fb3aac672751c2c4bdfb9aee2d49389e92f))
* **System/Nodes:** не использовать banned=disabled для ссылки Rack [YTFRONT-4603] ([565684b](https://github.com/ytsaurus/ytsaurus-ui/commit/565684b62a53d3a2f46d0a585a11c5667b19ed83))

{% endcut %}


{% cut "**1.75.1**" %}

**Дата релиза:** 2024-12-18


**Страница релиза:** [1.75.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.75.1)


**Docker-образ:** [ghcr.io/ytsaurus/ui:1.75.1](https://github.com/orgs/ytsaurus/packages/container/ui/324935827?tag=1.75.1)


#### [1.75.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.75.0...ui-v1.75.1) (2024-12-18)

#### Новые возможности

* **Attributes:** добавлена кнопка скачивания [YTFRONT-4310] ([6710c0b](https://github.com/ytsaurus/ytsaurus-ui/commit/6710c0b67ff012f3ebec4de3108c25827177f8a5))
* **CellPreviewModal:** добавлена поддержка предпросмотра изображений [[#773](https://github.com/ytsaurus/ytsaurus-ui/issues/773)] ([7b266dc](https://github.com/ytsaurus/ytsaurus-ui/commit/7b266dc2f15d2a80dff861121244e927c4b2664a))
* **Navigation/Table:** добавлена поддержка усечённого предпросмотра изображений [[#773](https://github.com/ytsaurus/ytsaurus-ui/issues/773)] ([34daef1](https://github.com/ytsaurus/ytsaurus-ui/commit/34daef10fef4c22d0f546b2ec6350c0008135212))
* **QueryTracker/Table:** добавлена поддержка усечённого предпросмотра изображений [[#773](https://github.com/ytsaurus/ytsaurus-ui/issues/773)] ([39684bc](https://github.com/ytsaurus/ytsaurus-ui/commit/39684bc7d68ebb914d8bdddf27fa9f08da491242))
* **VCS:** новый порядок сортировки списка [YTFRONT-4520] ([cb6b000](https://github.com/ytsaurus/ytsaurus-ui/commit/cb6b0008fecf69dd052dcde2ec249539ccb40a78))


#### Исправления

* **Accounts:** значение фильтра medium в url [YTFRONT-4567] ([d61ff09](https://github.com/ytsaurus/ytsaurus-ui/commit/d61ff09edd2f841816132ddbf93c98b7234829fe))
* **Accounts/Create:** использовать 'inherit_acl=false' только если родитель является 'root' [YTFRONT-4561] ([0a59da8](https://github.com/ytsaurus/ytsaurus-ui/commit/0a59da8bcbe846c07c41a72c40e96d56202198b5))
* **ACL:** использовать режим 'keep-missing-fields' для 'Manage Responsibles'/'Manage Inheritance' [YTFRONT-4560] ([1e00b7a](https://github.com/ytsaurus/ytsaurus-ui/commit/1e00b7ae5846d30b4a7009c2b1e7b518306f536d))
* **ManageTokensModal:** исправлен формат времени [[#914](https://github.com/ytsaurus/ytsaurus-ui/issues/914)] ([565b205](https://github.com/ytsaurus/ytsaurus-ui/commit/565b2050959b9b2687385bc6d753d5acf2aebb14))* **OAuth:** исправлен редирект на предыдущую страницу вместо / ([77e3471](https://github.com/ytsaurus/ytsaurus-ui/commit/77e347147f95b601e3dd3a691f4e3f8077f79f88))
* **Scheduling/Overview:** добавлен нижний отступ [YTFRONT-4530] ([43837e8](https://github.com/ytsaurus/ytsaurus-ui/commit/43837e8a8dc395cb02ba512bfe0cc0faa75ece44))
* **Toaster:** перенос строки в содержимом тостера [YTFRONT-4543] ([eb8faba](https://github.com/ytsaurus/ytsaurus-ui/commit/eb8fabaa788f6e78c6921c7bbd4a19280d6f3cc5))

{% endcut %}


{% cut "**1.74.0**" %}

**Дата релиза:** 2024-12-09


**Страница релиза:** [1.74.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.74.0)


**Docker-образ:** [ghcr.io/ytsaurus/ui:1.74.0](https://github.com/orgs/ytsaurus/packages/container/ui/319304938?tag=1.74.0)


#### [1.74.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.73.0...ui-v1.74.0) (2024-12-09)


#### Новые возможности

* **Navigation/CellPreview:** добавлен предпросмотр ячеек динамической таблицы [[#776](https://github.com/ytsaurus/ytsaurus-ui/issues/776)] ([4fbbb06](https://github.com/ytsaurus/ytsaurus-ui/commit/4fbbb06dcc428e1191f89d33367f15877539a7c2))
* **Queries:** автодополнение алиасов и колонок [YTFRONT-4486] ([b47d3fb](https://github.com/ytsaurus/ytsaurus-ui/commit/b47d3fb1fc215777c0b594dd7e7643c897e830ba))
* **Queries:** изменён порядок движков [YTFRONT-4498] ([9a8dce8](https://github.com/ytsaurus/ytsaurus-ui/commit/9a8dce8470268a076756c3d8692dd7f5e66ba7d6))
* **Queries:** изменён порядок вкладок результатов запроса [YTFRONT-4381] ([d851ddf](https://github.com/ytsaurus/ytsaurus-ui/commit/d851ddfac14f6de1e55ee3c4192fcff6ddb92388))
* **Groups:** теперь мы можем создавать новую группу через UI [[#634](https://github.com/ytsaurus/ytsaurus-ui/issues/634)] ([99766cf](https://github.com/ytsaurus/ytsaurus-ui/commit/99766cf33bae8153e1c238899513664c0fb6f12c))
* **ACL:** отдельный диалог для 'Edit inheritance' [YTFRONT-3836] ([83d5965](https://github.com/ytsaurus/ytsaurus-ui/commit/83d5965d2e3cff0d77c38765604c669f2d9a6f63))
* **Bundles/Bundle:** добавлена вкладка 'TabletErrors' для страницы бандла [YTFRONT-4119] ([7ec446a](https://github.com/ytsaurus/ytsaurus-ui/commit/7ec446a64561c842cb7911b9318d9b1b7b16f270))
* **Navigation/DynTable:** получение ошибок таблетов из tabletErrosApi [YTFRONT-4119] ([b082a3d](https://github.com/ytsaurus/ytsaurus-ui/commit/b082a3d830d0ef6da12b53ea271c64605738ccde))
* **Scheduling:** скрыть все алерты [YTFRONT-4322] ([63d60fa](https://github.com/ytsaurus/ytsaurus-ui/commit/63d60faeb702c451029ee987caf66120d1181255))
* **logout:** oauth logout сделан опциональным [[#488](https://github.com/ytsaurus/ytsaurus-ui/issues/488)] ([1b5591a](https://github.com/ytsaurus/ytsaurus-ui/commit/1b5591a6d0a7dda2654a23cc65e439dd0e7e857b))
* **Queries:** номер строки monaco в url [YTFRONT-4505] ([2433ed6](https://github.com/ytsaurus/ytsaurus-ui/commit/2433ed6235a9f61833df32c70b0f0b3d711db29c))


#### Исправления

* **Navigation:** дополнительно к a62d64acbc23a7eff7d4cfb4406fea6b6a1a3887 [YTFRONT-4511] ([b17545f](https://github.com/ytsaurus/ytsaurus-ui/commit/b17545f2735efac23f1f80bac42e777f2d9e6209))
* **Navigation:** навигация с клавиатуры [YTFRONT-4493] ([3b9fb24](https://github.com/ytsaurus/ytsaurus-ui/commit/3b9fb24c2b3aae09177e10827e99213b45b9a978))
* **Operations:** формат значения тултипа джоба [YTFRONT-4211] ([de238b1](https://github.com/ytsaurus/ytsaurus-ui/commit/de238b19e4ce1ce66666c3c5dddb4e05693174b4))
* **System:** обслуживание провайдеров меток времени [YTFRONT-4452] ([fa837cd](https://github.com/ytsaurus/ytsaurus-ui/commit/fa837cd575aea8d30f6c6a434bbeffa65c16868c))
* **UI:** исправление праздничной темы ([67db4f4](https://github.com/ytsaurus/ytsaurus-ui/commit/67db4f41aaa7e84449a88cdeff96f78361b0c88b))
* **Components:** использование поиска подстроки для 'Filter hosts' ([ac2c477](https://github.com/ytsaurus/ytsaurus-ui/commit/ac2c477abb47c86ba098264c8e7132b40e547422))
* **Docs:** исправлены некорректные url документации ([9c4a8db](https://github.com/ytsaurus/ytsaurus-ui/commit/9c4a8db47461f14881d128b7887630331cbfbd6f))
* **System:** исправлена опечатка, теперь проверяется корректный атрибут "maintenance" ([c97f9be](https://github.com/ytsaurus/ytsaurus-ui/commit/c97f9be7cb8db3a8dc1efaeb82252755ad4d5367))
* **ACL:** улучшены названия кнопок [YTFRONT-3836] ([a45be15](https://github.com/ytsaurus/ytsaurus-ui/commit/a45be15d96f4c2c287cda7e28cfb9ab04eac0aa9))
* **ACL:** inheritAcl/inheritResponsible должны корректно проверяться [YTFRONT-4492] ([dca379f](https://github.com/ytsaurus/ytsaurus-ui/commit/dca379f66f892e74ea8a291aa8074e38449cbba7))
* **Navigation/ReplicatedTable:** атрибут /[@tablet](https://github.com/tablet)_error_count должен влиять на количество ошибок таблетов [YTFRONT-4447] ([5860748](https://github.com/ytsaurus/ytsaurus-ui/commit/58607481c9e642774d26b285b02c8f8092982be3))
* **Navigation:** разрешить 'Tablet errors' для узлов с `/@tablet_error_count >= 0` [YTFRONT-3951] ([637def7](https://github.com/ytsaurus/ytsaurus-ui/commit/637def78c2c448e0dfd4ce363222a8ba7fbef5a4))
* **Groups:** разворачивать дерево групп при непустом фильтре групп [[#853](https://github.com/ytsaurus/ytsaurus-ui/issues/853)] ([1255e14](https://github.com/ytsaurus/ytsaurus-ui/commit/1255e1401e68199eda89c8106061eaec8a067061))
* **Navigation:** возвращена кнопка 'Request permissions' [YTFRONT-4511] ([a62d64a](https://github.com/ytsaurus/ytsaurus-ui/commit/a62d64acbc23a7eff7d4cfb4406fea6b6a1a3887))
* **Operations/Details/Specifiction:** не отображать пустую команду [YTFRONT-4507] ([cadc5fc](https://github.com/ytsaurus/ytsaurus-ui/commit/cadc5fc9472240a1cb526aa107f777c8774c6bad))
* **Queries:** кнопка share в safari [YTFRONT-4503] ([38842f4](https://github.com/ytsaurus/ytsaurus-ui/commit/38842f49be3ea945a91dc2f463f8448c5f92173e))
* **Scheduling/ACL:** ошибка при переключении на другой пул из вкладки ACL [YTFRONT-4487] ([06809d8](https://github.com/ytsaurus/ytsaurus-ui/commit/06809d8cb03ea36b6214670c190d526bed2c0ebd))
* **Settigns:** экспорт типа DiscribedSettings [YTFRONT-4499] ([5ab93eb](https://github.com/ytsaurus/ytsaurus-ui/commit/5ab93ebc59c15f9257c39804effa7134181c3c9a))

{% endcut %}


{% cut "**1.68.1**" %}

**Дата релиза:** 2024-11-18


**Страница релиза:** [1.68.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.68.1)


#### [1.68.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.68.0...ui-v1.68.1) (2024-11-18)


#### Новые возможности

* **PoolTreeSuggestControl:** теперь можно выбрать несколько пулов деревьев при создании/редактировании клики [[#841](https://github.com/ytsaurus/ytsaurus-ui/issues/841)] ([89c79a8](https://github.com/ytsaurus/ytsaurus-ui/commit/89c79a8733128c8ce0214bbe0857b583bce1d449))
* **Queries:** изменён вес автоподсказки пути [YTFRONT-4479] ([9504939](https://github.com/ytsaurus/ytsaurus-ui/commit/95049392606b78cae579dc9e13734cb5b65f46fd))
* **Queries:** пользователям разрешено копировать id запроса ([f6acc89](https://github.com/ytsaurus/ytsaurus-ui/commit/f6acc897caed62d6a160e657d2e605d8b0182d5a))
* **Queries:** информационные узлы в ошибке запроса [YTFRONT-4342] ([c2f4f3f](https://github.com/ytsaurus/ytsaurus-ui/commit/c2f4f3febff66c4330e3db4b919e7896a244b55f))
* **System:** заголовки секций должны быть липкими [YTFRONT-4420] ([7e04dae](https://github.com/ytsaurus/ytsaurus-ui/commit/7e04dae01262c2b4cf0c977acd269ef6593ae1e6))


#### Исправления

* **Breadcrumbs:** не работают ссылки во всплывающем окне хлебных крошек [YTFRONT-4121] ([bad5795](https://github.com/ytsaurus/ytsaurus-ui/commit/bad579593e391bfb73ec0f45e2b99c8e26659063))
* **Components/Nodes:** исправления для add_maintenance/remove_maintenance [YTFRONT-4480] ([85ef77d](https://github.com/ytsaurus/ytsaurus-ui/commit/85ef77dfbb540a0c253e754c2cefaa0db884a191))
* **Navigation/DeleteModal:** сделать заголовок списка удаляемых элементов и флажок "permanently delete" липкими [YTFRONT-4245] ([3da8d44](https://github.com/ytsaurus/ytsaurus-ui/commit/3da8d4436b3f1fc93e7d764e5e67bfb8dc2524fc))
* **Navigation/ReplicatedTable:** переименована колонка [YTFRONT-4327] ([79f664b](https://github.com/ytsaurus/ytsaurus-ui/commit/79f664b663de172c8711e3aa255c4b73729514d1))
* **Navigation:** корректный формат данных в ошибках [YTFRONT-4251] ([ed6096e](https://github.com/ytsaurus/ytsaurus-ui/commit/ed6096e7f1ce337546a0271506db6f5b7f9798a6))
* **Queries:** горячие клавиши запуска запроса [YTFRONT-4462] ([7d69305](https://github.com/ytsaurus/ytsaurus-ui/commit/7d6930558cbdd214044e07b16f7f8a63c748efac))


{% endcut %}


{% cut "**1.66.0**" %}

**Дата релиза:** 2024-11-01


**Страница релиза:** [1.66.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.66.0)


**Docker-образ:** [ghcr.io/ytsaurus/ui:1.66.0](https://github.com/orgs/ytsaurus/packages/container/ui/299155508?tag=1.66.0)


#### [1.66.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.65.0...ui-v1.66.0) (2024-11-01)


#### Новые возможности

* **Users:** добавлена вкладка "Change password" в UsersPageEditor, теперь можно менять пароли пользователей через UI [[#633](https://github.com/ytsaurus/ytsaurus-ui/issues/633)] ([2a06c23](https://github.com/ytsaurus/ytsaurus-ui/commit/2a06c237ce10a94d7970a2b09462fdc31aa9a352))
* **Users:** добавлена кнопка "create new", которая позволяет создать нового пользователя [[#633](https://github.com/ytsaurus/ytsaurus-ui/issues/633)] ([543dcf0](https://github.com/ytsaurus/ytsaurus-ui/commit/543dcf04764d9158cdc222fcccac71f1a3840836))
* **Users:** добавлено поле "Name" в диалог UsersPageEditor, теперь можно переименовывать пользователей через UI [[#633](https://github.com/ytsaurus/ytsaurus-ui/issues/633)] ([bcfaead](https://github.com/ytsaurus/ytsaurus-ui/commit/bcfaead8bb9f2e65ed78f0da3099130b3930626f))
* **Users:** добавлена кнопка remove, которая позволяет удалить пользователя [[#633](https://github.com/ytsaurus/ytsaurus-ui/issues/633)] ([461f6d9](https://github.com/ytsaurus/ytsaurus-ui/commit/461f6d9ee5dce37d49b4e5452e1739e6b7630001))


#### Исправления

* **Operation/Details:** минифицированная ошибка React [#31](https://github.com/ytsaurus/ytsaurus-ui/issues/31) [YTFRONT-4417] ([6324642](https://github.com/ytsaurus/ytsaurus-ui/commit/6324642bfd1474cb9c688637d61a90e3cbd5c42b))

{% endcut %}


{% cut "**1.65.0**" %}

**Дата релиза:** 2024-10-25


**Страница релиза:** [1.65.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.65.0)


**Docker-образ:** [ghcr.io/ytsaurus/ui:1.65.0](https://github.com/orgs/ytsaurus/packages/container/ui/295203548?tag=1.65.0)


#### [1.65.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.64.0...ui-v1.65.0) (2024-10-25)


#### Новые возможности

* **Operation/Jobs:** добавлена колонка TaskName в таблицу [[#828](https://github.com/ytsaurus/ytsaurus-ui/issues/828)] ([90c8586](https://github.com/ytsaurus/ytsaurus-ui/commit/90c8586df5bbb50ec08da012e0eab1b37dd64b36))
* **Queries:** ACO по умолчанию [[#436](https://github.com/ytsaurus/ytsaurus-ui/issues/436)] ([0eba698](https://github.com/ytsaurus/ytsaurus-ui/commit/0eba6989e45b147a7ddd0ab0bfc753a2c3a68e2b))
* **System:** новые цвета кластеров [YTFRONT-4409] ([f7cb2c0](https://github.com/ytsaurus/ytsaurus-ui/commit/f7cb2c06fa65bd6bf59f7a45cd8ef8bc7cdba8d7))
* **System:** новое короткое имя regexp [YTFRONT-4386] ([ebe523f](https://github.com/ytsaurus/ytsaurus-ui/commit/ebe523f7a4323eb765cd41dbd0eccff1231b826c))
* **UIFactory:** добавлен метод renderCustomPreloaderError, позволяющий отображать собственную страницу ошибки ([b580749](https://github.com/ytsaurus/ytsaurus-ui/commit/b580749cb803e0bacb830b3a0fd33b9fbe2b9646))
* **UIFactory:** defaultUIFactory вынесен в отдельный файл [YTFRONT-3814] ([dfc8930](https://github.com/ytsaurus/ytsaurus-ui/commit/dfc8930d7925f4a6389cd6600f1ce165f2fb2852))


#### Исправления ошибок

* **ACL:** права доступа должны быть отсортированы [YTFRONT-4432] ([881c08c](https://github.com/ytsaurus/ytsaurus-ui/commit/881c08c2c2883c6d0288682c12fbbf3e3dafd4d6))
* **ClusterPage:** выравнивание текста "Загрузка &lt;имя кластера&gt;..." по центру страницы ([f367d5b](https://github.com/ytsaurus/ytsaurus-ui/commit/f367d5b3bc5b7b612b611a7aa0b70f00f909ebf5))
* **ColumnHeader/SortIcon:** добавлена подсказка для направления сортировки (+allowUnordered) [YTFRONT-3801] ([911e457](https://github.com/ytsaurus/ytsaurus-ui/commit/911e45748fc818c50a962ef65b74c2b62b92ed96))
* **Components:** неверное название колонки памяти таблета [YTFRONT-4408] ([21cc198](https://github.com/ytsaurus/ytsaurus-ui/commit/21cc198c475c62f2ee6041fc9a1b2c3057a39155))
* **Navigation:** корректное имя кластера в YQL-запросе [YTFRONT-4274] ([4b9cab5](https://github.com/ytsaurus/ytsaurus-ui/commit/4b9cab511c4ba1b65d02df8923a44cea0beba3ee))
* **Navigation/MapNode:** возможность выбора строк по клику на первую ячейку [YTFRONT-4391] ([85e915c](https://github.com/ytsaurus/ytsaurus-ui/commit/85e915cf2c72f4137e9b73b87b7c1748db5b5094))
* **Navigation/Queue:** разрешена вкладка Queue для replication_table/chaos_replicated_table [YTFRONT-4144] ([228db6a](https://github.com/ytsaurus/ytsaurus-ui/commit/228db6a3a4c9a57db0d36812d8fd2e412be0c901))
* **Navigation/Table:** перетаскиваемый селектор строк должен работать корректно [YTFRONT-4396] ([d702adb](https://github.com/ytsaurus/ytsaurus-ui/commit/d702adb6b2e50f81a4d60cd67f77308b454ee14e))
* **Navigation/TopRow/PathEditor:** выделение текста при фокусе редактора [YTFRONT-4387] ([fd4beb6](https://github.com/ytsaurus/ytsaurus-ui/commit/fd4beb695e05e35d7648ee0ee430ce759f90a825))
* **Navigation/RequestPermissionsButton:** небольшие исправления стилей [YTFRONT-4379] ([9eefa05](https://github.com/ytsaurus/ytsaurus-ui/commit/9eefa050f002a9a4dd829b32c4cf75bf4de068cb))
* **Odin:** единый формат DatePicker в odin ([5a25c2f](https://github.com/ytsaurus/ytsaurus-ui/commit/5a25c2f9aec338b39bf49a7e9a8836516d6a0980))
* **Operations:** сохранение пула в URL [YTFRONT-4355] ([ee2fe0e](https://github.com/ytsaurus/ytsaurus-ui/commit/ee2fe0e5b7bc6e64c26cdc4290c7887fc554cedf))
* **PathViewer:** просмотрщик путей теперь по умолчанию выполняет команду list, поскольку команда "get /" может привести к проблемам с производительностью [[#814](https://github.com/ytsaurus/ytsaurus-ui/issues/814)] ([006d215](https://github.com/ytsaurus/ytsaurus-ui/commit/006d21576975feb2d20d6919f88c62542ab4ff30))
* **System/Nodes:** небольшие исправления [YTFRONT-3297] ([3d78cfe](https://github.com/ytsaurus/ytsaurus-ui/commit/3d78cfe2b99cec3c779eead885d6e8cd5440f413))


{% endcut %}


{% cut "**1.60.1**" %}

**Дата релиза:** 2024-10-02


**Страница релиза:** [1.60.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.60.1)


**Docker-образ:** [ghcr.io/ytsaurus/ui:1.60.1](https://github.com/orgs/ytsaurus/packages/container/ui/283122529?tag=1.60.1)


#### [1.60.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.60.0...ui-v1.60.1) (2024-10-02)

#### Новые возможности

* **Navigation/Table/CellPreviewModal:** добавлена поддержка предпросмотра строк [[#765](https://github.com/ytsaurus/ytsaurus-ui/issues/765)] ([e779d16](https://github.com/ytsaurus/ytsaurus-ui/commit/e779d161b928bc372be57cfaae199311c99dae1d))
* **Navigation:** открытие журналов доступа в qt [YTFRONT-4345] ([97a42b8](https://github.com/ytsaurus/ytsaurus-ui/commit/97a42b87d55c78e92224f43c28bac4d9fd8932ca))
* **Navigation/MapNode:** возможность переопределить иконку узла через UIFactory.getNavigationMapNodeSettings ([c97a4a0](https://github.com/ytsaurus/ytsaurus-ui/commit/c97a4a09bcd8cfc40f5e5eeec203ee1aab417948))
* **Navigation/Queue:** добавлена секция alerts [YTFRONT-4144] ([8b0157c](https://github.com/ytsaurus/ytsaurus-ui/commit/8b0157c08dac8757a04a4129c83ea8de332f6861))
* **System:** кнопка обслуживания [YTFRONT-4217] ([3c5d0d2](https://github.com/ytsaurus/ytsaurus-ui/commit/3c5d0d225d244204b87fde6dc182489130ad20e6))

#### Исправления ошибок

* **BFF** исправлено логирование ошибок axios в функции sendAndLogError ([b9239dc](https://github.com/ytsaurus/ytsaurus-ui/commit/b9239dc43feab214b4e3520b21e662755be4f33a))
* **Navigation:** всплывающее окно выбора пула [YTFRONT-4380] ([f52eb90](https://github.com/ytsaurus/ytsaurus-ui/commit/f52eb90da82306d6bf191a0d1375f3c30eaa3aac))
* **Navigation/Consumer,Navigation/Queue:** отображение ошибок [YTFRONT-4144] ([914a6a0](https://github.com/ytsaurus/ytsaurus-ui/commit/914a6a066ce30928673749bd8e2250c51a3e637b))
* **Navigation/Table/CellPreview:** исправлено открытие предпросмотра для таблицы со смещением [[#778](https://github.com/ytsaurus/ytsaurus-ui/issues/778)] ([7347349](https://github.com/ytsaurus/ytsaurus-ui/commit/7347349c9adaedb1a8d7ea4a933a8316f2b296d2))
* **Queries:** автодополнение пути chyt spyt [YTFRONT-4368] ([df3cff1](https://github.com/ytsaurus/ytsaurus-ui/commit/df3cff140ee9d7bee48c19be55cc66d00e06cdcd))
* **Queries:** не показывать vcs, если vcsSettings пуст ([7df0b04](https://github.com/ytsaurus/ytsaurus-ui/commit/7df0b045ecba077363d03471b8d196301d6b8a65))
* **Queries:** исправлены adhoc-графики ([2c441c5](https://github.com/ytsaurus/ytsaurus-ui/commit/2c441c5d79cc680000038d2bfc670e247e488e08))
* **Sort,Merge:** устранены ошибки отсутствующих узлов [YTFRONT-4392] ([cf79a79](https://github.com/ytsaurus/ytsaurus-ui/commit/cf79a79ac5366bd5e547eb18ebb2284cc5ae6234))
* **System:** исправлен цвет текста статистики в темной теме ([f1c3ec3](https://github.com/ytsaurus/ytsaurus-ui/commit/f1c3ec3fc73a5bac17c0bffa2c3229a47c710ec3))
* **YQLTable:** исправлено исключение в предпросмотре усеченных ячеек ([a351378](https://github.com/ytsaurus/ytsaurus-ui/commit/a35137899782ab251f9515db8d87ad92f05c23b1))

{% endcut %}


{% cut "**1.58.1**" %}

**Дата релиза:** 2024-09-10


**Страница релиза:** [1.58.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.58.1)


**Docker-образ:** [ghcr.io/ytsaurus/ui:1.58.1](https://github.com/orgs/ytsaurus/packages/container/ui/271454584?tag=1.58.1)


#### [1.58.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.58.0...ui-v1.58.1) (2024-09-10)

#### Новые возможности

* **ACL:** добавлено поле inheritedFrom [YTFRONT-3836] ([4bd121d](https://github.com/ytsaurus/ytsaurus-ui/commit/4bd121d7b61907997d0e525e73d0312a43d01a50))
* **ACL:** унаследованные роли должны отображаться отдельно [YTFRONT-3836] ([7edb2c4](https://github.com/ytsaurus/ytsaurus-ui/commit/7edb2c42293088b865c6205fa3085929f082d10f))
* **ACL:** использование @idm_roles для ACO (+имя tvm) [YTFRONT-3836] ([03f139e](https://github.com/ytsaurus/ytsaurus-ui/commit/03f139ed5c38c2e211bafddab8b5bb4e3805c918))
* **Components/Nodes:** добавлен прогресс GPU [YTFRONT-4306] ([7ec1f62](https://github.com/ytsaurus/ytsaurus-ui/commit/7ec1f62eb996fdf10cc0d78a375ed07fd33f9a35))
* **Components:** отображение всех тегов [YTFRONT-4315] ([723e772](https://github.com/ytsaurus/ytsaurus-ui/commit/723e77216750e3608b284b7953d43cbdee65c0d3))
* **Markdown:** использование @diplodoc/transform [YTFRONT-4108] ([3b33bc9](https://github.com/ytsaurus/ytsaurus-ui/commit/3b33bc9ef85069159b53aa12e7ca4c0eb09bf8b9))
* **Navigation/CreateTableModal:** изменение имени по умолчанию для новых таблиц [YTFRONT-4249] ([cc19d6b](https://github.com/ytsaurus/ytsaurus-ui/commit/cc19d6bc71607751a5d019dfc6ce8fa1261a6e1c))
* **Navigation/Flow:** добавлена новая вкладка [YTFRONT-3978] ([1ef39d7](https://github.com/ytsaurus/ytsaurus-ui/commit/1ef39d7cee11e4e6a6f3eb4bfb044e95b4f6fc60))
* **Operations:** возможность отключить оптимизацию фильтров на странице операций через конфигурацию кластера [[#700](https://github.com/ytsaurus/ytsaurus-ui/issues/700)] ([771294a](https://github.com/ytsaurus/ytsaurus-ui/commit/771294ab5bb33b2b11413da2c52ec8e85d175f3d))
* **Queries:** добавлена возможность определения VCS [YTFRONT-4257] ([3e0df9b](https://github.com/ytsaurus/ytsaurus-ui/commit/3e0df9b3b64bb515d748118f8a60d4f88f3274a2))
* **Queries:** представлена POC-версия adhoc-визуализации результатов запросов [[#641](https://github.com/ytsaurus/ytsaurus-ui/issues/641)] ([6dd9896](https://github.com/ytsaurus/ytsaurus-ui/commit/6dd98968ce15cf9667619f0c710d6a3dec8c21dc))
* **Queries:** новая вкладка навигации [YTFRONT-4235] ([428a72c](https://github.com/ytsaurus/ytsaurus-ui/commit/428a72c7163bc353a5524445e956d4ca1ff50e9e))
* **Queries:** новый формат ACO для запросов [YTFRONT-4238] ([a3ba06a](https://github.com/ytsaurus/ytsaurus-ui/commit/a3ba06a2317b1f54fdd23f52d7bf5795dabc4643))
* **Queries:** селектор spyt clicue [YTFRONT-4219] ([6288c73](https://github.com/ytsaurus/ytsaurus-ui/commit/6288c73e4a1919312aae55040ce2baab331e1875))
* **Queries:** навигация vcs [YTFRONT-4147] ([58be722](https://github.com/ytsaurus/ytsaurus-ui/commit/58be72232945ef8bcbf17327e3041a5c263256af))
* **Queries:** кнопка "Поделиться запросом" [YTFRONT-4239] ([67e84bc](https://github.com/ytsaurus/ytsaurus-ui/commit/67e84bc383ebce81a57928d44d384a5ed7ab0d99))
* **Table:** добавлена кнопка "Просмотр" для усеченных ячеек [[#655](https://github.com/ytsaurus/ytsaurus-ui/issues/655)] ([c688f1f](https://github.com/ytsaurus/ytsaurus-ui/commit/c688f1f6e4b674c1cb79bafc523ded16948e0516))
* **Table/Excel:** возможность настройки uploadTableExcelBaseUrl и exportTableBaseUrl для каждого кластера [[#717](https://github.com/ytsaurus/ytsaurus-ui/issues/717)] ([88dec84](https://github.com/ytsaurus/ytsaurus-ui/commit/88dec846b765b5e4f9413de245aad6ca956819b9))
* **javascript-wrapper:** добавлены новые команды для pipelines [YTFRONT-3978] ([da70313](https://github.com/ytsaurus/ytsaurus-ui/commit/da70313424b8042e6782d8fe9a642c9703465d54))
* **uikit6:** обновлены зависимости [[#502](https://github.com/ytsaurus/ytsaurus-ui/issues/502)] ([5a92c5f](https://github.com/ytsaurus/ytsaurus-ui/commit/5a92c5fbbfccf43a788946b3ab9e95ebca0e74bf))
* **YQLTable:** добавлена кнопка "просмотр" для усеченных ячеек [[#702](https://github.com/ytsaurus/ytsaurus-ui/issues/702)] ([ee776c1](https://github.com/ytsaurus/ytsaurus-ui/commit/ee776c1158eaaf21a53ae6226dbd5ba83427c646))
* обновлены @gravity-ui/charkit, @gravity-ui/yagr [YTFRONT-4305] ([a65be74](https://github.com/ytsaurus/ytsaurus-ui/commit/a65be74a5d9017a5c0d8159f80982891e3afc8cc))

#### Исправления ошибок

* **Components/Nodes:** исправлена фильтрация по стойкам ([b58e8ba](https://github.com/ytsaurus/ytsaurus-ui/commit/b58e8ba8de094e635d2d697589d68e2418b5660b))
* **Components/Node:** отображение десятичных CPU [[#675](https://github.com/ytsaurus/ytsaurus-ui/issues/675)] ([b42b0bb](https://github.com/ytsaurus/ytsaurus-ui/commit/b42b0bb147cc13abe05715e6a0fb453724d2ec50))
* **Navigation/ReplicatedTable:** добавлена информационная иконка для «Automatic mode switch» [YTFRONT-4327] ([5446fc3](https://github.com/ytsaurus/ytsaurus-ui/commit/5446fc381fd17695cdbd22b280c53d0deb5e8a86))
* **Navigation/Table:** использование POST-запросов для чтения таблиц [YTFRONT-4259] ([7281e79](https://github.com/ytsaurus/ytsaurus-ui/commit/7281e79d41f3db5dabe056378b7c276412a4ed5a))
* **Operations/Operation/JobsMonitor:** использование флага 'with_monitoring_descriptor' [YTFRONT-4346] ([bbf5415](https://github.com/ytsaurus/ytsaurus-ui/commit/bbf54154a389895f64eb7e6c04dbdc15aee30e40))
* **System/Masters:** небольшие исправления макета с предупреждениями [YTFRONT-4295] ([2134144](https://github.com/ytsaurus/ytsaurus-ui/commit/2134144fb7829d6eb2010d65103376019a932036))
**ClustersMenu:** страница не должна ломаться с фильтром '[' [YTFRONT-4272] ([7eb5c7c](https://github.com/ytsaurus/ytsaurus-ui/commit/7eb5c7cfedc28935034041a82ef989ff44e4c460))
* **Components/Nodes:** устранены дубликаты узлов [YTFRONT-4268] ([131c857](https://github.com/ytsaurus/ytsaurus-ui/commit/131c8574b6cc1e076bb8076437ea542a7c149415))
* **Components/Nodes:** исправлен фильтр предупреждений [YTFRONT-4301] ([861f57c](https://github.com/ytsaurus/ytsaurus-ui/commit/861f57c5c213c0c22e6df8f46f16e2d0c3d2a188))
* **Componens/Nodes/Node:** исправлена ширина всплывающего окна памяти [[#502](https://github.com/ytsaurus/ytsaurus-ui/issues/502)] ([fc9c882](https://github.com/ytsaurus/ytsaurus-ui/commit/fc9c882e1177cce8f5f007c6a8a0d187724ffb1d))
* **MaintenancePage:** переработана активация обслуживания ([c7ed6e4](https://github.com/ytsaurus/ytsaurus-ui/commit/c7ed6e4702add1a9e62dae2e278104f3be01c007))
* **Navigation:** корректный вывод чисел в ошибках таблетов [YTFRONT-4251] ([fe02b58](https://github.com/ytsaurus/ytsaurus-ui/commit/fe02b5879e092fc1f4d674b7594b6eb8eb3d10fd))
* **Navigation:** не сбрасывать contentMode при навигации [[#511](https://github.com/ytsaurus/ytsaurus-ui/issues/511)] ([916ede3](https://github.com/ytsaurus/ytsaurus-ui/commit/916ede342fac224d6c077704be929999ab326863))
* **Navigation:** элементы выпадающего списка хлебных крошек теперь кликабельны [[#528](https://github.com/ytsaurus/ytsaurus-ui/issues/528)] ([2df7319](https://github.com/ytsaurus/ytsaurus-ui/commit/2df73197d827d0912ef8203f0357f1dfda681ecd))
* **Navigation/Favourites:** возможность добавлять элементы, когда значение undefined ([69c9202](https://github.com/ytsaurus/ytsaurus-ui/commit/69c920222ce1f3a0381efc70aa65da190f04b0e0))
* **Navigation/File:** разрешено удаленное копирование для типа узла 'file' [YTFRONT-4296] ([aff83de](https://github.com/ytsaurus/ytsaurus-ui/commit/aff83def8abee98eb1937b9495f828d553fd4d16))
* **Navigation/MapNode:** небольшие исправления CSS [YTFRONT-4291] ([e5932f8](https://github.com/ytsaurus/ytsaurus-ui/commit/e5932f817c8ceacdca713ad532814d0b8f7b3f39))
* **Navigation/RemoteCopy:** исправлена неактивная кнопка «Подтвердить» [YTFRONT-4296] ([a6b7bde](https://github.com/ytsaurus/ytsaurus-ui/commit/a6b7bdebb3db6639174761fe4551066276baaed0))
* **Navigation/Table:** не выбрасывать `undefined` в качестве исключения [YTFRONT-4312] ([d3e94cd](https://github.com/ytsaurus/ytsaurus-ui/commit/d3e94cd2893970a2982bf3943da0dfd181f649f8))
* **Odin:** неавтономная страница odin теперь получает корректный кластер ([b4739a4](https://github.com/ytsaurus/ytsaurus-ui/commit/b4739a4503028aebcfefc0f8ae42fe3b29e26b50))
* **Operations/Details/MetaTable:** мерцание интерфейса при наведении на длинное имя пула [YTFRONT-4308] ([395c849](https://github.com/ytsaurus/ytsaurus-ui/commit/395c8496e6770c9da1263c9b9e6589bfd157e991))
* **Operation/Job/Statistics:** обработка undefined [YTFRONT-4300] ([ca0da3c](https://github.com/ytsaurus/ytsaurus-ui/commit/ca0da3c32e34403491a9c68fc78e769ac06989ce))
* **Operation/Specification/Input:** исправление для операций 'remote_copy' [YTFRONT-4265] ([502bd53](https://github.com/ytsaurus/ytsaurus-ui/commit/502bd539edacac20c4f9e6caf7e4360488440cd1))
* **OperationPool:** небольшие исправления CSS ([9f9b32d](https://github.com/ytsaurus/ytsaurus-ui/commit/9f9b32d74c09780b207c6db1c52f327ed3549be1))
* **OperationsList:** исправлены неверные фильтры при размытии OperationSuggestFilter [[#705](https://github.com/ytsaurus/ytsaurus-ui/issues/705)] ([a738c5b](https://github.com/ytsaurus/ytsaurus-ui/commit/a738c5b182a192cb0e5c847ab31e90675da1c144))
* **Queries:** исправлена ошибка при переключении на другой запрос с открытой вкладкой статистики ([ef61008](https://github.com/ytsaurus/ytsaurus-ui/commit/ef61008be538119dfc8754b20c06105dbf8058ff))
ui/commit/b39aa3e873f44dd45da2c7bf8005ccb93294a40e))
* **Queries:** множественный ACO с обратной совместимостью [YTFRONT-4238] ([7efe878](https://github.com/ytsaurus/ytsaurus-ui/commit/7efe87881d5fd69d8a86252c0d401f8c950bb7a2))
* **Queries:** перенаправление на yt-операции из выполняющихся YQL-запросов [[#522](https://github.com/ytsaurus/ytsaurus-ui/issues/522)] ([2a91613](https://github.com/ytsaurus/ytsaurus-ui/commit/2a916136fee38055f3e85ad1325829dd68e2fcd0))
* **Queries:** поддержка темной темы в таблице статистики ([b3f1d57](https://github.com/ytsaurus/ytsaurus-ui/commit/b3f1d5766f880605c6aa975fc515a8cca568a933))
* **Queries:** использование опции treatValAsData из `@gravity-ui/unipika` по умолчанию ([2009d96](https://github.com/ytsaurus/ytsaurus-ui/commit/2009d96f9b822dbbd3e043628faa681731c4cd77))
* **Queries:** новый дизайн кнопки "Поделиться" [YTFRONT-4286] ([7d66e6c](https://github.com/ytsaurus/ytsaurus-ui/commit/7d66e6c56bc9f9489bb0ee89a2de3791dfd22103))
* **Queries/Editor:** автодополнение пути [YTFRONT-4264] ([ab9ba1f](https://github.com/ytsaurus/ytsaurus-ui/commit/ab9ba1f4497c70649a8e92df9666c2cf2ff9ed24))
* **Queries/Result:** исправление для пустых блоков [YTFRONT-4323] ([b39aa3e](https://github.com/ytsaurus/ytsaurus-))
* **Scheduling/CreatePool:** отказ от валидации имени пула [YTFRONT-4319] ([7bd7852](https://github.com/ytsaurus/ytsaurus-ui/commit/7bd7852ab57bd084b6fe9f24086272a4fd3b7aa9))
* **System/Nodes:** возможность разворачивать группы узлов [YTFRONT-3297] ([7c8330e](https://github.com/ytsaurus/ytsaurus-ui/commit/7c8330e49ead3b01ae7bd962a9887e05940ec10d))
* **Table/CellPreviewModal:** исправлено закрепление элемента управления при прокрутке [[#703](https://github.com/ytsaurus/ytsaurus-ui/issues/703)] ([c5e91cb](https://github.com/ytsaurus/ytsaurus-ui/commit/c5e91cbee5d122c32784145dbc2d49e76a5ab434))
* **Tablet:** исправлен URL узла [YTFRONT-4269] ([82de290](https://github.com/ytsaurus/ytsaurus-ui/commit/82de2908b58d2f18f583c6869db6de601064725c))
* **uiSettings:** возможность указывать loginPageSettings для каждого кластера ([be872cc](https://github.com/ytsaurus/ytsaurus-ui/commit/be872ccd9a52d5ad3d04f694602713f1b971759b))
* **YFM/Markdown**: добавлена поддержка темной и светлой тем путем переопределения стилей yfm [[#712](https://github.com/ytsaurus/ytsaurus-ui/issues/712)] ([b7cce12](https://github.com/ytsaurus/ytsaurus-ui/commit/b7cce12fcbf2612d69b528932fc39b160f8bb464))
* **Navigation/MapNode:** использование 'navmode=auto' при клике на узел ([3d473c4](https://github.com/ytsaurus/ytsaurus-ui/commit/3d473c426daea116c610860e7c15141f963e381f))
* **Queries:** исправления страницы запросов [YTFRONT-4340] ([676354e](https://github.com/ytsaurus/ytsaurus-ui/commit/676354e75f8b2d8457ceda002f2943b167cd0192))
* **Navigation/File:** разрешено удаленное копирование для типа узла 'file' [YTFRONT-4296] ([459cef0](https://github.com/ytsaurus/ytsaurus-ui/commit/459cef089c994511aa2d4117acf3bab3e2cd39d8))
* **Navigation/RemoteCopy:** исправлена неактивная кнопка «Подтвердить» [YTFRONT-4296] ([3ce933a](https://github.com/ytsaurus/ytsaurus-ui/commit/3ce933a38678371fc3675fefa4a0bda71f67e481))

{% endcut %}


{% cut "**1.46.2**" %}

**Дата релиза:** 2024-07-21


**Страница релиза:** [1.46.2](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.46.2)


**Docker-образ:** [ghcr.io/ytsaurus/ui:1.46.2](https://github.com/orgs/ytsaurus/packages/container/ui/246513825?tag=1.46.2)


#### [1.46.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.46.1...ui-v1.46.2) (2024-07-21)


#### Исправления ошибок

* **Operation/Specification/Input:** исправление для операций 'remote_copy' [YTFRONT-4265] ([b9ba7d9](https://github.com/ytsaurus/ytsaurus-ui/commit/b9ba7d901f38b49d75c10c072339dce9072d9f0e))

{% endcut %}


{% cut "**1.46.0**" %}

**Дата релиза:** 2024-07-02


**Страница релиза:** [1.46.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.46.0)


**Docker-образ:** [ghcr.io/ytsaurus/ui:1.46.0](https://github.com/orgs/ytsaurus/packages/container/ui/238180350?tag=1.46.0)


#### [1.46.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.45.0...ui-v1.46.0) (2024-07-02)


#### Новые возможности

* **Components:** предупреждение для офлайн-узла [YTFRONT-4153] ([e95801e](https://github.com/ytsaurus/ytsaurus-ui/commit/e95801ee4abef7b6e47d8a6f5c96e6b8dcd87cbb))
* **Login**: добавлена возможность переопределить текст по умолчанию на странице входа [[#636](https://github.com/ytsaurus/ytsaurus-ui/issues/636)] ([28a47ec](https://github.com/ytsaurus/ytsaurus-ui/commit/28a47ec5ccf66d085e886391ab6fa3ff15b7372d))
* **Navigation/Table**: добавлена поддержка новых типов `date32`/`datetime64`/`timestamp64`/`interval64` [YTFRONT-4087] ([6f2c8e5](https://github.com/ytsaurus/ytsaurus-ui/commit/6f2c8e51c23f0099d1715cf21156fd36f43a34e4))
* **ManageTokens:** пользователю разрешено выпускать токены и управлять ими из UI [[#241](https://github.com/ytsaurus/ytsaurus-ui/issues/241)] ([6bdd6d2](https://github.com/ytsaurus/ytsaurus-ui/commit/6bdd6d2d6ae767a90a2c72f629325b0d6c56db3a))
* **Job:** добавлен элемент мета-таблицы 'Job trace' [YTFRONT-4182] ([00c0691](https://github.com/ytsaurus/ytsaurus-ui/commit/00c06919c5a31ea45068c9dbfe3f3ce5e0bbef3b))
* **Query:** заголовок с данными запроса [YTFRONT-4186] ([5282fb7](https://github.com/ytsaurus/ytsaurus-ui/commit/5282fb77dc5038bff78d55d25f146882c04adfda))
* **QueryTracker:** добавлено поле поиска на вкладке статистики в query tracker [[#301](https://github.com/ytsaurus/ytsaurus-ui/issues/301)] ([551e66a](https://github.com/ytsaurus/ytsaurus-ui/commit/551e66aaa127c0db31abefa92b41782a598a4899))
* **UIFactory:** добавлен метод UIFactory.getNavigationExtraTabs() ([bddf57c](https://github.com/ytsaurus/ytsaurus-ui/commit/bddf57cf45c52a0ab1002df5827ef49698c7644f))
* **UIFactory:** добавлен метод UIFactory.getMapNodeExtraCreateActions(...) ([ae6ae51](https://github.com/ytsaurus/ytsaurus-ui/commit/ae6ae5187e29787d09a26144215736ade9b8d1f4))
* **UIFactory:** добавлен метод UIFactory.renderAppFooter() [YTFRONT-4173] ([616ff0b](https://github.com/ytsaurus/ytsaurus-ui/commit/616ff0b2d8786df0eb1d05f7f45169984cb20162))

#### Исправления

* **AccountsGeneralTab:** не показывать TabletAccountingNotice, если включён enable_per_account_tablet_accounting ([7de2eb5](https://github.com/ytsaurus/ytsaurus-ui/commit/7de2eb5f25e9216b9dd03e3bf2d8131397cd77e9))
* **ACL:** запрос ACL для ACO [[#576](https://github.com/ytsaurus/ytsaurus-ui/issues/576)] ([0f46beb](https://github.com/ytsaurus/ytsaurus-ui/commit/0f46beb68fa523ada3200123769ac95927d0b3ff))
* **Auth:** использование имён cookie с двоеточием [[#587](https://github.com/ytsaurus/ytsaurus-ui/issues/587)] ([79a4254](https://github.com/ytsaurus/ytsaurus-ui/commit/79a42545c1a2684fb10aaa20e754c9fa60a9ae14))
* **ClusterPage:** проблема с футером на странице кластера [YTFRONT-4173] ([8800bbc](https://github.com/ytsaurus/ytsaurus-ui/commit/8800bbc205ffe2bc0211fb4c1ac081967287c695))
* **Clusters:** изменение flex grow для body [YTFRONT-4221] ([097da73](https://github.com/ytsaurus/ytsaurus-ui/commit/097da73a10fb8ab2f1392313148cb74b2ba00867))
* **DownloadManager:** исправление для диапазонов [YTFRONT-4215] ([0f117ca](https://github.com/ytsaurus/ytsaurus-ui/commit/0f117ca913abd8a57d252b40904d1b050bd5c39e))
* **Navigation:** проблема виджета с футером [YTFRONT-4221] ([5b4bbe1](https://github.com/ytsaurus/ytsaurus-ui/commit/5b4bbe152f8ba3701b895b0f8b0bf21726e1bf17))
* **ManageTokens:** отображение null в списке токенов, если tokenPrefix неизвестен [[#626](https://github.com/ytsaurus/ytsaurus-ui/issues/626)] ([135c92e](https://github.com/ytsaurus/ytsaurus-ui/commit/135c92e4e31b7776b0e5ce6604e0d143522012ff))
* **ManageTokens:** исправлено зависание окна ввода пароля ([c2e20ab](https://github.com/ytsaurus/ytsaurus-ui/commit/c2e20ab1623d2781a45d0fa4b93781d972cebaf4))
* **ManageTokens:** отключена горизонтальная прокрутка в таблице ([65e0b6a](https://github.com/ytsaurus/ytsaurus-ui/commit/65e0b6acf270d42a46680e9ea5e4b110eccb8b2c))
* **query/custom-result-tab:** показывать вкладку при наличии результата запроса ([bf64b2c](https://github.com/ytsaurus/ytsaurus-ui/commit/bf64b2cf9aef5e5d6281799e45c831eb58f910f3))
* **Query:** вкладка запроса и прогресса [YTFRONT-4185] ([c74c0fc](https://github.com/ytsaurus/ytsaurus-ui/commit/c74c0fced87cc839839d2128d4e8a910c813b0d2))
* **Query:** выделение в строке ошибки [YTFRONT-4208] ([85508dc](https://github.com/ytsaurus/ytsaurus-ui/commit/85508dcdd79758f6e1c744b7d2805e7f25056cd7))
* **Query:** декодирование utf в таблице результатов [[#533](https://github.com/ytsaurus/ytsaurus-ui/issues/533)] ([7cadb62](https://github.com/ytsaurus/ytsaurus-ui/commit/7cadb62ffb276edece14266393a8a9f3b0345dfe))
* **System:** позиция nonvoting [YTFRONT-4209] ([901da6f](https://github.com/ytsaurus/ytsaurus-ui/commit/901da6f484b16cdd4b8c79beb2e53d422be6b48c))
* **System:** теперь выполняется попытка запроса к другим первичным мастерам, если первый не ответил корректно [[#529](https://github.com/ytsaurus/ytsaurus-ui/issues/529)] ([fc25ad4](https://github.com/ytsaurus/ytsaurus-ui/commit/fc25ad493adb410ae66876ca7746dd3665f6a04a))

{% endcut %}


{% cut "**1.41.1**" %}

**Дата релиза:** 2024-05-28


**Страница релиза:** [1.41.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.41.1)


#### [1.41.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.41.0...ui-v1.41.1) (2024-05-28)

#### Новые возможности

* **Components/Nodes:** улучшена сортировка колонок прогресса [YTFRONT-3801] ([3502577](https://github.com/ytsaurus/ytsaurus-ui/commit/3502577afb59ee36a05d975e0e69b8647ece9d5d))
* **DownloadManager/Excel:** добавлена опция «Number precision mode» [YTFRONT-4150] ([5a3a641](https://github.com/ytsaurus/ytsaurus-ui/commit/5a3a641a0427700df566d5b35f3a1c2581c9ff50))
* **Navigation/CreateTableModal:** добавлена опция «Optimize for» [YTFRONT-4139] ([be84c6a](https://github.com/ytsaurus/ytsaurus-ui/commit/be84c6a288647b02775e9cbc288b865ffc11538b))
* **Operation/Jobs:** добавлен фильтр `with_monitoring_descriptor` [YTFRONT-4078] ([aa575c9](https://github.com/ytsaurus/ytsaurus-ui/commit/aa575c9e3427c92f6f935fd5b25ef88887f2a911))


#### Исправления

* **AclUpdateMessage:** небольшое исправление вёрстки ([ed85d7f](https://github.com/ytsaurus/ytsaurus-ui/commit/ed85d7fab663ddc1a75c614aabb6a062a635b329))
* **ACL:** небольшое исправление мета-блока [YTFRONT-3836] ([a3859d0](https://github.com/ytsaurus/ytsaurus-ui/commit/a3859d059efff237bbd7f58b25b67f40bca8b99e))
* **Bundles:** ограничение памяти [YTFRONT-4170] ([26491e0](https://github.com/ytsaurus/ytsaurus-ui/commit/26491e0096c0c95371a88ea0d7d13cf14cf65018))
* **Bundles:** ограничение памяти [YTFRONT-4170] ([4be139c](https://github.com/ytsaurus/ytsaurus-ui/commit/4be139c64a7538e038443cdd8100150c1b8a00f8))
* **Operation/JobsMonitor:** вкладка отображается без задержки [YTFRONT-4077] ([9673252](https://github.com/ytsaurus/ytsaurus-ui/commit/96732524d0025dff154710c1b5b814da1d01865d))
* **Scheduling/ACL:** перезагрузка ACL при изменении пула [YTFRONT-4172] ([b697bf3](https://github.com/ytsaurus/ytsaurus-ui/commit/b697bf3e40fdc97a07d3d009201ec6e5bdafef17))
* **System/Master:** возвращены «Queue agents» [YTFRONT-4145] ([1a82e8e](https://github.com/ytsaurus/ytsaurus-ui/commit/1a82e8e88e9ce3069c279a4b931866a30734629a))
* **Table/Schema:** небольшое исправление CSS [YTFRONT-4166] ([6b9cca4](https://github.com/ytsaurus/ytsaurus-ui/commit/6b9cca40f314fd544f3f90a280b1db72f48404db))
* **TimelinePicker:** небольшое исправление [YTFRONT-4180] ([20326c0](https://github.com/ytsaurus/ytsaurus-ui/commit/20326c0bb47889a0d0ffe8fd55b25b6b11681ab1))

{% endcut %}


{% cut "**1.39.0**" %}

**Дата релиза:** 2024-05-23


**Страница релиза:** [1.39.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.39.0)


#### [1.39.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.38.2...ui-v1.39.0) (2024-05-23)


#### Новые возможности

* **System:** добавлена кнопка смены лидера ([43f5034](https://github.com/ytsaurus/ytsaurus-ui/commit/43f5034405fcf58bd045406551a97f52e7a4a3ed))


#### Исправления

* **axios/withXSRFToken:** дополнение к b7738a97c3177df02a3a9112112ac97e4afef118 ([88b5efa](https://github.com/ytsaurus/ytsaurus-ui/commit/88b5efafd5d4ec480ea50f75e15314974786f427))
* **Bundles:** ограничение памяти [YTFRONT-4170] ([4be139c](https://github.com/ytsaurus/ytsaurus-ui/commit/4be139c64a7538e038443cdd8100150c1b8a00f8))

{% endcut %}


{% cut "**1.38.0**" %}

**Дата релиза:** 2024-05-16


**Страница релиза:** [1.38.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.38.0)


#### [1.38.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.37.0...ui-v1.38.0) (2024-05-16)

#### Новые возможности

* **Navigation:** вывод атрибутов пути [YTFRONT-3869] ([8f90df0](https://github.com/ytsaurus/ytsaurus-ui/commit/8f90df06bfccfb7c7aeca8bc1dc9056a12eb5395))
* **Odin:** добавлен выбор времени на странице обзора [YTFRONT-2733] ([f3ad6ba](https://github.com/ytsaurus/ytsaurus-ui/commit/f3ad6ba43d55e1133f8f5655a8d20c3868ad68f1))

#### Исправления

* **login:** отображается сообщение об ошибке при вводе неверных учётных данных [[#490](https://github.com/ytsaurus/ytsaurus-ui/issues/490)] ([b6d7a34](https://github.com/ytsaurus/ytsaurus-ui/commit/b6d7a34c4ca0e4b1934ed75be252289b32d442df))
* **login:** страница входа остаётся после нажатия кнопки «Назад» в браузере ([688043e](https://github.com/ytsaurus/ytsaurus-ui/commit/688043e5fe4babecbe67625fa0004b21e3e676fa))
* **TabletCellBundle/Instances:** уведомление о выделении ресурсов [YTFRONT-4167] ([2218d61](https://github.com/ytsaurus/ytsaurus-ui/commit/2218d617dffc89a33cdab5ef4a1de908300a2545))
* **YQLTable:** исправлено отображение обрезанного значения в ячейке ([f5daaca](https://github.com/ytsaurus/ytsaurus-ui/commit/f5daaca691c0e94e7cfac2b712a2d392d66cfb5e))
* **login:** отображается сообщение об ошибке при вводе неверных учётных данных [[#490](https://github.com/ytsaurus/ytsaurus-ui/issues/490)] ([b6d7a34](https://github.com/ytsaurus/ytsaurus-ui/commit/b6d7a34c4ca0e4b1934ed75be252289b32d442df))
* **login:** страница входа остаётся после нажатия кнопки «Назад» в браузере ([688043e](https://github.com/ytsaurus/ytsaurus-ui/commit/688043e5fe4babecbe67625fa0004b21e3e676fa))
* исправление для релиза ([e8019bc](https://github.com/ytsaurus/ytsaurus-ui/commit/e8019bc26ec8a22a81eb2b5aef2683d9a39d3994))

{% endcut %}


{% cut "**1.33.0**" %}

**Дата релиза:** 2024-05-08


**Страница релиза:** [1.33.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.33.0)


#### [1.33.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.32.0...ui-v1.33.0) (2024-05-08)


#### Новые возможности

* **Navigation:** кнопка сортировки в колонках таблицы [YTFRONT-4135] ([44d67a3](https://github.com/ytsaurus/ytsaurus-ui/commit/44d67a3e51a564d4b78a5c9381d8205bd313d473))


#### Исправления

* **AccountsUsage:** исправление параметра `view` [YTFRONT-3737] ([7d31cda](https://github.com/ytsaurus/ytsaurus-ui/commit/7d31cdac26fafb4695a9893b8ad3e9e749bf9ba4))
* **AccountsUsage:** исправление выпадающих списков Select [YTFRONT-4155] ([63645e1](https://github.com/ytsaurus/ytsaurus-ui/commit/63645e1dda2d73155967ed0a47e8b523c46a13fa))
* **BundleEditorDialog:** улучшено сообщение об ошибке [YTFRONT-4148] ([d233f9c](https://github.com/ytsaurus/ytsaurus-ui/commit/d233f9ca8b409626874b77519c5f2c72e1daa77a))

{% endcut %}


{% cut "**1.32.0**" %}

**Дата релиза:** 2024-05-07


**Страница релиза:** [1.32.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.32.0)


#### [1.32.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.31.0...ui-v1.32.0) (2024-05-07)


#### Новые возможности

* добавлена возможность отображать пользовательскую вкладку запросов на странице query-tracker ([193d24b](https://github.com/ytsaurus/ytsaurus-ui/commit/193d24bcf12588579b27331e3553a72fc8b17ab8))
* **Query:** добавлен редактор файлов [YTFRONT-3984] ([3ca0b33](https://github.com/ytsaurus/ytsaurus-ui/commit/3ca0b33b3a834b090238d71763c833338699d0f2))


#### Исправления

* **Accounts:** проблема с выбором [YTADMINREQ-41653] ([a742969](https://github.com/ytsaurus/ytsaurus-ui/commit/a7429691c93d1884a2e2adf5d5b78b059c63bb9c))
* исправление ошибок линтера ([5250818](https://github.com/ytsaurus/ytsaurus-ui/commit/5250818deead43893caaf8f036493fa88e914442))
* **Host:** добавлен многоточие в тексте на хосте ([2342404](https://github.com/ytsaurus/ytsaurus-ui/commit/2342404235fa050acd6f3046f966f48ca3bbd133))
* **Host:** удалён classname ([70a7558](https://github.com/ytsaurus/ytsaurus-ui/commit/70a75582a7d77832d8402953e4560a0caeb955dd))
* **Host:** исправления после ревью ([5742f26](https://github.com/ytsaurus/ytsaurus-ui/commit/5742f2643ec7db5b67e641edb63ea0497119d74d))
* **Navigation:** ошибка текста в раскладке таблиц [YTFRONT-4133] ([ca58a9e](https://github.com/ytsaurus/ytsaurus-ui/commit/ca58a9e12eaabf1933ee7dc37bffd4d51581f3a8))
* **Navigation:** неправильный путь к символическим ссылкам [YTFRONT-4128] ([f43e6f7](https://github.com/ytsaurus/ytsaurus-ui/commit/f43e6f7621f770f8fd94c1a1d23a76ad539e029b))
* **Query:** ошибка автодополнения в старых версиях Safari [YTFRONT-4125] ([bacb350](https://github.com/ytsaurus/ytsaurus-ui/commit/bacb350f21499e421c5309bdd5cab4a12a329110))
* **Query:** создание запроса из таблицы [YTFRONT-4137] ([0fd3271](https://github.com/ytsaurus/ytsaurus-ui/commit/0fd3271fd9b543a4136502a52c749343a177e43f))

{% endcut %}


{% cut "**1.31.0**" %}

**Дата релиза:** 2024-04-19


**Страница релиза:** [1.31.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.31.0)


#### [1.31.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.30.0...ui-v1.31.0) (2024-04-19)


#### Новые возможности

* **ACL/RequestPermissions:** добавлено разрешение для навигации [[#474](https://github.com/ytsaurus/ytsaurus-ui/issues/474)] ([fb219aa](https://github.com/ytsaurus/ytsaurus-ui/commit/fb219aa6a3b8df0a33848d18b876499a29903fad))
* **ACL:** запрос разрешения на чтение для группы колонок [YTFRONT-3482] ([62e9504](https://github.com/ytsaurus/ytsaurus-ui/commit/62e95048be674fd45f114d35875841689f2003c1))
* **ACL:** отдельная вкладка для колонок [YTFRONT-3836] ([374003a](https://github.com/ytsaurus/ytsaurus-ui/commit/374003ac979bb5a91e2dfff5d447c88c411b51e3))
* **Query:** кнопка создания запроса заменена на ссылку [YTFRONT-4093] ([320cd98](https://github.com/ytsaurus/ytsaurus-ui/commit/320cd989a5b88ee93973e97113b243f06bb8968c))
* **Query:** автодополнение spyt ytql [YTFRONT-4118] ([ca86bb8](https://github.com/ytsaurus/ytsaurus-ui/commit/ca86bb84ceae35aa4b3cda11b38345ec7e26dc9c))


#### Исправления

* **ACL/RequestPermissions:** обработка пути из атрибутов ошибки [YTFRONT-3502] ([f078a89](https://github.com/ytsaurus/ytsaurus-ui/commit/f078a89950169a642a48366b122492ffbfbd4b60))
* **Navigation:** возможность открывать объекты без доступа [YTFRONT-3836] ([0ad6f51](https://github.com/ytsaurus/ytsaurus-ui/commit/0ad6f514d9fc11c8f868e6c78ec804d918a7db31))
* **QueryTracker:** исправлены параметры запроса для кнопок validate и explain в yql-запросах [[#370](https://github.com/ytsaurus/ytsaurus-ui/issues/370)] ([85c052e](https://github.com/ytsaurus/ytsaurus-ui/commit/85c052ee5abfa3f825739829602c238ddb902e54))
* **userSettings:** настройки пользователя не применяются при переходе на кластер со страницы со списком кластеров [[#471](https://github.com/ytsaurus/ytsaurus-ui/issues/471)] ([37c2642](https://github.com/ytsaurus/ytsaurus-ui/commit/37c26422e1aa6923fa5e48929ef7673f4afb038c))

{% endcut %}


{% cut "**1.30.0**" %}

**Дата релиза:** 2024-04-17


**Страница релиза:** [1.30.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.30.0)


#### [1.30.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.29.0...ui-v1.30.0) (2024-04-17)


#### Новые возможности

* **UserCard:** добавлена поддержка UserCard в UIFactory ([c99fae5](https://github.com/ytsaurus/ytsaurus-ui/commit/c99fae5158143a77fa43b0f92ad7a18aba6d2240))


#### Исправления

* **Query:** ошибка при разборе yson [YTFRONT-4110] ([11b71cf](https://github.com/ytsaurus/ytsaurus-ui/commit/11b71cf7a4f96bcf788b19a2c87313eaf1596214))
* **QueryTracker:** исправлены параметры запроса для кнопок validate и explain в yql-запросах [[#370](https://github.com/ytsaurus/ytsaurus-ui/issues/370)] ([65abfc5](https://github.com/ytsaurus/ytsaurus-ui/commit/65abfc5dbd5b82df1ceca4852b8b6a4bee7c6db8))
* **userSettings:** настройки пользователя не применяются при переходе на кластер со страницы со списком кластеров [[#471](https://github.com/ytsaurus/ytsaurus-ui/issues/471)] ([134f5c1](https://github.com/ytsaurus/ytsaurus-ui/commit/134f5c17d6c0880cb6769543429e7476733d9a49))

{% endcut %}


{% cut "**1.29.0**" %}

**Дата релиза:** 2024-04-12


**Страница релиза:** [1.29.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.29.0)


#### [1.29.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.28.1...ui-v1.29.0) (2024-04-12)


#### Новые возможности

* **QueryTracker:** поддержка validate и explain для yql-запросов [[#370](https://github.com/ytsaurus/ytsaurus-ui/issues/370)] ([2ba362e](https://github.com/ytsaurus/ytsaurus-ui/commit/2ba362e33cbcf3ba36443bb8e3c182b7b3617bb7))


#### Исправления

* **Navigation/Table:** синхронизация ширины заголовков с данными [YTFRONT-4109] ([cfb18df](https://github.com/ytsaurus/ytsaurus-ui/commit/cfb18dfd65e4595a8bc5b4ec29037c7b8841aeb0))
* **QueryTracker:** прогресс yql-запроса показывает неверный этап [[#368](https://github.com/ytsaurus/ytsaurus-ui/issues/368)] ([2c0fd6c](https://github.com/ytsaurus/ytsaurus-ui/commit/2c0fd6ca5a877fb2a3d5e513f42cf98ab6e4b06e))
* **QueryTracker:** шаги yql-запроса перенаправляют на неверную страницу [[#369](https://github.com/ytsaurus/ytsaurus-ui/issues/369)] ([d5ec33b](https://github.com/ytsaurus/ytsaurus-ui/commit/d5ec33ba72d34fcff628e33f8a518f1b29c2fd41))
* **Store:** изменена конфигурация redux toolkit [YTFRONT-4115] ([891ebdc](https://github.com/ytsaurus/ytsaurus-ui/commit/891ebdc15e4aa805632db5472ace701af16d8cae))
* **table:** отсутствуют заголовки в полноэкранном режиме предпросмотра таблицы [[#422](https://github.com/ytsaurus/ytsaurus-ui/issues/422)] ([0e82358](https://github.com/ytsaurus/ytsaurus-ui/commit/0e82358d58a3598722edaba69f28a125a07ba44c))

{% endcut %}


{% cut "**1.28.1**" %}

**Дата релиза:** 2024-04-10


**Страница релиза:** [1.28.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.28.1)


#### [1.28.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.28.0...ui-v1.28.1) (2024-04-10)


#### Исправления

* **Navigation:** эмодзи в именах [YTFRONT-4104] ([fbf8c12](https://github.com/ytsaurus/ytsaurus-ui/commit/fbf8c122a7e2a7f10774ff9a65de62a1c3a0273c))
* **Query:** клика отключена запросом из истории [YTFRONT-4105] ([747f9e2](https://github.com/ytsaurus/ytsaurus-ui/commit/747f9e2e6bc1966bb8d05b314eb233a33d637fdd))

{% endcut %}

{% cut "**1.28.0**" %}

**Дата релиза:** 2024-04-09


**Страница релиза:** [1.28.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.28.0)


#### [1.28.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.27.0...ui-v1.28.0) (2024-04-09)


#### Новые возможности

* добавлен redux toolkit [YTFRONT-4094] ([e750edb](https://github.com/ytsaurus/ytsaurus-ui/commit/e750edb38aac578a9d48b92a2e769641cf13534a))
* **QueryTracker:** новые колонки в списке запросов [[#267](https://github.com/ytsaurus/ytsaurus-ui/issues/267)] ([22d69a8](https://github.com/ytsaurus/ytsaurus-ui/commit/22d69a89cdcff82346649adcf64fb46f4cec1d66))


#### Исправления ошибок

* **configs:** добавлена обратная совместимость для YT_AUTH_CLUSTER_ID [[#349](https://github.com/ytsaurus/ytsaurus-ui/issues/349)] ([0deca57](https://github.com/ytsaurus/ytsaurus-ui/commit/0deca57a1a0ea3c32259ca8a83e340bd63514439))
* **Scheduling/Overview:** фильтр по имени заменён на pool-selector [YTFRONT-4075] ([2865e09](https://github.com/ytsaurus/ytsaurus-ui/commit/2865e09cf3ffa91dcfc4378876b9dc881b20e2d8))
* **Scheduling:** исправление фильтра пулов [[#460](https://github.com/ytsaurus/ytsaurus-ui/issues/460)] ([edf380d](https://github.com/ytsaurus/ytsaurus-ui/commit/edf380df6750cb3f5a2f23872dc4adee9247769d))

{% endcut %}


{% cut "**1.27.0**" %}

**Дата релиза:** 2024-04-04


**Страница релиза:** [1.27.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.27.0)


#### [1.27.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.26.0...ui-v1.27.0) (2024-04-04)


#### Новые возможности

* **Query:** добавлена подсказка к полю поиска qt [YTFRONT-4096] ([f5b2c7e](https://github.com/ytsaurus/ytsaurus-ui/commit/f5b2c7e8cc0000708b2407ed69dde7ce07ef4115))
* **Query:** добавлен автодополнение для yql и chyt [YTFRONT-4074] ([2e025aa](https://github.com/ytsaurus/ytsaurus-ui/commit/2e025aa8dab454e9d3a0fcc9c47967a8202d4af8))
* **Query:** редизайн заголовка страницы [YTFRONT-4041] ([b2d6696](https://github.com/ytsaurus/ytsaurus-ui/commit/b2d66969c46fae1bd68884298c92fda3be7183db))


#### Исправления ошибок

* **Bundles/Editor:** разрешено редактировать Memory/Reserved через 'Reset to default' [YTFRONT-4098] ([2b371fc](https://github.com/ytsaurus/ytsaurus-ui/commit/2b371fc64f137f5c18a9f839ede5a95131527269))
* **Bundles:** исправлены параметры запроса [YTFRONT-4072] ([92fd224](https://github.com/ytsaurus/ytsaurus-ui/commit/92fd2245d1c02e9fa50e37d2b969312a98e0bad9))
* удалена лишняя колонка [YTFRONT-4072] ([cc800a8](https://github.com/ytsaurus/ytsaurus-ui/commit/cc800a873c94cbbdf43622e1100b1886ebd75547))
* **Navigation:** неверный путь к таблице в уведомлении [YTFRONT-4091] ([21d87c1](https://github.com/ytsaurus/ytsaurus-ui/commit/21d87c193c24e82891a9b7457e11b516e11f29cf))
* **Navigation:** неверная ширина колонки схемы [YTFRONT-4092] ([32e80d9](https://github.com/ytsaurus/ytsaurus-ui/commit/32e80d90ca8a3bd7748ac7cec5c87e551583dabc))
* **Query:** исправлено отображение стадий прогресса [YTFRONT-4097] ([0098222](https://github.com/ytsaurus/ytsaurus-ui/commit/00982227f7219e864ec487cc8571b7d834c1a84b))
* **Query:** исправлено выделение текста ошибки [YTFRONT-4089] ([be5d802](https://github.com/ytsaurus/ytsaurus-ui/commit/be5d80270163541e7a77085b8d1c02391301c685))

{% endcut %}


{% cut "**1.26.0**" %}

**Дата релиза:** 2024-03-29


**Страница релиза:** [1.26.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.26.0)


#### [1.26.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.25.0...ui-v1.26.0) (2024-03-29)


#### Новые возможности

* **System:** вкладка мониторинга [YTFRONT-4022] ([24d1885](https://github.com/ytsaurus/ytsaurus-ui/commit/24d18859a40226347efc4475db199b526063aa21))

{% endcut %}


{% cut "**1.25.0**" %}

**Дата релиза:** 2024-03-28


**Страница релиза:** [1.25.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.25.0)


#### [1.25.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.24.1...ui-v1.25.0) (2024-03-28)


#### Новые возможности

* **ACL:** добавлена возможность описания пользовательских permissionFlags [YTFRONT-4073] ([17be3da](https://github.com/ytsaurus/ytsaurus-ui/commit/17be3da4a03c6f807a13216215fb724f77b6f44e))
* **Query:** прогресс для движка spyt [YTFRONT-3981] ([14a59b0](https://github.com/ytsaurus/ytsaurus-ui/commit/14a59b01712146e99113a27dba6a556c10f8ca69))


#### Исправления ошибок

* **navigation:** исправлена форма изменения описания [YTFRONT-4083] ([93ee0d1](https://github.com/ytsaurus/ytsaurus-ui/commit/93ee0d16c088e3aaebf7e71f825864ee945ba4be))

{% endcut %}


{% cut "**1.24.1**" %}

**Дата релиза:** 2024-03-26


**Страница релиза:** [1.24.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.24.1)


#### [1.24.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.24.0...ui-v1.24.1) (2024-03-26)


#### Исправления ошибок

* **Query:** подчёркивание в monaco [YTFRONT-4069] ([d2bc351](https://github.com/ytsaurus/ytsaurus-ui/commit/d2bc35179fd924a2150f28f4aa2d278213592d7b))

{% endcut %}


{% cut "**1.24.0**" %}

**Дата релиза:** 2024-03-21


**Страница релиза:** [1.24.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.24.0)


#### [1.24.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.23.1...ui-v1.24.0) (2024-03-21)


#### Новые возможности

* **Accounts:** добавлено модальное окно атрибутов [YTFRONT-3829] ([0af6f85](https://github.com/ytsaurus/ytsaurus-ui/commit/0af6f85395fdbdd2b0168d360a8d213dbde920a9))

{% endcut %}


{% cut "**1.23.1**" %}

**Дата релиза:** 2024-03-21


**Страница релиза:** [1.23.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.23.1)


#### [1.23.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.23.0...ui-v1.23.1) (2024-03-21)


#### Исправления ошибок

* **BundleEditorDialog:** кнопка подтверждения должна быть кликабельной [YTFRONT-4076] ([e8f10da](https://github.com/ytsaurus/ytsaurus-ui/commit/e8f10daba41fb088f2d505ab30054f51531c36b8))
* запрос настроек выполняется без кластера ([5243dc5](https://github.com/ytsaurus/ytsaurus-ui/commit/5243dc533d14576901d1749e029633b497de981e))

{% endcut %}


{% cut "**1.23.0**" %}

**Дата релиза:** 2024-03-20


**Страница релиза:** [1.23.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.23.0)


#### [1.23.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.22.3...ui-v1.23.0) (2024-03-20)


#### Новые возможности

* **Query:** новый компонент ошибки запроса [YTFRONT-4000] ([9d781f4](https://github.com/ytsaurus/ytsaurus-ui/commit/9d781f4633b57a287554b91e56ecde182e86abd4))

{% endcut %}


{% cut "**1.22.3**" %}

**Дата релиза:** 2024-03-18


**Страница релиза:** [1.22.3](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.22.3)


#### [1.22.3](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.22.2...ui-v1.22.3) (2024-03-18)


#### Исправления ошибок

* **CHYT:** не загружать пулы, пока defaultPoolTree пуст [YTFRONT-3863] ([0b2e823](https://github.com/ytsaurus/ytsaurus-ui/commit/0b2e823adcc8f794fc4ec11c3951b2d444d9fb68))

{% endcut %}


{% cut "**1.22.2**" %}

**Дата релиза:** 2024-03-18


**Страница релиза:** [1.22.2](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.22.2)


#### [1.22.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.22.1...ui-v1.22.2) (2024-03-18)


#### Исправления ошибок

* **CHYT/CreateModal:** использование RangeInputPicker для поля 'Instances count' [YTFRONT-3863] ([efdf7a5](https://github.com/ytsaurus/ytsaurus-ui/commit/efdf7a5b883381d1081acf3dc8fce8a28c4a077b))
* **CHYT:** дерево пулов по умолчанию должно загружаться корректно [YTFRONT-3683] ([4ef42f4](https://github.com/ytsaurus/ytsaurus-ui/commit/4ef42f4597961af707ddeecd73eb36f751270e88))
* **CHYT:** мелкие исправления [YTFRONT-3863] ([fedd43d](https://github.com/ytsaurus/ytsaurus-ui/commit/fedd43dae9928c298da02fb60d427a91c9143245))
* **main:** добавлены отсутствующие используемые зависимости в package.json ([be505ec](https://github.com/ytsaurus/ytsaurus-ui/commit/be505ec0a489406a5dfda6503cbf52ad23b475f9))
* использование css-переменной var(--yt-font-weight) ([847ba30](https://github.com/ytsaurus/ytsaurus-ui/commit/847ba308553e18507053800c888bfc5586a1815b))

{% endcut %}


{% cut "**1.22.1**" %}

**Дата релиза:** 2024-03-17


**Страница релиза:** [1.22.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.22.1)


#### [1.22.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.22.0...ui-v1.22.1) (2024-03-17)


#### Исправления ошибок

* добавлено описание миграции для ytAuthCluster -> allowPasswordAuth [[#349](https://github.com/ytsaurus/ytsaurus-ui/issues/349)] ([d1a9b2b](https://github.com/ytsaurus/ytsaurus-ui/commit/d1a9b2b3e011ba77df2ee48bc8959530e5186ed5))

{% endcut %}


{% cut "**1.22.0**" %}

**Дата релиза:** 2024-03-13


**Страница релиза:** [1.22.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.22.0)


**Docker-образ:** [ghcr.io/ytsaurus/ui:1.22.0](https://github.com/orgs/ytsaurus/packages/container/ui/223076043?tag=1.22.0)


#### [1.22.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.21.0...ui-v1.22.0) (2024-03-13)


#### Новые возможности

* **odin:** поддержка odin url для каждого кластера ([959cbb9](https://github.com/ytsaurus/ytsaurus-ui/commit/959cbb9fcc0c4685f36eaa80748895593da94022))

{% endcut %}

{% cut "**1.21.0**" %}

**Дата релиза:** 2024-03-12


**Страница релиза:** [1.21.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.21.0)


#### [1.21.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.20.0...ui-v1.21.0) (2024-03-12)


#### Новые возможности

* **Навигация:** добавлены новые типы узлов: rootstock, scion [YTFRONT-4046] ([1b5bdca](https://github.com/ytsaurus/ytsaurus-ui/commit/1b5bdcaa9f5ebc66fb2d10fae00706cd37eb0c32))


#### Исправления

* **Операции/JobsMonitor:** улучшено условие видимости [YTFRONT-3940] ([caedb0c](https://github.com/ytsaurus/ytsaurus-ui/commit/caedb0c27c7464b734b55808fd1dbb074104d286))

{% endcut %}


{% cut "**1.20.0**" %}

**Дата релиза:** 2024-03-06


**Страница релиза:** [1.20.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.20.0)


#### [1.20.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.19.0...ui-v1.20.0) (2024-03-06)


#### Новые возможности

* **Навигация:** добавлено быстрое редактирование заголовка [YTFRONT-3783] ([514120a](https://github.com/ytsaurus/ytsaurus-ui/commit/514120a01f48cad3ea3a577a47a12b5aafaa4606))

{% endcut %}


{% cut "**1.19.0**" %}

**Дата релиза:** 2024-03-04


**Страница релиза:** [1.19.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.19.0)


#### [1.19.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.18.1...ui-v1.19.0) (2024-03-04)


#### Новые возможности

* **Система:** добавлены //sys/[@master](https://github.com/master)_alerts [YTFRONT-3960] ([7b2503d](https://github.com/ytsaurus/ytsaurus-ui/commit/7b2503d9c2aa8c2fde4c805d8e91589bafc80d6e))


#### Исправления

* **Бандлы/BundleEditor:** улучшена проверка ресурсов [YTFRONT-4035] ([349ac37](https://github.com/ytsaurus/ytsaurus-ui/commit/349ac37a2163b1c8712101cd19377a62b5df78a9))
* **Бандлы/MetaTable:** добавлены иконки для состояния с деталями [YTFRONT-4038] ([96f7533](https://github.com/ytsaurus/ytsaurus-ui/commit/96f753355e3076c490591bd388c1561f34849ace))
* **Навигация:** скрыта лишняя ошибка `[code cancelled]` [YTFRONT-4034] ([e31cc61](https://github.com/ytsaurus/ytsaurus-ui/commit/e31cc61f57c6e89d6edf0627873c3f9d17f4d995))
* **Планировщик/Детали:** для 'Cannot read properties of undefined (reading 'cpu')' [YTFRONT-4042] ([d3be924](https://github.com/ytsaurus/ytsaurus-ui/commit/d3be924172b78b2cd8b371d5df9adf02a6cf9a45))
* **Система/Мастера:** не загружать hydra для discovery-серверов [YTFRONT-4043] ([d2513ff](https://github.com/ytsaurus/ytsaurus-ui/commit/d2513ff255d287e46540b5156bb3cc47236fc7df))

{% endcut %}


{% cut "**1.18.1**" %}

**Дата релиза:** 2024-02-27


**Страница релиза:** [1.18.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.18.1)


#### [1.18.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.18.0...ui-v1.18.1) (2024-02-27)


#### Исправления

* **Планировщик:** не использовать '.../orchid/scheduler/scheduling_info_per_pool_tree' [YTFRONT-3937] ([a5a93bb](https://github.com/ytsaurus/ytsaurus-ui/commit/a5a93bb5d61a8814c80c7f512ae7a4aaa4bcd764))

{% endcut %}


{% cut "**1.18.0**" %}

**Дата релиза:** 2024-02-27


**Страница релиза:** [1.18.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.18.0)


#### [1.18.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.17.1...ui-v1.18.0) (2024-02-27)


#### Новые возможности

* **Навигация:** добавлена возможность редактирования документа [YTFRONT-3921] ([98b6dba](https://github.com/ytsaurus/ytsaurus-ui/commit/98b6dba191c010b34b282098866ea5dc59ee724c))

{% endcut %}


{% cut "**1.17.1**" %}

**Дата релиза:** 2024-02-27


**Страница релиза:** [1.17.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.17.1)


#### [1.17.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.17.0...ui-v1.17.1) (2024-02-27)


#### Исправления

* **Запросы:** использовать POST-данные для параметров команды startQuery [YTFRONT-4023] ([d03c8e0](https://github.com/ytsaurus/ytsaurus-ui/commit/d03c8e0f65800a8332dac4165c43efe46a868885))
* неработающая агрегация в планировщике [YTFRONT-4031] ([d460549](https://github.com/ytsaurus/ytsaurus-ui/commit/d460549d4073572dff855a962b8e7c1085566415))

{% endcut %}


{% cut "**1.16.2**" %}

**Дата релиза:** 2024-02-22


**Страница релиза:** [1.16.2](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.16.2)


#### [1.16.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.16.1...ui-v1.16.2) (2024-02-22)


#### Исправления

* неработающая агрегация в планировщике [YTFRONT-4031] ([f2c2246](https://github.com/ytsaurus/ytsaurus-ui/commit/f2c224699e2ef9d1efa9b245ba2716dc5f511376))

{% endcut %}


{% cut "**1.17.0**" %}

**Дата релиза:** 2024-02-16


**Страница релиза:** [1.17.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.17.0)


#### [1.17.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.16.1...ui-v1.17.0) (2024-02-16)


#### Новые возможности

* добавлена мультикластерная аутентификация по паролю [[#349](https://github.com/ytsaurus/ytsaurus-ui/issues/349)] ([ddf4617](https://github.com/ytsaurus/ytsaurus-ui/commit/ddf4617387ab8f88f901f268d72e17eff66d0f57))
* отображение авторизованных кластеров [[#349](https://github.com/ytsaurus/ytsaurus-ui/issues/349)] ([d582bfc](https://github.com/ytsaurus/ytsaurus-ui/commit/d582bfcd490b8199e5713f24f7970697ba0513dd))

{% endcut %}


{% cut "**1.16.1**" %}

**Дата релиза:** 2024-02-14


**Страница релиза:** [1.16.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.16.1)


#### [1.16.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.16.0...ui-v1.16.1) (2024-02-14)


#### Исправления

* небольшое исправление ts-ошибки после перебазирования ([e5689aa](https://github.com/ytsaurus/ytsaurus-ui/commit/e5689aa02acb9e173b2689af100e76ced0b4e821))

{% endcut %}


{% cut "**1.16.0**" %}

**Дата релиза:** 2024-02-14


**Страница релиза:** [1.16.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.16.0)


#### [1.16.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.15.3...ui-v1.16.0) (2024-02-14)


#### Новые возможности

* **Операции/Детали:** улучшен live-предпросмотр [YTFRONT-3956] ([0b1ffb9](https://github.com/ytsaurus/ytsaurus-ui/commit/0b1ffb97fbbc52c24836802c23200661b0ab344e))
* **query-tracker:** управление aco для запросов [[#246](https://github.com/ytsaurus/ytsaurus-ui/issues/246)] ([8b79661](https://github.com/ytsaurus/ytsaurus-ui/commit/8b79661cabc4a949687c407e4abcc08762bd776f))

{% endcut %}


{% cut "**1.15.3**" %}

**Дата релиза:** 2024-02-12


**Страница релиза:** [1.15.3](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.15.3)


#### [1.15.3](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.15.2...ui-v1.15.3) (2024-02-12)


#### Исправления

* **Операции/DataFlow:** улучшено имя колонки для chunk_count [YTFRONT-3924] ([7477c48](https://github.com/ytsaurus/ytsaurus-ui/commit/7477c4837815b3b96923b81e5e66b32233e9346b))

{% endcut %}


{% cut "**1.15.2**" %}

**Дата релиза:** 2024-02-09


**Страница релиза:** [1.15.2](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.15.2)


#### [1.15.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.15.1...ui-v1.15.2) (2024-02-09)


#### Исправления

* **layout:** левое меню исчезает при длинных таблицах [[#225](https://github.com/ytsaurus/ytsaurus-ui/issues/225)] ([d54448a](https://github.com/ytsaurus/ytsaurus-ui/commit/d54448ab86c6ae888128ef485ff03d565331c2b2))
* **Навигация/MapNode:** не переносить экранированные символы ([60c0893](https://github.com/ytsaurus/ytsaurus-ui/commit/60c089364290fffae0d3ea88476c8a2cc6c52e40))
* **Список операций:** исправление 'unexpected error' после прерывания операции [YTFRONT-4013] ([7847e91](https://github.com/ytsaurus/ytsaurus-ui/commit/7847e91ec931654a97b23d0e9c09972cb91ce61a))
* **Планировщик:** разрешено создание пулов с родителем &lt;Root&gt; [[#274](https://github.com/ytsaurus/ytsaurus-ui/issues/274)] ([91aa32e](https://github.com/ytsaurus/ytsaurus-ui/commit/91aa32e45aee7700fd55b12c2a0e77011fdb40a7))
* **таблица:** левое меню исчезает при длинных таблицах [[#225](https://github.com/ytsaurus/ytsaurus-ui/issues/225)] ([4c4b015](https://github.com/ytsaurus/ytsaurus-ui/commit/4c4b015c1c327a9fc26dce39ca373fff3a4d709f))

{% endcut %}

{% cut "**1.15.1**" %}

**Дата релиза:** 2024-02-05


**Релизная страница:** [1.15.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.15.1)


#### [1.15.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.15.0...ui-v1.15.1) (2024-02-05)


#### Исправления

* **settings:** читать настройки из localStorage [[#341](https://github.com/ytsaurus/ytsaurus-ui/issues/341)] ([ea6ddbd](https://github.com/ytsaurus/ytsaurus-ui/commit/ea6ddbd7a2d8f8a7ff8b0ab9c2eba7af4acfe3cb))

{% endcut %}


{% cut "**1.15.0**" %}

**Дата релиза:** 2024-02-05


**Релизная страница:** [1.15.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.15.0)


#### [1.15.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.14.3...ui-v1.15.0) (2024-02-05)


#### Новые возможности

* добавить UISettings.reportBugUrl [[#336](https://github.com/ytsaurus/ytsaurus-ui/issues/336)] ([e86ccb2](https://github.com/ytsaurus/ytsaurus-ui/commit/e86ccb2865918a7b62c7857a1079c2248b855286))


#### Исправления

* **Operations/Details:** мелкий css-фикс [YTFRONT-3518] ([91d9b01](https://github.com/ytsaurus/ytsaurus-ui/commit/91d9b0172dd8de0c1d610bc9eb1ca8d2117b1dd6))
* **Scheduling/PoolEditor:** корректное значение для fifo_sort_parameters [YTFRONT-3957] ([34d5cdb](https://github.com/ytsaurus/ytsaurus-ui/commit/34d5cdb672b0eeadce80f10d1f828f1579e326ac))
* **SupportForm:** переработан api функции makeSupportContent [YTFRONT-3994] ([a563179](https://github.com/ytsaurus/ytsaurus-ui/commit/a563179b6ce5d85e79e24a58a4aa425b5708b281))

{% endcut %}


{% cut "**1.14.3**" %}

**Дата релиза:** 2024-02-01


**Релизная страница:** [1.14.3](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.14.3)


#### [1.14.3](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.14.2...ui-v1.14.3) (2024-02-01)


#### Исправления

* не ждать ответа от checkIsDeveloper [YTFRONT-3862] ([4f0470b](https://github.com/ytsaurus/ytsaurus-ui/commit/4f0470b8a8a0fdd2beae8f911ab7666c0cdc5bbe))
* **timestampProvider:** обновлять значение по умолчанию, если отсутствует `clock_cell` [YTFRONT-3946] ([8f44e05](https://github.com/ytsaurus/ytsaurus-ui/commit/8f44e05e1b6d655eab4fecdb1112647466625511))

{% endcut %}


{% cut "**1.14.2**" %}

**Дата релиза:** 2024-01-30


**Релизная страница:** [1.14.2](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.14.2)


#### [1.14.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.14.1...ui-v1.14.2) (2024-01-30)


#### Исправления

* улучшено сообщение об ошибке для executeBatch ([1d96e53](https://github.com/ytsaurus/ytsaurus-ui/commit/1d96e539ffa3f2a3a690d3b3b081e8dea6b3a2db))
* обновление @ytsaurus/javascript v0.6.1 ([935cee4](https://github.com/ytsaurus/ytsaurus-ui/commit/935cee4ea253b13c8246c1ae740bf2833fb706f0))

{% endcut %}


{% cut "**1.14.1**" %}

**Дата релиза:** 2024-01-30


**Релизная страница:** [1.14.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.14.1)


#### [1.14.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.14.0...ui-v1.14.1) (2024-01-30)


#### Исправления

* **Components/Node:** корректная обработка узлов со статусом 'offline' [YTFRONT-3993] ([53c4d16](https://github.com/ytsaurus/ytsaurus-ui/commit/53c4d169094d441df24b1c0b00210e186d1623af))
* **Navigation/Table/Merge:** обновление @ytsaurus/javascript-wrapper [YTFRONT-3953] ([f5b4128](https://github.com/ytsaurus/ytsaurus-ui/commit/f5b412879a8dffc453e2530878600f4723a95b0c))
* **Operations:** корректная отмена запросов [YTFRONT-3996] ([d3afc0f](https://github.com/ytsaurus/ytsaurus-ui/commit/d3afc0f4945e2679918f30061359fe70112b34cf))
* **xss:** исправлена XSS-уязвимость [YTFRONT-4004] ([7819f8a](https://github.com/ytsaurus/ytsaurus-ui/commit/7819f8a4c54c379d7e8300bbcc56b8192abb3e41))

{% endcut %}


{% cut "**1.14.0**" %}

**Дата релиза:** 2024-01-29


**Релизная страница:** [1.14.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.14.0)


#### [1.14.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.13.1...ui-v1.14.0) (2024-01-29)


#### Новые возможности

* **Scheduling:** загрузка данных только для видимых пулов [YTFRONT-3862] ([056f431](https://github.com/ytsaurus/ytsaurus-ui/commit/056f4319b034173728564cdb4f97f067665ff5af))


#### Исправления

* **Components/Node:** узлы со статусом 'offline' должны обрабатываться корректно [YTFRONT-3993] ([eb34e49](https://github.com/ytsaurus/ytsaurus-ui/commit/eb34e49e9ec12506f79d99a3cc32ce4ee1949f78))
* **Scheduling:** использовать pool_trees вместо scheduling_info_per_pool_tree [YTFRONT-3937] ([f745a67](https://github.com/ytsaurus/ytsaurus-ui/commit/f745a67285b1f649debc061a5938425626a07931))
* **support.js:** избавиться от _DEV_PATCH_NUMBER [YTFRONT-3862] ([35caa4b](https://github.com/ytsaurus/ytsaurus-ui/commit/35caa4b7305a52f7bc8aa75d494e2fc109172756))
* **support:** scheduler, master должны проверяться корректно [YTFRONT-3862] ([64e5583](https://github.com/ytsaurus/ytsaurus-ui/commit/64e5583485a48d5565964e8be380a68bf96b8910))

{% endcut %}


{% cut "**1.13.1**" %}

**Дата релиза:** 2024-01-29


**Релизная страница:** [1.13.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.13.1)


#### [1.13.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.13.0...ui-v1.13.1) (2024-01-29)


#### Исправления

* получение корректного cell_tag [YTFRONT-3946] ([dee458b](https://github.com/ytsaurus/ytsaurus-ui/commit/dee458b1aa052e0d56ae90b2b405e4b662bed2ad))

{% endcut %}


{% cut "**1.13.0**" %}

**Дата релиза:** 2024-01-26


**Релизная страница:** [1.13.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.13.0)


#### [1.13.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.12.2...ui-v1.13.0) (2024-01-26)


#### Новые возможности

* реализована авторизация через OAuth [YTFRONT-3903] ([38fcda4](https://github.com/ytsaurus/ytsaurus-ui/commit/38fcda40dacbd12be0deba573b9fc32f17d445b5))

{% endcut %}


{% cut "**1.12.2**" %}

**Дата релиза:** 2024-01-23


**Релизная страница:** [1.12.2](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.12.2)


#### [1.12.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.12.1...ui-v1.12.2) (2024-01-23)


#### Исправления

* добавлена валидация ресурсов бандла [YTFRONT-3931] ([72b17d3](https://github.com/ytsaurus/ytsaurus-ui/commit/72b17d3160dd557c3b3cb4b7c311c4f348be237e))
* ошибка nodejs при выходе из системы [[#292](https://github.com/ytsaurus/ytsaurus-ui/issues/292)] ([3e64d2c](https://github.com/ytsaurus/ytsaurus-ui/commit/3e64d2cef1760f7b40b866e806fc6f835d007cbd))

{% endcut %}


{% cut "**1.12.1**" %}

**Дата релиза:** 2024-01-22


**Релизная страница:** [1.12.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.12.1)


#### [1.12.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.12.0...ui-v1.12.1) (2024-01-22)


#### Исправления

* **Queries/Results:** синхронизация заголовка таблицы при изменении размера [[#294](https://github.com/ytsaurus/ytsaurus-ui/issues/294)] ([f625984](https://github.com/ytsaurus/ytsaurus-ui/commit/f6259848aa7159474ce929cc963f065383e3382b))

{% endcut %}


{% cut "**1.12.0**" %}

**Дата релиза:** 2024-01-22


**Релизная страница:** [1.12.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.12.0)


#### [1.12.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.11.2...ui-v1.12.0) (2024-01-22)


#### Новые возможности

* добавлена новая кнопка запроса [[#238](https://github.com/ytsaurus/ytsaurus-ui/issues/238)] ([b66fa31](https://github.com/ytsaurus/ytsaurus-ui/commit/b66fa31927187debe8361e7866f33dc62b211026))
* **Components/Node:** добавлена вкладка 'Unrecognized options' [YTFRONT-3936] ([520916b](https://github.com/ytsaurus/ytsaurus-ui/commit/520916baddf345ff1c5f082dd6be57e4a3514fdb))

{% endcut %}


{% cut "**1.11.2**" %}

**Дата релиза:** 2024-01-16


**Релизная страница:** [1.11.2](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.11.2)


#### [1.11.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.11.1...ui-v1.11.2) (2024-01-16)


#### Исправления

* **CHYT:** не отображать CHYT-страницу, если chyt_controller_base_url пуст [YTFRONT-3863] ([cb66484](https://github.com/ytsaurus/ytsaurus-ui/commit/cb664842ffdd6ae56461afdc85340ba6c9fbd602))

{% endcut %}

{% cut "**1.11.1**" %}

**Дата релиза:** 2024-01-11


**Страница релиза:** [1.11.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.11.1)


#### [1.11.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.11.0...ui-v1.11.1) (2024-01-11)


#### Исправления

* **CHYT:** мелкие исправления [YTFRONT-3863] ([b71db09](https://github.com/ytsaurus/ytsaurus-ui/commit/b71db097642001cf21adc67493da0763443fa931))

{% endcut %}


{% cut "**1.11.0**" %}

**Дата релиза:** 2024-01-09


**Страница релиза:** [1.11.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.11.0)


#### [1.11.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.10.0...ui-v1.11.0) (2024-01-09)


#### Новые возможности

* **CHYT:** добавлена страница CHYT со списком кликов [YTFRONT-3683] ([de0c74a](https://github.com/ytsaurus/ytsaurus-ui/commit/de0c74a368ab37b5aa953e965efab8d8a4d9b2e1))
* обновление @gravity-ui/dialog-fields v4.3.0 ([5f61464](https://github.com/ytsaurus/ytsaurus-ui/commit/5f614647b3c70084d682cbf233df727107425eea))


#### Исправления

* **PoolSuggestControl:** загрузка всех пулов ([d56d0df](https://github.com/ytsaurus/ytsaurus-ui/commit/d56d0df20f1e39dd93074283ec5bdc9edc2622e0))

{% endcut %}


{% cut "**1.10.0**" %}

**Дата релиза:** 2023-12-22


**Страница релиза:** [1.10.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.10.0)


#### [1.10.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.9.0...ui-v1.10.0) (2023-12-22)


#### Новые возможности

* отображение id локаций в таблице узлов данных [[#204](https://github.com/ytsaurus/ytsaurus-ui/issues/204)] ([29ff849](https://github.com/ytsaurus/ytsaurus-ui/commit/29ff849f75af54056b471ea0546cce96b9f7037d))
* запрет закрытия окна браузера с несохранённым текстом запроса [[#226](https://github.com/ytsaurus/ytsaurus-ui/issues/226)] ([e3d12e8](https://github.com/ytsaurus/ytsaurus-ui/commit/e3d12e8e9c65639f6892b5be1ab9031581400c11))
* **query-tracker:** скрытие боковой панели истории запросов [[#211](https://github.com/ytsaurus/ytsaurus-ui/issues/211)] ([5602087](https://github.com/ytsaurus/ytsaurus-ui/commit/5602087899f0bf07eeb20312ee31cab66a055719))


#### Исправления

* **Queries:** скрытие пустой вкладки прогресса [YTFRONT-3952] ([858b11f](https://github.com/ytsaurus/ytsaurus-ui/commit/858b11f1ba262456953a75121e430f686c6a6e36))
* отображение сложных типов в навигации [[#229](https://github.com/ytsaurus/ytsaurus-ui/issues/229)] ([1bef4ae](https://github.com/ytsaurus/ytsaurus-ui/commit/1bef4ae7493dfed7f30bb2a64ac81d65e189bb7c))

{% endcut %}


{% cut "**1.9.0**" %}

**Дата релиза:** 2023-12-20


**Страница релиза:** [1.9.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.9.0)


#### [1.9.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.8.0...ui-v1.9.0) (2023-12-20)


#### Новые возможности

* добавлен SettingMenuItem.props.useSwitch ([c3f5154](https://github.com/ytsaurus/ytsaurus-ui/commit/c3f5154212c4d30b81fed8910996be686444e95b))

{% endcut %}


{% cut "**1.8.0**" %}

**Дата релиза:** 2023-12-19


**Страница релиза:** [1.8.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.8.0)


#### [1.8.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.7.3...ui-v1.8.0) (2023-12-19)


#### Новые возможности

* обновление @gravity-ui/navigation v1.8.0 ([e5530e1](https://github.com/ytsaurus/ytsaurus-ui/commit/e5530e16155d9bf124acf41d825093e92206462a))

{% endcut %}


{% cut "**1.7.3**" %}

**Дата релиза:** 2023-12-18


**Страница релиза:** [1.7.3](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.7.3)


#### [1.7.3](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.7.2...ui-v1.7.3) (2023-12-18)


#### Исправления

* **main.js:** вынос MonacoEditor в отдельный чанк [YTFRONT-3814] ([2492c78](https://github.com/ytsaurus/ytsaurus-ui/commit/2492c78a187e1ba5be76b8254ffb5c2927b75c9d))

{% endcut %}


{% cut "**1.7.2**" %}

**Дата релиза:** 2023-12-13


**Страница релиза:** [1.7.2](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.7.2)


#### [1.7.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.7.1...ui-v1.7.2) (2023-12-13)


#### Исправления

* синхронизация packages/ui/package-lock.json ([381a97f](https://github.com/ytsaurus/ytsaurus-ui/commit/381a97f8ce0fde3ed85a316b133b0045d55af51e))

{% endcut %}


{% cut "**1.7.0**" %}

**Дата релиза:** 2023-12-13


**Страница релиза:** [1.7.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.7.0)


#### [1.7.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.6.0...ui-v1.7.0) (2023-12-13)


#### Новые возможности

* **query-tracker:** добавлена возможность прикреплять файлы к запросам [[#221](https://github.com/ytsaurus/ytsaurus-ui/issues/221)] ([16d4138](https://github.com/ytsaurus/ytsaurus-ui/commit/16d41384621d368e83b34bfc5d1de933afc7d7b9))


#### Исправления

* **Components/SetupModal:** исправлен фильтр Racks [YTFRONT-3944] ([0662e07](https://github.com/ytsaurus/ytsaurus-ui/commit/0662e07355402c5cbed8ffc3dd39997397532ea0))
* **RemoteCopy:** временно убраны erasure_codec, compression_codec [YTFRONT-3935] ([8518c96](https://github.com/ytsaurus/ytsaurus-ui/commit/8518c968200e92f6fe7242635515435fb70b1505))
* **Scheduling:** селектор дерева должен быть фильтруемым [YTFRONT-3948] ([3102f5e](https://github.com/ytsaurus/ytsaurus-ui/commit/3102f5ea9a04d03f75420999dbbeb776e481afbf))
* **System/Chunks:** i.get не является функцией [YTFRONT-3943] ([8c6a9e5](https://github.com/ytsaurus/ytsaurus-ui/commit/8c6a9e59f5f9ca31ab9f8fd7b881c847d130bd8a))

{% endcut %}


{% cut "**1.6.0**" %}

**Дата релиза:** 2023-12-08


**Страница релиза:** [1.6.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.6.0)


#### [1.6.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.5.2...ui-v1.6.0) (2023-12-08)


#### Новые возможности

* **Table/Schema:** добавлен столбец внешнего названия [YTFRONT-3939] ([074f638](https://github.com/ytsaurus/ytsaurus-ui/commit/074f6386e104989531b0d2432c581f5296f3b60d))


#### Исправления

* **query-tracker:** корректное отображение содержимого динамических системных столбцов [[#192](https://github.com/ytsaurus/ytsaurus-ui/issues/192)] ([271b1f6](https://github.com/ytsaurus/ytsaurus-ui/commit/271b1f6660627a5c10fbae1a60145e03c111acc6))

{% endcut %}


{% cut "**1.5.2**" %}

**Дата релиза:** 2023-12-06


**Страница релиза:** [1.5.2](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.5.2)


#### [1.5.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.5.1...ui-v1.5.2) (2023-12-05)


#### Исправления

* отсутствие повторного рендеринга при переключении запросов в пагинации ([99db6af](https://github.com/ytsaurus/ytsaurus-ui/commit/99db6af890cbce0213223d605fe604e5ea64b19c))
* **query tracker:** количество строк и флаг усечения теперь отображаются над таблицей результатов [[#210](https://github.com/ytsaurus/ytsaurus-ui/issues/210)] ([fe200b9](https://github.com/ytsaurus/ytsaurus-ui/commit/fe200b9a2f964f484aa0e820ea9c68c7b12d8d32))
* **query-tracker:** некорректное отображение результатов запросов с более чем 50 столбцами [#208](https://github.com/ytsaurus/ytsaurus-ui/issues/208) ([8e2ddc7](https://github.com/ytsaurus/ytsaurus-ui/commit/8e2ddc77b3b2691a346a3bd22be8b5d2558b61f5))

{% endcut %}


{% cut "**1.5.1**" %}

**Дата релиза:** 2023-12-01


**Страница релиза:** [1.5.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.5.1)


#### [1.5.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.5.0...ui-v1.5.1) (2023-12-01)


#### Исправления

* улучшение раздела Development в readme ([c06921b](https://github.com/ytsaurus/ytsaurus-ui/commit/c06921ba3956cd861411927c025607884d991f8b))

{% endcut %}


{% cut "**1.5.0**" %}

**Дата релиза:** 2023-12-01


**Страница релиза:** [1.5.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.5.0)


#### [1.5.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.4.0...ui-v1.5.0) (2023-12-01)


#### Новые возможности

* **QT:** добавлена кнопка переключения видимости списка запросов ([1ba0ccd](https://github.com/ytsaurus/ytsaurus-ui/commit/1ba0ccdc2c7e095c86660f472f72eee62210c710))
* **QT:** компонент прогресса и таймлайна [YTFRONT-3840] ([a092966](https://github.com/ytsaurus/ytsaurus-ui/commit/a092966199317abc8a637569bd28e9249ab8c5ac))


#### Исправления

* **QT:** выравнивание Loader по центру при запасном варианте ([3c1ad2b](https://github.com/ytsaurus/ytsaurus-ui/commit/3c1ad2bb88ca0c489de4d0afc511c28650b2d62a))
* **QT:** настройка стадии yql не должна влиять на запросы других движков ([1f2b253](https://github.com/ytsaurus/ytsaurus-ui/commit/1f2b253d7305ea870f27ed5cc28edfffe0fdeb43))

{% endcut %}

{% cut "**1.4.0**" %}

**Дата релиза:** 2023-11-17


**Страница релиза:** [1.4.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.4.0)


#### [1.4.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.3.1...ui-v1.4.0) (2023-11-17)


#### Новые возможности

* **Updater:** обработка событий 'visibilitychange' объекта window.document [YTFRONT-3835] ([76fe005](https://github.com/ytsaurus/ytsaurus-ui/commit/76fe0050e2d0c0c1fe969336cbfe57ba6c70432a))

{% endcut %}


{% cut "**1.3.1**" %}

**Дата релиза:** 2023-11-16


**Страница релиза:** [1.3.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.3.1)


#### [1.3.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.3.0...ui-v1.3.1) (2023-11-16)


#### Исправления

* **Accounts,Bundles:** улучшены значения по умолчанию для настроек аккаунтинга [YTFRONT-3891] ([8a79e4b](https://github.com/ytsaurus/ytsaurus-ui/commit/8a79e4b0bc1c2156e0dd10646cb069bd4b1e1dd0))
* **Accounts:** не использовать кэш после редактирования [YTFRONT-3920] ([0ab91a0](https://github.com/ytsaurus/ytsaurus-ui/commit/0ab91a0bdc80ef08c51d79894b4121afa6f9435e))
* **Jobs:** не использовать заглавные буквы для job-type [YTFRONT-3917] ([1f4e1bf](https://github.com/ytsaurus/ytsaurus-ui/commit/1f4e1bf45082c1e77a7a5f989b080e08196f069f))
* **Navigation/Consumer:** исправление селектора Target Queue [YTFRONT-3910] ([5358127](https://github.com/ytsaurus/ytsaurus-ui/commit/5358127d57339867b2124239eeb48ac8ca47555d))
* **Navigation/MapNode:** обрезка длинных имён с многоточием [YTFRONT-3913] ([67eddcb](https://github.com/ytsaurus/ytsaurus-ui/commit/67eddcb5d1a2529068610b7fdb7e60ab17506ed1))
* **Odin:** небольшое исправление вёрстки [YTFRONT-3909] ([8012884](https://github.com/ytsaurus/ytsaurus-ui/commit/8012884b26849902a19358937893695023c42b26))
* **reShortNameFromAddress:** улучшено значение по умолчанию [YTFRONT-3861] ([39f07ad](https://github.com/ytsaurus/ytsaurus-ui/commit/39f07ad0b9b98863033b2beccd3c6f8798a19006))
* **Scheduling:** исправление селектора pool-tree [YTFRONT-3918] ([3fb86bb](https://github.com/ytsaurus/ytsaurus-ui/commit/3fb86bb7f192afda9a5103184f65403117a75d6b))

{% endcut %}


{% cut "**1.3.0**" %}

**Дата релиза:** 2023-11-10


**Страница релиза:** [1.3.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.3.0)


#### [1.3.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.2.0...ui-v1.3.0) (2023-11-10)


#### Новые возможности

* добавлен метод UIFactory.getAllowedExperimentalPages [YTFRONT-3912] ([fca2666](https://github.com/ytsaurus/ytsaurus-ui/commit/fca266621a7731db5dfe979efde095d0d2c6c4d5))

{% endcut %}


{% cut "**1.2.0**" %}

**Дата релиза:** 2023-11-10


**Страница релиза:** [1.2.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.2.0)


#### [1.2.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.1.2...ui-v1.2.0) (2023-11-10)


#### Новые возможности

* **QT:** добавлен компонент Progress [YTFRONT-3840] ([69a787a](https://github.com/ytsaurus/ytsaurus-ui/commit/69a787a25d67f14d3a8687d65c35eb71f654a295))


#### Исправления

* **QT:** исправлено переключение вкладок результатов при polling [YTFRONT-3840] ([b304c2b](https://github.com/ytsaurus/ytsaurus-ui/commit/b304c2bc96d4018a133fea41e3a5f2bfb37d413a))
* **QT:** Plan: добавлены ссылки на таблицы и операции в узлы [YTFRONT-3840] ([080acbc](https://github.com/ytsaurus/ytsaurus-ui/commit/080acbc879c34e3c979dc81c61b36a488ddff18b))

{% endcut %}


{% cut "**1.1.2**" %}

**Дата релиза:** 2023-11-09


**Страница релиза:** [1.1.2](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.1.2)


#### [1.1.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.1.1...ui-v1.1.2) (2023-11-09)


#### Исправления

* конфликт babel config app-builder ([e435a25](https://github.com/ytsaurus/ytsaurus-ui/commit/e435a259a040fbf09490873210cca5ad37ff3e9d))

{% endcut %}


{% cut "**1.1.1**" %}

**Дата релиза:** 2023-10-27


**Страница релиза:** [1.1.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.1.1)


#### [1.1.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.1.0...ui-v1.1.1) (2023-10-27)


#### Исправления

* **Navigation/ACL**: форма создания group columns доступна только для map_nodes [YTFRONT-3901] ([32a8bf0](https://github.com/ytsaurus/ytsaurus-ui/commit/32a8bf0b043881f574d60839c328bb65806c0a01))
* **GroupsPage:** убран updater со страницы GroupsPage [YTFRONT-3835] ([548798b](https://github.com/ytsaurus/ytsaurus-ui/commit/548798ba7fc413d7e767d54e20aa89f5c276406b))
* **Users:** убран updater со страницы UsersPage [YTFRONT-3835] ([78cd8e8](https://github.com/ytsaurus/ytsaurus-ui/commit/78cd8e849907a6c8bab7b9e2714844e51c8861da))

{% endcut %}


{% cut "**0.23.1**" %}

**Дата релиза:** 2023-10-26


**Страница релиза:** [0.23.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.23.1)


#### [0.23.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.23.0...ui-v0.23.1) (2023-10-26)


#### Исправления

* форма создания group columns доступна только для map_nodes [YTFRONT-3901] ([bb3d183](https://github.com/ytsaurus/ytsaurus-ui/commit/bb3d1834ca8112d0afd7e5788372580e06a40b74))
* **GroupsPage:** убран updater со страницы GroupsPage [YTFRONT-3835] ([c68967c](https://github.com/ytsaurus/ytsaurus-ui/commit/c68967c4bb5fe7f0a75432ba1170ad041dac4a14))
* **Users:** убран updater со страницы UsersPage [YTFRONT-3835] ([1e1e887](https://github.com/ytsaurus/ytsaurus-ui/commit/1e1e88788fedd9ce2b3ddd22f171c844210dedec))

{% endcut %}


{% cut "**1.1.0**" %}

**Дата релиза:** 2023-10-20


**Страница релиза:** [1.1.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.1.0)


#### [1.1.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.0.2...ui-v1.1.0) (2023-10-20)


#### Новые возможности

* **ClusterAppearance:** добавлена возможность переопределять иконки кластеров [YTFRONT-3879] ([61e27f7](https://github.com/ytsaurus/ytsaurus-ui/commit/61e27f71390f08c101dbfd0df1650ee27a4014c2))
* **ClusterConfig:** добавлено поле 'externalProxy' [YTFRONT-3890] ([c172097](https://github.com/ytsaurus/ytsaurus-ui/commit/c172097b4324fc84ec720e3ba786f0baa6b5f5d2))
* **Compoents/Nodes:** добавлена колонка 'Flavors' [YTFRONT-3886] ([0256361](https://github.com/ytsaurus/ytsaurus-ui/commit/0256361235bf0b541f9cba4088408af97c27ede1))
* **UISettings:** добавлен параметр reShortNameFromAddress [YTFRONT-3861] ([fa433ba](https://github.com/ytsaurus/ytsaurus-ui/commit/fa433baaa65fa3debe895630e5727c8015d3d6f8))
* **unipika:** добавлен параметр UISettings.hidReferrerUrl (+e2e) [YTFRONT-3875] ([2ee7524](https://github.com/ytsaurus/ytsaurus-ui/commit/2ee75245d638694e5d61dccfc56e394960fddd81))
* **unipika:** добавлен параметр UISettings.reUnipikaAllowTaggedSources [YTFRONT-3875] ([6039c30](https://github.com/ytsaurus/ytsaurus-ui/commit/6039c3081c2d135ed45c2e330fec4cad1d3727b7))


#### Исправления

* **ACL:** неизвестные роли должны подсвечиваться [YTFRONT-3885] ([de284ca](https://github.com/ytsaurus/ytsaurus-ui/commit/de284ca31d4c1ccf0e7c79eb3263bb61a2197b22))
* **BundleEditorDialog:** key_filter_block_cache должен влиять на значение 'Free' памяти [YTFRONT-3825] ([2ffc449](https://github.com/ytsaurus/ytsaurus-ui/commit/2ffc44916afe804fd1c5c89727eb2d0ffb78f3db))
* **Components/Nodes:** user/system теги не должны быть шире ячейки таблицы ([0562e48](https://github.com/ytsaurus/ytsaurus-ui/commit/0562e48a0e7600bcc974dbfce9dd682a0b051c1c))
* **controllers/home:** убран заголовок 'Strict-Transport-Security' [YTFRONT-3896] ([1fd14b0](https://github.com/ytsaurus/ytsaurus-ui/commit/1fd14b0b3ab76f193730c2909c150dfeaa5e2e19))
* **Markdown:** YFM не должен дублировать заголовки [YTFRONT-3897] ([cc92e5b](https://github.com/ytsaurus/ytsaurus-ui/commit/cc92e5bcd688aaa601e3cc4bdfe31f21549efefc))

{% endcut %}


{% cut "**1.0.2**" %}

**Дата релиза:** 2023-10-09


**Страница релиза:** [1.0.2](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.0.2)


#### [1.0.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.0.1...ui-v1.0.2) (2023-10-09)


#### Исправления

* обновлены @gravity-ui/charkit v4.7.2, @gravity-ui/yagr v3.10.4 ([e647df2](https://github.com/ytsaurus/ytsaurus-ui/commit/e647df2cb980ae60f73afe7cbefd6bd1478f6e38))

{% endcut %}


{% cut "**1.0.1**" %}

**Дата релиза:** 2023-10-09


**Страница релиза:** [1.0.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.0.1)


#### [1.0.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.0.0...ui-v1.0.1) (2023-10-09)


#### Исправления

* попытка повторного запуска релиза ([ebcb80f](https://github.com/ytsaurus/ytsaurus-ui/commit/ebcb80f09df5a8ddd479f295dc701c01558748a3))

{% endcut %}


{% cut "**1.0.0**" %}

**Дата релиза:** 2023-10-09


**Страница релиза:** [1.0.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v1.0.0)


#### [1.0.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.23.0...ui-v1.0.0) (2023-10-09)


#### ⚠ КРИТИЧЕСКИЕ ИЗМЕНЕНИЯ

* обновление @gravity-ui/uikit v5

#### Новые возможности

* обновление @gravity-ui/uikit v5 ([1c89981](https://github.com/ytsaurus/ytsaurus-ui/commit/1c8998151fc8053bdbb4486359b6c53b53476dc3))

{% endcut %}


{% cut "**0.23.0**" %}

**Дата релиза:** 2023-10-02


**Страница релиза:** [0.23.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.23.0)


#### [0.23.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.22.0...ui-v0.23.0) (2023-10-02)


#### Новые возможности

* **Components/HttpProxies,RPCProxies:** добавлено модальное окно NodeMaintenance [YTFRONT-3792] ([f1a68bd](https://github.com/ytsaurus/ytsaurus-ui/commit/f1a68bdb534af5279e80a8fd703d09f8eaac77af))
* **Components/Nodes:** добавлено модальное окно NodeMaintenanceModal [YTFRONT-3792] ([1b01b70](https://github.com/ytsaurus/ytsaurus-ui/commit/1b01b70dcafde2beee6fcadc60beec6555085d4b))

{% endcut %}

{% cut "**0.22.0**" %}

**Дата релиза:** 2023-09-29


**Страница релиза:** [0.22.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.22.0)


#### [0.22.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.21.1...ui-v0.22.0) (2023-09-29)


#### Новые возможности

* **QT:** добавлена опция движка SPYT [YTFRONT-3872] ([ec04fe3](https://github.com/ytsaurus/ytsaurus-ui/commit/ec04fe33bafe280d376d8ccc457f6a15726b87de))


#### Исправления

* **Scheduling:** эфемерные пулы должны быть видимы [YTFRONT-3708] ([65dd571](https://github.com/ytsaurus/ytsaurus-ui/commit/65dd571d9fa46fbfbfdf2fa2a149b08e556c92ec))
* необработанная ошибка из консоли браузера ([9bfe4a4](https://github.com/ytsaurus/ytsaurus-ui/commit/9bfe4a489ec67d1e7556d1c815217823234a45f8))

{% endcut %}


{% cut "**0.21.1**" %}

**Дата релиза:** 2023-09-26


**Страница релиза:** [0.21.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.21.1)


#### [0.21.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.21.0...ui-v0.21.1) (2023-09-26)


#### Исправления

* **Components/Versions:** исправлены столбцы state, banned для Details [YTFRONT-3854] ([eed0e91](https://github.com/ytsaurus/ytsaurus-ui/commit/eed0e9137460c27caf6b7303bbab1270c6a4217e))
* **Components/Versions:** используется 'cluster_node' вместо 'node' [YTFRONT-3854] ([d60950d](https://github.com/ytsaurus/ytsaurus-ui/commit/d60950d4be467830dd924ea7f883eefffae000a8))

{% endcut %}


{% cut "**0.21.0**" %}

**Дата релиза:** 2023-09-19


**Страница релиза:** [0.21.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.21.0)


#### [0.21.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.20.0...ui-v0.21.0) (2023-09-19)


#### Новые возможности

* **Navigation:** используется атрибут [@effective_expiration](https://github.com/effective) [YTFRONT-3665] ([6eafe35](https://github.com/ytsaurus/ytsaurus-ui/commit/6eafe35452282cfaff96123e808da59db02964f3))


#### Исправления

* **System/Nodes:** небольшое исправление для firefox/safari [YTFRONT-3297] ([c537054](https://github.com/ytsaurus/ytsaurus-ui/commit/c537054a752b423bac9204348fe86de25fdabcc4))

{% endcut %}


{% cut "**0.20.0**" %}

**Дата релиза:** 2023-09-15


**Страница релиза:** [0.20.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.20.0)


#### [0.20.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.19.1...ui-v0.20.0) (2023-09-15)


#### Новые возможности

* **System/Nodes,HttpProxies,RPCProxies:** новый дизайн для Details [YTFRONT-3297] ([5fd5795](https://github.com/ytsaurus/ytsaurus-ui/commit/5fd5795c72a9643ab1a4cb6e17f328d00111110a))

{% endcut %}


{% cut "**0.19.1**" %}

**Дата релиза:** 2023-09-14


**Страница релиза:** [0.19.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.19.1)


#### [0.19.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.19.0...ui-v0.19.1) (2023-09-14)


#### Исправления

* **System/Masters:** небольшие исправления для флага 'voting' [YTFRONT-3832] ([0b7df45](https://github.com/ytsaurus/ytsaurus-ui/commit/0b7df45f2feba18ded63b9020d89a578c52fbf09))

{% endcut %}


{% cut "**0.19.0**" %}

**Дата релиза:** 2023-09-13


**Страница релиза:** [0.19.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.19.0)


#### [0.19.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.18.0...ui-v0.19.0) (2023-09-13)


#### Новые возможности

* **System/Master:** отображение флага 'nonvoting' [YTFRONT-3832] ([77a5953](https://github.com/ytsaurus/ytsaurus-ui/commit/77a5953d07ce5f53fd56f1267c8888a3e6cd2e6a))


#### Исправления

* **AccountQuotaEditor:** улучшена обработка /[@allow](https://github.com/allow)_children_limit_overcommit [YTFRONT-3839] ([d53ba9a](https://github.com/ytsaurus/ytsaurus-ui/commit/d53ba9a533f0f299b0bf2a1636c88c39000248a3))
* не загружать '[@alerts](https://github.com/alerts)' из Components/Nodes ([38e4a90](https://github.com/ytsaurus/ytsaurus-ui/commit/38e4a90bb877033525a9d729c1c772e2b701cef9))
* **Navigation/Jobs:** использование прямых ссылок для команд: read_file, get_job_input, get_job_stderr, get_job_fail_context [YTFRONT-3833] ([7f549b2](https://github.com/ytsaurus/ytsaurus-ui/commit/7f549b2e591d7da1d5e0370b979d62cbab903881))
* **Navigation:** добавлена возможность удалить таблицу из текущего пути [YTFRONT-3837] ([ad016ac](https://github.com/ytsaurus/ytsaurus-ui/commit/ad016ac17e0c51f33b2d9bb2364d7d1b00fa6e07))
* **PoolEditorDialog:** удалены поля 'Burst RAM', 'Flow RAM' [YTFRONT-3838] ([29541f4](https://github.com/ytsaurus/ytsaurus-ui/commit/29541f4981183a70fa8e8777bd4b8d736ea89c7f))

{% endcut %}


{% cut "**0.18.0**" %}

**Дата релиза:** 2023-09-11


**Страница релиза:** [0.18.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.18.0)


#### [0.18.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.17.0...ui-v0.18.0) (2023-09-07)


#### Новые возможности

* **BundleEditor:** проверка прав пользователя на 'write' [YTFRONT-3785] ([35dc1d0](https://github.com/ytsaurus/ytsaurus-ui/commit/35dc1d099956876a826d1f9088baae85169260b6))
* **Tablet:** отключение StoresDialog для таблета с более чем 200 сторами [YTFRONT-3766] ([d1f64d1](https://github.com/ytsaurus/ytsaurus-ui/commit/d1f64d1c9dce8e283b3c4ccde35857ac3217efca))


#### Исправления

* **Navigation:** отображение заголовков и хлебных крошек для кириллических узлов [YTFRONT-3784] ([715c1ad](https://github.com/ytsaurus/ytsaurus-ui/commit/715c1ad985d18076436cbe24c16d9707c41e9ace))
* **QT:** рефакторинг опроса, исправление бесконечного состояния выполнения [YTFRONT-3852] ([2c7b283](https://github.com/ytsaurus/ytsaurus-ui/commit/2c7b283e0020e72ae81156917c83d5a051ce80a3))

{% endcut %}


{% cut "**0.17.0**" %}

**Дата релиза:** 2023-08-31


**Страница релиза:** [0.17.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.17.0)


#### [0.17.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.16.1...ui-v0.17.0) (2023-08-30)


#### Новые возможности

* **QT:** проверка и отображение метаданных результатов запроса с ошибками [YTFRONT-3797] ([d9a6d15](https://github.com/ytsaurus/ytsaurus-ui/commit/d9a6d152233513486aba0c9f9e1818862af7f344))
* **QT:** подсветка ошибок в monaco-editor [YTFRONT-3797] ([7c74a8d](https://github.com/ytsaurus/ytsaurus-ui/commit/7c74a8d0e39e5f6dd49db48c77e7a1a771948c31))


#### Исправления

* **QT:** зависимости обновления данных вкладки результатов ([baf8166](https://github.com/ytsaurus/ytsaurus-ui/commit/baf81669223f9274d3a98d7e2739db314f06d1a6))
* кодирование параметров url, отображение экранированных символов для путей [YTFRONT-3784] ([6ff0a63](https://github.com/ytsaurus/ytsaurus-ui/commit/6ff0a6324bbd86e9e660149a4385e224e2db4350))

{% endcut %}


{% cut "**0.16.1**" %}

**Дата релиза:** 2023-08-21


**Страница релиза:** [0.16.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.16.1)


#### [0.16.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.16.0...ui-v0.16.1) (2023-08-21)


#### Исправления

* обновление версий monaco ([c54b83a](https://github.com/ytsaurus/ytsaurus-ui/commit/c54b83ad02cfbf873a5e3080c03417a6e166572e))

{% endcut %}


{% cut "**0.16.0**" %}

**Дата релиза:** 2023-08-16


**Страница релиза:** [0.16.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.16.0)


#### [0.16.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.15.0...ui-v0.16.0) (2023-08-15)


#### Новые возможности

* **Job/Specification:** чтение спецификации из 'job_spec_ext' [YTFRONT-3802] ([108f2e9](https://github.com/ytsaurus/ytsaurus-ui/commit/108f2e9be5bc68b63296f8f2dac1831aa528597a))

{% endcut %}


{% cut "**0.15.0**" %}

**Дата релиза:** 2023-08-08


**Страница релиза:** [0.15.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.15.0)


#### [0.15.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.14.2...ui-v0.15.0) (2023-08-07)


#### Новые возможности

* **CreateDirectoryModal:** добавлен параметр 'recursive' [YTFRONT-3805] ([6ffd436](https://github.com/ytsaurus/ytsaurus-ui/commit/6ffd4361210b4687e15ca75eec289d83207c90aa))
* **Navigation/Tablets:** добавлен overlapping_store_count в гистограмму dynTable [YTFRONT-3380] ([4232709](https://github.com/ytsaurus/ytsaurus-ui/commit/423270902484a3c578cf7178ca04c6a8265f5f4d))
* **OperationJobsTable:** форматирование значения столбца типа джоба [YTFRONT-3746] ([dba10c8](https://github.com/ytsaurus/ytsaurus-ui/commit/dba10c871a7c265d6a402e6cdf1255e8feb36d02))


#### Исправления

* исправление опечатки [YTFRONT-3804] ([daae1a9](https://github.com/ytsaurus/ytsaurus-ui/commit/daae1a9ed0fb6933d6ef4748778fdc53e8bf09a5))
* **OperationDetails:** улучшенная компоновка для 'Environment' [YTFRONT-3781] ([c28804e](https://github.com/ytsaurus/ytsaurus-ui/commit/c28804e5e2c7005cfa3751865b01e3177d4e1245))
* замена //sys/proxies на //sys/http_proxies [YTFRONT-3799] ([595e8fe](https://github.com/ytsaurus/ytsaurus-ui/commit/595e8fe86e3afbf7243b05ec1c2eda2e18ef299c))
* сортировка анализа состояния для url-mapping [YTFRONT-3707] ([b3c4e66](https://github.com/ytsaurus/ytsaurus-ui/commit/b3c4e665c86064102be2f9a44faad519cff27b6d))
* **Table/Dynamic:** поиск по ключам не работает [YTFRONT-3808] ([98341af](https://github.com/ytsaurus/ytsaurus-ui/commit/98341af2f8358b094defdba6daeedb0013552226))

{% endcut %}

{% cut "**0.14.2**" %}

**Дата релиза:** 2023-07-28


**Страница релиза:** [0.14.2](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.14.2)


#### [0.14.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.14.1...ui-v0.14.2) (2023-07-28)


#### Исправления

* **deploy:** небольшое исправление для superviord ([1d49499](https://github.com/ytsaurus/ytsaurus-ui/commit/1d494998b880a06c91cb01b36e7ca9cf049fe4ff))

{% endcut %}


{% cut "**0.14.1**" %}

**Дата релиза:** 2023-07-27


**Страница релиза:** [0.14.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.14.1)


#### [0.14.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.14.0...ui-v0.14.1) (2023-07-27)


#### Исправления

* **Dockerfile:** небольшое исправление для сборки образа ([ee00ecd](https://github.com/ytsaurus/ytsaurus-ui/commit/ee00ecdb9eebf4878737aae5d94a7c792b27dea7))

{% endcut %}


{% cut "**0.14.0**" %}

**Дата релиза:** 2023-07-27


**Страница релиза:** [0.14.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.14.0)


#### [0.14.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.13.1...ui-v0.14.0) (2023-07-27)


#### Новые возможности

* **dev:** использовать nodejs 18 ([9af6662](https://github.com/ytsaurus/ytsaurus-ui/commit/9af666268fd7e0c2e56317503a06edc86d792172))

{% endcut %}


{% cut "**0.13.1**" %}

**Дата релиза:** 2023-07-27


**Страница релиза:** [0.13.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.13.1)


#### [0.13.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.13.0...ui-v0.13.1) (2023-07-27)


#### Исправления

* **Components/Node/MemoryUsage:** использовать виртуализированную таблицу [YTFRONT-3796] ([267eeef](https://github.com/ytsaurus/ytsaurus-ui/commit/267eeeffbfa3a559d1cf74296492dbfb3644289d))
* **Components/Node:** вернуть пропавшие locations [YTFRONT-3796] ([d3686b3](https://github.com/ytsaurus/ytsaurus-ui/commit/d3686b31c76cd8ac365f54337ebe41434c420ea3))

{% endcut %}


{% cut "**0.13.0**" %}

**Дата релиза:** 2023-07-26


**Страница релиза:** [0.13.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.13.0)


#### [0.13.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.12.0...ui-v0.13.0) (2023-07-26)


#### Новые возможности

* **QT:** добавить переопределение настроек QT-запросов через UI [YTFRONT-3790] ([95479bb](https://github.com/ytsaurus/ytsaurus-ui/commit/95479bbabdd260e148879a3be2623ec9f008979f))

{% endcut %}


{% cut "**0.12.0**" %}

**Дата релиза:** 2023-07-21


**Страница релиза:** [0.12.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.12.0)


#### [0.12.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.11.3...ui-v0.12.0) (2023-07-21)


#### Новые возможности

* **Components/Nodes:** использовать attributes.paths и attributes.keys для списка узлов [YTFRONT-3378] ([a60ec5e](https://github.com/ytsaurus/ytsaurus-ui/commit/a60ec5e191a14221400b610c94aa15cf4fe670da))
* включить редактирование имени запроса [YTFRONT-3649] ([f375ea4](https://github.com/ytsaurus/ytsaurus-ui/commit/f375ea468543299b7a18d2b92417b9966c8e664c))


#### Исправления

* **PoolEditorDialog:** не отправлять запрос на изменение поля weight, если значение не изменилось [YTFRONT-3748] ([1d25e5b](https://github.com/ytsaurus/ytsaurus-ui/commit/1d25e5bd0fabb720a5121fdc11ade5692e2fccd2))
* QT форматирование десятичных результатов [YTFRONT-3782] ([58d6f66](https://github.com/ytsaurus/ytsaurus-ui/commit/58d6f66ac684774e1a45d656b8dfda9d9a9e5af8))

{% endcut %}


{% cut "**0.11.3**" %}

**Дата релиза:** 2023-07-14


**Страница релиза:** [0.11.3](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.11.3)


#### [0.11.3](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.11.2...ui-v0.11.3) (2023-07-14)


#### Исправления

* README ([cd2d3db](https://github.com/ytsaurus/ytsaurus-ui/commit/cd2d3dbe2e75341274a9c0a71f36db1ee34878f2))

{% endcut %}


{% cut "**0.11.2**" %}

**Дата релиза:** 2023-07-14


**Страница релиза:** [0.11.2](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.11.2)


#### [0.11.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.11.1...ui-v0.11.2) (2023-07-14)


#### Исправления

* ui lock ([03940a6](https://github.com/ytsaurus/ytsaurus-ui/commit/03940a6c2240cabc78f5592b99e7540d32c1531e))

{% endcut %}


{% cut "**0.11.1**" %}

**Дата релиза:** 2023-07-14


**Страница релиза:** [0.11.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.11.1)


#### Зависимости

* Обновлены следующие зависимости рабочей области
  * dependencies
    * @ytsaurus/javascript-wrapper обновлён с ^0.2.1 до ^0.3.0

{% endcut %}


{% cut "**0.11.0**" %}

**Дата релиза:** 2023-07-06


**Страница релиза:** [0.11.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.11.0)


#### [0.11.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.10.0...ui-v0.11.0) (2023-07-06)


#### Новые возможности

* Добавлена опция конфигурации UISettings.accountsMonitoring [YTFRONT-3698] ([71a8902](https://github.com/ytsaurus/ytsaurus-ui/commit/71a8902344892881bad6cd8d43f56a04efad3ebc))
* Добавлена опция конфигурации UISettings.bundlesMonitoring [YTFRONT-3698] ([ff7f90a](https://github.com/ytsaurus/ytsaurus-ui/commit/ff7f90ae7eb4404332ed627e5f34e8eeb3a109df))
* Добавлена опция конфигурации UISettings.operationsMonitoring [YTFRONT-3698] ([893f716](https://github.com/ytsaurus/ytsaurus-ui/commit/893f71618dd6929fb2eef8d3bb5d87e46f67e950))
* Добавлена опция конфигурации UISettings.schedulingMonitoring [YTFRONT-3698] ([eb1959b](https://github.com/ytsaurus/ytsaurus-ui/commit/eb1959bf5e9bb75c967c1c2cbff0ca84f70a4f59))
* **Components/Nodes:** Добавить режим просмотра 'Chaos slots' [YTFRONT-3333] ([9aa0461](https://github.com/ytsaurus/ytsaurus-ui/commit/9aa046177d4a25f1dd7d6ea10c1f58a628bc4e51))


#### Исправления

* улучшить поведение переключения движка QT / переработать кнопку Open Query Tracker [YTFRONT-3713] ([0453125](https://github.com/ytsaurus/ytsaurus-ui/commit/045312528754dde84bc4fcc7f9156248c7db2348))

{% endcut %}


{% cut "**0.10.0**" %}

**Дата релиза:** 2023-07-04


**Страница релиза:** [0.10.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.10.0)


#### [0.10.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.9.1...ui-v0.10.0) (2023-07-04)


#### Новые возможности

* **BundleControllerEditor:** добавить поле 'Reserved' [YTFRONT-3673] ([4e497e1](https://github.com/ytsaurus/ytsaurus-ui/commit/4e497e1b7f7f508d771ee54027dc2c627f706edf))

{% endcut %}


{% cut "**0.9.1**" %}

**Дата релиза:** 2023-06-26


**Страница релиза:** [0.9.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.9.1)


#### [0.9.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.9.0...ui-v0.9.1) (2023-06-26)


#### Исправления

* Небольшое исправление Changelog ([21ad9cf](https://github.com/ytsaurus/ytsaurus-ui/commit/21ad9cf5a4e4c9c8499eb54d5b9d2d1c8492e863))

{% endcut %}

{% cut "**0.9.0**" %}

**Дата релиза:** 2023-06-20


**Страница релиза:** [0.9.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.9.0)


#### [0.9.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.8.0...ui-v0.9.0) (2023-06-20)


#### Новые возможности

* включено действие удаления для OS [YTFRONT-3721] ([8f6d7ed](https://github.com/ytsaurus/ytsaurus-ui/commit/8f6d7ede82bbd41fdfd0fc9c562777c5409c952b))
* включена форма ManageAcl [YTFRONT-3721] ([6a49956](https://github.com/ytsaurus/ytsaurus-ui/commit/6a49956e0c97a4e972335bd068e43ea342cf5793))
* включена переопределение PERMISSIONS_SETTINGS с помощью UIFactory [YTFRONT-3721] ([99ab661](https://github.com/ytsaurus/ytsaurus-ui/commit/99ab6610385d34816e700a4838fbb24a624b077f))
* включена форма Request Permission для версии os [YTFRONT-3721] ([6634f50](https://github.com/ytsaurus/ytsaurus-ui/commit/6634f50c51b4f78ebbd54fbcc2fe0bdd5f8875c9))

{% endcut %}


{% cut "**0.8.0**" %}

**Дата релиза:** 2023-06-19


**Страница релиза:** [0.8.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.8.0)


#### [0.8.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.7.0...ui-v0.8.0) (2023-06-19)


#### Новые возможности

* модальное окно remote copy -> предлагать transfer_* пул, если он существует [YTFRONT-3511] ([19674ea](https://github.com/ytsaurus/ytsaurus-ui/commit/19674eade06d19adf6ab141a5662b2f922e305f1))


#### Исправления

* (PoolEditorDialog) добавлена проверка числа в поле Weight [YTFRONT-3748] ([84b4fde](https://github.com/ytsaurus/ytsaurus-ui/commit/84b4fde9605b1cb693478d1b0706c050da5c3ecb))

{% endcut %}


{% cut "**0.7.0**" %}

**Дата релиза:** 2023-06-16


**Страница релиза:** [0.7.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.7.0)


#### [0.7.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.6.4...ui-v0.7.0) (2023-06-16)


#### Новые возможности

* **Navigation/AttributesEditor:** разрешено редактирование '/[@expiration](https://github.com/expiration)_time' и '/[@expiration](https://github.com/expiration)_timout' [YTFRONT-3665] ([9983381](https://github.com/ytsaurus/ytsaurus-ui/commit/9983381cb7a4eaa09e5d82b5e8ed6232e49cd0b1))
* **System/Nodes:** добавлен фильтр 'Node type' [YTFRONT-3163] ([9e7a956](https://github.com/ytsaurus/ytsaurus-ui/commit/9e7a9564dcded3044f866d4ab55bb118a3a50a40))


#### Исправления

* стили таблиц на странице ACL [YTFRONT-3758] ([0c97d70](https://github.com/ytsaurus/ytsaurus-ui/commit/0c97d70b5504258e1a22eccc7ca4e4ac9f3b55d8))

{% endcut %}


{% cut "**0.6.4**" %}

**Дата релиза:** 2023-06-02


**Страница релиза:** [0.6.4](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.6.4)


#### [0.6.4](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.6.3...ui-v0.6.4) (2023-06-02)


#### Исправления

* избавление от ненужного console.log ([6d06778](https://github.com/ytsaurus/ytsaurus-ui/commit/6d06778b6f9eea1ab834807ee4e68061d362b5a9))

{% endcut %}


{% cut "**0.6.3**" %}

**Дата релиза:** 2023-06-02


**Страница релиза:** [0.6.3](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.6.3)


#### [0.6.3](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.6.2...ui-v0.6.3) (2023-06-02)


#### Исправления

* добавлен @gravity-ui/dialog-fields в peerDeps ([7a23bce](https://github.com/ytsaurus/ytsaurus-ui/commit/7a23bce132fbf479728b9ae85f1e88f3efc174e4))
* увеличен лимит jobsCount для вкладки JobsMonitor [YTFRONT-3752] ([9e61525](https://github.com/ytsaurus/ytsaurus-ui/commit/9e61525d601ccc42ecb94ed326dbee6e03f71728))

{% endcut %}


{% cut "**0.6.2**" %}

**Дата релиза:** 2023-06-01


**Страница релиза:** [0.6.2](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.6.2)


#### [0.6.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.6.1...ui-v0.6.2) (2023-06-01)


#### Исправления

* **Navigation:** Не загружать pool tree без необходимости [YTFRONT-3747] ([61192df](https://github.com/ytsaurus/ytsaurus-ui/commit/61192dfa7d2c38a0ace6a2bc0c80ae178a4ebedc))

{% endcut %}


{% cut "**0.6.1**" %}

**Дата релиза:** 2023-06-01


**Страница релиза:** [0.6.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.6.1)


#### [0.6.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.6.0...ui-v0.6.1) (2023-06-01)


#### Исправления

* **JobsMonitor:** исправлена опечатка в предупреждении ([6945d1e](https://github.com/ytsaurus/ytsaurus-ui/commit/6945d1e5f052281ff08e92a8b924ea224be1a2eb))

{% endcut %}


{% cut "**0.6.0**" %}

**Дата релиза:** 2023-05-25


**Страница релиза:** [0.6.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.6.0)


#### [0.6.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.5.1...ui-v0.6.0) (2023-05-25)


#### Новые возможности

* Добавлены разрешения 'register_queue_consumer', 'register_queue_consumer_vital' [YTFRONT-3327] ([d6bd889](https://github.com/ytsaurus/ytsaurus-ui/commit/d6bd8890c2e62c96448043ac44ffa70a83178142))
* **Navigation/Consumer:** модель изменена с 'many-to-one' на 'many-to-many' [YTFRONT-3327] ([2014422](https://github.com/ytsaurus/ytsaurus-ui/commit/2014422b5797000fdda66feb51ca441874b03e38))


#### Исправления

* **Account/General:** небольшое исправление стилей [YTFRONT-3741] ([7eea79a](https://github.com/ytsaurus/ytsaurus-ui/commit/7eea79adc103ecddd773144acfbe6e74e3f58863))
* **Scheduling/PoolSuggest:** улучшен порядок элементов [YTFRONT-3739] ([150db4f](https://github.com/ytsaurus/ytsaurus-ui/commit/150db4fc1c33cb448a1f9ba3faa6e88c1b67c33c))

{% endcut %}


{% cut "**0.5.1**" %}

**Дата релиза:** 2023-05-19


**Страница релиза:** [0.5.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.5.1)


#### [0.5.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.5.0...ui-v0.5.1) (2023-05-19)


#### Исправления

* (OperationsArchiveFilter) специфичность стилей ввода [3728] ([b587906](https://github.com/ytsaurus/ytsaurus-ui/commit/b587906c21552ba584fdff35a6b48a2582ddde70))
* (OperationsArchiveFilter) сброс времени при изменении даты и начальное значение пользовательской даты при переключении режимов [3728] ([fdfd045](https://github.com/ytsaurus/ytsaurus-ui/commit/fdfd0456d8fcf49bc24316f533d511c1b8275147))
* **TabletCellBundle:** улучшен макет для MetaTable [YTFRONT-3716] ([6904a4c](https://github.com/ytsaurus/ytsaurus-ui/commit/6904a4cd7574c1061cedcba9ba4454cf04791aec))

{% endcut %}


{% cut "**0.5.0**" %}

**Дата релиза:** 2023-05-12


**Страница релиза:** [0.5.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.5.0)


#### [0.5.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.4.2...ui-v0.5.0) (2023-05-10)


#### Новые возможности

* ACL добавлены собственные фильтры для разрешений объекта [YTFRONT-3720] ([d9dfed1](https://github.com/ytsaurus/ytsaurus-ui/commit/d9dfed146bd72c248003350dc0f1a3c228801dfc))


#### Исправления

* получение пути из атрибутов для компонента Schema [YTFRONT-3722] ([97bca2c](https://github.com/ytsaurus/ytsaurus-ui/commit/97bca2cea18c582697a7375396c7b17c89499e67))

{% endcut %}


{% cut "**0.4.2**" %}

**Дата релиза:** 2023-05-03


**Страница релиза:** [0.4.2](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.4.2)


#### [0.4.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.4.1...ui-v0.4.2) (2023-05-03)


#### Исправления

* **JobDetails/StatisticsIO:** итоговая строка должна отображаться корректно [YTFRONT-3723] ([980a4fc](https://github.com/ytsaurus/ytsaurus-ui/commit/980a4fc19564e36aee4716dd35259af986b78ff0))
* **Navigation:TableMeta:** скрытие атрибутов динамических таблиц для статических таблиц [3725] ([4fa79f7](https://github.com/ytsaurus/ytsaurus-ui/commit/4fa79f7b9e3182c542510060d682f2421fe9ca85))
* **Navigation/Table:** исправлена ошибка для конкретного значения localStorage.SAVED_COLUMN_SETS [YTFRONT-3710] ([529d8bf](https://github.com/ytsaurus/ytsaurus-ui/commit/529d8bf9577171335e42fcd72378c358c7a38a62))
* **Scheduling/Overview:** добавлено больше уровней для стилей [YTFRONT-3724] ([d3dca2b](https://github.com/ytsaurus/ytsaurus-ui/commit/d3dca2b6323ce24dbe18b6cf978cdbc1843ddf8a))
* **TabletCellBundle:** улучшен макет для meta-table [YTFRONT-3716] ([f1073b8](https://github.com/ytsaurus/ytsaurus-ui/commit/f1073b82480a13d64e40aae460da73290de09e36))

{% endcut %}


{% cut "**0.4.1**" %}

**Дата релиза:** 2023-04-28


**Страница релиза:** [0.4.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.4.1)


#### [0.4.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.4.0...ui-v0.4.1) (2023-04-28)


#### Исправления

* **Navigation/MapNode:** имена не должны обрезаться многоточием [YTFRONT-3711] ([8a48398](https://github.com/ytsaurus/ytsaurus-ui/commit/8a48398007ca289881668032f8b17dabda2dafde))

{% endcut %}

{% cut "**0.4.0**" %}

**Дата релиза:** 2023-04-27


**Страница релиза:** [0.4.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.4.0)


#### [0.4.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.3.1...ui-v0.4.0) (2023-04-27)


#### Новые возможности

* добавлен флаг 'Stale' в метаданные джоба [YTFRONT-3712] ([6ed4597](https://github.com/ytsaurus/ytsaurus-ui/commit/6ed45979195ca638b99cd895c3aa0a80fe07b561))
* добавлена подсказка для наследуемых popover ([d4c76ab](https://github.com/ytsaurus/ytsaurus-ui/commit/d4c76ab893db9a4bacf77b1eb245408644194a37))
* исправлена фильтрация субъектов для групп ([93d8500](https://github.com/ytsaurus/ytsaurus-ui/commit/93d85003639d426a2835949f482fec088cf19f0b))
* удалена подсветка ([dc74075](https://github.com/ytsaurus/ytsaurus-ui/commit/dc74075ab1ab795c3ef935e15ed01f267e432459))
* разделены и отфильтрованы objectPermissions ([93d8500](https://github.com/ytsaurus/ytsaurus-ui/commit/93d85003639d426a2835949f482fec088cf19f0b))
* **Таблицы:** добавлен флаг 'Combine chunks' в модальное окно Merge/Erase ([aeec0ca](https://github.com/ytsaurus/ytsaurus-ui/commit/aeec0cabd87d4ec896f54972240a7708cfa9f531))


#### Исправления ошибок

* размеры колонок сетки ACL ([e0bd03b](https://github.com/ytsaurus/ytsaurus-ui/commit/e0bd03bcab24913f6fac82c3438bdb6db39e893f))
* многоточие в колонке субъекта ACL ([7a7fd4e](https://github.com/ytsaurus/ytsaurus-ui/commit/7a7fd4e2e91e0911100dc2c5c2070797c60783bc))
* **BundleController:** корректная обработка случая, когда контроллер бандла недоступен [YTFRONT-3636] ([940a441](https://github.com/ytsaurus/ytsaurus-ui/commit/940a44155aac3624567ba0c709b962ee9957c717))
* **Навигация:** добавлен флаг 'disabled' для кнопки 'More actions' [YTFRONT-3705] ([fa4226a](https://github.com/ytsaurus/ytsaurus-ui/commit/fa4226a082521c7ef693178fbf53a599e31a49b0))
* **Операции/Статистика:** исправлено странное поведение кнопки 'Collapse all' [YTFRONT-3719] ([e4d55aa](https://github.com/ytsaurus/ytsaurus-ui/commit/e4d55aacfb60c8d0b319c8098c1485c3e46a7b0a))

{% endcut %}


{% cut "**0.3.1**" %}

**Дата релиза:** 2023-04-19


**Страница релиза:** [0.3.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.3.1)


#### [0.3.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.3.0...ui-v0.3.1) (2023-04-19)


#### Исправления ошибок

* **Таблицы/Схема:** небольшая правка ширины колонок [YTFRONT-3667] ([0abe89d](https://github.com/ytsaurus/ytsaurus-ui/commit/0abe89d7d570c662ae6646622b904f98f9297e7f))

{% endcut %}


{% cut "**0.3.0**" %}

**Дата релиза:** 2023-04-18


**Страница релиза:** [0.3.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.3.0)


#### [0.3.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.2.3...ui-v0.3.0) (2023-04-18)


#### Новые возможности

* **Операции/Статистика:** добавлен фильтр по пулу (statistics-v2) [YTFRONT-3598] ([8b03968](https://github.com/ytsaurus/ytsaurus-ui/commit/8b039687f2e9025baa9bdaec861866ac2c3443ef))

{% endcut %}


{% cut "**0.2.3**" %}

**Дата релиза:** 2023-04-17


**Страница релиза:** [0.2.3](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.2.3)


#### [0.2.3](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.2.2...ui-v0.2.3) (2023-04-17)


#### Исправления ошибок

* возвращена телеметрия ([b24d977](https://github.com/ytsaurus/ytsaurus-ui/commit/b24d977b78273105f8e3f49b1ad3d0946160320b))

{% endcut %}


{% cut "**0.2.2**" %}

**Дата релиза:** 2023-04-14


**Страница релиза:** [0.2.2](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.2.2)


#### [0.2.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.2.1...ui-v0.2.2) (2023-04-14)


#### Исправления ошибок

* добавлено  для таблетов с 0 ячеек [YTFRONT-3696] ([63acb21](https://github.com/ytsaurus/ytsaurus-ui/commit/63acb214a5f25ad6458daddc8db9d5fa93eed91f))
* переработан выбор шрифта [YTFRONT-3691] ([717fa89](https://github.com/ytsaurus/ytsaurus-ui/commit/717fa89ad5aceca74e6587d444b672d50ba2ed07))

{% endcut %}


{% cut "**0.2.1**" %}

**Дата релиза:** 2023-04-07


**Страница релиза:** [0.2.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.2.1)


#### [0.2.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.2.0...ui-v0.2.1) (2023-04-07)


#### Исправления ошибок

* удалены ненужные файлы ([f4b51c2](https://github.com/ytsaurus/ytsaurus-ui/commit/f4b51c2a5a79705913adf3377e1590ad5368d1fb))

{% endcut %}


{% cut "**0.2.0**" %}

**Дата релиза:** 2023-04-06


**Страница релиза:** [0.2.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.2.0)


#### [0.2.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.1.0...ui-v0.2.0) (2023-04-06)


#### Новые возможности

* добавлен список обучающих материалов ([8241d3a](https://github.com/ytsaurus/ytsaurus-ui/commit/8241d3a933877113b4a1b3a452e84a46417bbebe))


#### Исправления ошибок

* исправлены опечатки и отсутствующая ссылка abc в форме редактирования бандла [YTFRONT-3676] ([aa617d3](https://github.com/ytsaurus/ytsaurus-ui/commit/aa617d3fad7ad1bfd14e0217d44599dd895bc24b))

{% endcut %}


{% cut "**0.1.0**" %}

**Дата релиза:** 2023-04-05


**Страница релиза:** [0.1.0](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.1.0)


#### [0.1.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.0.4...ui-v0.1.0) (2023-04-05)


#### Новые возможности

* добавлена кнопка создания запроса из таблицы ([7c94ee5](https://github.com/ytsaurus/ytsaurus-ui/commit/7c94ee5286d96c0ffb617d16e41020b4e92e08d7))
* добавлен QT proxy ([c624e5d](https://github.com/ytsaurus/ytsaurus-ui/commit/c624e5d847d96dbd9045bb38019811c367ea666c))


#### Исправления ошибок

* добавлена настройка queryTrackerCluster для ya-env. Сброс настройки экрана после закрытия QTWidget. ([215a72b](https://github.com/ytsaurus/ytsaurus-ui/commit/215a72b6c5ca023c97ded4208d4d50c8f1d8642a))
* EditableAsText с элементами управления (используется в QT TopRowElement) ([574dbae](https://github.com/ytsaurus/ytsaurus-ui/commit/574dbae73de87f726152ffba809a03629c4a7d3a))
* исправлена кодировка в тексте и результатах запросов ([d3e2780](https://github.com/ytsaurus/ytsaurus-ui/commit/d3e2780852a82b90ad0af2aace6c7a697220ad5c))
* исправлена некорректная компоновка редактора бандлов ([b389ea8](https://github.com/ytsaurus/ytsaurus-ui/commit/b389ea85ce0d5e46318c8e1f00081a56fec02851))
* исправлен алиас CHYT по умолчанию ([9231085](https://github.com/ytsaurus/ytsaurus-ui/commit/9231085899ef64dd73add88989771e509fa43346))
* стилизация черновиков запросов. Исправлены запросы без finish_time. Чтение схемы из get_result_table ([fdd121a](https://github.com/ytsaurus/ytsaurus-ui/commit/fdd121a589090f045bc805b1106f02444539c609))

{% endcut %}


{% cut "**0.0.4**" %}

**Дата релиза:** 2023-03-24


**Страница релиза:** [0.0.4](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.0.4)


#### [0.0.4](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.0.3...ui-v0.0.4) (2023-03-24)


#### Исправления ошибок

* **ui:** добавлено поле 'files' в package.json ([cb51d75](https://github.com/ytsaurus/ytsaurus-ui/commit/cb51d756af502f25fab413ff26c20b5e2ce90abf))

{% endcut %}


{% cut "**0.0.3**" %}

**Дата релиза:** 2023-03-24


**Страница релиза:** [0.0.3](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.0.3)


#### [0.0.3](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.0.2...ui-v0.0.3) (2023-03-24)


#### Исправления ошибок

* исправлен docker-образ ytsaurus/ui для локального режима ([5033eb9](https://github.com/ytsaurus/ytsaurus-ui/commit/5033eb9c1c5ab4aaf8029c678847231eb7a6bd18))

{% endcut %}


{% cut "**0.0.2**" %}

**Дата релиза:** 2023-03-24


**Страница релиза:** [0.0.2](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v0.0.2)


#### [0.0.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.0.1...ui-v0.0.2) (2023-03-24)


#### Исправления ошибок

* добавлен отсутствующий конфиг ([e391c59](https://github.com/ytsaurus/ytsaurus-ui/commit/e391c59fdf5c0ee72e8899daae0dfd3d4e34f4e7))

{% endcut %}
