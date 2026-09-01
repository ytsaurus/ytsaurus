## YTsaurus server


Все основные компоненты поставляются в виде docker-образов.




**Релизы:**

{% cut "**25.3.1**" %}

**Дата релиза:** 2026-03-25


**Страница релиза:** [25.3.1](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/ytsaurus/25.3.1)


**Docker-образ:** [ghcr.io/ytsaurus/ytsaurus:stable-25.3.1](https://github.com/orgs/ytsaurus/packages/container/ytsaurus/773248759?tag=stable-25.3.1)


Чтобы установить YTsaurus Server 25.3.1, [обновите](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release%2F0.27.0) k8s-operator до версии 0.27.0.

#### Queue Agent
##### Исправления:
- Исправлено создание вторичного индекса в скрипте init_queue_agent_state, [9840c02](https://github.com/ytsaurus/ytsaurus/commit/9840c02ebe08e450b88ba00efa23c5af0a8b1eda)

{% endcut %}


{% cut "**25.3.0**" %}

**Дата релиза:** 2026-03-13


**Страница релиза:** [25.3.0](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/ytsaurus/25.3.0)


**Docker-образ:** [ghcr.io/ytsaurus/ytsaurus:stable-25.3.0](https://github.com/orgs/ytsaurus/packages/container/ytsaurus/738197686?tag=stable-25.3.0)


#### Обзор

В этом документе приведены основные изменения, улучшения, новые возможности и исправления ошибок, появившиеся в **YTsaurus 25.3.0**. Благодарим всех контрибьюторов и мейнтейнеров за их работу.

YTsaurus Server 25.3.0 поддерживается начиная с версии [0.27.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release%2F0.27.0) Kubernetes-оператора.

#### Значительные изменения

- Добавлено обнаружение GPU через gpu-agent.

- Автоматическая подстройка размера джобов для упорядоченных и сортированных типов и стадий операций: стадии sorted_merge, ordered_merge, sorted_reduce, ordered_map, ordered_map и sorted_reduce операции Map-Reduce.

- Поддержка ограничений `compressed_data_size` для MR-операций.

- Протокол распределённой записи для файлов.

- Поддержка row cache для следующих за таблет-селлом пиров (в среде с несколькими пирами в таблет-селле).

- [экспериментально] Поддержка сложных типов данных в формате Arrow.

- [экспериментально] Построчный ACL для статических таблиц
  * поддерживается CHYT >= 2.18
  * поддержка SPYT ожидается в ближайших релизах
  * поддержка YQL ожидается в ближайших релизах YQL/QT.

- [экспериментально] Массовая вставка (bulk insert) в пользовательской транзакции. Это необходимое условие для функции REPLACE INTO //dynamic/table в YQL, доступной с недавнего релиза.

#### Критические изменения
- Загрузка слепка, содержащего list-узлы, приведёт к падению мастер-сервера. Чтобы этого избежать, отключите опцию `alert_on_list_node_load` в динамическом конфиге. List-узлы будут полностью удалены в следующей мажорной версии.

- Учёт ресурсов таблетов (количество таблетов и статическая память таблетов) теперь
по умолчанию выполняется в разрезе бандлов, а не аккаунтов. Это применяется только к новым кластерам.

- Используйте `@banned` вместо `@banned_queue_agent_instance` для бана инстансов queue agent.


---
#### Полный список изменений

#### Планировщик и GPU
##### Новые возможности и изменения:
- Добавлены детальные метрики для неиспользуемых ресурсов узлов во время планирования, [2ea0a01](https://github.com/ytsaurus/ytsaurus/commit/2ea0a01d66dd22a46e5f3f4aa192b0773d9e08fb).
- Реализован улучшенный алгоритм распределения fair share, [3a80d43](https://github.com/ytsaurus/ytsaurus/commit/3a80d431aefc909c63bf4ab8607e20fbceab964a)
  - Использование step-функций для gang-операций
  - Поддержка умного суммирования step-функций
  - Поддержка векторного предложения для распределения fair share.
- Добавлена валидация лимитов ресурсов exe-узлов. Если узел не соответствует настроенному лимиту, планировщик поднимает алерт, [b6aae47](https://github.com/ytsaurus/ytsaurus/commit/b6aae47a081d483197910515fea031482b31274e).
- Добавлены `type`, `user` и `title` в orchid операции (т.е. `//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/default/fair_share_info/operations/<op_id>/type`), [951c759](https://github.com/ytsaurus/ytsaurus/commit/951c759a45af2bb06510cf518b9200499bf49924).
- Добавлен метод обнаружения GPU через gpu-agent, [2e2d586](https://github.com/ytsaurus/ytsaurus/commit/2e2d5867ada1ea60e830aeeb9608e6cd48b84280).

##### Исправления и оптимизации:
- Разрешена точка по умолчанию в regexp валидации эфемерных пулов, [727d48f](https://github.com/ytsaurus/ytsaurus/commit/727d48fd01edec21432475414dedd4e7c9966f31).
- Изменён сенсор `effective_strong_guarantee_resources`, чтобы он отражал уменьшенные гарантии в случае нехватки ресурсов, [e441b19](https://github.com/ytsaurus/ytsaurus/commit/e441b195d78d1983036b576ba1ddc7c79b7c4ff1).
- Журнал событий планировщика теперь включает объём накопленного использования fair-ресурсов и накопленный дефицит использования, а также ранее собираемое использование ресурсов, [d1cdf71](https://github.com/ytsaurus/ytsaurus/commit/d1cdf718ea240303d6d4feed38f9479e96da8bcb).
- Используется только `transient_state` как `node_state` в API джобов, [1757964](https://github.com/ytsaurus/ytsaurus/commit/1757964291582fee0f9b6e36b868942711684de7).
- Исправлена утечка памяти в профилировании fair share дерева, [fd0af6b](https://github.com/ytsaurus/ytsaurus/commit/fd0af6bf1845f0862d1b5e7ea27f4c20b4a4f728).


#### Queue Agent
##### Новые возможности и изменения:
- Используйте `@banned` вместо `@banned_queue_agent_instance` для бана инстансов queue agent, [8d52909](https://github.com/ytsaurus/ytsaurus/commit/8d52909765ab582a634eb3b8c7c47270629c8337).
- Добавлена опция `enable_verbose_logging` в динамический конфиг, которая включает подробное журналирование для конкретных объектов из `verbose_logging_objects`, [7e9ba8e](https://github.com/ytsaurus/ytsaurus/commit/7e9ba8ee11c7d6f2030dc6f7abb4b721a954f4b2).
- Поддержка CRON-расписаний для экспорта очередей, [6535d06](https://github.com/ytsaurus/ytsaurus/commit/6535d065dc0f707e80525189b555298b0507655f).
- Поддержка MutationId в CreateQueueProducerSession, [0c14e76](https://github.com/ytsaurus/ytsaurus/commit/0c14e7646327d651dd0e4438792bfe5e65459e64).

##### Исправления и оптимизации:
- Исправлены падения queue agent, если объект-реплика имеет невалидный путь реплики, например содержит незакрытые `{` или `}`, [42454fa](https://github.com/ytsaurus/ytsaurus/commit/42454fa9a0435bdb3a5a8e753a4cedf8a6824729).
- Исправлен init_queue_agent_state в случае, если директория уже существует (происходит в k8s-операторе), [9e1ac47](https://github.com/ytsaurus/ytsaurus/commit/9e1ac47ac8ca731a8a24db338ee30543634a0e93).
- Исправлена потенциальная потеря данных при множественном экспорте из очереди
из-за некорректного объединения прогрессов экспорта очередей, [bd5d92a](https://github.com/ytsaurus/ytsaurus/commit/bd5d92a8bcb0c45743bc4d0fec0c6da8dee4ebdb).
- Исправлена передача мутирующих опций в `CreateQueueProducerSession` в RPC-прокси, [8ab3c7d](https://github.com/ytsaurus/ytsaurus/commit/8ab3c7da89fa380a0187b5c04dd8857b4d88da0a).
- Устанавливается атрибут `treat_as_queue_producer=%true` при создании queue_producer, [4537011](https://github.com/ytsaurus/ytsaurus/commit/4537011fd63851d5c09441195ac43f675b63b30d).

#### Прокси
##### Новые возможности и изменения:
- Добавлен `enable_complex_types` для поддержки сложных типов данных в формате Arrow, [b1c3280](https://github.com/ytsaurus/ytsaurus/commit/b1c328040b52b60e5f0ff30468d95daeedd0c66e).
- Введён протокол распределённой записи для файлов, [663f608](https://github.com/ytsaurus/ytsaurus/commit/663f608e050049fb7b2a781f4c52356226b556e9).
- `enable_allocation_tags` теперь включён по умолчанию в HTTP- и RPC-прокси, [58d5b3c](https://github.com/ytsaurus/ytsaurus/commit/58d5b3c5bcfe18acf18e4f365336fa3ddd00fa1d).
- Поддержка лимита конкурентности на пользователя в HTTP-прокси, [ff0eb5a](https://github.com/ytsaurus/ytsaurus/commit/ff0eb5aca87f39362e354c02bb84b2016fe0d08b).
- Добавлена специальная skiff-колонка `$remaining_row_bytes`, [ae0c801](https://github.com/ytsaurus/ytsaurus/commit/ae0c8017ae29a5ce30c72b6151a704845aac7d5e).
- Используются символьные имена ошибок для метрики `http_proxy/api_error_count`, [2009f63](https://github.com/ytsaurus/ytsaurus/commit/2009f639893e49fd3e7b6044ef9f4be8c3562702).
- Поддержка динамической перенастройки подсистемы сигнатур в HTTP-прокси, [421cb6b](https://github.com/ytsaurus/ytsaurus/commit/421cb6b424693df3f1b4898b77f7c6683b64edad).
- Поддержка динамической перенастройки подсистемы сигнатур в RPC-прокси, [ea8492c](https://github.com/ytsaurus/ytsaurus/commit/ea8492cde311d754a66695bf289a91cfad2f2bf0).
##### Исправления и оптимизации:
- `max_children_per_attach_request`, используемый в распределённой записи, перенесён из Client API в динамический конфиг соединения, [c3cec0b](https://github.com/ytsaurus/ytsaurus/commit/c3cec0bbd73e3db28f693074cb4c661f1e94c29e).
- Исправлен тип datetime в формате arrow, [48075f9](https://github.com/ytsaurus/ytsaurus/commit/48075f9c37bfcdc66b765b5ca95b1b01796d553a).
- Поддержка любого количества уровней словарно-кодированных слоёв в arrow-парсере, [2f4251f](https://github.com/ytsaurus/ytsaurus/commit/2f4251f7614870de9a7fcfc5ddb46ab09101c8ff).
- Исправлена возможная взаимоблокировка в кэше метаданных чанков, [7ed5831](https://github.com/ytsaurus/ytsaurus/commit/7ed5831560019ccf648f39a66be693aa2634ebf5).
- Сокращено количество метрик пулов, [0fe261a](https://github.com/ytsaurus/ytsaurus/commit/0fe261a8b907e09e2741f83b2061d66e639cf160).
- Обеспечена работа HTTP-прокси, когда мастер находится в режиме только для чтения, [56f4970](https://github.com/ytsaurus/ytsaurus/commit/56f497064430a2e15a06849b9cbc026fc6539440).

#### Динамические таблицы
##### Новые возможности и изменения:
- Пользователи могут указать, нужно ли агрегировать статистику их подзапросов. Поддерживаются Sum, max и argmax (имя узла), [b7e70fd](https://github.com/ytsaurus/ytsaurus/commit/b7e70fdde6d716c88fc3a5c826edf850659b6968).
- Добавлена опция для настройки выполнения select-запросов с join. Опция влияет на количество RPC-запросов к таблицам-словарям, [0c63661](https://github.com/ytsaurus/ytsaurus/commit/0c6366155274f38861ce0cfe9f765a920e24deea).
- Поддержка row cache для следующих за таблет-селлом пиров, [8ddd498](https://github.com/ytsaurus/ytsaurus/commit/8ddd49815e72060055b8ae36d6b947c7bbcb670a).
- Реализована функция `to_valid_utf8` для языка запросов YT, [0b30b04](https://github.com/ytsaurus/ytsaurus/commit/0b30b04eb5d07d523dce92c30384a03411804c98).
- Добавлены некоторые параметры для настройки производительности QL, [e85e54e](https://github.com/ytsaurus/ytsaurus/commit/e85e54eff667ab17702503f57aad987ffb37d7f5).
- Валидация того, что неверсионированные чанки имеют разумный размер чанка и блока
  перед монтированием динамической таблицы, [7e3f8fc](https://github.com/ytsaurus/ytsaurus/commit/7e3f8fc1b9492afbc0803a137347ce59399e6e2a).
- Учитывается префикс ключа первичного ключа, ограниченный предикатом в ORDER BY, и используется упорядоченное выполнение, если этого достаточно, [2e75e7e](https://github.com/ytsaurus/ytsaurus/commit/2e75e7e3ad830922b7dfa1fad19b71f09a364bb9).
- Фоновые задачи компакции и партиционирования могут выполняться в двухуровневом fair-share пуле потоков, [139fceb](https://github.com/ytsaurus/ytsaurus/commit/139fceb9156b25808eaa4e1f51ead87227abdac6).
- Учёт ресурсов таблетов (количество таблетов и статическая память таблетов) теперь
по умолчанию выполняется в разрезе бандлов, а не аккаунтов. Это применяется только к новым кластерам. Конфиги существующих кластеров не будут изменены при обновлении. Чтобы включить учёт в разрезе бандлов на существующем кластере, задайте следующие опции: `//sys/@config/tablet_manager/enable_tablet_resource_validation = %true`, `//sys/@config/security_manager/enable_tablet_resource_validation = %false`, [f6e73b6](https://github.com/ytsaurus/ytsaurus/commit/f6e73b64b518635510d95fee89fdd83e4ae58297).
- Реализован оператор CAST для языка запросов YT, [26e91b8](https://github.com/ytsaurus/ytsaurus/commit/26e91b802ab6226ecdad2d1ef9c0c56650a33b3d).
- Атрибут `@resource_quota` для бандлов таблет-селлов теперь интернируется и
дублируется в `@resource_limits`. Использование последнего предпочтительнее, [6e481ed](https://github.com/ytsaurus/ytsaurus/commit/6e481ed399f3319b38232d3f751f4bb69584fb6c).
- Реализован счетчик CPU на таблет для select на основе общего времени CPU и пропорций веса прочитанных данных на таблет, [b617d05](https://github.com/ytsaurus/ytsaurus/commit/b617d056a0a3590da5b218cd4078a1549e7d97ae).
- Добавлен трекер сброса журналов в контроллере перегрузки, [b09ce32](https://github.com/ytsaurus/ytsaurus/commit/b09ce32a57cef032db7b09cb0cec72033a20f919).
- Bulk insert и резервное копирование динамических таблиц разрешены по умолчанию для новых кластеров, [dac1857](https://github.com/ytsaurus/ytsaurus/commit/dac1857b97e06a384a21c294e9b9044c96a39dd7).

##### Исправления и оптимизации:
- Незаписанная метка времени (unflushed timestamp) теперь сохраняется для отмонтированных таблетов.
Это важно только для таблет-экшенов, поскольку обычное монтирование
устанавливает незаписанную метку времени на текущую, [f3ccfbd](https://github.com/ytsaurus/ytsaurus/commit/f3ccfbd350c706361a5becf1e6efd5dd7977e0d6).
- Троттлеры, связанные с удалением таблет-селла, теперь имеют ненулевой лимит по умолчанию:
`//sys/@config/tablet_manager/tablet_cell_decommissioner/decommission_throttler`,
`//sys/@config/tablet_manager/tablet_cell_decommissioner/kick_orphans_throttler`. Период вывода из эксплуатации и проверки осиротевших таблетов по умолчанию уменьшен до 10 секунд, [9f43c91](https://github.com/ytsaurus/ytsaurus/commit/9f43c91bb3a528baf0d9ae590d6f1f3b9a22c1c2).
- Исправлена ошибка assertion для пустого пула id динамических сторов, которая могла возникнуть,
когда таблет был перемещён балансировщиком и одновременно выполнялась overwrite bulk insert, [ae7ce40](https://github.com/ytsaurus/ytsaurus/commit/ae7ce40c35a5ed457366a13aac3f95eea0fc7bcc).
- Команда get-tablet-errors теперь более надёжна в случае кратковременных ошибок, [7acdf14](https://github.com/ytsaurus/ytsaurus/commit/7acdf14b268d2aad92fcf1261b12f87c1528ed4a).
- Счетчики профилирования таблетов создаются лениво для экономии памяти категории "profiling", [b286517](https://github.com/ytsaurus/ytsaurus/commit/b2865179e1c95aab48d6e227b0b8dc7dfac44fb8).
- Исправлено редкое падение при использовании array join и обычного join, когда последний зависит от результата первого, [750ee01](https://github.com/ytsaurus/ytsaurus/commit/750ee01c8c4cd5761b569fa6e63e8df5612f22c6).
- Предотвращены падения прокси во время select при несоответствии метаданных, [567922b](https://github.com/ytsaurus/ytsaurus/commit/567922be048a0f8baacde99c62ef41f086847165).
- Записи не должны вызывать исключений, даже если память `lookup_rows_cache` перераспределена, [1125ce4](https://github.com/ytsaurus/ytsaurus/commit/1125ce450333f312e9a198f41f65dd5bf1355896).
- Исправлен переизбыточный учёт динамической памяти таблетов, вызванный изменением ёмкости вектора в неизменяемом объекте между вызовами `Acquire` и `Release`, [0b16c29](https://github.com/ytsaurus/ytsaurus/commit/0b16c2982cf716f7dd6a323cc9b0be21c6b98a2f).
- Реализовано динамически включаемое устаревание оператора WITH INDEX без алиаса в select-запросах, [8013fee](https://github.com/ytsaurus/ytsaurus/commit/8013fee311f4c6b214c7341f07480c7dfcf0cb18).
- Исправлен учёт динамической памяти таблетов по категориям на фолловерах, [1acfa14](https://github.com/ytsaurus/ytsaurus/commit/1acfa14bfd12a4c24a157f345dbafd46a717d627).
- QL: исправлено падение при выполнении Group By с составным ключом, [5f0e711](https://github.com/ytsaurus/ytsaurus/commit/5f0e71186e2e74f995a8495ac4db21898234977f).
- Таблицы с ханками не должны обрабатываться так же, как полностью in-memory таблицы, [74d5ec3](https://github.com/ytsaurus/ytsaurus/commit/74d5ec3be1d594a55c7729a952bef30ed1f87016).
- Увеличен стек обработчика сигналов. Включена защита памяти для стека обработчика сигналов, чтобы избежать повреждения памяти из-за переполнения стека во время выполнения обработчика сигналов, [1fbbdd6](https://github.com/ytsaurus/ytsaurus/commit/1fbbdd63796abf0567d1fc00dcb3707b67dd212e).
- Исправлено расхождение слепков между replay и resave.
Дополнительно оптимизированы схематичные неверсионированные читатели для yson-колонок, [b4fd1a1](https://github.com/ytsaurus/ytsaurus/commit/b4fd1a149f5c5f4b58d1f68653bb477668238f4c).
- Исправлена генерация партиционных cookie для упорядоченных динамических таблиц, [6f592cd](https://github.com/ytsaurus/ytsaurus/commit/6f592cdbfe9933ae822c635533ce8cca28445387)

#### MapReduce
##### Новые возможности и изменения:
- Поддержка RLS в операциях, [10c9cf8](https://github.com/ytsaurus/ytsaurus/commit/10c9cf888b69c873a45399f5261bde6389af8cb0).
- `chunk_reader_statistics` теперь может сообщать отдельную статистику для каждого входного кластера. Включается флагом `enable_per_cluster_chunk_reader_statistics` в динамическом конфиге job_proxy, [4fbe259](https://github.com/ytsaurus/ytsaurus/commit/4fbe25987c218992f9da9be2a72a038d1b631f03).
- Добавлена опция для нижней границы лимита CPU пользовательского джоба, [2f51037](https://github.com/ytsaurus/ytsaurus/commit/2f510375233c9df50716d9db9b5ada1255173f80).
- Поддержка blob-формата для получения blob-таблиц как файлов внутри джобов, [c0b1f2d](https://github.com/ytsaurus/ytsaurus/commit/c0b1f2d18f3fa67770bd58ca25642ed7be9289b9).
- Удалённые операции теперь могут читать через пользовательские сети. Это контролируется на уровне controller-agent, поэтому все удалённые операции будут использовать одни и те же сети (но это всё ещё может быть список предпочтений, т.е. если у узла нет адреса в указанной сети, джоб попытается читать через следующую сеть в списке предпочтений), [e1b174f](https://github.com/ytsaurus/ytsaurus/commit/e1b174fe02ef703344fea34504cd42a5534c6a9c).
- Поддержка job_size_adjuster для: `sorted merge`, `ordered merge`, `sorted reduce stage in mapreduce`, `ordered map stage in mapreduce`, `sorted reduce`, `ordered map`, [fd8c3ee](https://github.com/ytsaurus/ytsaurus/commit/fd8c3ee67ef049d7f6923cbfdaad8c960b920bd2).
- Удалённые операции теперь могут быть ограничены максимальным весом данных.
CA оценивает вес данных, которые будут прочитаны с конкретного
удалённого кластера, и если он превышает порог, операция
не сможет материализоваться, [068c14b](https://github.com/ytsaurus/ytsaurus/commit/068c14b9dc5b9a327e728dbef86badda69550efe).
- Включено обнаружение новых мастер-селлов в job proxy, [51b6040](https://github.com/ytsaurus/ytsaurus/commit/51b604015a3135dac0d15bd04fe5e271aba927cc).
- Добавлена поддержка опции `compressed_data_size_per_partition_job` в операциях sort и map-reduce. Это позволяет пользователям контролировать размеры партиционных джобов на основе сжатого размера данных, аналогично существующим операциям map и merge. Функция гарантирует, что при нарезке входных данных для партиционных джобов учитываются как ограничения по весу данных, так и по сжатому размеру, обеспечивая лучший контроль над потреблением ресурсов джобами, [8a66379](https://github.com/ytsaurus/ytsaurus/commit/8a66379e7fc57608ec8ad944f171fdc62b28832b).
- Добавлены ограничения по сжатому размеру данных для сортированных операций, включая поддержку лимитов первичного сжатого размера данных и опции `consider_only_primary_size` для определения размера джобов, [692a2bf](https://github.com/ytsaurus/ytsaurus/commit/692a2bfac26600c0dfdff15776e589d8b1d81c19).
- Поддержка дополнительных джобов для gang-операций. Введены ранги gang, [0045969](https://github.com/ytsaurus/ytsaurus/commit/004596916a1e9b6af45a953efbf9af30801f7de8).
- Этот коммит унифицирует логику нарезки по сжатому размеру данных и по весу данных в неупорядоченном пуле чанков. Ранее максимальный сжатый размер данных на джоб использовался для нарезки по сжатому размеру, и его семантика отличалась от семантики максимального веса данных на джоб, [6cc70e2](https://github.com/ytsaurus/ytsaurus/commit/6cc70e2824fca903d8b763dad6521f159f37d92f).
- Введена опция `ForceJobSizeAdjuster` для операций sort и MR. Эта опция включает подстройку размера джобов, даже если соответствующий `data_weight` на джоб явно указан, что обычно характерно для операций YQL, [cce512c](https://github.com/ytsaurus/ytsaurus/commit/cce512c3c37be42997fee9cde84beee9791cdf06).

##### Исправления и оптимизации:
- RemoteCopy теперь корректно обрабатывает колоночные ACL
(т.е. будет требовать доступ на чтение ко всем колонкам таблицы), [d210cf8](https://github.com/ytsaurus/ytsaurus/commit/d210cf845671e3a43f9d3c4ec6e6dc3b6b528898).
- Запрещено указывать `user_files/file_name` со слэшем в пользовательских джобах.
Такой файл всё равно невозможно подготовить, [dbcdf6f](https://github.com/ytsaurus/ytsaurus/commit/dbcdf6ff17df6ec53a8f3f21c2d359210922e83d).
- `chunk_reader_statistics/wait_time` был ненадёжным из-за гонки в таймере.
В редких случаях, заметных на крупных инсталляциях, мы могли случайно
добавить большое значение (примерно время с момента последней перезагрузки exec-узла) к сообщаемому времени, [c495929](https://github.com/ytsaurus/ytsaurus/commit/c495929b46b95b6688704b752da8dbdcd21d90d9).
- Controller agent теперь будет резервировать память для выходных буферов
в job_proxy для vanilla-джобов, [667c927](https://github.com/ytsaurus/ytsaurus/commit/667c9273acf4c0ef48c17fac12ecd8e6beaf74ea).
- RemoteCopy динамической таблицы с ханками теперь корректно оценивает вес данных ханк-чанков, [439ac66](https://github.com/ytsaurus/ytsaurus/commit/439ac663714c87924c13f24732b9fd4e48197009).
- Исправлено падение CA, когда gang-джоб завершается, повторное использование аллокаций включено, и у задачи есть ожидающие джобы, [c7eb321](https://github.com/ytsaurus/ytsaurus/commit/c7eb321b12978441f3e6ae7e145596fef785b672).
- Исправлено падение controller agent при отказе от gang-джоба, [0d02dbc](https://github.com/ytsaurus/ytsaurus/commit/0d02dbc697eac7787c96eede22370395e69485dd).
- Запрещено делать stderr/core таблицы динамическими и совпадающими с выходными таблицами, [194774d](https://github.com/ytsaurus/ytsaurus/commit/194774d4f4bc71c0afcd28aec01c5732dbe36de2).
- Когда количество джобов указано явно, раннее завершение ввода джоба и ограничения по размеру пропускаются для обеспечения более равномерного распределения размеров джобов, и поднимается алерт, если количество срезов данных превышает max_data_slice_count, вместо молчаливого игнорирования этого случая для последнего джоба, [b6be9a2](https://github.com/ytsaurus/ytsaurus/commit/b6be9a26eb8c83a90edb5a2d64830e9f60d8db91).
- Исправлена «гонка» между плановым обновлением директории мастер-селлов и внеплановым. Два обновления пытаются применить один и тот же diff к директории мастер-селлов. Первое успешно применяется, а второе пытается применить изменения к уже обновлённой директории и падает, [97332dd](https://github.com/ytsaurus/ytsaurus/commit/97332dd8253332b5adfa98cc51eb86e355e8e6d2).


#### Мастер-сервер
##### Новые возможности и изменения:
- Добавлена массовая вставка в пользовательской транзакции, [83002e9](https://github.com/ytsaurus/ytsaurus/commit/83002e9567b928bf2a802fe9abfb59b4cb679344).
- Обработка субъектов, ожидающих удаления, [541ff93](https://github.com/ytsaurus/ytsaurus/commit/541ff935123f67bb183bfc36caba20a70f6ea783).
- Загрузка слепка, содержащего list-узлы, приведёт к падению мастер-сервера. Чтобы этого избежать, включите опцию `alert_on_list_node_load` в динамическом конфиге. List-узлы будут полностью удалены в следующей мажорной версии, [3964ec6](https://github.com/ytsaurus/ytsaurus/commit/3964ec6a05aaf05a344d6adbecc04d29f21d1c00).
- Разрешено указывать `expression` в ACE. Кроме того, `full_read` теперь предоставляет полное построчное чтение (симметрично колоночному full_read) и позволяет пользователю выполнять copy/concatenate/remote_copy всей таблицы, [9556b59](https://github.com/ytsaurus/ytsaurus/commit/9556b59b7b907be33eec95ca1cb5215dfc7d1ed6).
- Автоматическое удаление «висячих» локаций, о которых узлы долго не сообщали, включено по умолчанию, [1afccef](https://github.com/ytsaurus/ytsaurus/commit/1afccef8a3e76212dc911aad5dff2742455dc045).
- Введены ревизии-пререквизиты для запросов в Sequoia, [3362b3a](https://github.com/ytsaurus/ytsaurus/commit/3362b3a28b003af718e1db25aefe499d27f38e58).
- Добавлена поддержка быстрых (отрицательных) уровней сжатия Zstd от 1 до 7, [43165bb](https://github.com/ytsaurus/ytsaurus/commit/43165bbe9a9bb6e979387e7f324c0d0b3b603c41).
- Мастер больше не использует информацию о схеме таблицы в EndUpload; для отправки схемы используйте BeginUpload, [38ed4e1](https://github.com/ytsaurus/ytsaurus/commit/38ed4e152aaeb8c1049d1da90e68c65caee82acb).
- Улучшено кэширование компактных схем таблиц и YSON-схем, [31a0283](https://github.com/ytsaurus/ytsaurus/commit/31a0283703e00380919105be3bcd26b4a572f5c3).
- Введено жёсткое ограничение для `//sys/chunks`. Его можно настроить через `virtual_chunk_map_read_result_limit`, [f0c0fef](https://github.com/ytsaurus/ytsaurus/commit/f0c0fefc261683492886624411fa360e4a9367dc).
- Добавлена валидация пререквизитов при чтении, [04e63a3](https://github.com/ytsaurus/ytsaurus/commit/04e63a3d9e4020bbb4cd450723bf63a845b6834d).
- Добавлена метка MutationVersion во все журналы, записываемые внутри мутации, [0648d8d](https://github.com/ytsaurus/ytsaurus/commit/0648d8d3f27a8df5ee8ec97776d4d42026bf356f).
- Теперь также поддерживается локальное троттлирование чтения, аналогично троттлированию записи, [cff3569](https://github.com/ytsaurus/ytsaurus/commit/cff356907310b355eedf4aa379a15ebab10dc4e9).

##### Исправления и оптимизации:
- Удалены дублирующиеся алерты о неправильно настроенных ролях на мастер-ячейках в multicell manager. Теперь алерты будут появляться только на первичном мастере, [de4eec9](https://github.com/ytsaurus/ytsaurus/commit/de4eec90478f500ae30d8633bbd58159f4b9296b).
- Добавлена инвалидация кэша схем таблиц в случае неудачного парсинга компактной схемы таблицы, [81dcde8](https://github.com/ytsaurus/ytsaurus/commit/81dcde8a1f6755834f0ba88b9dcdb430eaf43b6a).
- Исправлен ответ `no such transaction` на `PingTransaction()` сразу после смены лидера, [62f80a8](https://github.com/ytsaurus/ytsaurus/commit/62f80a8d3b8dbe8b0c8e2c80c67bff154082854a).
- Исправлено расхождение атрибута `@maintenance_request` узла между старыми и новыми ячейками при добавлении ячейки, [83fac44](https://github.com/ytsaurus/ytsaurus/commit/83fac44a46378c65aab236ed4dce0edc296e36f5).
- Исправлена ошибка при проверке разрешения на полное чтение, [ae7de70](https://github.com/ytsaurus/ytsaurus/commit/ae7de709c17fb7a64cf352b79167d7943c5f5ec2).
- Исправлена выдача lease для мастер-ячейки, [76394af](https://github.com/ytsaurus/ytsaurus/commit/76394afa2e0d251ae4daa5052d192433581e0cb9).
- Исправлено разрешение символических ссылок в TObjectManager::ResolvePathToLocalObject, [8bcf04a](https://github.com/ytsaurus/ytsaurus/commit/8bcf04a2c3ff4c85862c6c7f04a2612cb8544ebc).
- Атрибут режима chunk merger учитывается, когда chunk merger отключён, [25375af](https://github.com/ytsaurus/ytsaurus/commit/25375afea99155f30abd94ace9e65f1e7604522f).
- Прекращена генерация mutation ID для запросов на чтение. Поскольку mutation ID считается частью сообщения запроса, генерация уникального mutation ID для каждого запроса ломала кэш object service, [682341c](https://github.com/ytsaurus/ytsaurus/commit/682341cee9d60d285c3b5d42208853bbe5016867).
- Включён флаг RemoveSecondaryCellDefaultRoles, [7a252af](https://github.com/ytsaurus/ytsaurus/commit/7a252afe29daff13ecd3c7237bae0787d6500388).
- Исправлены ID некоторых встроенных пользователей, появившихся в 25.2, [d4707fd](https://github.com/ytsaurus/ytsaurus/commit/d4707fdded91ca12669639de8cf91a2b8d3ca491).
- Исправлено отсутствие читаемости атрибутов вторичных индексов, [e1356fb](https://github.com/ytsaurus/ytsaurus/commit/e1356fb8389e85a3b2e4556bae09d7ad61470c50).
- Исправлен TAttributeFilter, [5d6ca6b](https://github.com/ytsaurus/ytsaurus/commit/5d6ca6be45c99c546cb7cad7cb9b79c253c86f7e).
- TCompactTableSchema заменён на TCompactTableSchemaPtr в кэшах схем таблиц, [afe8348](https://github.com/ytsaurus/ytsaurus/commit/afe83482724703bbb15211615c531d93bbfa69ce).
- Используется персистентный response keeper для коммита/отмены транзакций Кипариса, [2616719](https://github.com/ytsaurus/ytsaurus/commit/261671993cf8630c24b9af3bfa2248d98e20fb4b).
- Исправлено падение в HydraUpdateMasterCellChunkStatistics, когда ChunkScanExecutor выполняет более одного вызова OnChunkScan до фактического выполнения закоммиченной мутации HydraUpdateMasterCellChunkStatistics, [d061b28](https://github.com/ytsaurus/ytsaurus/commit/d061b285a52e0e43a131aac45d1e9e9deb46cc1d).
- Исправлено расхождение слепков в трекере Cypress proxy, [ef8c7fe](https://github.com/ytsaurus/ytsaurus/commit/ef8c7fe1943783a318c838d4927cd9981cda21dd).
- Исправлена ошибка «list node creation is forbidden» при установке списка в атрибут несуществующего узла, [52776ec](https://github.com/ytsaurus/ytsaurus/commit/52776ec9953781f5ef19de80a12e5e99c456fe61).
- Удалён полный broadcast из DoGetMulticellOwningNodes, [304e500](https://github.com/ytsaurus/ytsaurus/commit/304e500262e9437498bc47a83a26808ce69031c1).
- Исправлена валидация, когда пути ревизий отличаются от путей выполнения, [97fb456](https://github.com/ytsaurus/ytsaurus/commit/97fb456ce886270ea0c7db2d75c6a7336f2d586f).
- Исправлен атрибут статистики узла в случае, когда версия узла новее версии мастера и содержит нераспознанные категории памяти, [72d4a63](https://github.com/ytsaurus/ytsaurus/commit/72d4a631ab73a17535410dd530be3c821037cff4).

#### Узлы
##### Новые возможности и изменения:
- Поддержка динамической перенастройки подсистемы подписей в exec-узле, [1f0064d](https://github.com/ytsaurus/ytsaurus/commit/1f0064d1520a47b4d3f8e1c7281ac9ca09b4a87d).
- Поддержка динамической настройки отдельных таймаутов Porto API и параллелизма импорта слоёв; динамический конфиг перенесён и реструктурирован: `exec_node/volume_manager` -> `exec_node/slot_manager/volume_manager`, [a85c513](https://github.com/ytsaurus/ytsaurus/commit/a85c51346a131db01013b2051722495e99bee216).
- DataNode: поддержка чтения через direct io и huge pages, [55ca233](https://github.com/ytsaurus/ytsaurus/commit/55ca2336dc371f06bbe8ace2759020d87011184e).
- DataNode: реализован параллельный GetBlockSet, [57afc41](https://github.com/ytsaurus/ytsaurus/commit/57afc4100612a6e9c5c327d18e85bfbe878fac33).
- DataNode: добавлена формула io_weight для адаптивного вычисления io_weight, [2df6172](https://github.com/ytsaurus/ytsaurus/commit/2df6172d2c3a3ef350aec094d3c1e1878b5c56c6).

##### Исправления и оптимизации:
- Исправлено поглощение расширений метаданных чанков в meta aggregated writer, [f10f2cb](https://github.com/ytsaurus/ytsaurus/commit/f10f2cb1ad9bf3da97ce3e4d596e654d0e046b65).
- Исправлены пересекающиеся потоки heartbeat'ов от узлов к мастеру, [fa95012](https://github.com/ytsaurus/ytsaurus/commit/fa950126fc5ff82473ab7395ef4187301f5e4380).
- Исправлена гонка между регистрацией на первичном мастере и получением новых мастер-ячеек, [9dfa44b](https://github.com/ytsaurus/ytsaurus/commit/9dfa44b1665eb8e14ebf51fd798aef1b54ba79fa).
- Исправлена метрика `yt.exec_node.rpc_proxy_in_job_proxy_count`, добавлена метка `host`, [33f92a8](https://github.com/ytsaurus/ytsaurus/commit/33f92a8d4b96970b84d317ad3ea65feae70bc572).
- Батчирование запуска и остановки heartbeat'ов, [1b800da](https://github.com/ytsaurus/ytsaurus/commit/1b800da2eb7647648315ba07d4b8495f206bb95c).
- Добавлен конфиг синхронизатора директории мастер-ячеек в динамический конфиг узла кластера, [c2fb7d7](https://github.com/ytsaurus/ytsaurus/commit/c2fb7d7f455ee7cf7a568b1caac40b1c4b89ede9).
- Исправлен запуск heartbeat'ов узла до фактической регистрации, [49b021b](https://github.com/ytsaurus/ytsaurus/commit/49b021bc31435c1b204466038ba531355d349740).
- Исправлено падение в meta aggregated writer на повреждённых чанках, [0e97074](https://github.com/ytsaurus/ytsaurus/commit/0e970742581a6bdecdbe7113d4261a7a17ff83bf).
- Переиспользование lease-транзакции узла во время перерегистрации, [693e3e1](https://github.com/ytsaurus/ytsaurus/commit/693e3e13c08a5570d0101975bb4b096218acee98).


#### Прочее
##### Исправления и оптимизации:
- Категория памяти AllocFragmentation не должна учитываться в общей используемой памяти, поскольку это свободная память внутри кучи, [e301573](https://github.com/ytsaurus/ytsaurus/commit/e30157331d8a769e72fc80875244a666cff5d97e).
- Исправлена гонка данных на поле EndTime в TIODirection. Она возникает при одновременном вызове методов OnShutdown и GetStatistics. Управление параллельным доступом к этой структуре обеспечивается внешним интерфейсом (TFDConnectionImplPtr), поэтому структуру следует использовать под SpinLock (как и при прочих обращениях к ней), [365d1cf](https://github.com/ytsaurus/ytsaurus/commit/365d1cf7065768fa25d9c37b0220c8be19cf2142).
- Исправлена некорректная инициализация потоков в пуле потоков fair-share, [a9d0752](https://github.com/ytsaurus/ytsaurus/commit/a9d07520d77c2d9f86579a7ba259968ce0cc4926).
- Исправлена утечка памяти в логгере, [56dc38d](https://github.com/ytsaurus/ytsaurus/commit/56dc38d092012a8173b050602d3cab82640a664d).



{% endcut %}


{% cut "**25.2.2**" %}

**Дата релиза:** 2025-12-09


**Страница релиза:** [25.2.2](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/ytsaurus/25.2.2)


**Docker-образ:** [ghcr.io/ytsaurus/ytsaurus:stable-25.2.2](https://github.com/orgs/ytsaurus/packages/container/ytsaurus/633029914?tag=stable-25.2.2)


Чтобы установить YTsaurus Server 25.2.2, [обновите](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release%2F0.27.0) k8s-operator до версии 0.27.0.

#### Прокси
##### Возможности:
- Добавлена команда check_operation_permission, [ce62b11](https://github.com/ytsaurus/ytsaurus/commit/ce62b1111cae509cc5689f3c630ff17455dd6c38).
- Добавлен флаг порядка сортировки запросов в list_queries, [acdc001](https://github.com/ytsaurus/ytsaurus/commit/acdc001a5bcfb4c45a7c4dc2f8260c69f0fa514a)
- По умолчанию отключен новый поиск в query tracker, [17afa20](https://github.com/ytsaurus/ytsaurus/commit/17afa20ef8a3ca38284c4d1d404b105e0beb35a5)

{% endcut %}


{% cut "**25.2.1**" %}

**Дата релиза:** 2025-11-11


**Страница релиза:** [25.2.1](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/ytsaurus/25.2.1)


**Docker-образ:** [ghcr.io/ytsaurus/ytsaurus:stable-25.2.1](https://github.com/orgs/ytsaurus/packages/container/ytsaurus/574146599?tag=stable-25.2.1)


Чтобы установить YTsaurus Server 25.2.1, [обновите](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release%2F0.27.0) k8s-operator до версии 0.27.0.


#### Узлы данных
##### Исправления:
- Исправлена гонка при вычислении P2PWeight в TP2PSnooper, которая приводила к падениям вида `YT_VERIFY(Categories_[category].Used >= 0)` с категорией `p2p`, [d5556a3](https://github.com/ytsaurus/ytsaurus/commit/d5556a35be28525bcbb44c95fb7e7fdf3550d6af)


#### Queue Agent
##### Исправления:
- Исправлены падения queue agent, когда объект-реплика содержит некорректный путь реплики, например, незакрытые символы `{` или `}`, [bfed123](https://github.com/ytsaurus/ytsaurus/commit/bfed123bb1750bf1b3a5c06901f15e14cda3b67b).



#### Мастер
##### Исправления:
- Исправлена ошибка при проверке полного разрешения на чтение, [c15ffe0](https://github.com/ytsaurus/ytsaurus/commit/c15ffe0d6f84dbb2f6e73a92db0fddc127303054).
- Исправлено падение при попытке решардировать уже удаленную таблицу, [540400c](https://github.com/ytsaurus/ytsaurus/commit/540400c08b729e7a73b7b2e51349df99b650c21c)




{% endcut %}

{% cut "**25.2.0**" %}

**Дата релиза:** 2025-09-23


**Страница релиза:** [25.2.0](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/ytsaurus/25.2.0)


**Docker-образ:** [ghcr.io/ytsaurus/ytsaurus:stable-25.2.0](https://github.com/orgs/ytsaurus/packages/container/ytsaurus/524637705?tag=stable-25.2.0)


Для установки YTsaurus Server 25.2.0 [обновите](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release%2F0.27.0) k8s-operator до версии 0.27.0.

#### Существенные изменения
- Добавлена поддержка GPU Nvidia в k8s-operator. Улучшено обнаружение GPU-устройств в контейнере джоба.
[Документация](https://ytsaurus.tech/docs/en/admin-guide/gpu).
- Добавлен bundle controller для управления бандлами таблеточных ячеек на небольших кластерах. Этот компонент распределяет таблеточные узлы по бандлам, управляет обслуживанием узлов и контролирует распределение CPU и памяти по таблеточным узлам. [Документация](https://ytsaurus.tech/docs/en/admin-guide/bundle-controller).
- Добавлена поддержка multiproxy-режима в RPC-прокси. RPC-прокси (включая RPC Proxy в Job Proxy) могут быть настроены для работы с удалёнными кластерами. [Документация](https://ytsaurus.tech/docs/en/admin-guide/multiproxy).

#### Возможности языка запросов
- Добавлены функции `cardinality_state` и `cardinality_merge`.
- Добавлена поддержка функций для работы с временными метками для произвольных часовых поясов.
- Реализована функция `array_agg`.
- Добавлена поддержка простых подзапросов в секции FROM.

#### Изменения по умолчанию и устаревшие функции
- Включено `decommission_through_extra_peers` по умолчанию; это значительно сокращает время простоя при обслуживании таблеточного узла.
- Включено удалённое копирование hunk'ов по умолчанию.
- Включён учёт ресурсов таблетов по бандлам по умолчанию.
- Операции Remote Copy устанавливают некоторые системные атрибуты целевой таблицы, даже если в спецификации `copy_attributes` имеет значение false; эти атрибуты: `compression_codec`, `erasure_codec`, `optimize_for`.
- Объявлен устаревшим `list_node`. Мастер-серверы теперь будут выдавать предупреждение в журнале (уровень alert) после загрузки слепка, если он содержит list-узел. Это поведение можно отключить с помощью опции `alert_on_list_node_load`. Рекомендуется перейти на другие типы и удалить или заменить все оставшиеся list-узлы. Если этого не сделать, мастер-сервер не запустится в следующем мажорном обновлении. В этот релиз включён скрипт, который должен помочь в миграции в большинстве случаев. Его можно найти в `yt/yt/scripts/master/replace_list_nodes`. Мы опубликовали статью в блоге, объясняющую причины отказа от этого типа и предлагающую другие методы миграции. Подробнее [здесь](https://ytsaurus.tech/en/blog/ytsaurus-25-2-0#deprecation).

---
#### Полный журнал изменений

#### Планировщик и GPU

##### Новые возможности и изменения:
- Добавлена проверка лимитов ресурсов exe-узла. Если узел не соответствует заданному лимиту, планировщик выдает предупреждение, [c4b5dcd](https://github.com/ytsaurus/ytsaurus/commit/c4b5dcd1c275f05cd5559b155825a80e04b7bd7e).
- В регулярном выражении валидации имени пула по умолчанию разрешена точка, [93cf17a](https://github.com/ytsaurus/ytsaurus/commit/93cf17a715b6a73cc04932a0f712785898a37853).
- Добавлены `type`, `user` и `title` в orchid операции, [ffda123](https://github.com/ytsaurus/ytsaurus/commit/ffda12318037149b5828a40e8d9ab3d16c0278d1).
- Добавлена опция в конфиг планировщика и спецификацию операции, которая приводит к сбою операции, если указанные пулы не существуют, [30eebf3](https://github.com/ytsaurus/ytsaurus/commit/30eebf36d03c9285c57101e8d7db8e85cd9b6eb7).


#### Queue Agent

##### Новые возможности и изменения:
- Добавлена опция `enable_verbose_logging` в динамический конфиг, которая включает подробное журналирование для определённых объектов из `verbose_logging_objects`, [67efedf](https://github.com/ytsaurus/ytsaurus/commit/67efedf6eb175baa61a2431482dbd99d3fb40689).
- Учитываются экспорты очередей при обрезке очередей реплицированных таблиц и chaos-реплицированных таблиц, [c016ca9](https://github.com/ytsaurus/ytsaurus/commit/c016ca9c458fa2cc4f06b93e9ed2db009fc49a6c).
- Добавлена поддержка повторных попыток для метода `CreateQueueProducerSession`, [591d500](https://github.com/ytsaurus/ytsaurus/commit/591d500df1c7d88cfacb16079f821c6067b8faea).

##### Исправления и оптимизации:
- Исправлено `init_queue_agent_state` в случае, если каталог уже существует (происходит в k8s operator), [8aca0e2](https://github.com/ytsaurus/ytsaurus/commit/8aca0e254ac149a193f2b1e3b987271ef80e87fb).
- Исправлено `write_data_weight_rate` для пустых партиций, [3e1dd6d](https://github.com/ytsaurus/ytsaurus/commit/3e1dd6d0daa72bef4928d0a227eaa0748d2a6751).
- Предотвращён split-brain между экземплярами queue agent, приводящий к циклу перенаправлений в orchid, путём ограничения количества перенаправлений и добавления повторных попыток, [39b014d](https://github.com/ytsaurus/ytsaurus/commit/39b014d2a51395b9d360772c2de1cd595e9e9a8e).
- Исправлены сбои queue agent в некоторых сложных случаях при пересоздании очереди, [133b109](https://github.com/ytsaurus/ytsaurus/commit/133b1095f53027f51fd1df28fcdfc52c8b3071aa).
- Исправлена потенциальная потеря данных при наличии нескольких экспортов на очередь, [f51f9fa](https://github.com/ytsaurus/ytsaurus/commit/f51f9fa222507243a8e7cab58b1dd8455290cd45).

#### Прокси
##### Новые возможности и изменения:
- Добавлена поддержка multiproxy-режима в RPC-прокси: клиент может использовать multiproxy одного кластера для работы с другими подключёнными кластерами, [e0f98d7](https://github.com/ytsaurus/ytsaurus/commit/e0f98d75284d123ea87bbbf1abb2fc2977e03a82).
- Добавлен метод `get_current_user` в RPC- и HTTP-прокси, [ab3f903](https://github.com/ytsaurus/ytsaurus/commit/ab3f903e5708a0998ff565e6f25fd4ea95e8b7fb).
- Добавлена возможность настройки лимита пользователей для конкретной роли прокси, [14d49a0](https://github.com/ytsaurus/ytsaurus/commit/14d49a09cad2609bc23911a6899213a2b1fa5dc8).
- Расширена функциональность impersonation в HTTP-протоколе; теперь она разрешена для всех незаблокированных суперпользователей, [9eee43d](https://github.com/ytsaurus/ytsaurus/commit/9eee43d28df59c4b2090aa0437ced6bf225e6a3e).
- Разрешена отправка/чтение значений размером более 16 МБ в RPC-прокси через wire-протокол для методов работы со статическими таблицами, [df8fb64](https://github.com/ytsaurus/ytsaurus/commit/df8fb64910e8423e444a1694391ed21ac4fa2f55).
- Добавлены опции `to_lower` и `to_upper` в OAuth login_transformations, [0245614](https://github.com/ytsaurus/ytsaurus/commit/0245614f4647d9f409f3855c716cd920bd8f02de).
- Добавлена поддержка динамической перенастройки подсистемы подписей в HTTP- и RPC-прокси, [029f6ce](https://github.com/ytsaurus/ytsaurus/commit/029f6ce8c1eb5c3254c336129e2d616c60337b4b)

##### Исправления и оптимизации:
- Различные исправления в формате Arrow:
  - Исправлено чтение таблиц с колонками типа date, [1a23993](https://github.com/ytsaurus/ytsaurus/commit/1a239934462e269550c103cf81b813997ce473cc).
  - Добавлена возможность чтения таблиц с разным количеством колонок в метаданных чанка в формате Arrow, [6210035](https://github.com/ytsaurus/ytsaurus/commit/6210035056c12e3b078f7a3e01b562030f946192).
- Изменены опции кэширования в конфиге CypressUserManager для OAuthAuthenticator. Перейдите на опции, совместимые с AsyncExpiringCache ("expire\_after\_\*\_time"). Более старые опции ("cache\_ttl", "optimistic\_cache\_ttl") объявлены устаревшими и будут удалены в будущих версиях, [1ecabbc](https://github.com/ytsaurus/ytsaurus/commit/1ecabbc9831fb6fd8b41b12feb5173309a70d21b).
- Исправление для CVE-2023-33460: утечка памяти в yajl 2.1.0 при использовании функции yajl_tree_parse. [f7b9064](https://github.com/ytsaurus/ytsaurus/commit/f7b9064f099d977cdfb2644f4ef11a57745049f7)
- Исправлена возможная взаимоблокировка в кэше метаданных чанков, [7c68dbe](https://github.com/ytsaurus/ytsaurus/commit/7c68dbe45457d1254046e1d9c22c6bff55cf3138).
- Исправлен расчёт `state_counts` и `type_counts` в методе `list_jobs`, [84d7713](https://github.com/ytsaurus/ytsaurus/commit/84d7713a74b8f7c311ea523408d8d42d51eb8fa6).
- Устанавливается атрибут `treat_as_queue_producer=%true` при создании queue_producer, [88eac20](https://github.com/ytsaurus/ytsaurus/commit/88eac20c53b28015773e0f854ac2f62ded5fcf56).
- HTTP-прокси теперь работает, когда мастер находится в режиме только для чтения, [ccb0228](https://github.com/ytsaurus/ytsaurus/commit/ccb0228011eb203b079e1c544ea984dd5123712d).
- Улучшена обработка ошибок нехватки памяти, [db04463](https://github.com/ytsaurus/ytsaurus/commit/db04463b87f6e0256d8d61dcd1f453d6897151fe).
- При нехватке памяти отбрасываются только тяжёлые запросы, [646071a](https://github.com/ytsaurus/ytsaurus/commit/646071ac381adcf90cd4567428aa911ef6266e23).

#### Динамические таблицы
##### Новые возможности и изменения:
- Добавлена поддержка массовой вставки (bulk insert) в пользовательской транзакции, [3fe8c73](https://github.com/ytsaurus/ytsaurus/commit/3fe8c7325d761b81783997b5cf7bb694ff72c8c6).
- Учитывается префикс первичного ключа, ограниченный предикатом в ORDER BY, и используется упорядоченное выполнение, если этого достаточно, [f8dbc00](https://github.com/ytsaurus/ytsaurus/commit/f8dbc005ba504073faed4dafd62406abd60fd85a).
- Разрешено выполнение фоновых задач компактификации и партиционирования в рамках пула потоков two-level-fair-share, [55c2dfd](https://github.com/ytsaurus/ytsaurus/commit/55c2dfdd05a0529f92d384a0b1cfa0534645a000).
- Добавлены функции `cardinality_state` и `cardinality_merge` для QL, [a267e20](https://github.com/ytsaurus/ytsaurus/commit/a267e20d0c744f3d048f6f5a98f21fa6a62265ca).
- Добавлены функции для работы с временными метками для произвольных часовых поясов в QL, [55178fe](https://github.com/ytsaurus/ytsaurus/commit/55178fe4ad5c92624f84d34268f1a0cb6b7eb72b).
- Реализована функция `array_agg` для QL, [d656eec](https://github.com/ytsaurus/ytsaurus/commit/d656eec732e41d14fe1912a27e1f48ff9f71e216).
- Добавлена поддержка RegisterChunkReplicasOnStoresUpdate для упорядоченных таблиц; уменьшено количество запросов к мастеру, необходимых для чтения сброшенных чанков через API таблеточного узла, [155fe69](https://github.com/ytsaurus/ytsaurus/commit/155fe694477bf6d59fb3c8120cb2ee17bb34c96a).
- Приведение double->(unsigned) integer теперь избегает неопределённого поведения и работает одинаково независимо от движка выполнения за счёт ограничения значений и преобразования NaN в ноль, [8c492fa](https://github.com/ytsaurus/ytsaurus/commit/8c492fa46268add942182ea52e9ea9827af1d07c).
- Оптимизирована запись в таблицы со вторичными индексами при определённых условиях, [4066880](https://github.com/ytsaurus/ytsaurus/commit/4066880b1752a211c0909a98a978a6238a6ceb85).
- Включено `decommission_through_extra_peers` по умолчанию; это значительно сокращает время простоя при обслуживании таблеточного узла, [c34ff21](https://github.com/ytsaurus/ytsaurus/commit/c34ff216c3d2e2454a282d01272f07475b6e7ec1).
- Разрешено разворачивание колонок с типом Any через индексы; их содержимое проверяется в рантайме, [d0eb7ad](https://github.com/ytsaurus/ytsaurus/commit/d0eb7ad2c0e44cd0cab9a71a524a5be96a3ca942).
- Плавное перемещение таблетов с hunk'ами, [d5357ea](https://github.com/ytsaurus/ytsaurus/commit/d5357eaade428ed8abe2a8091d5ab04bb1bb037e).
- Добавлена опция для уменьшения времени сериализации путём отдельной сериализации транзакции в каждой группе блокировок в каждой строке, [fe90e97](https://github.com/ytsaurus/ytsaurus/commit/fe90e97c3cb519298d57dc66136426db027736a1).
- Оптимизация QL: SELECT-запросы с GROUP BY и JOIN группируют строки ДО выполнения join, когда это возможно, [75e8e64](https://github.com/ytsaurus/ytsaurus/commit/75e8e6439fdc8f8ea182dfcc2051f1e239e9801c).
- Добавлен метод для возврата замораживаемых или отмонтируемых таблиц в смонтированное состояние, [bbf1101](https://github.com/ytsaurus/ytsaurus/commit/bbf110130c07de7091e7f927ca5838209f1d99d7).
- Добавлена поддержка простых подзапросов в секции FROM в QL, [a7e0701](https://github.com/ytsaurus/ytsaurus/commit/a7e0701609febe6881d3eb078baf4c0af0878e73).
- Добавлены счётчики профилирования в таблеточных узлах для команд `pull_queue`/`pull_queue_consumer`, [6aebfc1](https://github.com/ytsaurus/ytsaurus/commit/6aebfc1a758092864dae6b25177ec4339bc1c2db).
- SELECT-запросы теперь корректно выбирают случайную in-sync реплику, даже если кандидаты принадлежат одному кластеру, [efdf083](https://github.com/ytsaurus/ytsaurus/commit/efdf083a7d8cc0e7345f8b7705b87c0ec6a7ee47).
- Добавлен `total_grouped_row_count` в статистику QL, [e37b81f](https://github.com/ytsaurus/ytsaurus/commit/e37b81f5199263dd9d15ef1116cd0be0463f8145).
- Добавлен трекер отбрасывания журналов в контроллере перегрузки, [8cd772d](https://github.com/ytsaurus/ytsaurus/commit/8cd772d009304ea80fa70fe9b562036c2e2e83a5).
- Включён учёт ресурсов таблетов по бандлам по умолчанию, [4f78e41](https://github.com/ytsaurus/ytsaurus/commit/4f78e419a8eb11a7929028b9171c87206d35ef2f).
- Атрибут @resource_quota для бандлов таблеточных ячеек теперь интернируется и дублируется в @resource_limits, [27ed1ef](https://github.com/ytsaurus/ytsaurus/commit/27ed1ef4516b0ee1c089bc107be5f3103365977a).
- Добавлена поддержка Remote Copy для динамических таблиц со словарями сжатия, [0791aea](https://github.com/ytsaurus/ytsaurus/commit/0791aea8bf67e71fd2cef192116cf191bb3ea9f5).
- Представлено расширение "evaluatable schema" для вторичных индексов, которое позволяет индексировать выражения, [bf45155](https://github.com/ytsaurus/ytsaurus/commit/bf451559e91b05f8c4136d9364c8d9d6a60a7db0).
- Включено удалённое копирование hunk'ов по умолчанию, [9f2c5f4](https://github.com/ytsaurus/ytsaurus/commit/9f2c5f4fe77adc6498c3a61c2c44019f3c6fecde).
- Оптимизация QL: используется lookup join, когда левый подплан является селективным, [03d25a9](https://github.com/ytsaurus/ytsaurus/commit/03d25a9e8cdd11912ffc2918a97f437b0278ada4).
- Улучшена производительность функции `timestamp_floor_week`, [06a643c](https://github.com/ytsaurus/ytsaurus/commit/06a643c45e9df782755e7202a4ded36e0626e794).
- Добавлена проверка erasure codec в journal writer, [ae194de](https://github.com/ytsaurus/ytsaurus/commit/ae194deef92ab585ae2b75b94404530e3aa32891).

##### Исправления и оптимизации:
- Исправлен учёт динамической памяти таблетов по категориям на фолловерах, [d147efd](https://github.com/ytsaurus/ytsaurus/commit/d147efd923a71ccad4676bf701804b94fc4b9a26).
- Увеличен стек обработчика сигналов; включена защита памяти для стека обработчика сигналов во избежание повреждения памяти из-за переполнения стека при выполнении обработчика сигналов, [7ba96aa](https://github.com/ytsaurus/ytsaurus/commit/7ba96aa4b515beedb479b14ba7c0d8e4dbc81c6e).
- Исправлен учёт памяти row cache, [bcadf28](https://github.com/ytsaurus/ytsaurus/commit/bcadf2893913a6743debc7012f83d6dd6fda75a4).
- Исправлена функция `to_any` — приведение `EValueType::Composite` к `EValueType::Any` теперь работает как ожидается. Некоторым функциям разрешено работать с обоими типами, [20f22b7](https://github.com/ytsaurus/ytsaurus/commit/20f22b77cbfde756cf23ad13a3a11ecba1e586de).
- Исправлена ошибка, связанная с разворачиванием вторичных индексов, которая приводила к сбоям, когда предикат запроса содержал list_contains(expr), где expr не был ссылкой, [61b2b4e](https://github.com/ytsaurus/ytsaurus/commit/61b2b4e40869ef7c70a5c37a90002aee4221ff15).
- Исправлены сбои в прокси при выборке из таблицы с некорректной вычисляемой колонкой, [913edd3](https://github.com/ytsaurus/ytsaurus/commit/913edd308dddca0fe204cefc6269579f42b87474).
- Исправлено select_rows, который не ждал блокировки при чтении через lookup, [336ab90](https://github.com/ytsaurus/ytsaurus/commit/336ab9054e164941f8e00bb7a7a65e7f7c2d555b).
- Исправлен неверный выбор invoker при переходе в пул потоков запросов, [8d93be7](https://github.com/ytsaurus/ytsaurus/commit/8d93be71145d5f1184451e482bc878d931c65562).
- Неправильная конфигурация row cache больше не приводит к OOM, [7db2519](https://github.com/ytsaurus/ytsaurus/commit/7db25191b60baa3b3db0a163aa6dd881a7b96a15).
- Исправлена ошибка неверной оценки размера метаданных в scan-формате, которая приводила к чрезмерной фрагментации чанков во время компактификации, [11a59e3](https://github.com/ytsaurus/ytsaurus/commit/11a59e379bb49e2ce7b4b0db975e93a08cb2b821).
- Исправлена ошибка, из-за которой вывод диапазонов (range inference) порождал некорректные ключи и диапазоны, [aa7597d](https://github.com/ytsaurus/ytsaurus/commit/aa7597d67ff43009fd8f271b95b7e275df1c0344).
- Исправлено повреждение данных чисел с плавающей точкой в динамических таблицах в scan-формате, [8fcbf50](https://github.com/ytsaurus/ytsaurus/commit/8fcbf507d8e1f02698058c3bbec4355d8e385ce2).
- Добавлен `push_down_group_by` для реплицированных таблиц, [cb64762](https://github.com/ytsaurus/ytsaurus/commit/cb64762aa5d4d331e1ab3f0d388d4695686ade8a).
- Исправлен учёт часовых поясов в функциях `timestamp_floor_*_localtime`, [07f30b0](https://github.com/ytsaurus/ytsaurus/commit/07f30b07c73740ff7034592765195febf7277d53).
- В списках чанков упорядоченных динамических таблетов используется физическое, а не логическое количество чанков, [96c1a7f](https://github.com/ytsaurus/ytsaurus/commit/96c1a7fbff6493e44f40476d3743255703164407).
- Устранена утечка памяти, вызванная отменёнными select-запросами, [75521e1](https://github.com/ytsaurus/ytsaurus/commit/75521e1cd3e86f6f21dfef8915c85c1e6f9fac0e).
- Предикат join теперь используется при выводе диапазонов для подзапросов, получающих данные из словарей, [d62d232](https://github.com/ytsaurus/ytsaurus/commit/d62d2329eed6c057e004a20eeacc29e0c9aab82b).
- Исправлена ошибка, приводящая к segfault в таблицах с вложенными колонками, [45cb542](https://github.com/ytsaurus/ytsaurus/commit/45cb542f8c883e7d559fee880065ddc5abde2e34).
- Исправлена несовместимость функции AVG в QL в некоторых случаях, [ef7e062](https://github.com/ytsaurus/ytsaurus/commit/ef7e062df0da5dad3efe10f6a099208aabb9da2d).

#### MapReduce
##### Новые возможности и изменения:
- Добавлена поддержка динамической перенастройки подсистемы подписей в exec-узле, [029f6ce](https://github.com/ytsaurus/ytsaurus/commit/029f6ce8c1eb5c3254c336129e2d616c60337b4b)
- Исправлены проблемы нарезки входных данных, [3bb3594](https://github.com/ytsaurus/ytsaurus/commit/3bb35948bb3ea1d2cc4fcc55a01231fe1e8aca65).
- Добавлена опция, которая задаёт нижнюю границу лимита CPU пользовательского джоба, [c4675d4](https://github.com/ytsaurus/ytsaurus/commit/c4675d4b55fd8a58c91d8626c13958e0f72f4e6c).
- Добавлена поддержка NBD-сетевых дисков в `disk_request`, [a83d345](https://github.com/ytsaurus/ytsaurus/commit/a83d34522c977e7a712462c909fde5ab5dff6310).
- Добавлена поддержка дополнительных джобов для gang-операций и введены gang-ранги, [d45f1f7](https://github.com/ytsaurus/ytsaurus/commit/d45f1f73d306dde623bdf432109d527c0a6fd46b).
- Добавлено новое соединение с доставкой, защищённое от сбоев (delivery fenced connection), работающее на обычном ядре Linux; оно может использоваться для CPU-интенсивных или GPU-джобов для предотвращения прерывания джоба при обрыве соединения, [d7861cd](https://github.com/ytsaurus/ytsaurus/commit/d7861cd3a6ad67144d7e1ec2bf508d52850ee3e8).
- Улучшена оценка размера сжатых входных данных для операций с колоночной статистикой, [68474e7](https://github.com/ytsaurus/ytsaurus/commit/68474e7ceba85788a169b37b56421557d26a6f8f).
- CA теперь по умолчанию получают схему из внешних ячеек, [b5932ab](https://github.com/ytsaurus/ytsaurus/commit/b5932ab6a734f01717bfde46812305fcef54ed81).
- Начальная поддержка нескольких джобов в одном аллокации, [ee037ac](https://github.com/ytsaurus/ytsaurus/commit/ee037ac8352969e6a43c3d087573e986e1937032).
- Имя хоста в контейнерах теперь строится в формате `slot-{slot_index}.{exec_node_hostname}`, [fe028ee](https://github.com/ytsaurus/ytsaurus/commit/fe028eebfcd8239f7d08492474e4b95a5a90dc35).
- Операции RemoteCopy теперь устанавливают некоторые системные атрибуты целевой таблицы, даже если в спецификации `copy_attributes` имеет значение false; эти атрибуты: `compression_codec`, `erasure_codec`, `optimize_for`, [25be378](https://github.com/ytsaurus/ytsaurus/commit/25be3785e95e49e1639ccef1ff49fc7077f1e1c7).
- Исправлена метрика `yt.exec_node.rpc_proxy_in_job_proxy_count`, добавлена метка `host`, [416d8bf](https://github.com/ytsaurus/ytsaurus/commit/416d8bf1c4c3be290f522d3472e80e7a0ca3f020).

##### Исправления и оптимизации:
- Передаётся фактическая ошибка вместо `Job failed by external request`, [bc9b656](https://github.com/ytsaurus/ytsaurus/commit/bc9b6564a4248f955d8efe2668fac0262d58dfc8).

#### Мастер-сервер

##### Новые возможности и изменения:
- Значение optimize_for по умолчанию теперь можно настраивать для статических и динамических таблиц, [9e5d4a8](https://github.com/ytsaurus/ytsaurus/commit/9e5d4a8c66c3956bf7d9a2d30a7266590294778e).
- Представлен TCompactTableSchema для уменьшения объёма памяти процесса мастер-сервера (в отличие от TTableSchema, он содержит только protobuf-сериализованную схему [666861a](https://github.com/ytsaurus/ytsaurus/commit/666861ab1251db2d645bfc5120564bf49266381e).
- Введено жёсткое ограничение на размер ответа для read-запросов к `//sys/chunks`, настраиваемое через `virtual_chunk_map_read_result_limit`. Ранее доступ к этой виртуальной карте мог привести к сбою мастер-сервера из-за чрезмерного создания файберов и последующего выделения памяти. Это временное решение, пока мы работаем над улучшенным исправлением, [61eb1f5](https://github.com/ytsaurus/ytsaurus/commit/61eb1f5af55f3ac4857a32972116be9b7569360f).
- Изменены команды copy/move: теперь требуется явный флаг для удаления вторичных индексов; ранее это было поведением по умолчанию, [0668453](https://github.com/ytsaurus/ytsaurus/commit/06684533f35295172063265d697ac37afe61b02f).
- Пользователям разрешена аутентификация с использованием их алиасов. Это предварительный шаг для обеспечения совместимости при переходе системных пользователей на новую схему именования (например, `job` -> `yt-job`), [b52bbb5](https://github.com/ytsaurus/ytsaurus/commit/b52bbb5ffc4ff917d26b87559700f8601cdb93ec).
- Read-запросы к внешним атрибутам (обычно обслуживаемым во вторичных ячейках) теперь аннотируются корректной идентификацией пользователя, [49fd63b](https://github.com/ytsaurus/ytsaurus/commit/49fd63bbd6d97e3ba54ee2971e1bd76d1092058f). Это в первую очередь необходимо для корректного распределения нагрузки в профилировании и мониторинге.
- Добавлено ограничение скорости обработки heartbeat'ов data-узла на уровне реплик чанка, [5ca5f62](https://github.com/ytsaurus/ytsaurus/commit/5ca5f62d7a1250897c44ae32c00caa391083274b).
- В атрибуты таблиц добавлено больше информации о вторичных индексах, [cd9ebbe](https://github.com/ytsaurus/ytsaurus/commit/cd9ebbe551ac93b6e93e585e598db354cbfd1d16).
- Блокировка выходных динамических таблиц операций перенесена с контроллера на нативный протокол, [9ac2870](https://github.com/ytsaurus/ytsaurus/commit/9ac2870a4835ec47a67dffc185d57c741e5255b4).
- Запрещён отзыв роли `chunk_host` у ячеек с чанками и роли `cypress_node_host` у ячеек с нативными узлами Кипариса, [5d802d7](https://github.com/ytsaurus/ytsaurus/commit/5d802d75f50c35fdf5e47b891cded0adb1b273ec).

##### Исправления и оптимизации:
- Прекращена генерация mutation ID для запросов на чтение. Mutation ID считаются частью сообщения запроса, и генерация уникального mutation ID для каждого запроса ранее ломала кэш object service, [3d1d9eb](https://github.com/ytsaurus/ytsaurus/commit/3d1d9eb212e72897130481967be753def5037315).
- Используется персистентный response keeper для запросов коммита/отмены транзакций Кипариса, [b6da7ab](https://github.com/ytsaurus/ytsaurus/commit/b6da7abb213c375607680cec4de2bc4cfaca5cc9).
- Исправлено падение в HydraUpdateMasterCellChunkStatistics, когда ChunkScanExecutor\_ вызывал OnChunkScan более одного раза до выполнения закоммиченной мутации HydraUpdateMasterCellChunkStatistics, [0c2d095](https://github.com/ytsaurus/ytsaurus/commit/0c2d095bd4159c11139085e4a4e1c9b89c1262d7).
- Исправлена ошибка «list node creation is forbidden» при попытке установить список в атрибут несуществующего узла, [bca5761](https://github.com/ytsaurus/ytsaurus/commit/bca5761ce7fbc723553633961f16f06af41b348b).
- Удалён broadcast из DoGetMulticellOwningNodes, [75a99bd](https://github.com/ytsaurus/ytsaurus/commit/75a99bd4d499ac660d031e93520bead4ec8cd5d0).
- Исправлена потенциальная потеря наследуемых атрибутов, когда узел, созданный с флагом «force», перезаписывал другой узел, [86d07f9](https://github.com/ytsaurus/ytsaurus/commit/86d07f9894a3c3f98a5631b64cc7ed1145057776).
- Исправлена валидация ревизий, когда пути ревизий отличаются от путей выполнения, [f39e9e8](https://github.com/ytsaurus/ytsaurus/commit/f39e9e8718b08a86cf2eec60faff37f2e1eb485f), [1663a23](https://github.com/ytsaurus/ytsaurus/commit/1663a2392a39e5f7a6ca8047145c8f031bfed64a).
- Запрещено создание таблиц, являющихся индексами самих себя, [f6404a8](https://github.com/ytsaurus/ytsaurus/commit/f6404a8c9be898caea87a29ef7bd618221f9b183).
- Исправлено создание вторичных индексов за порталом, [96ad84e](https://github.com/ytsaurus/ytsaurus/commit/96ad84e8567a5a017c4e73f1c4dbb3e271f21b1a).
- Исправлено аккуратное обновление реквизиции чанков в chunk merger, [996e11c](https://github.com/ytsaurus/ytsaurus/commit/996e11c7c05ea48caa519665cdb6a4a9a8a9ef99).
- Исправлены проверки статистики при удалении мастер-ячейки, [b558b10](https://github.com/ytsaurus/ytsaurus/commit/b558b10581fe97ec9a9ef119be7424fec0ba65d5).
- Исправлена обработка heartbeat'ов джобов на ещё не зарегистрированной новой ячейке, [4733cbb](https://github.com/ytsaurus/ytsaurus/commit/4733cbbcafd2cf4b73d59ae80938353664b0fefd).
- Исправлен репликатор чанков, не учитывавший переопределение коэффициента репликации для конкретного медиума в некоторых сценариях, [d6bab2f](https://github.com/ytsaurus/ytsaurus/commit/d6bab2f7f251418db1fda526a729cb9aff735de9).
- Исправлен ID встроенной группы `admins`, [29529ec](https://github.com/ytsaurus/ytsaurus/commit/29529ec69ce454c03e97290c435150225e5ea36b).
- Исправлено разыменование нулевого указателя в HydraCreateForeignObject, [a30d422](https://github.com/ytsaurus/ytsaurus/commit/a30d422999fe9224c1bcd678b4f5aeb3a1160cd5).
- Удалён устаревший шард ZooKeeper, [558b7b5](https://github.com/ytsaurus/ytsaurus/commit/558b7b5fac04c1bb6c99a7132e4ddc115e9cf0dc).
- Исправлена ошибка, из-за которой мастер-сервер не менял надёжность exec-узлов, [4e9becf](https://github.com/ytsaurus/ytsaurus/commit/4e9becf2dc2992ad8a714dcdcf404f1ba40758bf).
- Исправлена гонка между координатором транзакций, коммитящим транзакцию, и ячейкой с экспортированным объектом, снимающей ссылку на этот объект, [5b72aad](https://github.com/ytsaurus/ytsaurus/commit/5b72aad7cafe1f0b44dd76acced1a52ecd4e7264).
- Исправлено ручное слияние узлов Кипариса для транзакций планировщика, [8a80023](https://github.com/ytsaurus/ytsaurus/commit/8a80023a50661a42abba895aacda70d118adf845).
- Исправлено падение мастера при установке YSON-словаря с дублирующимися ключами в пользовательский атрибут, [867354b](https://github.com/ytsaurus/ytsaurus/commit/867354bffa92a4a9991d360c770e1219c4c12f81).
- Исправлено сравнение строк в валидации shallow merge, чтобы оно не приводило к сбою джоба, [404a790](https://github.com/ytsaurus/ytsaurus/commit/404a790b962ee26f1a4c2085d5bfc8b223ff6199).
- Исправлено падение, вызванное чтением атрибута `@local_scan_flags`, [d8743cb](https://github.com/ytsaurus/ytsaurus/commit/d8743cbae113dc99a3c53612f253e45c8eab4b08).
- Исправлена недетерминированная ошибка, вызванная недетерминированным порядком загрузки полей YSON-структуры при отсутствии двух и более обязательных полей. Поскольку сообщение об ошибке является частью ответа на мутацию мастера, это могло приводить к алерту «state hashes differ» на мастере, [d907ada](https://github.com/ytsaurus/ytsaurus/commit/d907ada9984b6b711e4d5dd02d36d9e333df5dbb).
- Исправлена обработка TAttributeFilter, [488b343](https://github.com/ytsaurus/ytsaurus/commit/488b34393bb091a92befc8f6e4a6bc700da0d670).
- Исправлена блокировка для конкатенации в режиме дозаписи, [c1f5c7e](https://github.com/ytsaurus/ytsaurus/commit/c1f5c7ed4454d1637303ad5ae4843b25ca611e04).
- Исправлена ошибка, связанная с патчем совместимости для мнимых локаций чанков, [f591951](https://github.com/ytsaurus/ytsaurus/commit/f591951182e9555c7ca58173ec14a75e7b6d41a7).

#### Узлы данных
##### Новые возможности и изменения:
- Добавлена поддержка erasure-кодирования в оценке размера чтения, [4b3a28e](https://github.com/ytsaurus/ytsaurus/commit/4b3a28e1089de7d9bf790d912cd874b092f16696).
- Добавлен флаг `enable_read_size_estimation` для отключения оценки размера чтения (по умолчанию `true`), [4b3a28e](https://github.com/ytsaurus/ytsaurus/commit/4b3a28e1089de7d9bf790d912cd874b092f16696).

##### Исправления и оптимизации:
- Исправлено поглощение расширений метаданных чанков в meta aggregated writer, [677d2d4](https://github.com/ytsaurus/ytsaurus/commit/677d2d4d6665f87c9bd25a7e1ae6a6ca86f67a9d).
- Исправлена ошибка вычисления коэффициента сжатия в оценке размера чтения на основе тяжёлой колоночной статистики; добавлены юнит-тесты для отлова подобных ошибок, [4b3a28e](https://github.com/ytsaurus/ytsaurus/commit/4b3a28e1089de7d9bf790d912cd874b092f16696).
- Добавлен конфиг синхронизатора директории мастер-ячеек в динамический конфиг узла кластера, [d049363](https://github.com/ytsaurus/ytsaurus/commit/d04936326ce00b11022cb5d029b4094c818dfcbd).
- Исправлен запуск heartbeat'ов узла до фактической регистрации, [58e442b](https://github.com/ytsaurus/ytsaurus/commit/58e442baa20350412949b31072a9f8aa1585d4b3).
- Исправлено падение в meta aggregated writer на повреждённых чанках, [5653dfb](https://github.com/ytsaurus/ytsaurus/commit/5653dfbae6192331c3312fbe0e8cc714ae3e7c2e).
- Переиспользование lease-транзакции узла во время перерегистрации, [d0eb92b](https://github.com/ytsaurus/ytsaurus/commit/d0eb92b986f8993e7c8b223bc85e3ffb73e5059f).
- Исправлено падение при отключении узла во время начала отправки heartbeat, [66efd89](https://github.com/ytsaurus/ytsaurus/commit/66efd898cb7ffb68b263498ffae7d29952204aa9).

#### Прочее
##### Новые возможности и изменения:
- Добавлены сенсоры для вызовов mlock, [085a74c](https://github.com/ytsaurus/ytsaurus/commit/085a74cac407e46117cae0a4f536693eab21a45c).
- Реализован stockpile относительно лимита памяти пользовательских джобов (это необходимо для exec-узлов в кластерах, ориентированных на динамические таблицы), [8b7c91b](https://github.com/ytsaurus/ytsaurus/commit/8b7c91b9bebc99d685559687145347c999707059).

##### Исправления и оптимизации:
- Прекращена отмена lease-транзакции узла при перерегистрации, [6da69bd](https://github.com/ytsaurus/ytsaurus/commit/6da69bd2b67e34e99d360d91200bb1f262b8a99f).
- Исправлено неопределённое поведение в chunked memory pool, [ec99700](https://github.com/ytsaurus/ytsaurus/commit/ec997008c02ff7b9e934698512d07fcfd5778690).
- Исправлено неопределённое поведение в сжатии zstd при журналировании, [870ca53](https://github.com/ytsaurus/ytsaurus/commit/870ca53f9a19697d1a86f9462ce3bfdfec9738b3).
- Исправлена ошибка в RPC-сервисе, из-за которой тяжёлый запрос, поставленный в очередь, мог использовать propagating storage (например, trace context) от другого запроса, [7745b84](https://github.com/ytsaurus/ytsaurus/commit/7745b84081672cfbe5ccd8c2f50498a44081ba3f).
- Улучшено отслеживание используемой памяти в concurrent cache, [122fd89](https://github.com/ytsaurus/ytsaurus/commit/122fd89ce22e6eb1f6cb48672463a0c264af9069).
- Используются 64-битные счётчики для бакетов гистограмм, [94fe6d3](https://github.com/ytsaurus/ytsaurus/commit/94fe6d36acebe32f2609ff1b7be7547dbeeaa446).



{% endcut %}


{% cut "**25.1.0**" %}

**Дата релиза:** 2025-07-28


**Страница релиза:** [25.1.0](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/ytsaurus/25.1.0)


**Docker-образ:** [ghcr.io/ytsaurus/ytsaurus:stable-25.1.0](https://github.com/orgs/ytsaurus/packages/container/ytsaurus/472899068?tag=stable-25.1.0)


_Чтобы установить YTsaurus Server 25.1.0, [обновите](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release%2F0.25.0) k8s-operator до версии 0.25.0._

#### Основные изменения

#### API
  - Улучшение API операций и джобов.
  - Добавлен учёт памяти для запросов в HTTP-прокси.

#### Compute
  - Реализована проверка прав для операций с использованием объектов контроля доступа.
  - Gang-операции стали production-решением. Gang-операции — это особый режим планирования ванильных операций, который особенно полезен для распределённого обучения ML-моделей.
  - Добавлен API `PatchSpec` для изменения спеки операции на лету.
  - [экспериментально] Планировщик remote copy теперь учитывает ограничения пропускной способности между кластерами.
  - Добавлена job shell для CRI-окружений джобов.

#### Storage
  - Поддержан тип `decimal256`.
  - Поддержано вычисление нематериализованных вычисляемых колонок в статических таблицах.

#### Возможности языка запросов
  - Добавлена подсказка `allow_async_replica` в дескрипторы таблиц QL для включения отката к асинхронным репликам в select-запросах.
  - Функции времени в QL теперь поддерживают `localtime` и используют таблицы поиска для повышения производительности.
  - Функция `AVG` в QL теперь поддерживает double и беззнаковые целые числа.
  - Функции `list_contains` и `list_has_intersection` в QL теперь поддерживают строго типизированные составные значения и null.
  - Запрос `EXPLAIN` теперь работает с реплицированными таблицами.

#### Динамические таблицы
  - Реализована постепенная глобальная компактификация («реинкарнация чанков») для динамических таблиц.
  - Поддержана remote copy для динамических таблиц с hunk-чанками.

#### Значения по умолчанию
 - Формат чтения по умолчанию для динамических таблиц изменён на `scan`.
 - Версионированная remote copy включена по умолчанию.
 - Remote copy hunk-чанков включена по умолчанию.
 - Стратегия размещения two-random-choices для целей записи включена по умолчанию.

#### Полный журнал изменений

#### Планировщик и GPU

##### Новые возможности и изменения:
- Добавлен алерт о нераспознанных опциях в конфиге пула, [6b2770b](https://github.com/ytsaurus/ytsaurus/commit/6b2770b65a51766fbd1ad53d81d6b409a2115f00).
- Добавлена поддержка генерации временных токенов, действующих в течение операции и хранящихся в защищённом хранилище операции, [c91fd05](https://github.com/ytsaurus/ytsaurus/commit/c91fd057421160254f41d6c394673bf654fc358d).
- Добавлена опция, разрешающая запуск gang-операций только в FIFO-пулах, [edcac9a](https://github.com/ytsaurus/ytsaurus/commit/edcac9a3e348b18fd03cb82fb9e15e782b1b4a58).
- Реализована проверка прав для операций с использованием объектов контроля доступа, [bd64281](https://github.com/ytsaurus/ytsaurus/commit/bd64281b11d8fef1a8f497c07ee1a0c5372073d9).
- Улучшение API операций и джобов:
  - Добавлены `attributes` и `events` в `list_jobs` [25a405d0c88](https://github.com/ytsaurus/ytsaurus/commit/25a405d0c88).
  - Доступ к архиву операций и `//sys/operations` закрыт по умолчанию.
  - Добавлены инкарнации операций в архив и поддержан соответствующий фильтр в API [6601c8dbbb1](https://github.com/ytsaurus/ytsaurus/commit/6601c8dbbb1).
- Добавлена поддержка управления операциями при наличии права `manage` на пул [177380edfed](https://github.com/ytsaurus/ytsaurus/commit/177380edfed).
- Поддержана новая логика расчёта переподписки лимита CPU [d6d1d08a91f](https://github.com/ytsaurus/ytsaurus/commit/d6d1d08a91f).
- Добавлена возможность запускать проверки GPU в изолированных томах и настраивать их через опции операции, [631f328](https://github.com/ytsaurus/ytsaurus/commit/631f3288fa3eb07176c777c93bcf3afddfaf30dc).

##### Исправления и оптимизации:
- Исправлена ошибка при одновременном указании опций `offloading` и `schedule_in_single_tree` [e36e910b718](https://github.com/ytsaurus/ytsaurus/commit/e36e910b718).
- Исправлено усечение fair share в FIFO-пулах для ванильных операций с одним аллокацией [5d8e22b67fa](https://github.com/ytsaurus/ytsaurus/commit/5d8e22b67fa).
- Удалены некоторые compats планировщика и устаревшие опции.

#### Queue Agent

##### Новые возможности и изменения:
- Добавлено поле `controller_info` в Orchid queue agent для обнаружения зависших проходов контроллера и отслеживания счётчиков ошибок, [74be9b4](https://github.com/ytsaurus/ytsaurus/commit/74be9b4ab44689d56f8110dc2313e1dc7c1e3057).
- Прогресс экспорта теперь включает детали о последней итерации экспорта, [6ec743a](https://github.com/ytsaurus/ytsaurus/commit/6ec743a21e4f4ed8e917fc17e822875367709ad3).
- Добавлены агрегированные метрики алертов для queue agent, [d2bf505](https://github.com/ytsaurus/ytsaurus/commit/d2bf505c9b3bafa8f0966cdd0234c4929416c293).

##### Исправления и оптимизации:
- Добавлены повторные попытки с экспоненциальной задержкой для предотвращения перегрузки в случае неправильно настроенных экспортов, [ae4ead4](https://github.com/ytsaurus/ytsaurus/commit/ae4ead4a1e9e5b86bbc9ffde2637488fb1dbe6ff).
- Исправлен сбой при повторном использовании каталога экспорта после воссоздания очереди без сброса прогресса экспорта, [49826ed](https://github.com/ytsaurus/ytsaurus/commit/49826ed8d9872d3d094224e0811335a2647ef7c3).

#### Прокси

##### Новые возможности и изменения:
- Добавлена поддержка HTTP-прокси в обработчике `discover_proxies`, [830e543](https://github.com/ytsaurus/ytsaurus/commit/830e543c1bbde6069180bdf451c807ff0175bdb6).
- Сохранены исходные типы колонок (например, `timestamp`) в ответах `web_json` от `select_rows`, [f4eb42d](https://github.com/ytsaurus/ytsaurus/commit/f4eb42d1b4e8521b8d70d83ec19ef50cb63647c5).
- Включена генерация и проверка подписи в HTTP-прокси с использованием публичных ключей Кипариса, [f8d0c7d](https://github.com/ytsaurus/ytsaurus/commit/f8d0c7d4f096d335b7fc5313f360dcab6d47ceb2).
- Добавлен учёт памяти для лёгких/тяжёлых HTTP-запросов, [06f7aeb](https://github.com/ytsaurus/ytsaurus/commit/06f7aeb2a484a802b4a401fcfd0c3216c9cdc33f).
- Добавлен параметр `require_sync_replica` в обработчик `push_queue_producer`, [1324b1b](https://github.com/ytsaurus/ytsaurus/commit/1324b1bb4aa2ec33249447dcc38257836b9e5ca0).
- Включено параллельное выполнение `discover_versions` между компонентами, [f06afce](https://github.com/ytsaurus/ytsaurus/commit/f06afce8d73c34082343b85f577f8ae3edcb114d).
- Добавлена опция конфига `create_user_if_not_exists` для предотвращения создания пользователя при OAuth-аутентификации (Issue #930), [8470ed6](https://github.com/ytsaurus/ytsaurus/commit/8470ed63a46beb3c483c885d62a195fbfd4ac77f).
- Добавлен флаг `require_password_in_authentication_commands` для возможности пропуска проверки пароля в командах аутентификации, [723db18](https://github.com/ytsaurus/ytsaurus/commit/723db18f80a3ceff00ed67d6a9cb5872b9c1ffda).
- Добавлен `cache_key_mode` для управления гранулярностью кэширования учётных данных, [df66eb5](https://github.com/ytsaurus/ytsaurus/commit/df66eb5a00ab7d338fb0972f618ad28862a2a440).
- Добавлена опция `EnableCookies` в запрос `PartitionTable`, возвращающая непрозрачный cookie для `CreateTablePartitionReader`, [f2f1ce6](https://github.com/ytsaurus/ytsaurus/commit/f2f1ce6039fd84e6cd13341e90db0a834ea1580e).
- Добавлен метод `CreateTablePartitionReader` для чтения партиции таблицы без запросов к мастеру, [f2f1ce6](https://github.com/ytsaurus/ytsaurus/commit/f2f1ce6039fd84e6cd13341e90db0a834ea1580e).
- Добавлена поддержка проверки подписи с использованием публичных ключей Кипариса, [12c8532](https://github.com/ytsaurus/ytsaurus/commit/12c85321bfd4fd41f32db7ee356345e0aee9c2b0).
- Добавлен обработчик для получения событий трассировки джобов, [8ee855e](https://github.com/ytsaurus/ytsaurus/commit/8ee855ef01ae55a6c500d6ec6029bac3b3c8260c).

##### Исправления и оптимизации:
- Переведено сжатие HTTP-потоков на выделенный пул потоков, [4f61857](https://github.com/ytsaurus/ytsaurus/commit/4f61857cbabea72e403b269f361de5b01bb6d746).
- Добавлены проверки совместимости типов в Arrow-парсере с использованием специфичных для YT типов, [bd0a6ff](https://github.com/ytsaurus/ytsaurus/commit/bd0a6ff376a1c2ec28e10d4ee4d476d70bb8a131).
- Добавлен конфиг `UploadTransactionPingPeriod` для корректной обработки таймаутов транзакций загрузки, [f10f749](https://github.com/ytsaurus/ytsaurus/commit/f10f749527d924fece0e52a0248cf0c58ed8d313).
- Arrow-писатель HTTP-прокси встроен непосредственно в Arrow-энкодер для устранения дублирования, [99293b6](https://github.com/ytsaurus/ytsaurus/commit/99293b6cf4c9a157209d556bc8e6f128be3832e1).
- Включено чтение Arrow-таблиц с разным количеством колонок в метаданных чанка, [6210035](https://github.com/ytsaurus/ytsaurus/commit/6210035056c12e3b078f7a3e01b562030f946192).
- Исправлен неверный тип данных в драйвере для ввода `push_queue_producer`, [ebffb74](https://github.com/ytsaurus/ytsaurus/commit/ebffb746ada4d63b768c7627e9bdee110b686cbd).
- Улучшения Decimal:
  - Добавлена поддержка вложенных `decimal128` и `decimal256` в Arrow.
  - Исправлена некорректная кодировка фиксированной длины `decimal256(n, p)` для малой точности, теперь используется переменная длина, как у `decimal128`, [58c6c65](https://github.com/ytsaurus/ytsaurus/commit/58c6c6590919a2bfbb88c9f5b833324b86623ead).

#### Динамические таблицы

##### Новые возможности и изменения:
- Добавлено отравление row-cache для обнаружения ошибок памяти, [4933fe9](https://github.com/ytsaurus/ytsaurus/commit/4933fe97dd68a40b2c5cdce4d2aa77000fdfb8dc).
- Добавлена подсказка `allow_async_replica` в дескрипторы таблиц QL для включения отката к асинхронным репликам в select-запросах, [27ac9b9](https://github.com/ytsaurus/ytsaurus/commit/27ac9b90f5d50dfc63490d86f24fb88bc042e74b).
- Добавлен метод возврата замораживающихся или отмонтирующихся таблиц в смонтированное состояние, [cdb2027](https://github.com/ytsaurus/ytsaurus/commit/cdb2027bf1b1de28a9a025a14375da5cd29bcd75).
- Включено использование пула потоков fair-share для lookup-запросов, [b19ecb3](https://github.com/ytsaurus/ytsaurus/commit/b19ecb34beae00c66f1c11ad3e6a19681f711087).
- Введён протокол, позволяющий выполнять запись в таблеты во время плавного перемещения, [3ab47cd](https://github.com/ytsaurus/ytsaurus/commit/3ab47cd930accca3d38fac269e67b8ae8862d29b).
- Bundle controller теперь может управлять лимитами памяти для запросов, [c6c6de4](https://github.com/ytsaurus/ytsaurus/commit/c6c6de47a8a8f0d64668a6a219e369f5c20a73ed).
- Функции времени в QL теперь поддерживают `localtime` и используют таблицы поиска для повышения производительности, [265160e](https://github.com/ytsaurus/ytsaurus/commit/265160e382062e7e19673da27f0242a636b7c51f).
- Функция `AVG` в QL теперь поддерживает double и беззнаковые целые числа, [d597f37](https://github.com/ytsaurus/ytsaurus/commit/d597f37096f44c7cc418da5cb986e06ed4377625).
- Функции `list_contains` и `list_has_intersection` в QL теперь поддерживают строго типизированные составные значения и null, [aeb6b24](https://github.com/ytsaurus/ytsaurus/commit/aeb6b24220a07385ab1975b610a6af06a75ed9fc).
- Добавлены счётчики профилирования для команд `pull_queue` и `pull_queue_consumer` на таблет-нодах, [6aebfc1](https://github.com/ytsaurus/ytsaurus/commit/6aebfc1a758092864dae6b25177ec4339bc1c2db).
- Запрос `EXPLAIN` теперь работает с реплицированными таблицами, [fcb3dba](https://github.com/ytsaurus/ytsaurus/commit/fcb3dbab1a50858856e5aa52977a185620bb4b56).
- Формат чтения по умолчанию для динамических таблиц изменён на `scan`, [f2ccc73](https://github.com/ytsaurus/ytsaurus/commit/f2ccc73dbd5b2883d733b16145ae16f55e7fe272).
- Select-запросы теперь выбирают случайные синхронные реплики, даже если они находятся в том же кластере, [efdf083](https://github.com/ytsaurus/ytsaurus/commit/efdf083a7d8cc0e7345f8b7705b87c0ec6a7ee47).
- Добавлен `total_grouped_row_count` в статистику QL, [e37b81f](https://github.com/ytsaurus/ytsaurus/commit/e37b81f5199263dd9d15ef1116cd0be0463f8145).
- Включена поддержка row-cache для select-запросов, полезная для join по словарю, [bba767b](https://github.com/ytsaurus/ytsaurus/commit/bba767b3f8671bddb8f6aed77d0ee25eed16d056).
- Реализована постепенная глобальная компактификация («реинкарнация чанков») для динамических таблиц, [6d405af](https://github.com/ytsaurus/ytsaurus/commit/6d405af0192ab66acf71bccbf38f2f02fda966bc).
- Добавлена поддержка формата lookup с колонками времени, [180af7c](https://github.com/ytsaurus/ytsaurus/commit/180af7c4b45cacaa66783809c9c7b4759c385348).
- Добавлен параметр `allow_reign_change` в конфиг таблет-ноды для тестирования сбоя при смене reign, [3754c50](https://github.com/ytsaurus/ytsaurus/commit/3754c50c141d89ef9ce6fe6a40df24e941d900ba).
- Введены состояния для вторичных индексов: `invalid`, `bijective`, `injective` и `unknown`, [81068e0](https://github.com/ytsaurus/ytsaurus/commit/81068e0145fcb6899cc8e351fcd72398ca55a763).
- Версионированная remote copy включена по умолчанию, [0862748](https://github.com/ytsaurus/ytsaurus/commit/08627486d980cc99aa0e8ec14104c7c6fbf43dea).
- Добавлена поддержка remote copy для hunk-чанков с erasure-кодированием, [2b783b0](https://github.com/ytsaurus/ytsaurus/commit/2b783b04de6e5ade76d159ce05e674bb9545b50f).
- `GROUP BY + LIMIT` без `ORDER BY` теперь выполняется параллельно, если не выполняются определённые условия. Влияет на поведение `WITH TOTALS`, [c296f24](https://github.com/ytsaurus/ytsaurus/commit/c296f24846653dff7b7e6bc2b3c913e05fa479ce).
- Remote copy для динамических таблиц с hunk-чанками теперь поддерживается (кроме случаев со словарями сжатия и striped erasure), [34f16d0](https://github.com/ytsaurus/ytsaurus/commit/34f16d0fd4c307134608f27cfb5cc028b3cbc771).
- Команда `dump-snapshot` теперь поддерживает режим `checksum` для отладки расхождений слепков, [5daa913](https://github.com/ytsaurus/ytsaurus/commit/5daa91303d7210689ecb117614b908523bd429af).

##### Исправления и оптимизации:
- Исправлены проблемы с точностью float при scan-чтении путём сериализации float как double, [0b78e7a](https://github.com/ytsaurus/ytsaurus/commit/0b78e7ad5b4a4ec48ece1f92501172bbeff8ec6d).
- Исправлено некорректное поведение с часовыми поясами в функциях `timestamp_floor_*_localtime`, [17f3dd1](https://github.com/ytsaurus/ytsaurus/commit/17f3dd1debf37d5b2a0222cf3d906b40e523ae12).
- Переход с логического на физическое количество чанков в списках чанков упорядоченных динамических таблетов, [2efe013](https://github.com/ytsaurus/ytsaurus/commit/2efe013a9ba51a105c3ce490b6cffdcdcc415c36).
- Автоматическое использование lookup join, когда левый подплан селективен, [4b8b207](https://github.com/ytsaurus/ytsaurus/commit/4b8b207a6ea179d480bc05f5478a226d31c3da99).
- Исправлены segfault при чтении таблиц с вложенными колонками, [45cb542](https://github.com/ytsaurus/ytsaurus/commit/45cb542f8c883e7d559fee880065ddc5abde2e34).
- Bundle controller теперь пропускает неисправные бандлы вместо блокировки прогресса, [dc149b2](https://github.com/ytsaurus/ytsaurus/commit/dc149b27843635d16d2416f09a21ffba8b4de702).

#### MapReduce

##### Новые возможности и изменения:
- Добавлены различные опции сплиттера джобов в спеки операций, [e2998a4](https://github.com/ytsaurus/ytsaurus/commit/e2998a41e1794bfc139709f1df6cfd282cf82e33).
- Сохранены job cookie во время перезапусков gang-операций и разрешён перезапуск уже завершённых джобов при смене инкарнации, [e3f7655](https://github.com/ytsaurus/ytsaurus/commit/e3f7655698256ea6c257171d17ba8af7f130a299).
- Добавлен контроллер инкарнаций операций, который перезапускает все джобы при завершении одного (полезно для распределённого ML), [fb9c7d3](https://github.com/ytsaurus/ytsaurus/commit/fb9c7d3d97563cb4e7d8d80270fe1be4739606d3).
- Добавлена job shell для CRI-окружений джобов, [6b18f2f](https://github.com/ytsaurus/ytsaurus/commit/6b18f2f3d836bc63e6e89d44dcce328a5bc2d958).
- Разрешено динамическое обновление `job_count` для ванильных задач, [3a5cfef](https://github.com/ytsaurus/ytsaurus/commit/3a5cfef7f0a2a87cd5b48637d03467dc49f82f91).
- Добавлен API `PatchSpec` для изменения спеки операции на лету (изначально поддерживает `max_failed_job_count`), [3a5cfef](https://github.com/ytsaurus/ytsaurus/commit/3a5cfef7f0a2a87cd5b48637d03467dc49f82f91).
- Операции `RemoteCopy` теперь всегда копируют ключевые системные атрибуты (`compression_codec`, `erasure_codec`, `optimize_for`), даже если `copy_attributes` имеет значение false, [25be378](https://github.com/ytsaurus/ytsaurus/commit/25be3785e95e49e1639ccef1ff49fc7077f1e1c7).
- Планировщик remote copy теперь учитывает ограничения пропускной способности между кластерами, [e0af4fd](https://github.com/ytsaurus/ytsaurus/commit/e0af4fdbdea4fcab2561b803364f153078195223).
- Контроллер-агенты теперь всегда получают схемы из вторичных ячеек по ID схемы, [af00687](https://github.com/ytsaurus/ytsaurus/commit/af0068716ea05530aade3474f15e59c7b2ab9d16).

##### Исправления и оптимизации:
- Исправлена ошибка, из-за которой телепортация одного чанка в неупорядоченном пуле могла завершиться сбоем, [897ffff](https://github.com/ytsaurus/ytsaurus/commit/897ffff6c1ed4f7d21d52b3fb456ff8fa8b7023b).
- Возвращается конкретная ошибка вместо общей `Job failed by external request`, [703aae5](https://github.com/ytsaurus/ytsaurus/commit/703aae58eb1b2a515c2aa9e49d4e9ae63bc3a39e).

#### Мастер-сервер

##### Новые возможности и изменения:
- Добавлены метрики для лимитов скорости чтения/записи запросов и размера очереди запросов на пользователя, [0052341](https://github.com/ytsaurus/ytsaurus/commit/0052341c563cc3c9eb2a3bdca11aaf7d36eabce4).
- Предотвращено отличие путей-пререквизитов от путей выполнения, [50156f0](https://github.com/ytsaurus/ytsaurus/commit/50156f0aa5382824a725ab27d3d1b2dc6210293b).
- Поддержана непрозрачность атрибута `@schema`, [4f2c6ad](https://github.com/ytsaurus/ytsaurus/commit/4f2c6ad11f2bc951642546e33b790705911cee3d).
- Разрешено удаление мастер-ячейки без простоя других компонентов (кроме мастеров), [16bd5ba](https://github.com/ytsaurus/ytsaurus/commit/16bd5baf39e018085f42d8776dffefe16e648d27).
- Представлен новый конвейер для операций копирования между ячейками, [e7eea1e](https://github.com/ytsaurus/ytsaurus/commit/e7eea1ed49fb49d12d64639161e33943a4e06fa4).
- Добавлен `TSnapshotLoadContextGuard` для предоставления доступа к режиму только для чтения во время загрузки слепка, [ee94027](https://github.com/ytsaurus/ytsaurus/commit/ee9402746c38adf4b23d5166cc4f41fdc01c4f20).
- Добавлена проверка расширений чанков агрегированного писателя метаданных во время merge-джобов мастера, [de86b64](https://github.com/ytsaurus/ytsaurus/commit/de86b64ca13ed72993e83e5e6bccedcb25fff0ef).
- Снижено потребление памяти `TTableNode` за счёт оптимизации перечислений и полей реквизиции чанков, [90226b0](https://github.com/ytsaurus/ytsaurus/commit/90226b0af60dffa15565e7f4642c0e0482038c96).
- Локальный исполнитель чтения всегда включён, [521af69](https://github.com/ytsaurus/ytsaurus/commit/521af696a57c69c7b19855b310dacae1fce7e6a9).
- Переработаны атрибуты `@hunk_primary_medium` и `@hunk_media`:
  - Теперь они могут быть nullable.
  - Hunk-чанки могут размещаться на другом медиуме, чем чанки таблицы.
  - Репликация учитывается репликатором чанков и исходным писателем.
  - Поведение при установке и очистке теперь чётко определено, [508e000](https://github.com/ytsaurus/ytsaurus/commit/508e000430a6637a030c1575c7963efe1e37723a).
- Добавлена опция `snapshot-dump-scope-filter` для ограничения вывода при дампе слепка мастера, [8a73474](https://github.com/ytsaurus/ytsaurus/commit/8a734741b3b893596562e8b0968a82983679ea41).
- Стратегия размещения two-random-choices для целей записи включена по умолчанию, [1a153fb](https://github.com/ytsaurus/ytsaurus/commit/1a153fb75b539f97bb3534cbd2a24b5405c4c91c).
- Введён Sequoia response keeper для отслеживания ответов, [f886fa1](https://github.com/ytsaurus/ytsaurus/commit/f886fa13b8efa2c0959587a6e0bc6f5a5541a846).
- Добавлен режим `checksum` в CLI-команду `dump-snapshot` для отладки расхождений слепков, [5daa913](https://github.com/ytsaurus/ytsaurus/commit/5daa91303d7210689ecb117614b908523bd429af).
- Включено вычисление нематериализованных вычисляемых колонок в статических таблицах, [221672d](https://github.com/ytsaurus/ytsaurus/commit/221672d07a7b81987beff311ec22319d631a8278).

##### Исправления и оптимизации:
- Исправлена ошибка валидации при отличии путей-пререквизитов от путей выполнения, [76ceba0](https://github.com/ytsaurus/ytsaurus/commit/76ceba0c03c5fce8bc042dcb3726a3997a2153d5).
- Предотвращено создание пользователями таблиц, являющихся индексами самих себя, [f384a9f](https://github.com/ytsaurus/ytsaurus/commit/f384a9f9ea286f4cc4b018498bca6596664c0c85).
- Исправлен репликатор чанков для корректной обработки переопределений репликации для конкретного медиума, [2e0ca64](https://github.com/ytsaurus/ytsaurus/commit/2e0ca6446ad66eeba31e4c3a2b91d56bf11c0293).
- Исправлено обновление реквизиции чанков во время слияния, [29cc496](https://github.com/ytsaurus/ytsaurus/commit/29cc4968ed660253c6b8167993a67684a716eb9f).
- Исправлена проблема дублирующейся валидации путей, [3beea00](https://github.com/ytsaurus/ytsaurus/commit/3beea00256a370a74e5a9c1cb4b56e437a4159ad).
- Исправлена проверка статистики мастера при удалении ячейки, [2e93998](https://github.com/ytsaurus/ytsaurus/commit/2e9399802d02177cc0cd82069da18e09f5ade73a).
- Исправлена обработка переопределения репликации в репликаторе чанков (дублирующее исправление), [8c711f8](https://github.com/ytsaurus/ytsaurus/commit/8c711f8b07d3f3ee55b7b4fb503b736072b66cdb).
- Исправлен ID группы для встроенной группы `admins`, [f687614](https://github.com/ytsaurus/ytsaurus/commit/f687614331b358448d59af9438dfb7398a4909e0).
- Исправлен сбой, вызванный разыменованием нулевого указателя в `HydraCreateForeignObject`, [8ba1cab](https://github.com/ytsaurus/ytsaurus/commit/8ba1cabb0ccf6c958374ea2fe4948762609a1823).
- Удалён устаревший код шардов ZooKeeper, [c0482a7](https://github.com/ytsaurus/ytsaurus/commit/c0482a74746e10d16900608dfdf7c4455a81a751).
- Исправлены обновления статуса надёжности для exec-нод, [4e9becf](https://github.com/ytsaurus/ytsaurus/commit/4e9becf2dc2992ad8a714dcdcf404f1ba40758bf).
- Исправлена гонка между коммитом транзакции и очисткой экспортированных объектов, [5b72aad](https://github.com/ytsaurus/ytsaurus/commit/5b72aad7cafe1f0b44dd76acced1a52ecd4e7264).
- Устранены ложные алерты, вызванные безвредными гонками экспортированных объектов, [8a80023](https://github.com/ytsaurus/ytsaurus/commit/8a80023a50661a42abba895aacda70d118adf845).
- Исправлено падение при установке YSON-словаря с дублирующимися ключами в пользовательский атрибут, [867354b](https://github.com/ytsaurus/ytsaurus/commit/867354bffa92a4a9991d360c770e1219c4c12f81).
- Исправлена логика сравнения строк в валидации shallow merge, [404a790](https://github.com/ytsaurus/ytsaurus/commit/404a790b962ee26f1a4c2085d5bfc8b223ff6199).
- Очищены отложенные перезапуски контейнеров во избежание проблем с повторной инициализацией, [1f0d9f7](https://github.com/ytsaurus/ytsaurus/commit/1f0d9f7bccc8faae5302f68e32589f1d1a963068).
- Исправлено падение при чтении атрибута `@local_scan_flags`, [d8743cb](https://github.com/ytsaurus/ytsaurus/commit/d8743cbae113dc99a3c53612f253e45c8eab4b08).
- Исправлены недетерминированные сообщения об ошибках, вызванные неупорядоченными полями YSON, [d907ada](https://github.com/ytsaurus/ytsaurus/commit/d907ada9984b6b711e4d5dd02d36d9e333df5dbb).
- Исправлена проблема совместимости с мнимыми локациями чанков, [f591951](https://github.com/ytsaurus/ytsaurus/commit/f591951182e9555c7ca58173ec14a75e7b6d41a7).

#### Прочее

##### Новые возможности и изменения:
- Добавлено отслеживание использования памяти для журналирования, [f0351fa](https://github.com/ytsaurus/ytsaurus/commit/f0351fa2aa2278c7ac804c93074e91d70e724138).
- Добавлено отслеживание использования памяти для кэша реплик чанков, [aa5053d](https://github.com/ytsaurus/ytsaurus/commit/aa5053d5b243a3356d95388fb51094e2ed43ea68).
- Добавлено отслеживание использования памяти для сенсоров, [8396ecd](https://github.com/ytsaurus/ytsaurus/commit/8396ecddde795abc6b52642c07fbe56b5b76581d).
- Введена поддержка запуска нескольких демонов в одном процессе `ytserver` (режим multidaemon), [2986da8](https://github.com/ytsaurus/ytsaurus/commit/2986da8386705b72d1f2de8b4a6a9de21b4c05ea).
- Добавлена санитизация меток мониторинга, [5bebc7a](https://github.com/ytsaurus/ytsaurus/commit/5bebc7aabdc9439f49e63b8a9104e8256a273940).
- Добавлена поддержка типа `decimal256` (точность до 76 знаков), включая формат Skiff как `int256`, [implicit].
- Добавлена опция `message_level_overrides` в конфиг журналирования для тонкого управления во время выполнения, [4e13e2a](https://github.com/ytsaurus/ytsaurus/commit/4e13e2a32db1cb46272330c131c1c4ca3f50994d).

##### Исправления и оптимизации:
- Исправлена ошибка, из-за которой тяжёлые RPC-запросы, поставленные в очередь, могли некорректно переиспользовать хранилище (например, trace context) от несвязанных запросов, [6bd19a0](https://github.com/ytsaurus/ytsaurus/commit/6bd19a014d512d4d5f2bcac54ae2087f65cce3f9).
- Улучшено отслеживание использования памяти row cache, [122fd89](https://github.com/ytsaurus/ytsaurus/commit/122fd89ce22e6eb1f6cb48672463a0c264af9069).
- Счётчики гистограмм теперь используют 64-битные значения, [94fe6d3](https://github.com/ytsaurus/ytsaurus/commit/94fe6d36acebe32f2609ff1b7be7547dbeeaa446).
- Исправлено распространение `TLargeColumnarStatisticsExt` в MAW, [bee2ba3](https://github.com/ytsaurus/ytsaurus/commit/bee2ba31007789d05a6799a7e38add72dab6a8b7).
- Исправлена обработка неожиданного состояния в логике heartbeat'ов узла данных, [2e16721](https://github.com/ytsaurus/ytsaurus/commit/2e16721f4f2aec16089ee6eae580e8bca03347ba).
- Корректно обрабатываются отключения пиров в NBD-сервере, [b1f2acf](https://github.com/ytsaurus/ytsaurus/commit/b1f2acf7363a9e5dfc123f2cdf60483d3e9a37cb).
- Очередь NBD теперь использует выделенный пул потоков, настраиваемый через динамический конфиг, [5e9c7fe](https://github.com/ytsaurus/ytsaurus/commit/5e9c7fe3167aa81403b0b4f8ced3cafd0ad544a2).



{% endcut %}


{% cut "**24.2.1**" %}

**Дата релиза:** 2025-07-28


**Страница релиза:** [24.2.1](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/ytsaurus/24.2.1)


**Docker-образ:** [ghcr.io/ytsaurus/ytsaurus:stable-24.2.1](https://github.com/orgs/ytsaurus/packages/container/ytsaurus/472503591?tag=stable-24.2.1)


_Для установки YTsaurus Server 24.2.1 [обновите](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release%2F0.22.0) k8s-operator до версии 0.22.0._

#### Прокси
Возможности:
- Поддержка HTTP-прокси в обработчике `discover_proxies`, [0dc02db](https://github.com/ytsaurus/ytsaurus/commit/0dc02db399a7e3e6255f8716102add5bf404bd39).
- Поддержка полной результирующей таблицы в результатах YQL-запросов, [2d7b0d3](https://github.com/ytsaurus/ytsaurus/commit/2d7b0d3e761361ff3157e7a36465ee79478ab3c7).

Исправления:
- Установка атрибута `treat_as_queue_producer=%true` при создании `queue_producer`, [1ee68e1](https://github.com/ytsaurus/ytsaurus/commit/1ee68e1fa7409d125605ba4047f655532f42f6ee).
- Исправление обработчика `PartitionTables` для упорядоченного режима, [100bdc4]( https://github.com/ytsaurus/ytsaurus/commit/100bdc425c787902212c041822ef9557ebb7b932).

#### Мастер
Исправления:
- Исправление сбоя в `GetIteratorOrCrash` в `TChunkMerger::HydraFinalizeChunkMergeSessions`, [5eda095]( https://github.com/ytsaurus/ytsaurus/commit/5eda0952c2a3453d853a83c24b16b3a5d7f31d49).
- Исправление зависаний Object Service, [31e2dfd](https://github.com/ytsaurus/ytsaurus/commit/31e2dfd7dbe7ad001f11d30fa1c59bc7a7a9ca21).
- Исправление гонки данных на cache cookies в Object Service, [1d79d8d](https://github.com/ytsaurus/ytsaurus/commit/1d79d8d37d81438389a5280c595e8254cfc6f678).

#### Queue Agent
Возможности:
- Добавлен флаг для выбора реализации экспорта очередей, [3ec2e69](https://github.com/ytsaurus/ytsaurus/commit/3ec2e6945cf0af0f64d16e94ee5aa66e902abbf1).
Исправления:

Исправления:
- Исправление скрипта `init_queue_agent_state` для корректной обработки случаев перезапуска, [ae43add](https://github.com/ytsaurus/ytsaurus/commit/ae43add3ecd560c1f49fc41b40a2b0bd6b1f402f).
- Исправление возможной потери данных при нескольких экспортах для одной очереди из-за некорректного объединения прогрессов экспорта очередей, [c44e13b](https://github.com/ytsaurus/ytsaurus/commit/c44e13b5271ff91c5860c91448fbd704924ddf35).

#### Tablet Balancer
Исправления:
- Исправление пересчёта отклонения при поиске правого индекса в параметризованном merge, [b7df500](https://github.com/ytsaurus/ytsaurus/commit/b7df5009926f4d0386fa924902da46e71e63bb54)

#### Прочее
Возможности:
- Поддержка `ytprof` для всех компонентов.

{% endcut %}


{% cut "**24.2.0**" %}

**Дата релиза:** 2025-03-19


**Страница релиза:** [24.2.0](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/ytsaurus/24.2.0)


**Docker-образ:** [ghcr.io/ytsaurus/ytsaurus:stable-24.2.0](https://github.com/orgs/ytsaurus/packages/container/ytsaurus/377574644?tag=stable-24.2.0)


_Для установки YTsaurus Server 24.2.0 [обновите](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release%2F0.22.0) k8s-operator до версии 0.22.0._

---
#### Известная проблема
- Расширение кластера новыми мастер-селлами временно отключено из-за ошибки. Проблема будет устранена в следующей версии 25.1.

---

#### Планировщик и GPU
Новые возможности и изменения:
- Добавлена поддержка управления доступом на основе ACO в операциях.
- Добавлен метод `get_job_trace` в API джобов.
- Добавлена возможность завершить операцию ошибкой, если она запущена в несуществующем пуле.
- Расширены параметры конфигурации для выгрузки операций в пуловые деревья.

Исправления и оптимизации:
- Исправлены проблемы планирования новых аллокаций сразу после приостановки операции.
- Оптимизировано обновление fair share в управляющем потоке.

#### Queue Agent
Новые возможности и изменения:
- Экспорт очередей теперь учитывается при обрезке очередей реплицированных и chaos-реплицированных таблиц.
- Добавлена суммарная агрегация для метрик lag в партициях потребителей.
- Рефакторинг экспорта очередей: добавлены повторные попытки и ограничение частоты запросов.

Исправления и оптимизации:
- Исправлена возможная приостановка контроллера очередей за счёт добавления таймаута к запросам `GetOrderedTabletSafeTrimRowCount`.
- Исправлены параметры блокировки при получении разделяемой блокировки на каталог экспорта.
- Устранены проблемы с истечением срока действия клиентов для chaos-очередей и потребителей при изменении подключения к кластеру.

#### Прокси
Новые возможности и изменения:
- Поддержка формата YAML для структурированных данных. Подробнее в RFC: [Поддержка YAML-формата](https://github.com/ytsaurus/ytsaurus/wiki/%5BRFC%5D-YAML-format-support).
- Добавлен конфигурационный флаг create_user_if_not_exists для отключения автоматического создания пользователей при OAuth-аутентификации. [Issue](https://github.com/ytsaurus/ytsaurus/issues/930).
- Добавлен параметр `cache_key_mode` для управления гранулярностью кэширования учётных данных.
- Реализован новый обработчик для получения событий job trace.

Исправления и оптимизации:
- Обработчик `discover_proxies` теперь возвращает ошибку, когда тип прокси — `http`.
- Исправлено переполнение буфера в куче в парсере Arrow.
- При недостаточном объёме памяти для обработки RPC-ответов теперь возвращается повторяемая ошибка `Unavailable` вместо неповторяемой `MemoryPressure`.
- Оптимизирован метод `concatenate`.

#### Kafka-прокси
Представлен новый компонент: Kafka-прокси. Эта MVP-версия обеспечивает интеграцию с очередями YTsaurus по протоколу Kafka. Сейчас поддерживается минимальный API для записи в очереди и чтения с балансировкой нагрузки через потребительские группы.

#### Динамические таблицы
Новые возможности и изменения:
- Представлена Versioned Remote Copy для таблиц с hunks.

Исправления и оптимизации:
- Исправлены проблемы со вторичными индексами в мультиселловых кластерах (особенно больших).
- Повышены стабильность и производительность chaos-репликации.
  
#### MapReduce
Новые возможности и изменения:
- Запрещён cluster_connection в операциях remote copy.
- Представлена однокластерная телепортация для операций auto-merge.
- Поддерживается объединение таблиц с совместимыми (не обязательно идентичными) схемами.
- Выполнен рефакторинг кода в рамках подготовки к внедрению gang-операций.
- Выполнен рефакторинг кода для поддержки повторного использования аллокаций на уровне джобов.
- Улучшено журналирование каталога для каждого джоба в режиме job-proxy.

Исправления и оптимизации:
- Оптимизировано получение ресурсов джобов в exec-узлах.
- Исправлены случаи потери метрик в exec-узлах.

#### Мастер-сервер
Новые возможности и изменения:
- Добавлена возможность получения схем входных/выходных таблиц из внешней ячейки по ID схемы.
- Тип узла list объявлен устаревшим; его создание теперь запрещено.
- Представлена новая стратегия распределения целей записи на основе алгоритма «два случайных выбора».
- Реализован автоматический механизм отключения репликации на дата-ноды в вышедших из строя дата-центрах. Его можно настроить в `//sys/@config/chunk_manager/data_center_failure_detector`.
- Добавлена пессимистическая проверка увеличения использования ресурсов при смене основного медиума.
- Запрещены некоторые erasure-кодеки в операциях remote copy.
- Добавлен атрибут групп узлов для узла

Исправления и оптимизации:
- Исправлена гонка между коммитом координатора транзакций и отменой ссылки на ячейку для экспортированных объектов, [8d6721a](https://github.com/ytsaurus/ytsaurus/commit/8d6721a16bb6a1bc26c9f0d1dc5506f32635e6b6).
- Исправлено ручное объединение узлов Кипариса для транзакций планировщика, [f87a2ad](https://github.com/ytsaurus/ytsaurus/commit/f87a2ad466c2352be9ba7bfee6e7d93796a9eb6a).
- Исправлен сбой мастера при установке YSON-словаря с дублирующимися ключами в пользовательском атрибуте, [0cfad80](https://github.com/ytsaurus/ytsaurus/commit/0cfad80f415c23233ca748e345cd9af91169f4c3).
- Исправлено сравнение строк при проверке мелкого объединения для предотвращения сбоев джобов, [3c282d4](https://github.com/ytsaurus/ytsaurus/commit/3c282d4e9f50aa00d861b7a6f1ca388fea18e51d).
- Исправлен сбой при чтении атрибута `@local_scan_flags`, [5b4c954](https://github.com/ytsaurus/ytsaurus/commit/5b4c954c09ac6e1adc55aa6a5d7baff8f894fb61).
- Исправлена недетерминированная загрузка полей YSON-структур, которая могла вызывать алерт «state hashes differ» из-за несогласованных сообщений об ошибках при отсутствии нескольких обязательных полей, [0553e21](https://github.com/ytsaurus/ytsaurus/commit/0553e2182a0df502592abdd1fcd8ac3c6afd64ad).
- Исправлена проблема, при которой узлы, удерживаемые транзакциями, мешали очистке, запускаемой атрибутом `expiration_time`.
- Исправлена ошибка, из-за которой ломались метрики аккаунта при добавлении нового аккаунта.
- Исправлена ошибка в управлении доступом на основе атрибутов, из-за которой всегда вычислялась первая запись.
- Исправлена проблема, из-за которой фолловеры Hydra могли навсегда зависнуть после потери мутации.
- Ограничено количество списков чанков на сессию слияния чанков для предотвращения перегрузки мастера.
- Исправлена некорректная проверка состояния узла в процессе удаления.
- Улучшена передача инкрементальных heartbeat-сообщений для предотвращения зависания чанков в очереди уничтожения.
- Оптимизирован механизм слияния чанков за счёт сокращения ненужных обновлений заявок.



{% endcut %}

{% cut "**24.1.0**" %}

**Дата релиза:** 2024-11-07


**Страница релиза:** [24.1.0](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/ytsaurus/24.1.0)


**Docker-образ:** [ghcr.io/ytsaurus/ytsaurus:stable-24.1.0](https://github.com/orgs/ytsaurus/packages/container/ytsaurus/304107085?tag=stable-24.1.0)


_Чтобы установить YTsaurus Server 24.1.0, [обновите](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases) k8s-operator до версии 0.17.0._

#### Планировщик и GPU
Возможности и изменения:
- Поддержка приоритизации пулов при корректировке сильных гарантий из-за недостатка суммарных ресурсов кластера.
- Поддержка приоритизации операций на этапе назначения модулей в алгоритме планирования GPU.
- Поддержка ограничений на потребность в ресурсах джобов для каждого пул-дерева.
- Добавление пользовательского TTL для джобов в архиве операций.
- Добавление сбора трейсов пользовательских джобов в формате Trace Event Format.
- Рефакторинг конфигурации и Orchid exec-нод.
- Логическое разделение джобов и аллокаций.
- Добавление настраиваемого размера буфера входных данных в джобах для более эффективных прерываний.

Исправления и оптимизации:
- Исправление трассировки heartbeat-ов exec-нод в планировщике и контроллер-агентах.
- Оптимизация общего алгоритма планирования аллокаций и вычисления fair share.
- Оптимизация обработки heartbeat-ов планировщик <-> CA и exec-нода <-> CA.

#### Queue Agent
Возможности:
- Статический экспорт очередей теперь обрабатывается так же, как и витальный потребитель при обрезке очередей, поэтому неэкспортированные строки не будут обрезаться.
- Добавление функциональности для бана экземпляров queue agent через атрибут Кипариса.
- Использование суммарного веса данных и временной метки из меты потребителя для метрик потребителя.


Исправления:
- Исправление ошибки в обработке очередей/потребителей с невалидными атрибутами (например, `auto_trim_config`).
- Исправление видимости алармов из атрибута `@queue_status`.
- Больше не игнорируются потребители, находящиеся выше размера очереди.
- Переименование `write_registration_table_mapping` -> `write_replicated_table_mapping` в динамическом конфиге.
- Использование общей блокировки вместо эксклюзивной для каталогов назначения при статическом экспорте.

#### Прокси
Возможности:
- Реализация обработчиков queue producer для отправки в очереди ровно один раз (`PushQueueProducer`, `CreateQueueProducerSession`).
- Добавление обработчика типа объекта `queue_consumer` и `queue_producer`, чтобы их можно было создавать без явного указания схемы. Пример: `yt create queue_consumer <path>`.
- Поддержка повторов при копировании между клетками.
- Добавление типов float и date в формате Arrow.
- Добавление отслеживания памяти для запросов `read_table`.
- Отклонение тяжелых запросов при нехватке памяти.
- Отправка метрик `bytes_out` и `bytes_in` во время выполнения запроса.
- Сохранение `cumulative_data_weight` и `timestamp` в мете потребителя.
- Переименование `PullConsumer` -> `PullQueueConsumer` и `AdvanceConsumer` -> `AdvanceQueueConsumer`. Старые обработчики пока продолжают существовать для обратной совместимости.

CHYT:
- Добавление авторизации через HTTP-заголовок X-ClickHouse-Key.
- Добавление липкого распределения запросов на основе session id/sticky cookie.
- Добавление нового http-обработчика "/chyt" для запросов chyt (обработчик "/query" устарел, но продолжает работать для обратной совместимости).
- Добавление возможности выделить отдельный порт для нового http-обработчика, чтобы поддерживать запросы без пользовательского пути в URL.
- Алиас клики может быть указан через параметры "chyt.clique_alias" или "user" (только для новых обработчиков).
- HTTP GET запросы теперь только для чтения для совместимости с ClickHouse (только для новых обработчиков).

Исправления:
- Заполнение типа индекса словарного кодирования в формате Arrow.
- Исправление null, void и опциональных композитных колонок в формате Arrow.
- Исправление метрик `yt.memory.heap_usage`.

#### Динамические таблицы
Возможности:
- Вторичные индексы: базовые, частичные, list и уникальные.
- Оптимизация запросов, которые группируют и сортируют по одним и тем же ключам.
- Балансировка таблетов с использованием коэффициента загрузки (требуется отдельный балансировщик таблетов).
- Общая блокировка на запись — запись в одну строку из разных транзакций без блокировки.
- Балансировщик клиента Rpc-прокси на основе алгоритма выбора из двух случайных вариантов.
- Словарь сжатия для Hunks и hash-индекса.
  
#### MapReduce
Возможности:
- Поддержка входных таблиц из удаленных кластеров в операциях.
- Улучшение контроля над тем, как данные разбиваются на джобы для приложений ML-обучения.
- Поддержка чтения по последней временной метке в операциях MapReduce над динамическими таблицами.
- Меньше информации о конфигурации раскрывается потенциальному атакующему.

Исправления:
- Исправление телепортации одного чанка в неупорядоченном пуле.
- Исправление дисконнекта агента при удалении аккаунта.
- Исправление вывода промежуточных схем для входных данных с фильтрами по колонкам.
- Исправление падения контроллер-агента на несовместимых путях пользовательских статистик.

Оптимизации:
- Добавление JobInputCache: кэш в памяти на exe-нодах, хранящий данные, читаемые несколькими джобами, работающими на одной ноде.

#### Мастер-сервер

Возможности:
- Данные персистентности Hydra для таблет-клеток теперь по умолчанию в основном хранятся в новом месте `//sys/hydra_persistence`. Двойственность с предыдущим местом хранения (`//sys/tablet_cells`) будет устранена в будущих релизах.
- Поддержка наследования `@chunk_merger_mode` после копирования в каталог с установленным `@chunk_merger_mode`.
- Добавление повторного планирования с backoff для нод, объединенных chunk merger-ом, в случае временного сбоя при их объединении.
- Добавление опции использования алгоритма двух случайных вариантов при выделении целей для записи.
- Добавление команды add-maintenance в CLI.
- Поддержка link-нод между шардами внутри клетки.
- Проброс пользователя транзакции в реплики транзакции для корректного учета времени CPU, затраченного на их коммит или аборт.
- Динамическое распространение информации о новых мастер-клетках на другие компоненты кластера и сокращение времени простоя при добавлении новых мастер-клеток.

Оптимизации:
- Снижение потребления памяти мастер-сервером за счет уменьшения размера table-нод.
- Ускорение джобов удаления на data-нодах.
- Вынос сервиса трекера exec-нод из потока автомата.
- Не-data-ноды теперь удаляются немедленно (вместо удаления по локациям) и независимо от data-нод.
- Вынос вызовов запросов репликации транзакций из потока автомата.

Исправления:
- Исправление разыменования nullptr при разрешении атрибутов queue agent и yql agent.
- Учет переопределения медиума в IO-движке при перезапуске ноды.
- Исправление режима ребалансировки в чанковом дереве таблицы после слияния ветвящихся таблиц.
- Исправление санитизации hostname-ов в ошибках для cellar-нод.
- Исправление потери контекста трейса для некоторых колбэков и rpc-вызовов.
- Исправление персистентности атрибута `@last_seen_time` для пользователей.
- Исправление обработки неизвестных расширений меты чанка мета-агрегирующим писателем.
- Исправление падения нод при повторах heartbeat-ов, когда мастера недоступны длительное время.
- Исправление несогласованности статистики таблицы между нативными и внешними клетками после копирования таблицы в середине обновления статистики.
- Исправление случайной потери логического веса запроса в проксирующем chunk-сервисе.
- Исправление редкого падения при экспорте чанка.
- Исправление иногда возникающего зависания лизинговых транзакций таблет-клеток.
- Повторы нативного клиента теперь более надежны.
- Исправление хостинга чанков первичной клетки для мультиклеточности.
- Исправление падения, связанного с началом эпохи инкамбенси до завершения восстановления.
- Перезапуск выборов, если хранилище журнала изменений для голосующего пира заблокировано в режиме только для чтения (исправление Hydra для таблет-нод).
- Исправление падения при отсутствии схемы при импорте чанка.
- Исправление падения, связанного с перезапуском эпохи, в трекере истечения срока действия.
- В каталоге мастер-клеток теперь выводится аларм о неизвестной роли клетки вместо падения.

#### Прочее
Возможности:
- Добавление возможности перенаправлять stdout в stderr в пользовательских джобах (опция `redirect_stdout_to_stderr` в спеке операции).
- Добавление писателя в журнал для динамических таблиц.

{% endcut %}


{% cut "**23.2.1**" %}

**Дата релиза:** 2024-07-31


**Страница релиза:** [23.2.1](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/ytsaurus/23.2.1)


**Docker-образ:** [ghcr.io/ytsaurus/ytsaurus:stable-23.2.1](https://github.com/orgs/ytsaurus/packages/container/ytsaurus/251277095?tag=stable-23.2.1)


#### Планировщик и GPU
Возможности:
  * Отключение записи `//sys/scheduler/event_log` по умолчанию.
  * Добавление облегченных запущенных операций.

Исправления:
  * Различные оптимизации в планировщике.
  * Улучшение профилирования суммарного использования ресурсов и лимитов.
  * Время подготовки джоба больше не учитывается в статистике GPU.

#### Queue Agent
Исправления:
  * Нормализация имени кластера при регистрации потребителя очереди.

#### Прокси
Возможности:
  * RPC proxy API для Query Tracker.
  * Изменен формат и добавлены метаданные для выданных пользовательских токенов.
  * Поддержка ротации TLS-сертификатов для HTTP-прокси.
  * Совместимость с последним релизом Query Tracker.

Исправления:
  * Отсутствие повторов при ошибке ответа Read-Only.
  * Исправление отзыва отдельного токена аутентификации.
  * Исправление отслеживания памяти на пользователя (проброс тегов аллокации в дочерний контекст).
  * Исправление формата Arrow для опциональных типов.

#### Динамические таблицы
Возможности:
  * Общие блокировки на запись.
  * Увеличение максимального количества ключевых колонок до 128.
  * Реализован array join в YT QL.

Исправления:
  * Ограничение времени отставания реплики для таблиц, в которые редко пишут.
  * Исправление возможной потери записей журнала при прерывании журнальной сессии.
  * Исправление в backup manager.
  * Исправление некоторых ошибок в chaos-репликации динамических таблиц.
  
#### MapReduce
Возможности:
  * Объединенные троттлеры на локацию, ограничивающие суммарную входящую и исходящую полосу пропускания.
  * Опции в спеке операции для принудительного ограничения памяти для контейнеров пользовательских джобов.
  * Использование codegen-компаратора в SimpleSort и PartitionSort, если это возможно.

Исправления:
  * Улучшенные теги профилирования для метрик job-прокси.
  * Исправления для удаленного копирования с erasure-восстановлением.
  * Исправление any_to_composite конвертера, когда несколько схем имеют одинаково названные композитные колонки.
  * Исправления для API-метода partition_table.
  * Исправления в новом live preview.
  * Джобы больше не завершаются с ошибкой при сбоях связи с супервизором.
  * Добавлены множественные повторы в интеграции CRI executor/docker image.
  * Упорядочен сбор статистики памяти джобов, переименованы некоторые статистики.

#### Мастер-сервер
Возможности:
  * Параллелизация и вынос чтений виртуальных карт.
  * Аварийный флаг для отключения контроля доступа на основе атрибутов.
  * Улучшена производительность коммита/аборта транзакций.
  * Включена загрузка снепшотов по умолчанию.

Исправления:
  * Исправления и оптимизации для управления репликами чанков Sequoia.
  * Исправление нескольких возможных падений мастера.
  * Исправления для обновления мастера с доступностью только для чтения.
  * Исправления для зависших инкрементальных heartbeat-ов и потерянных обновлений реплик на отключенных локациях.
  * Исправление сенсоров на аккаунт при создании нового аккаунта.

#### Прочее
Возможности:
  * Предоставление конфигурации через orchid стало опциональным.
  * Поддержка некоторых опций c-ares в конфиге YT.
  * Поддержка IP-адресов при проверке TLS-сертификатов в RPC.

Исправления:
   * Исправление утечки счетчика соединений в http-сервере.
   * Отслеживание и ограничение памяти, используемой поставленными в очередь RPC-запросами.
   * Улучшенное отслеживание памяти для буферов RPC-соединений.
   * Исправление конфигурации резолвера адресов.


{% endcut %}

{% cut "**23.2.0**" %}

**Дата релиза:** 2024-02-29

**Страница релиза:** [23.2.0](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/ytsaurus/23.2.0)

**Docker-образ:** [ghcr.io/ytsaurus/ytsaurus:stable-23.2.0](https://github.com/orgs/ytsaurus/packages/container/ytsaurus/222908760?tag=stable-23.2.0)

#### Планировщик

Множество внутренних изменений, связанных с разработкой новой механики планирования, которая разделяет джобы и выделение ресурсов на исполняющих узлах (exec nodes). Эти изменения включают модификацию протокола взаимодействия между планировщиками, контроллерами и исполняющими узлами, а также добавление большого объёма новой логики для введения аллокаций в исполняющих узлах, контроллерах и планировщиках.

Список значимых изменений и исправлений:
  - Оптимизирована производительность потоков Control и NodeShard планировщика.
  - Оптимизирована производительность основного алгоритма планирования за счёт учёта только подмножества операций в большинстве хартбитов узлов.
  - Оптимизированы накладные расходы на запуск операции за счёт отказа от создания отладочной транзакции, если не заданы таблицы stderr или core.
  - Добавлено приоритетное планирование для пулов с гарантиями ресурсов.
  - Учтено использование диска в алгоритме вытеснения джобов.
  - Добавлено вытеснение по назначению модуля операции в алгоритме планирования GPU-сегментов.
  - Добавлены исправления для алгоритмов планирования GPU.
  - Добавлено троттлинг хартбитов узлов по сложности планирования.
  - Добавлено троттлинг длительности выполнения параллельно планируемых джобов.
  - Повторное использование дескрипторов мониторинга джобов в рамках одной операции.
  - Поддержка дескрипторов мониторинга в map-операциях.
  - Поддержка фильтрации джобов по дескрипторам мониторинга в команде `list_jobs`.
  - Исправлено отображение джобов, исчезающих из-за сбоя узла, как выполняющихся и «устаревших» в UI.
  - Улучшена настройка эфемерных субпулов.
  - Скрытие пользовательских токенов в журналах планировщика и job proxy.
  - Поддержка настраиваемой максимальной пропускной способности для каналов между job proxy и пользовательским джобом.

#### Queue Agent

Помимо небольших улучшений, наиболее значимые возможности включают настройку периодического экспорта партиционированных данных из очередей в статические таблицы и поддержку использования реплицированных и chaos динамических таблиц в качестве очередей и потребителей.

Возможности:
- Поддержка chaos-реплицированных таблиц в качестве очередей и потребителей.
- Поддержка snapshot-экспорта из очередей в статические таблицы.
- Поддержка очередей и потребителей, являющихся символическими ссылками на другие очереди и потребителей.
- Поддержка обрезки строк в очередях по времени жизни.
- Поддержка регистрации и отмены регистрации потребителя в очереди из другого кластера.

Исправления:
- Обрезка очередей по `object_id`, а не по `path`.
- Исправлены метрики веса данных прочитанных строк через потребителя.
- Исправлена обработка замороженных таблетов в очереди.

#### Прокси

Возможности:
- Добавлена возможность вызова `pull_consumer` без указания `offset`, который будет взят из таблицы `consumer`.
- Добавлен обработчик `advance_consumer` для очередей.
- Ранняя реализация формата `arrow` для чтения и записи статических таблиц.
- Поддержка преобразования типов для внутренних полей сложных типов.
- Добавлены новые сенсоры мониторинга использования памяти на пользователя в RPC-прокси.
- Использование ACO для управления правами доступа в RPC-прокси.
- Представлены TCP-прокси для SPYT.
- Поддержка OAuth-авторизации.

Исправления:
- Исправлен возврат запрошенных системных колонок в формате `web_json`.

#### Динамические таблицы

Возможности:
- Улучшения языка запросов динамических таблиц:
    - Новый выводитель диапазонов (range inferrer).
    - Добавлены различные SQL-операторы (<>, length, ||, yson_length, argmin, argmax, coalesce).
- Добавлены бэкапы для таблиц с ханками.
- Новый threadpool с fair share для оператора select и сети.
- Добавлена фильтрация по части ключа для range-выборок.
- Добавлен контроллер перегрузки.
- Более равномерное распределение нагрузки между RPC-прокси.
- Добавлены метрики размера для каждой таблицы.
- Хранение тяжёлых метаданных чанков в блоках.

#### MapReduce

Возможности:
- RemoteCopy теперь поддерживает файловые объекты Кипариса, помимо таблиц.
- Добавлена поддержка экспериментов для отдельных джобов.
- Ранняя реализация CRI (container runtime interface) окружения джобов и поддержка внешних docker-образов.
- Новый live preview для выходных таблиц MapReduce.
- Добавлена поддержка arrow как входного формата для MapReduce.
- Поддержка GPU-ресурсов в исполняющих узлах и планировщиках.

Улучшения:
- Улучшено отслеживание памяти в узлах данных (master jobs, сессии записи блобов, p2p-отслеживание).
- Переработан учёт памяти в контроллерах.

#### Мастер-сервер

Заметные или потенциально ломающие изменения:
  - Чтение запросов теперь по умолчанию обрабатывается в многопоточном режиме.
  - Режим «только чтение» теперь сохраняется между перезапусками. Для выхода из него следует использовать команду `yt-admin master-exit-read-only`.
  - Тип `list_node` устарел. Вместо него рекомендуется использовать `map_node` или `document`.
  - RPC-вызов `ChunkService::ExecuteBatch` устарел и разделён на отдельные вызовы. Пакетный chunk service заменён проксирующим chunk service.
  - Новые типы транзакционных объектов: `system_transaction`, `nested_system_transaction`. Поддержка транзакционных действий в обычных транзакциях Кипариса теперь устарела.
  - Версия 2 библиотеки Hydra теперь включена по умолчанию. Версия 1 официально устарела.

Возможности:
  - Теперь можно обновлять мастер-серверы без простоя на чтение, оставляя неголосующие пиры для обслуживания запросов на чтение, пока основной кворум находится на обслуживании.
  - Узел данных теперь можно пометить как ожидающий перезапуска, сообщая репликатору игнорировать его отсутствие в течение заданного времени, чтобы избежать ненужных всплесков репликации.
  - Команда `add_maintenance` теперь поддерживает HTTP- и RPC-прокси.
  - Управление доступом на основе атрибутов: пользователю может быть присвоен набор тегов, а запись списка контроля доступа (ACE) может содержать фильтр по тегам.

Оптимизации и исправления:
  - Response keeper теперь постоянный. Пиру не требуется период прогрева перед началом лидерства.
  - Метаданные чанков теперь включают схемы. Это открывает путь к ряду значительных оптимизаций.
  - Уменьшен размер хартбитов узлов данных.
  - Чанки и списки чанков теперь загружаются из снапшота параллельно.
  - Исправлено чрезмерное потребление памяти в мультиселловых конфигурациях.
  - Улучшен код учёта для корректной обработки неограниченных квот и предотвращения отрицательного использования памяти мастером.

Кроме того, достигнут прогресс в проекте Sequoia, посвящённом масштабированию мастер-сервера за счёт выноса части его состояния в динамические таблицы. (До продакшн-готовности пока далеко.)

#### Прочее

Улучшения:
- Добавлена динамизация конфигурации RPC-сервера.
- Добавлена поддержка альтернативного имени хоста пира для Bus TLS.
- Корректная обработка Content-Encoding в веб-сервере мониторинга.
- Возвращён атрибут «host» в ошибки.
- Добавлена поддержка опции --version в бинарниках ytserver.
- Добавлена дополнительная метаинформация в журналы сервера в формате yson/json (fiberId, traceId, sourceFile).

{% endcut %}
