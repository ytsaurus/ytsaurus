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
- Исправлено создание вторичного индекса в скрипте `init_queue_agent_state`, [9840c02](https://github.com/ytsaurus/ytsaurus/commit/9840c02ebe08e450b88ba00efa23c5af0a8b1eda)

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

- Автоматическая подстройка размера джобов для упорядоченных и сортированных типов и стадий операций: стадии `sorted_merge`, `ordered_merge`, `sorted_reduce`, `ordered_map`, `ordered_map` и `sorted_reduce` операции Map-Reduce.

- Поддержка ограничений `compressed_data_size` для MR-операций.

- Протокол распределённой записи для файлов.

- Поддержка `row_cache` для следующих за таблет-селлом пиров (в среде с несколькими пирами в таблет-селле).

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
- Устанавливается атрибут `treat_as_queue_producer=%true` при создании `queue_producer`, [4537011](https://github.com/ytsaurus/ytsaurus/commit/4537011fd63851d5c09441195ac43f675b63b30d).

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
[Документация](https://ytsaurus.tech/docs/ru/admin-guide/gpu).
- Добавлен bundle controller для управления бандлами таблеточных ячеек на небольших кластерах. Этот компонент распределяет таблеточные узлы по бандлам, управляет обслуживанием узлов и контролирует распределение CPU и памяти по таблеточным узлам. [Документация](https://ytsaurus.tech/docs/ru/admin-guide/bundle-controller).
- Добавлена поддержка multiproxy-режима в RPC-прокси. RPC-прокси (включая RPC Proxy в Job Proxy) могут быть настроены для работы с удалёнными кластерами. [Документация](https://ytsaurus.tech/docs/ru/admin-guide/multiproxy).

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
- Объявлен устаревшим `list_node`. Мастер-серверы теперь будут выдавать предупреждение в журнале (уровень alert) после загрузки слепка, если он содержит list-узел. Это поведение можно отключить с помощью опции `alert_on_list_node_load`. Рекомендуется перейти на другие типы и удалить или заменить все оставшиеся list-узлы. Если этого не сделать, мастер-сервер не запустится в следующем мажорном обновлении. В этот релиз включён скрипт, который должен помочь в миграции в большинстве случаев. Его можно найти в `yt/yt/scripts/master/replace_list_nodes`. Мы опубликовали статью в блоге, объясняющую причины отказа от этого типа и предлагающую другие методы миграции. Подробнее [здесь](https://ytsaurus.tech/ru/blog/ytsaurus-25-2-0#deprecation).

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
- Исправление для CVE-2023-33460: утечка памяти в yajl 2.1.0 при использовании функции `yajl_tree_parse`. [f7b9064](https://github.com/ytsaurus/ytsaurus/commit/f7b9064f099d977cdfb2644f4ef11a57745049f7)
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

{% cut "**25.4.2**" %}

**Release date:** 2026-09-23


## Overview

YTsaurus 25.4.2 is a patch release on top of 25.4.1. For the full list of changes, features and
migration notes see the [25.4.0 release notes](https://github.com/ytsaurus/ytsaurus/releases/tag/docker%2Fytsaurus%2F25.4.0).

YTsaurus Server 25.4.x is supported since 0.27.0 version of the Kubernetes operator.

## What's changed since 25.4.1

### Kafka Proxy

- Various fixes, [22e6287](https://github.com/ytsaurus/ytsaurus/commit/22e6287).

{% endcut %}


{% cut "**25.4.1**" %}

**Release date:** 2026-09-03


## Overview

YTsaurus 25.4.1 is a patch release on top of **25.4.0**. For the full list of changes, features and
migration notes see the [25.4.0 release notes](https://github.com/ytsaurus/ytsaurus/releases/tag/docker%2Fytsaurus%2F25.4.0).

YTsaurus Server 25.4.x is supported since [0.27.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release%2F0.27.0) version of the Kubernetes operator.

## What's changed since 25.4.0

### Dynamic Tables
- Bulk insert under user transaction is now enabled by default, [c6644b5](https://github.com/ytsaurus/ytsaurus/commit/c6644b5d24d).

### Scheduler
- Fix precision error when comparing operation demand with module capacity in scheduling segments, [c91fcbe](https://github.com/ytsaurus/ytsaurus/commit/c91fcbe2cd0).


{% endcut %}


{% cut "**25.4.0**" %}

**Release date:** 2026-07-29



## Overview

This document summarizes the key changes, improvements, features, and bug fixes introduced in **YTsaurus 25.4.0**. Thank you to all contributors and maintainers for their work.

YTsaurus Server 25.4.0 is supported since [0.27.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release%2F0.27.0) version of the Kubernetes operator.

## Significant changes

- **Row-Level Security (RLS) — end-to-end support.** ACEs now accept an `expression`, restricting access to individual rows; `full_read` is extended row-wise (covers `copy`/`concatenate`/`remote_copy`). Wired through master, proxy `read_table` and controller agent in operations; CHYT 2.18+ already supports it, SPYT 2.9.0 is required and YQL/QT — 0.4.1.

- **Bulk-insert under user transaction — now general use.** Introduced as experimental in 25.3 (the prerequisite for `REPLACE INTO //dynamic/table` in YQL), it is promoted to general availability in 25.4 — safe to use in production workloads.

- **Authorized node expiration.** Master now remembers who set `expiration_time`/`expiration_timeout` and, with `enable_authorized_expiration` on, runs the removal under that user. Prevents accidental cross-account deletions of shared subtrees and gives an auditable lifecycle for auto-expiring nodes.

- **Distributed write protocol for files.** Files can now be written from many client processes in parallel, the same way tables already could. Removes the single-writer bottleneck for large artifacts and enables fan-out uploads from operations and pipelines.

- **`transfer_bundle_resources` command.** Atomically moves resource quotas (tablet count, tablet static memory) between two tablet cell bundles in a single mutation, making bundle rebalancing safe for automation and the bundle controller.

- **Heavy hitters for lookup queries.** Dynamic tables expose raw and data-weighted heavy hitters for lookups, giving operators a first-class way to find the keys behind cache pressure, hot tablets and skewed read patterns.

- **`check_operation_permission` API.** Lets users and orchestrators ask the scheduler whether a given user has a given permission on an operation, mirroring `check_permission` for Cypress objects.

- **Chunk filtration via `input_query` in path attributes.** Filters chunks based on a query directly in path attributes — useful for read optimizations, [a12fb8b](https://github.com/ytsaurus/ytsaurus/commit/a12fb8b55619d57c13592d876263c2e05eedc5aa).

- **Compressed-data-size constraints for sorted operations.** Adds primary compressed-data-size limits and a `consider_only_primary_size` job-sizing option that accounts only for the primary size when slicing jobs, [1057b82](https://github.com/ytsaurus/ytsaurus/commit/1057b829c51b93680ae5a0db3a90e1ec317cc543).

- **CPU overload handling on data node.** Integrates CPU overload handling into the data node; adds dynamic adjustment of pending-disk-read / pending-disk-write memory limits and the maximum session count, [5a4fe1f](https://github.com/ytsaurus/ytsaurus/commit/5a4fe1f315e49e7add5a3dcb7360899444de46eb).

- **YTQL syntax v3 — expressivity upgrades.** Several long-standing limitations are lifted:
  - Expressions allowed in `IN` and `BETWEEN`.
  - Integer index in `GROUP BY` / `GROUP BY KEY` (`GROUP BY 1, 2`).
  - New string functions `make_ngrams` and `split`.
  - Reads from tablet followers in `SELECT` queries.
  - Fetch join executed on proxy.
  - Array join with lists of composite and `any` values.

## Breaking changes

- **List nodes have been completely removed.** Loading a snapshot that contains list nodes will crash master-server. To ensure that no list nodes exist before the update, the following steps can be taken:
1. If `alert_on_list_node_load` option in the master dynamic config is set to `true`, then validation should have produced an alert when loading from snapshot in 25.2 and a crash in 25.3. If no alerts or crashes were observed, then it should be safe to update.
2. Otherwise, it is advised to use the dry-run option `--validate-snapshot` with the master-server binary version 25.4 to validate that the snapshot can be loaded before updating, [0abb6d3](https://github.com/ytsaurus/ytsaurus/commit/0abb6d394ae55f8025558d69b2100b4fa3d8eba2).

- **Pool tree config cleanup.** Removed `enable_scheduling_tags` (the feature it disabled is no longer relevant) and `max_share_ratio` from pool config (it was used to limit guarantees pre-vector-HDRF), [d807f21](https://github.com/ytsaurus/ytsaurus/commit/d807f213201a3d8621670c9393a9b6eaa46a1437).

- **Deprecating `list_queue_consumer_registrations`.** New `disallow_list_all_registrations` option in queue consumer registration manager config; this will become the default in subsequent releases after a migration phase, [3a49cef](https://github.com/ytsaurus/ytsaurus/commit/3a49cef6d9c5761736f4c492e5a5758620c23181).

## Full changelog

### Scheduler and GPU
#### New Features & Changes:
- Introduce `check_operation_permission` API to verify a user's permission on an operation, [05236de](https://github.com/ytsaurus/ytsaurus/commit/05236dea42005b9d96ab389565ce2a7ef9e32896).
- Add `allow_children_guarantees` option for scheduler pools to forbid child pools from having guarantees, [8b93622](https://github.com/ytsaurus/ytsaurus/commit/8b93622a1fe8cc3b56d4c6ccbd08d62f97dd81e5).
- Introduce `list-job-traces` API to retrieve a job's GPU traces, [d495b01](https://github.com/ytsaurus/ytsaurus/commit/d495b0102af93f629b20a2adc9a221fbab6bf2a5).
- Allow `scheduling_tag_filter` for operations spanning several pool trees, [5c17ea5](https://github.com/ytsaurus/ytsaurus/commit/5c17ea5d5e225c2b135e3d728e9901c37b3976af).
- Increase default allocation preemption timeout to 30 seconds, [f3d1c5a](https://github.com/ytsaurus/ytsaurus/commit/f3d1c5ae864a7c8a0417b3f32d14b4d8d902c2cd).
- Alert when an operation's spec is too large, [9067544](https://github.com/ytsaurus/ytsaurus/commit/9067544414412ce4f794101c7d8846c849986a88).

#### Fixes & Optimizations:
- Removed `enable_scheduling_tags` from pool tree config and `max_share_ratio` from pool config (both obsolete), [d807f21](https://github.com/ytsaurus/ytsaurus/commit/d807f213201a3d8621670c9393a9b6eaa46a1437).
- Removed obsolete checks that blocked preemption when an ancestor pool was starving, [721def2](https://github.com/ytsaurus/ytsaurus/commit/721def28efcf69550e3c1c35759746c107d6569a).
- Allow resource limits overcommit during preemptive scheduling to mitigate resource distribution issues, [273bbc6](https://github.com/ytsaurus/ytsaurus/commit/273bbc600f9545fa1bd917841d4084940687f954).

### Queue Agent
#### New Features & Changes:
- Add a profiling metric exposing whether a queue agent instance is banned, so external systems can alert on instances banned for too long, [8095e03](https://github.com/ytsaurus/ytsaurus/commit/8095e03f57c31687b106e8bb7e2a73b516fc119f).
- Server-side support for the `queue_agent_controller_liveness` Odin check in multi-cluster environments, [c960ce5](https://github.com/ytsaurus/ytsaurus/commit/c960ce5772797d56389d55b5a880185c1f24af85).

#### Fixes & Optimizations:

### Proxy
#### New Features & Changes:
- New `disallow_list_all_registrations` option in queue consumer registration manager config — a transitional step toward removing `list_queue_consumer_registrations`, [3a49cef](https://github.com/ytsaurus/ytsaurus/commit/3a49cef6d9c5761736f4c492e5a5758620c23181).
- Support `LinkNode` with attributes via RPC, [570c83f](https://github.com/ytsaurus/ytsaurus/commit/570c83f316e60b00dee9da669af9931de4e9d49c).
- Support RLS (Row-Level Security) in the `read_table` command, [9ee66dd](https://github.com/ytsaurus/ytsaurus/commit/9ee66dd3708f4087b630c68b535c2bb9b1ca471a).
- Register `chyt_http(s)_server` in Cypress, [69595e0](https://github.com/ytsaurus/ytsaurus/commit/69595e0e4872c7eba08f57a89365ff60a6bfcce6).

#### Fixes & Optimizations:
- Fix datetime mapping to protobuf integral types, [c04424a](https://github.com/ytsaurus/ytsaurus/commit/c04424aa0e2aeb7d78705c05414a6c00e7fc0dd1).
- HTTP server now starts listening only after `Start()` is called, [8af9288](https://github.com/ytsaurus/ytsaurus/commit/8af9288c0a15fe9f6944c8a6e1a41f6c08eaeb3b).

### Dynamic Tables
#### New Features & Changes:
- New `transfer_bundle_resources` command — atomically transfers resources between two tablet cell bundles, [e2479e2](https://github.com/ytsaurus/ytsaurus/commit/e2479e29e77ab9cf7bb68f5f40ec33827f754ecc).
- A configurable share of compaction tasks is reserved for the most starving `{table, compaction reason}` pairs, [5f2ad34](https://github.com/ytsaurus/ytsaurus/commit/5f2ad3481d1910205f46cd837d1aaff5656746e7).
- Heavy hitters and data-weighted heavy hitters for lookup queries, [75e474e](https://github.com/ytsaurus/ytsaurus/commit/75e474ea0d0dc3663ccf8d6697b8c9cca07ce2c5).
- YTQL `make_ngrams` function, [c848ed3](https://github.com/ytsaurus/ytsaurus/commit/c848ed3bf4ff9017e1a22189295b35373214b84e).
- YTQL `split` function — splits a string by a delimiter into a list, [d355e0b](https://github.com/ytsaurus/ytsaurus/commit/d355e0b216eac5e3363f71576711c9bc045f934f).
- Expressions in `IN` and `BETWEEN` (syntax v3), [128294e](https://github.com/ytsaurus/ytsaurus/commit/128294e03b10f4e51ec56d75a9a91b95cd5d8c1e).
- Integer index in `GROUP BY` / `GROUP BY KEY` (syntax v3), [359dfb3](https://github.com/ytsaurus/ytsaurus/commit/359dfb314b554ac070af11b6596acecab835dfe9).
- Aggregating columns are now allowed for unfolding indices; if the unfolded value contains repeats (e.g. `[1;2;3;1]`), table values are aggregated, [8b5954a](https://github.com/ytsaurus/ytsaurus/commit/8b5954ab0143da720af30c25cfc027ec046aae8f).
- Reads from followers enabled in `select` queries, [f8c5fde](https://github.com/ytsaurus/ytsaurus/commit/f8c5fdef325d8bb916d0213f437b2fab85a82e11).
- Proxy subqueries supported in `explain-query`, [0f2799f](https://github.com/ytsaurus/ytsaurus/commit/0f2799fef934603f44738a1d0f466d83ee1c6954).
- Fetch join on proxy, [3c3515c](https://github.com/ytsaurus/ytsaurus/commit/3c3515c9e9eb00c9bf7c4f279c23293deb15c4b4).
- Array join with lists of composite and any values, [be8bc8c](https://github.com/ytsaurus/ytsaurus/commit/be8bc8ccf3baae0a9c14dc00df6e9c209b5907e2).

#### Fixes & Optimizations:
- Sample keys are now tracked under the `tablet_background` category, [009a33d](https://github.com/ytsaurus/ytsaurus/commit/009a33ddd1f74965b4e29eb16ed07c6d44196b1d).
- Query builder produces more balanced ASTs for predicates, [913c370](https://github.com/ytsaurus/ytsaurus/commit/913c370ca25352bd319945e7e9fd7d8682ba438a).
- Avoid excessive memory footprint from memory-tracking in-memory chunks within a memory pool, [8f7f624](https://github.com/ytsaurus/ytsaurus/commit/8f7f624a5417af2ed70baa5c220caa0dcb46307a).
- Fix compaction timestamp digest for `min_data_versions=1`, [7a2a576](https://github.com/ytsaurus/ytsaurus/commit/7a2a5764d1c7e7adfee1b2218cf496b34b35262a).
- Improved performance of aggregate column evaluation, [7ffe4d2](https://github.com/ytsaurus/ytsaurus/commit/7ffe4d276a4dccbd860106a175e56d78915503e9).
- Slimmer QL codegen cache, [ba32890](https://github.com/ytsaurus/ytsaurus/commit/ba328902793b126d5e702955c284cdc89d790fb5).
- Fix crashes from incorrect ids produced by range inference on `is_prefix(...)` expressions, [9c64c4a](https://github.com/ytsaurus/ytsaurus/commit/9c64c4a7d72cd028082274619fc1dd7d2462ac22).
- Add missing `/overload_controller` prefix to profiling sensors, [01f2324](https://github.com/ytsaurus/ytsaurus/commit/01f23249c795f2d982ca5be9b5ebd978019fe329).
- Fix `CASE` operator ignoring the `use_canonical_null_relations` option, [5b48f8b](https://github.com/ytsaurus/ytsaurus/commit/5b48f8b44c5a98b4b720ef01e207c9330b29d996).
- Fix array join when unpacking null lists, [3f8ca76](https://github.com/ytsaurus/ytsaurus/commit/3f8ca760e5a939c28d00202e051b4396bb5da41a).
- Minor use-after-free fix, [2e99e6d](https://github.com/ytsaurus/ytsaurus/commit/2e99e6d2c1ec9b6332afd3c2a0b889d2afea3d74).
- Fix segfault in YTQL codegen for queries with nested subqueries under certain conditions, [63fcaa9](https://github.com/ytsaurus/ytsaurus/commit/63fcaa95f8d8db3535acc0ad3acc1ff53032816f).
- Add missing casts for the `CAST` operator in YTQL, [910cf8b](https://github.com/ytsaurus/ytsaurus/commit/910cf8b213861101762e3a4e1c394c1b7526176b).

### MapReduce
#### New Features & Changes:
- Job API in job proxy — exposed to user jobs over Unix domain sockets. Initial endpoint `OnProgressSaved` bumps a job's preemptible-progress start time, [50011e3](https://github.com/ytsaurus/ytsaurus/commit/50011e34c3a6b5965450a67e775ad778d6f19244).
- Use a code-generated comparator in partition reduce when the schema is known, for higher performance, [984cee5](https://github.com/ytsaurus/ytsaurus/commit/984cee50daee8b38850c5de1adbebc6eddedd7c1).
- Exec node chunk cache renamed to artifact cache; metrics moved to `yt/exec_node/artifact_cache`, [70fb3e0](https://github.com/ytsaurus/ytsaurus/commit/70fb3e02c969280515b37b31ee032bbef3714e92).
- Chunk filtration via `input_query` in path attributes — useful for read optimizations, [a12fb8b](https://github.com/ytsaurus/ytsaurus/commit/a12fb8b55619d57c13592d876263c2e05eedc5aa).
- Compressed-data-size constraints for sorted operations, including primary compressed-data-size limits and a `consider_only_primary_size` job-sizing option, [1057b82](https://github.com/ytsaurus/ytsaurus/commit/1057b829c51b93680ae5a0db3a90e1ec317cc543).

#### Fixes & Optimizations:
- Fix readers in MR operations not accounting for nested tables, [ea0179f](https://github.com/ytsaurus/ytsaurus/commit/ea0179f54fe297cec4fd2c0935dd89f3029ee822).

### Master Server
#### New Features & Changes:

- Permission check during removal of expired nodes (authorized expiration). New attributes `expiration_time_user`, `expiration_timeout_user`, `expiration_time_last_reset_time`, `expiration_timeout_last_reset_time` and config entries `enable_authorized_expiration`, `expiration_attempt_limit`, `expiration_persist_period`, [5bc8e76](https://github.com/ytsaurus/ytsaurus/commit/5bc8e7615854d2775d329a591b970e13161b35c2).
- Skip node disposal when a node was restarted, [8616a6b](https://github.com/ytsaurus/ytsaurus/commit/8616a6b4c6b5c838956f933d53fc67e8baeb39eb).
- List nodes have been completely removed. Loading a snapshot containing list nodes will crash master-server, [0abb6d3](https://github.com/ytsaurus/ytsaurus/commit/0abb6d394ae55f8025558d69b2100b4fa3d8eba2).
- "Lie" about `full_read` permission in authorization errors. When a user lacks even basic `read`, the error mentions only basic `read`, so they don't unnecessarily request `full_read`, [655e1a4](https://github.com/ytsaurus/ytsaurus/commit/655e1a468b9a90d3423dfca0253ae289c4697652).
- Removed duplicate alerts about misconfigured roles on master cells in multicell manager — alerts now only fire on the primary master, [1e5aeff](https://github.com/ytsaurus/ytsaurus/commit/1e5aeff33f4dd30bf58d619932a56dd47bb66549).

#### Fixes & Optimizations:
- Fix requests like `create //a/b&` previously failing with "no such child 'b'", [28cecce](https://github.com/ytsaurus/ytsaurus/commit/28cecce8f10b7137e7c404d0d28030ad894ccc43).
- Secondary index objects now receive a proper ACD (matching the indexed table), [57f8185](https://github.com/ytsaurus/ytsaurus/commit/57f81852eb4745babd4c1caedfe5fab8e97752a2).

### Node
#### New Features & Changes:
- Option to skip waiting for preceding blocks in `PutBlocks` requests, [5b23e49](https://github.com/ytsaurus/ytsaurus/commit/5b23e496caee6e015fbe081fa453d762fb64dc64).
- Metrics for huge-pages I/O operations, [320f828](https://github.com/ytsaurus/ytsaurus/commit/320f82816e7cf03ccba2e89e436ecd1f36cab818).
- Integrate CPU overload handling into data node; allow dynamic adjustment of pending-disk-read / pending-disk-write memory limits and the maximum session count, [5a4fe1f](https://github.com/ytsaurus/ytsaurus/commit/5a4fe1f315e49e7add5a3dcb7360899444de46eb).

#### Fixes & Optimizations:
- Fix race in node lease handling: when a node is banned and its lease tx is aborted while a registration is in flight, the new registration could reuse the already-aborted lease, [fe7b03f](https://github.com/ytsaurus/ytsaurus/commit/fe7b03f06c0ee7230beec745a85cba68e9011204).

### Other

- Make `TAsyncExpiringCache` sharded, [dd06cc5](https://github.com/ytsaurus/ytsaurus/commit/dd06cc5409adcd0e668795ce98b3ab14cd024a05).
- Time at which a memory profile was collected and its collection duration are added to the profile dump, [9aa42a7](https://github.com/ytsaurus/ytsaurus/commit/9aa42a737b0e33efce5f368dcf0632dbe9a093ae).
- Fix extra trace contexts when reading columnar chunks outside the schemaless multi-chunk reader context, [f0f0103](https://github.com/ytsaurus/ytsaurus/commit/f0f01032254371a2ea8603cdab14dcc673ac4f78).
- Fix races on memory guard (was not thread-safe, unlike memory tracker), [640e137](https://github.com/ytsaurus/ytsaurus/commit/640e137828c63b47da10eafb9e17ae079fde0911).

{% endcut %}


{% cut "**25.3.1**" %}

**Release date:** 2026-03-25


To install YTsaurus Server 25.3.1 [update](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release%2F0.27.0) the k8s-operator to version 0.27.0.

### Queue Agent
#### Fixes:
- Fix secondary index creation in init_queue_agent_state script, [9840c02](https://github.com/ytsaurus/ytsaurus/commit/9840c02ebe08e450b88ba00efa23c5af0a8b1eda)

{% endcut %}


{% cut "**25.3.0**" %}

**Release date:** 2026-03-13


## Overview

This document summarizes the key changes, improvements, features, and bug fixes introduced in **YTsaurus 25.3.0**. Thank you to all contributors and maintainers for their work.

YTsaurus Server 25.3.0 is supported since [0.27.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release%2F0.27.0) version of the Kubernetes operator.

## Significant changes

- Added GPU discovery via gpu-agent.

- Automatic job size adjustment for ordered and sorted operation types and stages: sorted_merge, ordered_merge, sorted_reduce, ordered_map, ordered_map and sorted_reduce stages of Map-Reduce operation. 

- Support compressed_data_size job constraints for MR-operations.

- Distributed write protocol for files.

- Support row cache for following tablet cell peers (in multi-peer tablet cell environment).

- [experimental] Support for complex data types in Arrow format.

- [experimental] Row based ACL for static tables
  * supported by CHYT >= 2.18
  * support by SPYT is expected in upcoming releases
  * support by YQL is expected in upcoming releases of YQL/QT.
  
- [experimental] Bulk-insert under user transaction. This is a prerequisite for REPLACE INTO //dynamic/table feature of YQL, available since recent release.

## Breaking changes
- Loading a snapshot that contains list nodes will crash master-server. To avoid this, disable `alert_on_list_node_load` option in dynamic config. List nodes will be removed completely in the next major version.

- Tablet resource accounting (tablet count and tablet static memory) is now
by default performed on per-bundle basis instead of per-account. This is only applied to newly deployed clusters.

- Use `@banned` instead of `@banned_queue_agent_instance` for banning queue agent instances.


---
## Full changelog

### Scheduler and GPU
#### New Features & Changes:
- Add detailed metrics for unutilized node resources during scheduling, [2ea0a01](https://github.com/ytsaurus/ytsaurus/commit/2ea0a01d66dd22a46e5f3f4aa192b0773d9e08fb).
- Implement improved fair share distribution algorithm, [3a80d43](https://github.com/ytsaurus/ytsaurus/commit/3a80d431aefc909c63bf4ab8607e20fbceab964a)
  - Use step functions for gang operations
  - Support smart summation of step functions
  - Support vector suggestion for fair share distribution.
- Add validation of exe node resource limits. If a node does not meet configured limit scheduler raises an alert, [b6aae47](https://github.com/ytsaurus/ytsaurus/commit/b6aae47a081d483197910515fea031482b31274e).
- Added `type`, `user` and `title` to operation orchid (i.e. `//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/default/fair_share_info/operations/<op_id>/type`), [951c759](https://github.com/ytsaurus/ytsaurus/commit/951c759a45af2bb06510cf518b9200499bf49924).
- Add gpu discovery method via gpu-agent, [2e2d586](https://github.com/ytsaurus/ytsaurus/commit/2e2d5867ada1ea60e830aeeb9608e6cd48b84280).

#### Fixes & Optimizations:
- Allow dot by default in ephemeral pool validation regexp, [727d48f](https://github.com/ytsaurus/ytsaurus/commit/727d48fd01edec21432475414dedd4e7c9966f31).
- Change `effective_strong_guarantee_resources` sensor, so that it reflects decreased guarantees in the event of resource shortage, [e441b19](https://github.com/ytsaurus/ytsaurus/commit/e441b195d78d1983036b576ba1ddc7c79b7c4ff1).
- The scheduler event log now includes the amount of accumulated fair resource usage and the accumulated usage deficit, along with the previously collected resource usage, [d1cdf71](https://github.com/ytsaurus/ytsaurus/commit/d1cdf718ea240303d6d4feed38f9479e96da8bcb).
- Use only `transient_state` as `node_state` in jobs API, [1757964](https://github.com/ytsaurus/ytsaurus/commit/1757964291582fee0f9b6e36b868942711684de7).
- Fix memory leak in fair share tree profiling, [fd0af6b](https://github.com/ytsaurus/ytsaurus/commit/fd0af6bf1845f0862d1b5e7ea27f4c20b4a4f728).


### Queue Agent
#### New Features & Changes:
- Use `@banned` instead of `@banned_queue_agent_instance` for banning queue agent instances, [8d52909](https://github.com/ytsaurus/ytsaurus/commit/8d52909765ab582a634eb3b8c7c47270629c8337).
- Add `enable_verbose_logging` option in the dynamic config which enables verbose logging for specific objects from `verbose_logging_objects`, [7e9ba8e](https://github.com/ytsaurus/ytsaurus/commit/7e9ba8ee11c7d6f2030dc6f7abb4b721a954f4b2).
- Support CRON schedules for queue exports, [6535d06](https://github.com/ytsaurus/ytsaurus/commit/6535d065dc0f707e80525189b555298b0507655f).
- Support MutationId in CreateQueueProducerSession, [0c14e76](https://github.com/ytsaurus/ytsaurus/commit/0c14e7646327d651dd0e4438792bfe5e65459e64).

#### Fixes & Optimizations:
- Fix queue agent crashes in case replica object has invalid replica path, e.g. contains non-terminated `{` or `}`, [42454fa](https://github.com/ytsaurus/ytsaurus/commit/42454fa9a0435bdb3a5a8e753a4cedf8a6824729).
- Fix init_queue_agent_state in case directory already exists (occurs in k8s operator), [9e1ac47](https://github.com/ytsaurus/ytsaurus/commit/9e1ac47ac8ca731a8a24db338ee30543634a0e93).
- Fix potential data loss in case of multiple exports per queue
due to incorrect merging of queue export progresses, [bd5d92a](https://github.com/ytsaurus/ytsaurus/commit/bd5d92a8bcb0c45743bc4d0fec0c6da8dee4ebdb).
- Fix passing mutating options in `CreateQueueProducerSession` in RPC proxy, [8ab3c7d](https://github.com/ytsaurus/ytsaurus/commit/8ab3c7da89fa380a0187b5c04dd8857b4d88da0a).
- Set attribute `treat_as_queue_producer=%true` during queue_producer creation, [4537011](https://github.com/ytsaurus/ytsaurus/commit/4537011fd63851d5c09441195ac43f675b63b30d).

### Proxy
#### New Features & Changes:
- Introduced `enable_complex_types` to enable support for complex data types in Arrow format, [b1c3280](https://github.com/ytsaurus/ytsaurus/commit/b1c328040b52b60e5f0ff30468d95daeedd0c66e).
- Introduce distributed write protocol for files, [663f608](https://github.com/ytsaurus/ytsaurus/commit/663f608e050049fb7b2a781f4c52356226b556e9).
- Make `enable_allocation_tags` true by default in HTTP and RPC proxy, [58d5b3c](https://github.com/ytsaurus/ytsaurus/commit/58d5b3c5bcfe18acf18e4f365336fa3ddd00fa1d).
- Support concurrency limit per user in http proxy, [ff0eb5a](https://github.com/ytsaurus/ytsaurus/commit/ff0eb5aca87f39362e354c02bb84b2016fe0d08b).
- Introduce a special skiff-column `$remaining_row_bytes`, [ae0c801](https://github.com/ytsaurus/ytsaurus/commit/ae0c8017ae29a5ce30c72b6151a704845aac7d5e).
- Use symbolic error names for `http_proxy/api_error_count` metric, [2009f63](https://github.com/ytsaurus/ytsaurus/commit/2009f639893e49fd3e7b6044ef9f4be8c3562702).
- Support dynamic reconfiguration of signature subsystem in HTTP proxy, [421cb6b](https://github.com/ytsaurus/ytsaurus/commit/421cb6b424693df3f1b4898b77f7c6683b64edad).
- Support dynamic reconfiguration of signature subsystem in RPC proxy, [ea8492c](https://github.com/ytsaurus/ytsaurus/commit/ea8492cde311d754a66695bf289a91cfad2f2bf0).
#### Fixes & Optimizations:
- Move `max_children_per_attach_request` used in distributed write from Client API to connection dynamic config, [c3cec0b](https://github.com/ytsaurus/ytsaurus/commit/c3cec0bbd73e3db28f693074cb4c661f1e94c29e).
- Fix datetime type in arrow format, [48075f9](https://github.com/ytsaurus/ytsaurus/commit/48075f9c37bfcdc66b765b5ca95b1b01796d553a).
- Support for any number of levels dictionary-encoded layers in arrow parser, [2f4251f](https://github.com/ytsaurus/ytsaurus/commit/2f4251f7614870de9a7fcfc5ddb46ab09101c8ff).
- Fix possible deadlock in the chunk meta cache, [7ed5831](https://github.com/ytsaurus/ytsaurus/commit/7ed5831560019ccf648f39a66be693aa2634ebf5).
- Reduce number of pools metrics, [0fe261a](https://github.com/ytsaurus/ytsaurus/commit/0fe261a8b907e09e2741f83b2061d66e639cf160).
- Make HTTP proxy work when master in read-only mode, [56f4970](https://github.com/ytsaurus/ytsaurus/commit/56f497064430a2e15a06849b9cbc026fc6539440).

### Dynamic Tables
#### New Features & Changes:
- Allow users to specify if they would like their subquery statistics to be aggregated. Sum, max and argmax (node name) are supported, [b7e70fd](https://github.com/ytsaurus/ytsaurus/commit/b7e70fdde6d716c88fc3a5c826edf850659b6968).
- Added an option for tuning the execution of select queries with joins. The option affects quantity of RPCs sent to dictionary tables, [0c63661](https://github.com/ytsaurus/ytsaurus/commit/0c6366155274f38861ce0cfe9f765a920e24deea).
- Support row cache for following tablet cell peers, [8ddd498](https://github.com/ytsaurus/ytsaurus/commit/8ddd49815e72060055b8ae36d6b947c7bbcb670a).
- Implemented `to_valid_utf8` function for YT query language, [0b30b04](https://github.com/ytsaurus/ytsaurus/commit/0b30b04eb5d07d523dce92c30384a03411804c98).
- Added some parameters for QL performance tuning, [e85e54e](https://github.com/ytsaurus/ytsaurus/commit/e85e54eff667ab17702503f57aad987ffb37d7f5).
- Validate that unversioned chunks have reasonable chunk and block size
  prior to mounting the dynamic table, [7e3f8fc](https://github.com/ytsaurus/ytsaurus/commit/7e3f8fc1b9492afbc0803a137347ce59399e6e2a).
- Consider primary key prefix constrained by predicate in ORDER BY and use ordered execution if it is sufficient, [2e75e7e](https://github.com/ytsaurus/ytsaurus/commit/2e75e7e3ad830922b7dfa1fad19b71f09a364bb9).
- Allow background compaction and partition tasks to be executed within the two-level-fair-share thread pool, [139fceb](https://github.com/ytsaurus/ytsaurus/commit/139fceb9156b25808eaa4e1f51ead87227abdac6).
- Tablet resource accounting (tablet count and tablet static memory) is now
by default performed on per-bundle basis instead of per-account. This is only applied to newly deployed clusters. Configs of the existing clusters will not be modified during the update. To enable per-bundle accounting on the existing cluster, set the following options: `//sys/@config/tablet_manager/enable_tablet_resource_validation = %true`, `//sys/@config/security_manager/enable_tablet_resource_validation = %false`, [f6e73b6](https://github.com/ytsaurus/ytsaurus/commit/f6e73b64b518635510d95fee89fdd83e4ae58297).
- Implemented CAST operator for YT query language, [26e91b8](https://github.com/ytsaurus/ytsaurus/commit/26e91b802ab6226ecdad2d1ef9c0c56650a33b3d).
- The `@resource_quota` attribute for tablet cell bundles is now interned and
mirrored with `@resource_limits`. The usage of the latter is preferable, [6e481ed](https://github.com/ytsaurus/ytsaurus/commit/6e481ed399f3319b38232d3f751f4bb69584fb6c).
- A per-tablet select CPU counter is implemented based on total CPU time and per-tablet read data weight proportions, [b617d05](https://github.com/ytsaurus/ytsaurus/commit/b617d056a0a3590da5b218cd4078a1549e7d97ae).
- Add log drop tracker in overload controller, [b09ce32](https://github.com/ytsaurus/ytsaurus/commit/b09ce32a57cef032db7b09cb0cec72033a20f919).
- Allow bulk insert and dynamic table backups by default for new clusters, [dac1857](https://github.com/ytsaurus/ytsaurus/commit/dac1857b97e06a384a21c294e9b9044c96a39dd7).

#### Fixes & Optimizations:
- Unflushed timestamp is now preserved for unmounted tablets.
This is meaningful for tablet actions only because regular mount
sets unflushed timestamp to the current timestamp, [f3ccfbd](https://github.com/ytsaurus/ytsaurus/commit/f3ccfbd350c706361a5becf1e6efd5dd7977e0d6).
- Throttlers related to tablet cell removal now have non-zero limit by default:
`//sys/@config/tablet_manager/tablet_cell_decommissioner/decommission_throttler`,
`//sys/@config/tablet_manager/tablet_cell_decommissioner/kick_orphans_throttler`. Default decommission and orphan check period is decreased to 10 seconds, [9f43c91](https://github.com/ytsaurus/ytsaurus/commit/9f43c91bb3a528baf0d9ae590d6f1f3b9a22c1c2).
- Fix for empty dynamic store id pool assertion error which could have occurred
when tablet was moved by balancer and overwrite bulk insert was performed at
the same time, [ae7ce40](https://github.com/ytsaurus/ytsaurus/commit/ae7ce40c35a5ed457366a13aac3f95eea0fc7bcc).
- get-tablet-errors command now is more reliable in case of transient short-living errors, [7acdf14](https://github.com/ytsaurus/ytsaurus/commit/7acdf14b268d2aad92fcf1261b12f87c1528ed4a).
- Create tablet profiling counters lazily to save memory of "profiling" category, [b286517](https://github.com/ytsaurus/ytsaurus/commit/b2865179e1c95aab48d6e227b0b8dc7dfac44fb8).
- Fix rare crash when using array join and regular join and the latter depends on the result of the former, [750ee01](https://github.com/ytsaurus/ytsaurus/commit/750ee01c8c4cd5761b569fa6e63e8df5612f22c6).
- Avoid proxy crashes during selects upon metadata mismatch, [567922b](https://github.com/ytsaurus/ytsaurus/commit/567922be048a0f8baacde99c62ef41f086847165).
- Writes should not throw even if `lookup_rows_cache` memory is overcommitted, [1125ce4](https://github.com/ytsaurus/ytsaurus/commit/1125ce450333f312e9a198f41f65dd5bf1355896).
- Fix overaccounting of tablet dynamic memory caused by changing capacity of vector in an immutable object between `Acquire` and `Release` calls, [0b16c29](https://github.com/ytsaurus/ytsaurus/commit/0b16c2982cf716f7dd6a323cc9b0be21c6b98a2f).
- Implemented dynamically enableable deprecation of WITH INDEX operator without an alias in select queries, [8013fee](https://github.com/ytsaurus/ytsaurus/commit/8013fee311f4c6b214c7341f07480c7dfcf0cb18).
- Fix per-category tablet dynamic memory accounting at followers, [1acfa14](https://github.com/ytsaurus/ytsaurus/commit/1acfa14bfd12a4c24a157f345dbafd46a717d627).
- QL: fixed crash when performing Group By with a composite key, [5f0e711](https://github.com/ytsaurus/ytsaurus/commit/5f0e71186e2e74f995a8495ac4db21898234977f).
- Tables with hunks should not really be treated the same way the fully in-memory tables are treated, [74d5ec3](https://github.com/ytsaurus/ytsaurus/commit/74d5ec3be1d594a55c7729a952bef30ed1f87016).
- Signal handler stack enlarged. Enabled memory protection for signal handler stack to avoid memory corruption due to stack overflow during signal handler execution, [1fbbdd6](https://github.com/ytsaurus/ytsaurus/commit/1fbbdd63796abf0567d1fc00dcb3707b67dd212e).
- Fixed snapshot divergence between replay and resave.
Additionally optimized schemaful unversioned readers for yson columns, [b4fd1a1](https://github.com/ytsaurus/ytsaurus/commit/b4fd1a149f5c5f4b58d1f68653bb477668238f4c).
- Fix partition cookie generation for ordered dynamic tables, [6f592cd](https://github.com/ytsaurus/ytsaurus/commit/6f592cdbfe9933ae822c635533ce8cca28445387)

### MapReduce
#### New Features & Changes:
- Support RLS in operations, [10c9cf8](https://github.com/ytsaurus/ytsaurus/commit/10c9cf888b69c873a45399f5261bde6389af8cb0).
- `chunk_reader_statistics` now can report separate statistics for each input cluster. Enabled by the `enable_per_cluster_chunk_reader_statistics` feature flag in job_proxy's dynamic config, [4fbe259](https://github.com/ytsaurus/ytsaurus/commit/4fbe25987c218992f9da9be2a72a038d1b631f03).
- Add option for lower bound of user job CPU limit, [2f51037](https://github.com/ytsaurus/ytsaurus/commit/2f510375233c9df50716d9db9b5ada1255173f80).
- Support blob format for getting blob tables as files inside jobs, [c0b1f2d](https://github.com/ytsaurus/ytsaurus/commit/c0b1f2d18f3fa67770bd58ca25642ed7be9289b9).
- Remote operations can now read via custom networks. This is controlled on controller-agent level, so all remote operations will via the same networks (but this can still be a preference list, i.e. if a node does not have an address in the specified network, job will try to read via the next network in the preference list), [e1b174f](https://github.com/ytsaurus/ytsaurus/commit/e1b174fe02ef703344fea34504cd42a5534c6a9c).
- Support job_size_adjuster for: `sorted merge`, `ordered merge`, `sorted reduce stage in mapreduce`, `ordered map stage in mapreduce`, `sorted reduce`, `ordered map`, [fd8c3ee](https://github.com/ytsaurus/ytsaurus/commit/fd8c3ee67ef049d7f6923cbfdaad8c960b920bd2).
- Remote operations now can be restricted by max data weight.
CA estimates data weight that will be read from a particular
remote cluster and if it exceeds the threshold, operation will
fail to materialize, [068c14b](https://github.com/ytsaurus/ytsaurus/commit/068c14b9dc5b9a327e728dbef86badda69550efe).
- Enabled new master cells discovery in job proxy, [51b6040](https://github.com/ytsaurus/ytsaurus/commit/51b604015a3135dac0d15bd04fe5e271aba927cc).
- Added support for `compressed_data_size_per_partition_job` option in sort and map-reduce operations. This allows users to control partition job sizes based on compressed data size, similar to existing map and merge operations. The feature ensures both data weight and compressed data size constraints are respected when slicing input data for partition jobs, providing better control over job resource consumption, [8a66379](https://github.com/ytsaurus/ytsaurus/commit/8a66379e7fc57608ec8ad944f171fdc62b28832b).
- Add compressed data size constraints for sorted operations, including support for primary compressed data size limits and `consider_only_primary_size` option for job sizing, [692a2bf](https://github.com/ytsaurus/ytsaurus/commit/692a2bfac26600c0dfdff15776e589d8b1d81c19).
- Support extra jobs for gang operations. Introduce gang ranks, [0045969](https://github.com/ytsaurus/ytsaurus/commit/004596916a1e9b6af45a953efbf9af30801f7de8).
- This commit unifies the logic for slicing by compressed data size and by data weight in the unordered chunk pool. Previously, max compressed data size per job was used to slice by compressed size, and its semantics differed from that of max data weight per job, [6cc70e2](https://github.com/ytsaurus/ytsaurus/commit/6cc70e2824fca903d8b763dad6521f159f37d92f).
- Introduced option `ForceJobSizeAdjuster` for sort and MR operations. This options enabled job size adjusting even when corresponding `data_weight` per job is explicitly specified, which is typically the case for YQL operations, [cce512c](https://github.com/ytsaurus/ytsaurus/commit/cce512c3c37be42997fee9cde84beee9791cdf06).

#### Fixes & Optimizations:
- RemoteCopy will now correctly handle columnar ACLs
(i.e. will require read access to all table columns), [d210cf8](https://github.com/ytsaurus/ytsaurus/commit/d210cf845671e3a43f9d3c4ec6e6dc3b6b528898).
- Forbid specifying `user_files/file_name` with slash in userjobs.
It is impossible to prepare such file anyway, [dbcdf6f](https://github.com/ytsaurus/ytsaurus/commit/dbcdf6ff17df6ec53a8f3f21c2d359210922e83d).
- `chunk_reader_statistics/wait_time` was not reliable due to the race in the timer.
In rare cases, though visible in large installations, we could accidentally
add a high value (roughly the time since the last exec node reboot) to the reported time, [c495929](https://github.com/ytsaurus/ytsaurus/commit/c495929b46b95b6688704b752da8dbdcd21d90d9).
- Controller agent will now reserve memory for output buffers
in vanilla job's job_proxy, [667c927](https://github.com/ytsaurus/ytsaurus/commit/667c9273acf4c0ef48c17fac12ecd8e6beaf74ea).
- RemoteCopy of dynamic table with hunks will now correctly estimate data weight of hunk chunks, [439ac66](https://github.com/ytsaurus/ytsaurus/commit/439ac663714c87924c13f24732b9fd4e48197009).
- Fix CA crash when gang job completes, allocation reusing is enabled and task has waiting jobs, [c7eb321](https://github.com/ytsaurus/ytsaurus/commit/c7eb321b12978441f3e6ae7e145596fef785b672).
- Fix controller agent crash during gang job abandoning, [0d02dbc](https://github.com/ytsaurus/ytsaurus/commit/0d02dbc697eac7787c96eede22370395e69485dd).
- Do not allow stderr/core tables to be dynamic and coincide with output tables, [194774d](https://github.com/ytsaurus/ytsaurus/commit/194774d4f4bc71c0afcd28aec01c5732dbe36de2).
- When the job count is explicit, early job input completion and size constraints are now skipped to ensure more even job size distribution, and an alert is raised if the number of data slices exceeds max_data_slice_count instead of silently ignoring this case for the last job, [b6be9a2](https://github.com/ytsaurus/ytsaurus/commit/b6be9a26eb8c83a90edb5a2d64830e9f60d8db91).
- Fix "race" between planned update of master cell directory and scheduled out of band. Two updates try to apply same diff to master cell directory. The first one applied successfully and the second one tried to apply changes to already updated directory and crashed, [97332dd](https://github.com/ytsaurus/ytsaurus/commit/97332dd8253332b5adfa98cc51eb86e355e8e6d2).


### Master Server
#### New Features & Changes:
- Add bulk insert under user transaction, [83002e9](https://github.com/ytsaurus/ytsaurus/commit/83002e9567b928bf2a802fe9abfb59b4cb679344).
- Handle of pending removal subjects, [541ff93](https://github.com/ytsaurus/ytsaurus/commit/541ff935123f67bb183bfc36caba20a70f6ea783).
- Loading a snapshot that contains list nodes will crash master-server. To avoid this, enable `alert_on_list_node_load` option in dynamic config. List nodes will be removed completely in the next major version, [3964ec6](https://github.com/ytsaurus/ytsaurus/commit/3964ec6a05aaf05a344d6adbecc04d29f21d1c00).
- Allow specifying `expression` in ACEs. Also, `full_read` now grants row wise full read (symmetric to columnar full_read) and allows user to copy/concatenate/remote_copy the whole table, [9556b59](https://github.com/ytsaurus/ytsaurus/commit/9556b59b7b907be33eec95ca1cb5215dfc7d1ed6).
- Automatic removal of dangling locations that haven't been reported by nodes for a long time is enabled by default, [1afccef](https://github.com/ytsaurus/ytsaurus/commit/1afccef8a3e76212dc911aad5dff2742455dc045).
- Introduce prerequisite revisions for requests in Sequoia, [3362b3a](https://github.com/ytsaurus/ytsaurus/commit/3362b3a28b003af718e1db25aefe499d27f38e58).
- Add support for fast (negative) Zstd compression levels from 1 to 7, [43165bb](https://github.com/ytsaurus/ytsaurus/commit/43165bbe9a9bb6e979387e7f324c0d0b3b603c41).
- Master no longer uses table schema information in EndUpload, use BeginUpload to send schema instead, [38ed4e1](https://github.com/ytsaurus/ytsaurus/commit/38ed4e152aaeb8c1049d1da90e68c65caee82acb).
- Better compact table schema & yson schema caching, [31a0283](https://github.com/ytsaurus/ytsaurus/commit/31a0283703e00380919105be3bcd26b4a572f5c3).
- Introduced hard limit for `//sys/chunks`. It can be configured using `virtual_chunk_map_read_result_limit`, [f0c0fef](https://github.com/ytsaurus/ytsaurus/commit/f0c0fefc261683492886624411fa360e4a9367dc).
- Added validation of prerequisites on read, [04e63a3](https://github.com/ytsaurus/ytsaurus/commit/04e63a3d9e4020bbb4cd450723bf63a845b6834d).
- Add MutationVersion label to all logs written within a mutation, [0648d8d](https://github.com/ytsaurus/ytsaurus/commit/0648d8d3f27a8df5ee8ec97776d4d42026bf356f).
- Local read throttling is also supported now, similar to write throttling, [cff3569](https://github.com/ytsaurus/ytsaurus/commit/cff356907310b355eedf4aa379a15ebab10dc4e9).

#### Fixes & Optimizations:
- Removed duplicate alerts about misconfigured roles on the master cells in multicell manager. Alerts from now on will light up only on the primary master, [de4eec9](https://github.com/ytsaurus/ytsaurus/commit/de4eec90478f500ae30d8633bbd58159f4b9296b).
- Added table schema cache invalidation in case of failed compact table schema parsing, [81dcde8](https://github.com/ytsaurus/ytsaurus/commit/81dcde8a1f6755834f0ba88b9dcdb430eaf43b6a).
- Fix `no such transaction` response on `PingTransaction()` immediately after leader switch, [62f80a8](https://github.com/ytsaurus/ytsaurus/commit/62f80a8d3b8dbe8b0c8e2c80c67bff154082854a).
- Fix node `@maintenance_request` divergence between old and new cells on cell addition, [83fac44](https://github.com/ytsaurus/ytsaurus/commit/83fac44a46378c65aab236ed4dce0edc296e36f5).
- Fix bug in full read permission check, [ae7de70](https://github.com/ytsaurus/ytsaurus/commit/ae7de709c17fb7a64cf352b79167d7943c5f5ec2).
- Fixed issuing leases for master cell, [76394af](https://github.com/ytsaurus/ytsaurus/commit/76394afa2e0d251ae4daa5052d192433581e0cb9).
- Fix resolve of symlinks in TObjectManager::ResolvePathToLocalObject, [8bcf04a](https://github.com/ytsaurus/ytsaurus/commit/8bcf04a2c3ff4c85862c6c7f04a2612cb8544ebc).
- Respect chunk merger mode attribute when chunk merger is disabled, [25375af](https://github.com/ytsaurus/ytsaurus/commit/25375afea99155f30abd94ace9e65f1e7604522f).
- Stop generating mutation ID for read requests. Since mutation ID is considered as part of request message generating unique mutation ID for every requests breaks object service cache, [682341c](https://github.com/ytsaurus/ytsaurus/commit/682341cee9d60d285c3b5d42208853bbe5016867).
- Enable RemoveSecondaryCellDefaultRoles flag, [7a252af](https://github.com/ytsaurus/ytsaurus/commit/7a252afe29daff13ecd3c7237bae0787d6500388).
- Fix ids for some builtin users that were introduced in 25.2, [d4707fd](https://github.com/ytsaurus/ytsaurus/commit/d4707fdded91ca12669639de8cf91a2b8d3ca491).
- Fixed lack of readability of secondary index attributes, [e1356fb](https://github.com/ytsaurus/ytsaurus/commit/e1356fb8389e85a3b2e4556bae09d7ad61470c50).
- Fix TAttributeFilter, [5d6ca6b](https://github.com/ytsaurus/ytsaurus/commit/5d6ca6be45c99c546cb7cad7cb9b79c253c86f7e).
- Replace TCompactTableSchema with TCompactTableSchemaPtr in table schema caches, [afe8348](https://github.com/ytsaurus/ytsaurus/commit/afe83482724703bbb15211615c531d93bbfa69ce).
- Use persistent response keeper for commit/abort ypress transaction, [2616719](https://github.com/ytsaurus/ytsaurus/commit/261671993cf8630c24b9af3bfa2248d98e20fb4b).
- Fix core in HydraUpdateMasterCellChunkStatistics when ChunkScanExecutor makes more than one call of OnChunkScan before actual execution of commited HydraUpdateMasterCellChunkStatistics mutation, [d061b28](https://github.com/ytsaurus/ytsaurus/commit/d061b285a52e0e43a131aac45d1e9e9deb46cc1d).
- Fix snapshot divergence in Cypress proxy tracker, [ef8c7fe](https://github.com/ytsaurus/ytsaurus/commit/ef8c7fe1943783a318c838d4927cd9981cda21dd).
- Fix "list node creation is forbidden" error when setting list to attribute of unexisting node, [52776ec](https://github.com/ytsaurus/ytsaurus/commit/52776ec9953781f5ef19de80a12e5e99c456fe61).
- Removed full broadcast from DoGetMulticellOwningNodes, [304e500](https://github.com/ytsaurus/ytsaurus/commit/304e500262e9437498bc47a83a26808ce69031c1).
- Fix of validation revisions paths differ from execution paths, [97fb456](https://github.com/ytsaurus/ytsaurus/commit/97fb456ce886270ea0c7db2d75c6a7336f2d586f).
- Fix node statistics attribute in case when node version is more recent than the master one and contains unrecognized memory categories, [72d4a63](https://github.com/ytsaurus/ytsaurus/commit/72d4a631ab73a17535410dd530be3c821037cff4).

### Node
#### New Features & Changes:
- Support dynamic reconfiguration of signature subsystem in exec node, [1f0064d](https://github.com/ytsaurus/ytsaurus/commit/1f0064d1520a47b4d3f8e1c7281ac9ca09b4a87d).
- Support configuring individual Porto API timeouts & layer import concurrency dynamically, move & restructure dynamic config `exec_node/volume_manager` -> `exec_node/slot_manager/volume_manager`, [a85c513](https://github.com/ytsaurus/ytsaurus/commit/a85c51346a131db01013b2051722495e99bee216).
- DataNode: support reading via direct io & huge pages [55ca233](https://github.com/ytsaurus/ytsaurus/commit/55ca2336dc371f06bbe8ace2759020d87011184e).
- DataNode: implement parallel GetBlockSet [57afc41](https://github.com/ytsaurus/ytsaurus/commit/57afc4100612a6e9c5c327d18e85bfbe878fac33).
- DataNode: add io_weight formula for adaptive io_weight calculation [2df6172](https://github.com/ytsaurus/ytsaurus/commit/2df6172d2c3a3ef350aec094d3c1e1878b5c56c6).

#### Fixes & Optimizations:
- Fixed chunk meta extensions absorption in meta aggregated writer, [f10f2cb](https://github.com/ytsaurus/ytsaurus/commit/f10f2cb1ad9bf3da97ce3e4d596e654d0e046b65).
- Fix intersecting heartbeat flows from nodes to master, [fa95012](https://github.com/ytsaurus/ytsaurus/commit/fa950126fc5ff82473ab7395ef4187301f5e4380).
- Fix race between registration on primary master and receiving new master cells, [9dfa44b](https://github.com/ytsaurus/ytsaurus/commit/9dfa44b1665eb8e14ebf51fd798aef1b54ba79fa).
- Fix `yt.exec_node.rpc_proxy_in_job_proxy_count` metric, add `host` label, [33f92a8](https://github.com/ytsaurus/ytsaurus/commit/33f92a8d4b96970b84d317ad3ea65feae70bc572).
- Heartbeat start and stop batching, [1b800da](https://github.com/ytsaurus/ytsaurus/commit/1b800da2eb7647648315ba07d4b8495f206bb95c).
- Add config for master cell directory synchronizer into cluster node dynamic config, [c2fb7d7](https://github.com/ytsaurus/ytsaurus/commit/c2fb7d7f455ee7cf7a568b1caac40b1c4b89ede9).
- Fixed start of node heartbeats before actual registration, [49b021b](https://github.com/ytsaurus/ytsaurus/commit/49b021bc31435c1b204466038ba531355d349740).
- Fix core in meta aggregated writer on corrupted chunks, [0e97074](https://github.com/ytsaurus/ytsaurus/commit/0e970742581a6bdecdbe7113d4261a7a17ff83bf).
- Reusal of node lease transaction during reregistration, [693e3e1](https://github.com/ytsaurus/ytsaurus/commit/693e3e13c08a5570d0101975bb4b096218acee98).


### Other
#### Fixes & Optimizations:
- AllocFragmentation memory category should not be accounted in total memory used since it is free memory within heap, [e301573](https://github.com/ytsaurus/ytsaurus/commit/e30157331d8a769e72fc80875244a666cff5d97e).
- This patch is fixing data race on EndTime field in TIODirection. It happens on concurrent call of OnShutdown and GetStatistics methods. Concurrent control of this structure is given for outer interface(TFDConnectionImplPtr) those we should use this structure under SpinLock(as in other access to this structure), [365d1cf](https://github.com/ytsaurus/ytsaurus/commit/365d1cf7065768fa25d9c37b0220c8be19cf2142).
- Fixed bad initialization of threads within fair-share thread pool, [a9d0752](https://github.com/ytsaurus/ytsaurus/commit/a9d07520d77c2d9f86579a7ba259968ce0cc4926).
- Fix memory leak in logger, [56dc38d](https://github.com/ytsaurus/ytsaurus/commit/56dc38d092012a8173b050602d3cab82640a664d).



{% endcut %}


{% cut "**25.2.2**" %}

**Release date:** 2025-12-09


To install YTsaurus Server 25.2.2 [update](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release%2F0.27.0) the k8s-operator to version 0.27.0.

### Proxy
##### Features:
- Introduce check_operation_permission, [ce62b11](https://github.com/ytsaurus/ytsaurus/commit/ce62b1111cae509cc5689f3c630ff17455dd6c38).
- Introduce query sorting order flag in list_queries, [acdc001](https://github.com/ytsaurus/ytsaurus/commit/acdc001a5bcfb4c45a7c4dc2f8260c69f0fa514a)
- Disable new query tracker search by default, [17afa20](https://github.com/ytsaurus/ytsaurus/commit/17afa20ef8a3ca38284c4d1d404b105e0beb35a5)

{% endcut %}


{% cut "**25.2.1**" %}

**Release date:** 2025-11-11


To install YTsaurus Server 25.2.1 [update](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release%2F0.27.0) the k8s-operator to version 0.27.0.


### Data Nodes
#### Fixes:
- Fix of a race condition in P2PWeight computation in TP2PSnooper that caused crashes of the form `YT_VERIFY(Categories_[category].Used >= 0)` with `p2p` category, [d5556a3](https://github.com/ytsaurus/ytsaurus/commit/d5556a35be28525bcbb44c95fb7e7fdf3550d6af)


### Queue Agent
#### Fixes:
- Fix queue agent crashes in case replica object has invalid replica path, e.g. contains non-terminated `{` or `}`, [bfed123](https://github.com/ytsaurus/ytsaurus/commit/bfed123bb1750bf1b3a5c06901f15e14cda3b67b).



### Master
#### Fixes:
- Fix bug in full read permission check, [c15ffe0](https://github.com/ytsaurus/ytsaurus/commit/c15ffe0d6f84dbb2f6e73a92db0fddc127303054).
- Fix crashing when trying to reshard a table that was already removed, [540400c](https://github.com/ytsaurus/ytsaurus/commit/540400c08b729e7a73b7b2e51349df99b650c21c)




{% endcut %}


{% cut "**25.2.0**" %}

**Release date:** 2025-09-23


To install YTsaurus Server 25.2.0 [update](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release%2F0.27.0) the k8s-operator to version 0.27.0.

## Significant changes
- Added support for Nvidia GPU in k8s-operator. Improved GPU devices discovery in job container.
[Documentation](https://ytsaurus.tech/docs/en/admin-guide/gpu).
- Added bundle controller for managing tablet cell bundles on small clusters. This component distributes tablet nodes between bundles, manages node maintenance and controls CPU and memory distribution over tablet nodes. [Documentation](https://ytsaurus.tech/docs/en/admin-guide/bundle-controller).
- Added support for multiproxy mode in RPC proxies. RPC Proxies (including RPC Proxy in Job Proxy) could be configured to operate with remote clusters. [Documentation](https://ytsaurus.tech/docs/en/admin-guide/multiproxy).

### Query language features
- Added `cardinality_state` and `cardinality_merge` functions.
- Added support for timestamp functions for arbitrary time zones. 
- Implemented `array_agg` function.
- Added support for simple subqueries in FROM clause.

### Default changes and deprecations
- Enabled `decommission_through_extra_peers` by default; which significantly reduces downtime of tablet node maintenance.
- Enabled hunks remote copy by default.
- Switched to per-bundle tablet resource accounting by default.
- Remote copy operations set some system attributes on destination table, even if `copy_attributes` is set to false in spec; these attributes are: `compression_codec`, `erasure_codec`, `optimize_for`.
- Deprecation of `list_node`. Master-servers will now issue an alert-level log message after loading a snapshot if it contains a list node. This behaviour can be turned off using `alert_on_list_node_load` option. Consider switching to other types and removing or replacing all remaining list nodes. If not done, this will result in master-server not starting in the next major update. With this release, we've bundled a script that should help you migrate in the vast majority of cases. You can find it at `yt/yt/scripts/master/replace_list_nodes`. We've published a blog post explaining our reasoning for deprecating this type and suggesting other migration methods. More details [here](https://ytsaurus.tech/en/blog/ytsaurus-25-2-0#deprecation).

---
## Full changelog

### Scheduler and GPU

#### New Features & Changes:
- Added validation of exe node resource limits. If a node does not meet configured limit scheduler raises an alert, [c4b5dcd](https://github.com/ytsaurus/ytsaurus/commit/c4b5dcd1c275f05cd5559b155825a80e04b7bd7e).
- Allowed dot by default in pool name validation regexp, [93cf17a](https://github.com/ytsaurus/ytsaurus/commit/93cf17a715b6a73cc04932a0f712785898a37853).
- Added `type`, `user` and `title` to operation orchid, [ffda123](https://github.com/ytsaurus/ytsaurus/commit/ffda12318037149b5828a40e8d9ab3d16c0278d1).
- Added option to scheduler config and operation spec that fails operation if specified pools do not exist, [30eebf3](https://github.com/ytsaurus/ytsaurus/commit/30eebf36d03c9285c57101e8d7db8e85cd9b6eb7).


### Queue Agent

#### New Features & Changes:
- Added `enable_verbose_logging` option in the dynamic config which enables verbose logging for specific objects from `verbose_logging_objects`, [67efedf](https://github.com/ytsaurus/ytsaurus/commit/67efedf6eb175baa61a2431482dbd99d3fb40689).
- Take queue exports into account in case of trimming replicated table and chaos replicated table queues, [c016ca9](https://github.com/ytsaurus/ytsaurus/commit/c016ca9c458fa2cc4f06b93e9ed2db009fc49a6c).
- Added support for retries for `CreateQueueProducerSession` method, [591d500](https://github.com/ytsaurus/ytsaurus/commit/591d500df1c7d88cfacb16079f821c6067b8faea).

#### Fixes & Optimizations:
- Fixed `init_queue_agent_state` in case directory already exists (occurs in k8s operator), [8aca0e2](https://github.com/ytsaurus/ytsaurus/commit/8aca0e254ac149a193f2b1e3b987271ef80e87fb).
- Fixed `write_data_weight_rate` for empty partitions, [3e1dd6d](https://github.com/ytsaurus/ytsaurus/commit/3e1dd6d0daa72bef4928d0a227eaa0748d2a6751).
- Prevented split-brain between queue agent instances leading to orchid redirect loop by limiting the number of redirection and add retries, [39b014d](https://github.com/ytsaurus/ytsaurus/commit/39b014d2a51395b9d360772c2de1cd595e9e9a8e).
- Fixed queue agent crashes in some tricky cases with queue recreation, [133b109](https://github.com/ytsaurus/ytsaurus/commit/133b1095f53027f51fd1df28fcdfc52c8b3071aa).
- Fixed potential data loss in case of multiple exports per queue, [f51f9fa](https://github.com/ytsaurus/ytsaurus/commit/f51f9fa222507243a8e7cab58b1dd8455290cd45).

### Proxy
#### New Features & Changes:
- Added support for multiproxy mode in RPC proxies: client can use multiproxy of one cluster to operate other connected clusters, [e0f98d7](https://github.com/ytsaurus/ytsaurus/commit/e0f98d75284d123ea87bbbf1abb2fc2977e03a82).
- Introduced `get_current_user` method in RPC and HTTP proxies, [ab3f903](https://github.com/ytsaurus/ytsaurus/commit/ab3f903e5708a0998ff565e6f25fd4ea95e8b7fb).
- Added the ability to configure a user limit for a specific proxy role, [14d49a0](https://github.com/ytsaurus/ytsaurus/commit/14d49a09cad2609bc23911a6899213a2b1fa5dc8).
- Extended impersonation functionality in HTTP protocol; which is now allowed for all unbanned superusers, [9eee43d](https://github.com/ytsaurus/ytsaurus/commit/9eee43d28df59c4b2090aa0437ced6bf225e6a3e).
- Allowed sending/reading values larger than 16MB to RPC proxy via wire protocol to/from methods dealing with static tables, [df8fb64](https://github.com/ytsaurus/ytsaurus/commit/df8fb64910e8423e444a1694391ed21ac4fa2f55).
- Added `to_lower` and `to_upper` option to OAuth login_transformations, [0245614](https://github.com/ytsaurus/ytsaurus/commit/0245614f4647d9f409f3855c716cd920bd8f02de).
- Added support for dynamic reconfiguration of signature subsystem in HTTP and RPC proxy, [029f6ce](https://github.com/ytsaurus/ytsaurus/commit/029f6ce8c1eb5c3254c336129e2d616c60337b4b)

#### Fixes & Optimizations:
- Various fixes in arrow format:
  - Fixed reading of tables with date type columns, [1a23993](https://github.com/ytsaurus/ytsaurus/commit/1a239934462e269550c103cf81b813997ce473cc).
  - Added the ability to read tables with different number of columns in chunk meta in arrow format, [6210035](https://github.com/ytsaurus/ytsaurus/commit/6210035056c12e3b078f7a3e01b562030f946192).
- Changed caching options in config of CypressUserManager for OAuthAuthenticator. Migrate to options compatible with AsyncExpiringCache ("expire\_after\_\*\_time"). Older options ("cache\_ttl", "optimistic\_cache\_ttl") are deprecated and will be removed in future versions, [1ecabbc](https://github.com/ytsaurus/ytsaurus/commit/1ecabbc9831fb6fd8b41b12feb5173309a70d21b).
- Fixed for CVE-2023-33460: Memory leak in yajl 2.1.0 with use of yajl_tree_parse function. [f7b9064](https://github.com/ytsaurus/ytsaurus/commit/f7b9064f099d977cdfb2644f4ef11a57745049f7)
- Fixed possible deadlock in the chunk meta cache, [7c68dbe](https://github.com/ytsaurus/ytsaurus/commit/7c68dbe45457d1254046e1d9c22c6bff55cf3138).
- Fixing the calculation of `state_counts` and `type_counts` in `list_jobs` method, [84d7713](https://github.com/ytsaurus/ytsaurus/commit/84d7713a74b8f7c311ea523408d8d42d51eb8fa6).
- Set attribute `treat_as_queue_producer=%true` during queue_producer creation, [88eac20](https://github.com/ytsaurus/ytsaurus/commit/88eac20c53b28015773e0f854ac2f62ded5fcf56).
- Made HTTP proxy functionable when master is in read-only mode, [ccb0228](https://github.com/ytsaurus/ytsaurus/commit/ccb0228011eb203b079e1c544ea984dd5123712d).
- Improvement of handling memory pressure errors, [db04463](https://github.com/ytsaurus/ytsaurus/commit/db04463b87f6e0256d8d61dcd1f453d6897151fe).
- In case of memory pressure Drop only heavy request, [646071a](https://github.com/ytsaurus/ytsaurus/commit/646071ac381adcf90cd4567428aa911ef6266e23).

### Dynamic Tables
#### New Features & Changes:
- Added support for bulk insert under user transaction, [3fe8c73](https://github.com/ytsaurus/ytsaurus/commit/3fe8c7325d761b81783997b5cf7bb694ff72c8c6).
- Consider primary key prefix constrained by predicate in ORDER BY and use ordered execution if it is sufficient, [f8dbc00](https://github.com/ytsaurus/ytsaurus/commit/f8dbc005ba504073faed4dafd62406abd60fd85a).
- Allowed background compaction and partition tasks to be executed within the two-level-fair-share thread pool, [55c2dfd](https://github.com/ytsaurus/ytsaurus/commit/55c2dfdd05a0529f92d384a0b1cfa0534645a000).
- Added `cardinality_state` and `cardinality_merge` functions for QL, [a267e20](https://github.com/ytsaurus/ytsaurus/commit/a267e20d0c744f3d048f6f5a98f21fa6a62265ca).
- Timestamp functions for arbitrary time zones are added to QL, [55178fe](https://github.com/ytsaurus/ytsaurus/commit/55178fe4ad5c92624f84d34268f1a0cb6b7eb72b).
- Implemented `array_agg` function for QL, [d656eec](https://github.com/ytsaurus/ytsaurus/commit/d656eec732e41d14fe1912a27e1f48ff9f71e216).
- Added support for RegisterChunkReplicasOnStoresUpdate for ordered tables; reduce the number of master requests required for reading flushed chunks via tablet node API, [155fe69](https://github.com/ytsaurus/ytsaurus/commit/155fe694477bf6d59fb3c8120cb2ee17bb34c96a).
- Casts double->(unsigned) integer now avoid undefined behavior and function the same regardless of execution engine by clamping values and converting NaNs to zero, [8c492fa](https://github.com/ytsaurus/ytsaurus/commit/8c492fa46268add942182ea52e9ea9827af1d07c).
- Optimize writes into tables with secondary indices under certain conditions, [4066880](https://github.com/ytsaurus/ytsaurus/commit/4066880b1752a211c0909a98a978a6238a6ceb85).
- Enabled `decommission_through_extra_peers` by default; which significantly reduces downtime of tablet node maintenance, [c34ff21](https://github.com/ytsaurus/ytsaurus/commit/c34ff216c3d2e2454a282d01272f07475b6e7ec1).
- Allowed columns with type Any to be unfolded via indices; their contents are checked in runtime, [d0eb7ad](https://github.com/ytsaurus/ytsaurus/commit/d0eb7ad2c0e44cd0cab9a71a524a5be96a3ca942).
- Smooth movement for tablets with hunks, [d5357ea](https://github.com/ytsaurus/ytsaurus/commit/d5357eaade428ed8abe2a8091d5ab04bb1bb037e).
- Added option to decrease serialization time by serializing transaction within each lock group in each row separately, [fe90e97](https://github.com/ytsaurus/ytsaurus/commit/fe90e97c3cb519298d57dc66136426db027736a1).
- Optimize QL: SELECT queries with GROUP BY and JOIN group rows BEFORE joining when possible, [75e8e64](https://github.com/ytsaurus/ytsaurus/commit/75e8e6439fdc8f8ea182dfcc2051f1e239e9801c).
- Added a method to return freezing or unmounting tables back to mounted state, [bbf1101](https://github.com/ytsaurus/ytsaurus/commit/bbf110130c07de7091e7f927ca5838209f1d99d7).
- Added support for simple subqueries in FROM clause in QL, [a7e0701](https://github.com/ytsaurus/ytsaurus/commit/a7e0701609febe6881d3eb078baf4c0af0878e73).
- Added profiling counters in tablet nodes for `pull_queue`/`pull_queue_consumer` commands, [6aebfc1](https://github.com/ytsaurus/ytsaurus/commit/6aebfc1a758092864dae6b25177ec4339bc1c2db).
- Select queries now properly choose a random in-sync replica even if candidates belong to the same cluster, [efdf083](https://github.com/ytsaurus/ytsaurus/commit/efdf083a7d8cc0e7345f8b7705b87c0ec6a7ee47).
- Added `total_grouped_row_count` to QL statistics, [e37b81f](https://github.com/ytsaurus/ytsaurus/commit/e37b81f5199263dd9d15ef1116cd0be0463f8145).
- Added log drop tracker in overload controller, [8cd772d](https://github.com/ytsaurus/ytsaurus/commit/8cd772d009304ea80fa70fe9b562036c2e2e83a5).
- Switched to per-bundle tablet resource accounting by default, [4f78e41](https://github.com/ytsaurus/ytsaurus/commit/4f78e419a8eb11a7929028b9171c87206d35ef2f).
- The @resource_quota attribute for tablet cell bundles is now interned and mirrored with @resource_limits, [27ed1ef](https://github.com/ytsaurus/ytsaurus/commit/27ed1ef4516b0ee1c089bc107be5f3103365977a).
- Added support for remote copy for dynamic table with compression dictionaries, [0791aea](https://github.com/ytsaurus/ytsaurus/commit/0791aea8bf67e71fd2cef192116cf191bb3ea9f5).
- Introduced "evaluatable schema" extension for secondary indices, which allows indexation of expressions, [bf45155](https://github.com/ytsaurus/ytsaurus/commit/bf451559e91b05f8c4136d9364c8d9d6a60a7db0).
- Enabled hunks remote copy by default, [9f2c5f4](https://github.com/ytsaurus/ytsaurus/commit/9f2c5f4fe77adc6498c3a61c2c44019f3c6fecde).
- Optimize QL: use lookup join when left subplan is selective, [03d25a9](https://github.com/ytsaurus/ytsaurus/commit/03d25a9e8cdd11912ffc2918a97f437b0278ada4).
- Improved performance of `timestamp_floor_week` function, [06a643c](https://github.com/ytsaurus/ytsaurus/commit/06a643c45e9df782755e7202a4ded36e0626e794).
- Added erasure codec validation in journal writer, [ae194de](https://github.com/ytsaurus/ytsaurus/commit/ae194deef92ab585ae2b75b94404530e3aa32891).

#### Fixes & Optimizations:
- Fixed per-category tablet dynamic memory accounting at followers, [d147efd](https://github.com/ytsaurus/ytsaurus/commit/d147efd923a71ccad4676bf701804b94fc4b9a26).
- Signal handler stack enlarged; enabled memory protection for signal handler stack to avoid memory corruption due to stack overflow during signal handler execution, [7ba96aa](https://github.com/ytsaurus/ytsaurus/commit/7ba96aa4b515beedb479b14ba7c0d8e4dbc81c6e).
- Fixed accounting of row cache memory, [bcadf28](https://github.com/ytsaurus/ytsaurus/commit/bcadf2893913a6743debc7012f83d6dd6fda75a4).
- Fixed `to_any` function - Cast of `EValueType::Composite` to `EValueType::Any` now works as expected. Allowed some functions to work with both of these types, [20f22b7](https://github.com/ytsaurus/ytsaurus/commit/20f22b77cbfde756cf23ad13a3a11ecba1e586de).
- Fixed a bug involving unfolding secondary indices that lead to crashes when query predicate contained list_contains(expr) where expr was not a reference, [61b2b4e](https://github.com/ytsaurus/ytsaurus/commit/61b2b4e40869ef7c70a5c37a90002aee4221ff15).
- Fixed crashes in proxies when selecting from a table with a malformed computed column, [913edd3](https://github.com/ytsaurus/ytsaurus/commit/913edd308dddca0fe204cefc6269579f42b87474).
- Fixed select_rows not waiting on locks when reading via lookup, [336ab90](https://github.com/ytsaurus/ytsaurus/commit/336ab9054e164941f8e00bb7a7a65e7f7c2d555b).
- Fixed bad invoker choice when migrating to query thread pool, [8d93be7](https://github.com/ytsaurus/ytsaurus/commit/8d93be71145d5f1184451e482bc878d931c65562).
- Misconfiguration of row cache no longer leads to OOM, [7db2519](https://github.com/ytsaurus/ytsaurus/commit/7db25191b60baa3b3db0a163aa6dd881a7b96a15).
- Fixed a bug with incorrect meta size estimation in scan format which caused overzealous chunk fragmentation during compaction, [11a59e3](https://github.com/ytsaurus/ytsaurus/commit/11a59e379bb49e2ce7b4b0db975e93a08cb2b821).
- Fixed the bug where range inference produced mishapen keys and ranges, [aa7597d](https://github.com/ytsaurus/ytsaurus/commit/aa7597d67ff43009fd8f271b95b7e275df1c0344).
- Fixed data corruption of floating point values in dynamic tables in scan format, [8fcbf50](https://github.com/ytsaurus/ytsaurus/commit/8fcbf507d8e1f02698058c3bbec4355d8e385ce2).
- Added `push_down_group_by` for replicated tables, [cb64762](https://github.com/ytsaurus/ytsaurus/commit/cb64762aa5d4d331e1ab3f0d388d4695686ade8a).
- Fixed considering timezones in `timestamp_floor_*_localtime` functions, [07f30b0](https://github.com/ytsaurus/ytsaurus/commit/07f30b07c73740ff7034592765195febf7277d53).
- Used physical chunk count instead of logical one in ordered dynamic tablet chunk lists, [96c1a7f](https://github.com/ytsaurus/ytsaurus/commit/96c1a7fbff6493e44f40476d3743255703164407).
- Eliminate a memory leak caused by cancelled selects, [75521e1](https://github.com/ytsaurus/ytsaurus/commit/75521e1cd3e86f6f21dfef8915c85c1e6f9fac0e).
- Join-predicate is now used in range inference for subqueries that fetch data from dictionaries, [d62d232](https://github.com/ytsaurus/ytsaurus/commit/d62d2329eed6c057e004a20eeacc29e0c9aab82b).
- Fixed bug leading to segfaults in table with nested columns, [45cb542](https://github.com/ytsaurus/ytsaurus/commit/45cb542f8c883e7d559fee880065ddc5abde2e34).
- Fixed incompatibility of AVG function in QL for some cases, [ef7e062](https://github.com/ytsaurus/ytsaurus/commit/ef7e062df0da5dad3efe10f6a099208aabb9da2d).

### MapReduce
#### New Features & Changes:
- Added support for dynamic reconfiguration of signature subsystem in exec node, [029f6ce](https://github.com/ytsaurus/ytsaurus/commit/029f6ce8c1eb5c3254c336129e2d616c60337b4b)
- Fixed input data slicing issues, [3bb3594](https://github.com/ytsaurus/ytsaurus/commit/3bb35948bb3ea1d2cc4fcc55a01231fe1e8aca65).
- Added option that forces lower bound of user job CPU limit, [c4675d4](https://github.com/ytsaurus/ytsaurus/commit/c4675d4b55fd8a58c91d8626c13958e0f72f4e6c).
- Added support for NBD network disks to `disk_request`, [a83d345](https://github.com/ytsaurus/ytsaurus/commit/a83d34522c977e7a712462c909fde5ab5dff6310).
- Added support for extra jobs for gang operations and introduced gang ranks, [d45f1f7](https://github.com/ytsaurus/ytsaurus/commit/d45f1f73d306dde623bdf432109d527c0a6fd46b).
- Added new delivery fenced connection that works on vanilla linux kernel; it may be used for CPU intensive or GPU jobs to prevent job abort on interruption, [d7861cd](https://github.com/ytsaurus/ytsaurus/commit/d7861cd3a6ad67144d7e1ec2bf508d52850ee3e8).
- Enhance input compressed size estimation for operations with columnar statistics, [68474e7](https://github.com/ytsaurus/ytsaurus/commit/68474e7ceba85788a169b37b56421557d26a6f8f).
- CAs now fetch schema from external cells by default, [b5932ab](https://github.com/ytsaurus/ytsaurus/commit/b5932ab6a734f01717bfde46812305fcef54ed81).
- Initial support of multiple jobs in one allocation, [ee037ac](https://github.com/ytsaurus/ytsaurus/commit/ee037ac8352969e6a43c3d087573e986e1937032).
- Hostname in containers is now built using `slot-{slot_index}.{exec_node_hostname}` format, [fe028ee](https://github.com/ytsaurus/ytsaurus/commit/fe028eebfcd8239f7d08492474e4b95a5a90dc35).
- RemoteCopy operations now set some system attributes on destination table, even if `copy_attributes` is set to false in spec; these attributes are: `compression_codec`, `erasure_codec`, `optimize_for`, [25be378](https://github.com/ytsaurus/ytsaurus/commit/25be3785e95e49e1639ccef1ff49fc7077f1e1c7).
- Fixed `yt.exec_node.rpc_proxy_in_job_proxy_count` metric, add `host` label, [416d8bf](https://github.com/ytsaurus/ytsaurus/commit/416d8bf1c4c3be290f522d3472e80e7a0ca3f020).

#### Fixes & Optimizations:
- Passed actual error instead of `Job failed by external request`, [bc9b656](https://github.com/ytsaurus/ytsaurus/commit/bc9b6564a4248f955d8efe2668fac0262d58dfc8).

### Master Server

#### New Features & Changes:
- Made the default optimize_for configurable for static and dynamic tables, [9e5d4a8](https://github.com/ytsaurus/ytsaurus/commit/9e5d4a8c66c3956bf7d9a2d30a7266590294778e).
- Introduced TCompactTableSchema to reduce master server process memory footprint (unlike TTableSchema, this holds just the protobuf-serialized schema [666861a](https://github.com/ytsaurus/ytsaurus/commit/666861ab1251db2d645bfc5120564bf49266381e).
- Introduced a hard limit on the response size for read requests to `//sys/chunks`, configurable via `virtual_chunk_map_read_result_limit`. Previously, accessing this virtual map could crash the master server due to excessive creation of fibers and subsequent memory allocation. This is a temporary mitigation while we work on a better fix, [61eb1f5](https://github.com/ytsaurus/ytsaurus/commit/61eb1f5af55f3ac4857a32972116be9b7569360f).
- Changed copy/move commands to require an explicit flag to drop secondary indices; previously this was the default behaviour, [0668453](https://github.com/ytsaurus/ytsaurus/commit/06684533f35295172063265d697ac37afe61b02f).
- Allowed users to authenticate with their aliases. This is a preliminary compatibility step toward migrating system users to a new naming scheme (e.g. `job` -> `yt-job`), [b52bbb5](https://github.com/ytsaurus/ytsaurus/commit/b52bbb5ffc4ff917d26b87559700f8601cdb93ec).
- Annotated read requests to external attributes (typically served at secondary cells) with the proper user identity, [49fd63b](https://github.com/ytsaurus/ytsaurus/commit/49fd63bbd6d97e3ba54ee2971e1bd76d1092058f). This is primarily for proper load attribution in profiling and monitoring.
- Added per-chunk replica throttling of data node heartbeat processing, [5ca5f62](https://github.com/ytsaurus/ytsaurus/commit/5ca5f62d7a1250897c44ae32c00caa391083274b).
- Include more information about secondary indices in table attributes, [cd9ebbe](https://github.com/ytsaurus/ytsaurus/commit/cd9ebbe551ac93b6e93e585e598db354cbfd1d16).
- Moved operation locking output dynamic tables from the controller to the native protocol, [9ac2870](https://github.com/ytsaurus/ytsaurus/commit/9ac2870a4835ec47a67dffc185d57c741e5255b4).
- Prohibited `chunk_host` role revocation from cells with chunks and `cypress_node_host` role revocation from cells with native Cypress nodes, [5d802d7](https://github.com/ytsaurus/ytsaurus/commit/5d802d75f50c35fdf5e47b891cded0adb1b273ec).

#### Fixes & Optimizations:
- Stopped generating mutation IDs for read requests. Mutation IDs are considered to be a part of the request message and generating a unique mutation ID for every request previously broke the object service cache, [3d1d9eb](https://github.com/ytsaurus/ytsaurus/commit/3d1d9eb212e72897130481967be753def5037315).
- Used the persistent response keeper for commit/abort requests for Cypress transactions, [b6da7ab](https://github.com/ytsaurus/ytsaurus/commit/b6da7abb213c375607680cec4de2bc4cfaca5cc9).
- Fixed crash in HydraUpdateMasterCellChunkStatistics when ChunkScanExecutor\_ called OnChunkScan more than once before the execution of the committed HydraUpdateMasterCellChunkStatistics mutation, [0c2d095](https://github.com/ytsaurus/ytsaurus/commit/0c2d095bd4159c11139085e4a4e1c9b89c1262d7).
- Fixed "list node creation is forbidden" error when attempting to set a list to an attribute of a nonexistent node, [bca5761](https://github.com/ytsaurus/ytsaurus/commit/bca5761ce7fbc723553633961f16f06af41b348b).
- Remove the broadcast from DoGetMulticellOwningNodes, [75a99bd](https://github.com/ytsaurus/ytsaurus/commit/75a99bd4d499ac660d031e93520bead4ec8cd5d0).
- Fixed potential missing inherited attributes when a node created with the "force" flag overwrote another node, [86d07f9](https://github.com/ytsaurus/ytsaurus/commit/86d07f9894a3c3f98a5631b64cc7ed1145057776).
- Fixed revision validation when revision paths differ from execution paths, [f39e9e8](https://github.com/ytsaurus/ytsaurus/commit/f39e9e8718b08a86cf2eec60faff37f2e1eb485f), [1663a23](https://github.com/ytsaurus/ytsaurus/commit/1663a2392a39e5f7a6ca8047145c8f031bfed64a).
- Forbid creation of tables that are indices of themselves, [f6404a8](https://github.com/ytsaurus/ytsaurus/commit/f6404a8c9be898caea87a29ef7bd618221f9b183).
- Fixed creation of secondary indices beyond portal, [96ad84e](https://github.com/ytsaurus/ytsaurus/commit/96ad84e8567a5a017c4e73f1c4dbb3e271f21b1a).
- Fixed careful chunk requisition update in chunk merger, [996e11c](https://github.com/ytsaurus/ytsaurus/commit/996e11c7c05ea48caa519665cdb6a4a9a8a9ef99).
- Fixed statistics checks during the removal of a master-cell, [b558b10](https://github.com/ytsaurus/ytsaurus/commit/b558b10581fe97ec9a9ef119be7424fec0ba65d5).
- Fixed job heartbeat processing on a yet non-registered new cell, [4733cbb](https://github.com/ytsaurus/ytsaurus/commit/4733cbbcafd2cf4b73d59ae80938353664b0fefd).
- Fixed the chunk replicator not respecting medium-specific replication factor override in certain scenarios, [d6bab2f](https://github.com/ytsaurus/ytsaurus/commit/d6bab2f7f251418db1fda526a729cb9aff735de9).
- Fixed the ID of the `admins` built-in group, [29529ec](https://github.com/ytsaurus/ytsaurus/commit/29529ec69ce454c03e97290c435150225e5ea36b).
- Fixed nullptr dereference in HydraCreateForeignObject, [a30d422](https://github.com/ytsaurus/ytsaurus/commit/a30d422999fe9224c1bcd678b4f5aeb3a1160cd5).
- Dropped legacy ZooKeeper shard, [558b7b5](https://github.com/ytsaurus/ytsaurus/commit/558b7b5fac04c1bb6c99a7132e4ddc115e9cf0dc).
- Fixed master-server not changing the reliability of the exec nodes, [4e9becf](https://github.com/ytsaurus/ytsaurus/commit/4e9becf2dc2992ad8a714dcdcf404f1ba40758bf).
- Fixed a race between the transaction coordinator committing a transaction and a cell with an exported object unreferencing said object, [5b72aad](https://github.com/ytsaurus/ytsaurus/commit/5b72aad7cafe1f0b44dd76acced1a52ecd4e7264).
- Fixed manual Cypress node merging for Scheduler transactions, [8a80023](https://github.com/ytsaurus/ytsaurus/commit/8a80023a50661a42abba895aacda70d118adf845).
- Fixed a master crash when setting a YSON dictionary with duplicated keys into a custom attribute, [867354b](https://github.com/ytsaurus/ytsaurus/commit/867354bffa92a4a9991d360c770e1219c4c12f81).
- Fixed row comparison in shallow merge validation so that it does not fail the job, [404a790](https://github.com/ytsaurus/ytsaurus/commit/404a790b962ee26f1a4c2085d5bfc8b223ff6199).
- Fixed a crash triggered by reading `@local_scan_flags` attribute, [d8743cb](https://github.com/ytsaurus/ytsaurus/commit/d8743cbae113dc99a3c53612f253e45c8eab4b08).
- Fixed non-deterministic error caused by the non-deterministic order of YSON struct field loading when two or more required fields are missing. Since error message is a part of the response for a master mutation, this could lead to "state hashes differ" alert on the master, [d907ada](https://github.com/ytsaurus/ytsaurus/commit/d907ada9984b6b711e4d5dd02d36d9e333df5dbb).
- Fixed TAttributeFilter handling, [488b343](https://github.com/ytsaurus/ytsaurus/commit/488b34393bb091a92befc8f6e4a6bc700da0d670).
- Fixed locking for concatenation in append mode, [c1f5c7e](https://github.com/ytsaurus/ytsaurus/commit/c1f5c7ed4454d1637303ad5ae4843b25ca611e04).
- Fixed a bug related to the compatibility patch for the imaginary chunk locations, [f591951](https://github.com/ytsaurus/ytsaurus/commit/f591951182e9555c7ca58173ec14a75e7b6d41a7).

### Data Node
#### New Features & Changes:
- Added support for erasure encoding in read size estimation, [4b3a28e](https://github.com/ytsaurus/ytsaurus/commit/4b3a28e1089de7d9bf790d912cd874b092f16696).
- Added flag `enable_read_size_estimation` to disable read size estimation (`true` by default), [4b3a28e](https://github.com/ytsaurus/ytsaurus/commit/4b3a28e1089de7d9bf790d912cd874b092f16696).

#### Fixes & Optimizations:
- Fixed chunk meta extensions absorption in meta aggregated writer, [677d2d4](https://github.com/ytsaurus/ytsaurus/commit/677d2d4d6665f87c9bd25a7e1ae6a6ca86f67a9d).
- Fixed bug in computing compression ratio in read size estimation based on heavy columnar statistics, add unit tests to address such kind of bugs, [4b3a28e](https://github.com/ytsaurus/ytsaurus/commit/4b3a28e1089de7d9bf790d912cd874b092f16696).
- Added config for master cell directory synchronizer into cluster node dynamic config, [d049363](https://github.com/ytsaurus/ytsaurus/commit/d04936326ce00b11022cb5d029b4094c818dfcbd).
- Fixed start of node heartbeats before actual registration, [58e442b](https://github.com/ytsaurus/ytsaurus/commit/58e442baa20350412949b31072a9f8aa1585d4b3).
- Fixed crash in meta aggregated writer on corrupted chunks, [5653dfb](https://github.com/ytsaurus/ytsaurus/commit/5653dfbae6192331c3312fbe0e8cc714ae3e7c2e).
- Reuse node lease transaction during node re-registration, [d0eb92b](https://github.com/ytsaurus/ytsaurus/commit/d0eb92b986f8993e7c8b223bc85e3ffb73e5059f).
- Fixed crash of node disconnecting while starting heartbeat report, [66efd89](https://github.com/ytsaurus/ytsaurus/commit/66efd898cb7ffb68b263498ffae7d29952204aa9).

### Other
#### New Features & Changes:
- Added sensors for mlock calls, [085a74c](https://github.com/ytsaurus/ytsaurus/commit/085a74cac407e46117cae0a4f536693eab21a45c).
- Implemented stockpile relative to user-jobs memory limit (it is necessary for exec nodes in dynamic-tables-oriented clusters), [8b7c91b](https://github.com/ytsaurus/ytsaurus/commit/8b7c91b9bebc99d685559687145347c999707059).

#### Fixes & Optimizations:
- Stopped aborting node lease transaction when re-register, [6da69bd](https://github.com/ytsaurus/ytsaurus/commit/6da69bd2b67e34e99d360d91200bb1f262b8a99f).
- Fixed UB in chunked memory pool, [ec99700](https://github.com/ytsaurus/ytsaurus/commit/ec997008c02ff7b9e934698512d07fcfd5778690).
- Fixed UB in logging zstd compression, [870ca53](https://github.com/ytsaurus/ytsaurus/commit/870ca53f9a19697d1a86f9462ce3bfdfec9738b3).
- Fixed a bug in RPC service where a heavy request which had been queued would use propagating storage (e.g. a trace context) from another request, [7745b84](https://github.com/ytsaurus/ytsaurus/commit/7745b84081672cfbe5ccd8c2f50498a44081ba3f).
- Improved tracking of memory used in concurrent cache, [122fd89](https://github.com/ytsaurus/ytsaurus/commit/122fd89ce22e6eb1f6cb48672463a0c264af9069).
- Used 64-bit counters for histogram buckets, [94fe6d3](https://github.com/ytsaurus/ytsaurus/commit/94fe6d36acebe32f2609ff1b7be7547dbeeaa446).



{% endcut %}


{% cut "**25.1.0**" %}

**Release date:** 2025-07-28


_To install YTsaurus Server 25.1.0 [update](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release%2F0.25.0) the k8s-operator to version 0.25.0._

## Significant changes

### API
  - Enhancement of operations/jobs API.
  - Introduced requests memory tracking in HTTP proxies.

### Compute
  - Implemented permission validation for operations using access control objects.
  - Gang-operations gone production. Gang-operations represent a special scheduling mode of vanilla-operations, particularly useful for distributed ML model training. 
  - Introduced PatchSpec API to modify operation spec at runtime.
  - [experimental] Remote copy scheduler now considers inter-cluster bandwidth limits.
  - Introduced job shell for CRI-based job environments.

### Storage
  - Supported `decimal256` type.
  - Support non-materialized computed column evaluation in static tables.

### Query language features
  - Added `allow_async_replica` hint to QL table descriptors to enable fallback to async replicas in selects.
  - Timestamp functions in QL now support localtime  and use lookup tables for better performance.
  - QL AVG  function now supports doubles and unsigned integers.
  - Functions list_contains  and list_has_intersection  in QL now support strongly typed composite values and nulls.
  - `EXPLAIN` query now works with replicated tables.

### Dynamic tables
  - Implemented gradual global compaction ("chunk reincarnation") for dynamic tables.
  - Supported remote copy for dynamic tables with hunk chunks.

### Default values
 - Changed default read format to scan  for dynamic tables.
 - Enabled versioned remote copy by default.
 - Enabled remote copy of hunk chunks by default.
 - Enabled two-random-choices allocation strategy for write targets by default.

## Full changelog

### Scheduler and GPU

#### New Features & Changes:
- Added alert for unrecognized options in the pool tree config, [6b2770b](https://github.com/ytsaurus/ytsaurus/commit/6b2770b65a51766fbd1ad53d81d6b409a2115f00).
- Added support for generating temporary tokens valid for the duration of an operation, stored in the operation’s secure vault, [c91fd05](https://github.com/ytsaurus/ytsaurus/commit/c91fd057421160254f41d6c394673bf654fc358d).
- Added an option to only allow gang operations to start in FIFO pools, [edcac9a](https://github.com/ytsaurus/ytsaurus/commit/edcac9a3e348b18fd03cb82fb9e15e782b1b4a58).
- Implemented permission validation for operations using access control objects, [bd64281](https://github.com/ytsaurus/ytsaurus/commit/bd64281b11d8fef1a8f497c07ee1a0c5372073d9).
- Enhancement of operations/jobs API:
  - Introduced `attributes` and `events` in `list_jobs`  [25a405d0c88](https://github.com/ytsaurus/ytsaurus/commit/25a405d0c88).
  - Access to operations archive and `//sys/operations` is closed by default.
  - Added operation incarnations to archive and supported corresponding filter in API [6601c8dbbb1](https://github.com/ytsaurus/ytsaurus/commit/6601c8dbbb1).
- Added support for managing operations within `manage` right on pool [177380edfed](https://github.com/ytsaurus/ytsaurus/commit/177380edfed). 
- Supported new logic for calculating cpu limit overcommit [d6d1d08a91f](https://github.com/ytsaurus/ytsaurus/commit/d6d1d08a91f).
- Added ability to run GPU checks in isolated volumes and configure them via operation options, [631f328](https://github.com/ytsaurus/ytsaurus/commit/631f3288fa3eb07176c777c93bcf3afddfaf30dc).

#### Fixes & Optimizations:
- Fixed bug when `offloading` and `schedule_in_single_tree` options specified simultaneously [e36e910b718](https://github.com/ytsaurus/ytsaurus/commit/e36e910b718)
- Fixed fair share truncation in FIFO pools for single allocation vanillas [5d8e22b67fa](https://github.com/ytsaurus/ytsaurus/commit/5d8e22b67fa).
- Dropped some scheduler compats and legacy options.

### Queue Agent

#### New Features & Changes:
- Added `controller_info` field to queue agent Orchid for detecting stuck controller passes and tracking error counters, [74be9b4](https://github.com/ytsaurus/ytsaurus/commit/74be9b4ab44689d56f8110dc2313e1dc7c1e3057).
- Export progress now includes details about the last export iteration, [6ec743a](https://github.com/ytsaurus/ytsaurus/commit/6ec743a21e4f4ed8e917fc17e822875367709ad3).
- Aggregated alert metrics for the queue agent have been added, [d2bf505](https://github.com/ytsaurus/ytsaurus/commit/d2bf505c9b3bafa8f0966cdd0234c4929416c293).

#### Fixes & Optimizations:
- Added retry backoffs to avoid overloading in case of misconfigured exports, [ae4ead4](https://github.com/ytsaurus/ytsaurus/commit/ae4ead4a1e9e5b86bbc9ffde2637488fb1dbe6ff).
- Fixed crash when reusing an export directory after recreating a queue without resetting export progress, [49826ed](https://github.com/ytsaurus/ytsaurus/commit/49826ed8d9872d3d094224e0811335a2647ef7c3).

### Proxy

#### New Features & Changes:
- Added support for HTTP proxies in the `discover_proxies` handler, [830e543](https://github.com/ytsaurus/ytsaurus/commit/830e543c1bbde6069180bdf451c807ff0175bdb6).
- Preserved original column types (e.g., `timestamp`) in `web_json` responses from `select_rows`, [f4eb42d](https://github.com/ytsaurus/ytsaurus/commit/f4eb42d1b4e8521b8d70d83ec19ef50cb63647c5).
- Enabled signature generation and verification in HTTP proxy using Cypress public keys, [f8d0c7d](https://github.com/ytsaurus/ytsaurus/commit/f8d0c7d4f096d335b7fc5313f360dcab6d47ceb2).
- Introduced memory tracking for light/heavy HTTP requests, [06f7aeb](https://github.com/ytsaurus/ytsaurus/commit/06f7aeb2a484a802b4a401fcfd0c3216c9cdc33f).
- Added `require_sync_replica` parameter to the `push_queue_producer` handler, [1324b1b](https://github.com/ytsaurus/ytsaurus/commit/1324b1bb4aa2ec33249447dcc38257836b9e5ca0).
- Enabled parallel execution of `discover_versions` across components, [f06afce](https://github.com/ytsaurus/ytsaurus/commit/f06afce8d73c34082343b85f577f8ae3edcb114d).
- Added `create_user_if_not_exists` config option to prevent user creation during OAuth authentication (Issue #930), [8470ed6](https://github.com/ytsaurus/ytsaurus/commit/8470ed63a46beb3c483c885d62a195fbfd4ac77f).
- Added `require_password_in_authentication_commands` flag to allow skipping password checks in auth commands, [723db18](https://github.com/ytsaurus/ytsaurus/commit/723db18f80a3ceff00ed67d6a9cb5872b9c1ffda).
- Introduced `cache_key_mode` to control the granularity of credential caching, [df66eb5](https://github.com/ytsaurus/ytsaurus/commit/df66eb5a00ab7d338fb0972f618ad28862a2a440).
- Added `EnableCookies` option to `PartitionTable` request, which returns an opaque cookie for `CreateTablePartitionReader`, [f2f1ce6](https://github.com/ytsaurus/ytsaurus/commit/f2f1ce6039fd84e6cd13341e90db0a834ea1580e).
- Added `CreateTablePartitionReader` method to read a table partition without master requests, [f2f1ce6](https://github.com/ytsaurus/ytsaurus/commit/f2f1ce6039fd84e6cd13341e90db0a834ea1580e).
- Added support for signature validation using Cypress public keys, [12c8532](https://github.com/ytsaurus/ytsaurus/commit/12c85321bfd4fd41f32db7ee356345e0aee9c2b0).
- Added a handler to retrieve job trace events, [8ee855e](https://github.com/ytsaurus/ytsaurus/commit/8ee855ef01ae55a6c500d6ec6029bac3b3c8260c).

#### Fixes & Optimizations:
- Switched to using a dedicated thread pool for HTTP stream compression, [4f61857](https://github.com/ytsaurus/ytsaurus/commit/4f61857cbabea72e403b269f361de5b01bb6d746).
- Added type compatibility checks in the Arrow parser using YT-specific types, [bd0a6ff](https://github.com/ytsaurus/ytsaurus/commit/bd0a6ff376a1c2ec28e10d4ee4d476d70bb8a131).
- Introduced `UploadTransactionPingPeriod` config to correctly handle upload transaction timeouts, [f10f749](https://github.com/ytsaurus/ytsaurus/commit/f10f749527d924fece0e52a0248cf0c58ed8d313).
- Embedded HTTP proxy Arrow writer directly into the Arrow encoder to remove duplication, [99293b6](https://github.com/ytsaurus/ytsaurus/commit/99293b6cf4c9a157209d556bc8e6f128be3832e1).
- Enabled reading of Arrow tables with different column counts in chunk metadata, [6210035](https://github.com/ytsaurus/ytsaurus/commit/6210035056c12e3b078f7a3e01b562030f946192).
- Fixed incorrect data type in driver for `push_queue_producer` input, [ebffb74](https://github.com/ytsaurus/ytsaurus/commit/ebffb746ada4d63b768c7627e9bdee110b686cbd).
- Decimal improvements:
  - Added support for nested `decimal128` and `decimal256` in Arrow.
  - Fixed incorrect fixed-length encoding of `decimal256(n, p)` for small precisions, now variable length like `decimal128`, [58c6c65](https://github.com/ytsaurus/ytsaurus/commit/58c6c6590919a2bfbb88c9f5b833324b86623ead).

### Dynamic Tables

#### New Features & Changes:
- Added row-cache poisoning to help detect memory errors, [4933fe9](https://github.com/ytsaurus/ytsaurus/commit/4933fe97dd68a40b2c5cdce4d2aa77000fdfb8dc).
- Added `allow_async_replica` hint to QL table descriptors to enable fallback to async replicas in selects, [27ac9b9](https://github.com/ytsaurus/ytsaurus/commit/27ac9b90f5d50dfc63490d86f24fb88bc042e74b).
- Added a method to return freezing or unmounting tables to the mounted state, [cdb2027](https://github.com/ytsaurus/ytsaurus/commit/cdb2027bf1b1de28a9a025a14375da5cd29bcd75).
- Enabled lookups to use the fair-share thread pool, [b19ecb3](https://github.com/ytsaurus/ytsaurus/commit/b19ecb34beae00c66f1c11ad3e6a19681f711087).
- Introduced a protocol that allows writing to tablets during smooth movement, [3ab47cd](https://github.com/ytsaurus/ytsaurus/commit/3ab47cd930accca3d38fac269e67b8ae8862d29b).
- Bundle controller can now manage memory limits for queries, [c6c6de4](https://github.com/ytsaurus/ytsaurus/commit/c6c6de47a8a8f0d64668a6a219e369f5c20a73ed).
- Timestamp functions in QL now support `localtime` and use lookup tables for better performance, [265160e](https://github.com/ytsaurus/ytsaurus/commit/265160e382062e7e19673da27f0242a636b7c51f).
- QL `AVG` function now supports doubles and unsigned integers, [d597f37](https://github.com/ytsaurus/ytsaurus/commit/d597f37096f44c7cc418da5cb986e06ed4377625).
- Functions `list_contains` and `list_has_intersection` in QL now support strongly typed composite values and nulls, [aeb6b24](https://github.com/ytsaurus/ytsaurus/commit/aeb6b24220a07385ab1975b610a6af06a75ed9fc).
- Added profiling counters for `pull_queue` and `pull_queue_consumer` commands on tablet nodes, [6aebfc1](https://github.com/ytsaurus/ytsaurus/commit/6aebfc1a758092864dae6b25177ec4339bc1c2db).
- `EXPLAIN` query now works with replicated tables, [fcb3dba](https://github.com/ytsaurus/ytsaurus/commit/fcb3dbab1a50858856e5aa52977a185620bb4b56).
- Changed default read format to `scan` for dynamic tables, [f2ccc73](https://github.com/ytsaurus/ytsaurus/commit/f2ccc73dbd5b2883d733b16145ae16f55e7fe272).
- Select queries now choose random in-sync replicas even from the same cluster, [efdf083](https://github.com/ytsaurus/ytsaurus/commit/efdf083a7d8cc0e7345f8b7705b87c0ec6a7ee47).
- Added `total_grouped_row_count` to QL statistics, [e37b81f](https://github.com/ytsaurus/ytsaurus/commit/e37b81f5199263dd9d15ef1116cd0be0463f8145).
- Enabled row-cache support for selects, useful for dictionary-style joins, [bba767b](https://github.com/ytsaurus/ytsaurus/commit/bba767b3f8671bddb8f6aed77d0ee25eed16d056).
- Implemented gradual global compaction ("chunk reincarnation") for dynamic tables, [6d405af](https://github.com/ytsaurus/ytsaurus/commit/6d405af0192ab66acf71bccbf38f2f02fda966bc).
- Added support for lookup format with timestamp columns, [180af7c](https://github.com/ytsaurus/ytsaurus/commit/180af7c4b45cacaa66783809c9c7b4759c385348).
- Added `allow_reign_change` parameter to tablet node config for crash-on-reign-change testing, [3754c50](https://github.com/ytsaurus/ytsaurus/commit/3754c50c141d89ef9ce6fe6a40df24e941d900ba).
- Introduced states for secondary indices: `invalid`, `bijective`, `injective`, and `unknown`, [81068e0](https://github.com/ytsaurus/ytsaurus/commit/81068e0145fcb6899cc8e351fcd72398ca55a763).
- Enabled versioned remote copy by default, [0862748](https://github.com/ytsaurus/ytsaurus/commit/08627486d980cc99aa0e8ec14104c7c6fbf43dea).
- Added support for remote copy of erasure-coded hunk chunks, [2b783b0](https://github.com/ytsaurus/ytsaurus/commit/2b783b04de6e5ade76d159ce05e674bb9545b50f).
- `GROUP BY + LIMIT` without `ORDER BY` is now executed in parallel unless certain conditions apply. Affects behavior of `WITH TOTALS`, [c296f24](https://github.com/ytsaurus/ytsaurus/commit/c296f24846653dff7b7e6bc2b3c913e05fa479ce).
- Remote copy for dynamic tables with hunk chunks is now supported (except with compression dictionaries and striped erasure), [34f16d0](https://github.com/ytsaurus/ytsaurus/commit/34f16d0fd4c307134608f27cfb5cc028b3cbc771).
- `dump-snapshot` command now supports `checksum` mode to help debug snapshot mismatches, [5daa913](https://github.com/ytsaurus/ytsaurus/commit/5daa91303d7210689ecb117614b908523bd429af).

#### Fixes & Optimizations:
- Fixed float precision issues in scan reads by serializing floats as doubles, [0b78e7a](https://github.com/ytsaurus/ytsaurus/commit/0b78e7ad5b4a4ec48ece1f92501172bbeff8ec6d).
- Fixed incorrect timezone behavior in `timestamp_floor_*_localtime` functions, [17f3dd1](https://github.com/ytsaurus/ytsaurus/commit/17f3dd1debf37d5b2a0222cf3d906b40e523ae12).
- Switched from logical to physical chunk count in ordered dynamic tablet chunk lists, [2efe013](https://github.com/ytsaurus/ytsaurus/commit/2efe013a9ba51a105c3ce490b6cffdcdcc415c36).
- Use lookup joins automatically when the left subplan is selective, [4b8b207](https://github.com/ytsaurus/ytsaurus/commit/4b8b207a6ea179d480bc05f5478a226d31c3da99).
- Fixed segfaults when reading tables with nested columns, [45cb542](https://github.com/ytsaurus/ytsaurus/commit/45cb542f8c883e7d559fee880065ddc5abde2e34).
- Bundle controller now skips faulty bundles instead of blocking progress, [dc149b2](https://github.com/ytsaurus/ytsaurus/commit/dc149b27843635d16d2416f09a21ffba8b4de702).

### MapReduce

#### New Features & Changes:
- Added various job splitter options to operation specs, [e2998a4](https://github.com/ytsaurus/ytsaurus/commit/e2998a41e1794bfc139709f1df6cfd282cf82e33).
- Preserved job cookies during gang operation restarts and allowed restarting already completed jobs on incarnation switch, [e3f7655](https://github.com/ytsaurus/ytsaurus/commit/e3f7655698256ea6c257171d17ba8af7f130a299).
- Added operation incarnation controller that restarts all jobs if one terminates (useful for distributed ML), [fb9c7d3](https://github.com/ytsaurus/ytsaurus/commit/fb9c7d3d97563cb4e7d8d80270fe1be4739606d3).
- Introduced job shell for CRI-based job environments, [6b18f2f](https://github.com/ytsaurus/ytsaurus/commit/6b18f2f3d836bc63e6e89d44dcce328a5bc2d958).
- Allowed dynamic updates to `job_count` for vanilla tasks, [3a5cfef](https://github.com/ytsaurus/ytsaurus/commit/3a5cfef7f0a2a87cd5b48637d03467dc49f82f91).
- Introduced `PatchSpec` API to modify operation spec at runtime (initially supports `max_failed_job_count`), [3a5cfef](https://github.com/ytsaurus/ytsaurus/commit/3a5cfef7f0a2a87cd5b48637d03467dc49f82f91).
- `RemoteCopy` operations now always copy key system attributes (`compression_codec`, `erasure_codec`, `optimize_for`) even if `copy_attributes` is false, [25be378](https://github.com/ytsaurus/ytsaurus/commit/25be3785e95e49e1639ccef1ff49fc7077f1e1c7).
- Remote copy scheduler now considers inter-cluster bandwidth limits, [e0af4fd](https://github.com/ytsaurus/ytsaurus/commit/e0af4fdbdea4fcab2561b803364f153078195223).
- Controller agents now always fetch schemas from secondary cells using schema IDs, [af00687](https://github.com/ytsaurus/ytsaurus/commit/af0068716ea05530aade3474f15e59c7b2ab9d16).

#### Fixes & Optimizations:
- Fixed a bug where teleportation of a single chunk in unordered pool could fail, [897ffff](https://github.com/ytsaurus/ytsaurus/commit/897ffff6c1ed4f7d21d52b3fb456ff8fa8b7023b).
- Returned specific error instead of generic `Job failed by external request`, [703aae5](https://github.com/ytsaurus/ytsaurus/commit/703aae58eb1b2a515c2aa9e49d4e9ae63bc3a39e).

### Master Server

#### New Features & Changes:
- Added metrics for read/write request rate limits and request queue size per user, [0052341](https://github.com/ytsaurus/ytsaurus/commit/0052341c563cc3c9eb2a3bdca11aaf7d36eabce4).
- Prevented prerequisite paths from differing from execution paths, [50156f0](https://github.com/ytsaurus/ytsaurus/commit/50156f0aa5382824a725ab27d3d1b2dc6210293b).
- Support opaqueness for `@schema` attribute, [4f2c6ad](https://github.com/ytsaurus/ytsaurus/commit/4f2c6ad11f2bc951642546e33b790705911cee3d).
- Allowed master cell removal without downtime of other components (except masters), [16bd5ba](https://github.com/ytsaurus/ytsaurus/commit/16bd5baf39e018085f42d8776dffefe16e648d27).
- Introduced a new pipeline for cross-cell copy operations, [e7eea1e](https://github.com/ytsaurus/ytsaurus/commit/e7eea1ed49fb49d12d64639161e33943a4e06fa4).
- Added `TSnapshotLoadContextGuard` to provide access to readonly mode during snapshot loading, [ee94027](https://github.com/ytsaurus/ytsaurus/commit/ee9402746c38adf4b23d5166cc4f41fdc01c4f20).
- Added validation of meta aggregated writer chunk extensions during master merge jobs, [de86b64](https://github.com/ytsaurus/ytsaurus/commit/de86b64ca13ed72993e83e5e6bccedcb25fff0ef).
- Reduced `TTableNode` memory usage by optimizing enums and chunk requisition fields, [90226b0](https://github.com/ytsaurus/ytsaurus/commit/90226b0af60dffa15565e7f4642c0e0482038c96).
- Always enabled the local read executor, [521af69](https://github.com/ytsaurus/ytsaurus/commit/521af696a57c69c7b19855b310dacae1fce7e6a9).
- Reworked `@hunk_primary_medium` and `@hunk_media` attributes:
  - They are now nullable.
  - Hunk chunks can be placed on a different medium than table chunks.
  - Replication is respected by the chunk replicator and initial writer.
  - Setting and clearing behavior is now well-defined, [508e000](https://github.com/ytsaurus/ytsaurus/commit/508e000430a6637a030c1575c7963efe1e37723a).
- Added `snapshot-dump-scope-filter` option to limit output during master snapshot dumps, [8a73474](https://github.com/ytsaurus/ytsaurus/commit/8a734741b3b893596562e8b0968a82983679ea41).
- Enabled two-random-choices allocation strategy for write targets by default, [1a153fb](https://github.com/ytsaurus/ytsaurus/commit/1a153fb75b539f97bb3534cbd2a24b5405c4c91c).
- Introduced Sequoia response keeper for tracking responses, [f886fa1](https://github.com/ytsaurus/ytsaurus/commit/f886fa13b8efa2c0959587a6e0bc6f5a5541a846).
- Added `checksum` mode to `dump-snapshot` CLI command for debugging snapshot mismatches, [5daa913](https://github.com/ytsaurus/ytsaurus/commit/5daa91303d7210689ecb117614b908523bd429af).
- Enabled non-materialized computed column evaluation in static tables, [221672d](https://github.com/ytsaurus/ytsaurus/commit/221672d07a7b81987beff311ec22319d631a8278).

#### Fixes & Optimizations:
- Fixed validation error when prerequisite and execution paths differ, [76ceba0](https://github.com/ytsaurus/ytsaurus/commit/76ceba0c03c5fce8bc042dcb3726a3997a2153d5).
- Prevented users from creating tables that are indices to themselves, [f384a9f](https://github.com/ytsaurus/ytsaurus/commit/f384a9f9ea286f4cc4b018498bca6596664c0c85).
- Fixed chunk replicator to correctly handle medium-specific replication overrides, [2e0ca64](https://github.com/ytsaurus/ytsaurus/commit/2e0ca6446ad66eeba31e4c3a2b91d56bf11c0293).
- Fixed chunk requisition updates during merging, [29cc496](https://github.com/ytsaurus/ytsaurus/commit/29cc4968ed660253c6b8167993a67684a716eb9f).
- Fixed duplicate validation issue on paths, [3beea00](https://github.com/ytsaurus/ytsaurus/commit/3beea00256a370a74e5a9c1cb4b56e437a4159ad).
- Fixed master statistics check on cell removal, [2e93998](https://github.com/ytsaurus/ytsaurus/commit/2e9399802d02177cc0cd82069da18e09f5ade73a).
- Fixed handling of replication override in chunk replicator (duplicate fix), [8c711f8](https://github.com/ytsaurus/ytsaurus/commit/8c711f8b07d3f3ee55b7b4fb503b736072b66cdb).
- Fixed group ID for built-in `admins` group, [f687614](https://github.com/ytsaurus/ytsaurus/commit/f687614331b358448d59af9438dfb7398a4909e0).
- Fixed crash caused by null pointer dereference in `HydraCreateForeignObject`, [8ba1cab](https://github.com/ytsaurus/ytsaurus/commit/8ba1cabb0ccf6c958374ea2fe4948762609a1823).
- Removed legacy ZooKeeper shard code, [c0482a7](https://github.com/ytsaurus/ytsaurus/commit/c0482a74746e10d16900608dfdf7c4455a81a751).
- Fixed reliability status updates for exec nodes, [4e9becf](https://github.com/ytsaurus/ytsaurus/commit/4e9becf2dc2992ad8a714dcdcf404f1ba40758bf).
- Fixed race between transaction commit and exported object cleanup, [5b72aad](https://github.com/ytsaurus/ytsaurus/commit/5b72aad7cafe1f0b44dd76acced1a52ecd4e7264).
- Eliminated false alerts caused by harmless exported object races, [8a80023](https://github.com/ytsaurus/ytsaurus/commit/8a80023a50661a42abba895aacda70d118adf845).
- Fixed crash when setting a YSON dictionary with duplicate keys into a custom attribute, [867354b](https://github.com/ytsaurus/ytsaurus/commit/867354bffa92a4a9991d360c770e1219c4c12f81).
- Fixed row comparison logic in shallow merge validation, [404a790](https://github.com/ytsaurus/ytsaurus/commit/404a790b962ee26f1a4c2085d5bfc8b223ff6199).
- Cleared pending container restarts to avoid reinitialization issues, [1f0d9f7](https://github.com/ytsaurus/ytsaurus/commit/1f0d9f7bccc8faae5302f68e32589f1d1a963068).
- Fixed crash when reading `@local_scan_flags` attribute, [d8743cb](https://github.com/ytsaurus/ytsaurus/commit/d8743cbae113dc99a3c53612f253e45c8eab4b08).
- Fixed non-deterministic error messages caused by unordered YSON fields, [d907ada](https://github.com/ytsaurus/ytsaurus/commit/d907ada9984b6b711e4d5dd02d36d9e333df5dbb).
- Fixed compatibility issue with imaginary chunk locations, [f591951](https://github.com/ytsaurus/ytsaurus/commit/f591951182e9555c7ca58173ec14a75e7b6d41a7).

### Other

#### New Features & Changes:
- Added memory usage tracking for logging, [f0351fa](https://github.com/ytsaurus/ytsaurus/commit/f0351fa2aa2278c7ac804c93074e91d70e724138).
- Added memory usage tracking for chunk replica cache, [aa5053d](https://github.com/ytsaurus/ytsaurus/commit/aa5053d5b243a3356d95388fb51094e2ed43ea68).
- Added memory usage tracking for sensors, [8396ecd](https://github.com/ytsaurus/ytsaurus/commit/8396ecddde795abc6b52642c07fbe56b5b76581d).
- Introduced support for running multiple daemons in a single `ytserver` process (multidaemon mode), [2986da8](https://github.com/ytsaurus/ytsaurus/commit/2986da8386705b72d1f2de8b4a6a9de21b4c05ea).
- Added sanitization of monitoring labels, [5bebc7a](https://github.com/ytsaurus/ytsaurus/commit/5bebc7aabdc9439f49e63b8a9104e8256a273940).
- Added support for `decimal256` type (up to 76 digits precision), including Skiff format as `int256`, [implicit].
- Added `message_level_overrides` option to logging config for fine-grained runtime control, [4e13e2a](https://github.com/ytsaurus/ytsaurus/commit/4e13e2a32db1cb46272330c131c1c4ca3f50994d).

#### Fixes & Optimizations:
- Fixed a bug where queued heavy RPC requests could incorrectly reuse storage (e.g. trace context) from unrelated requests, [6bd19a0](https://github.com/ytsaurus/ytsaurus/commit/6bd19a014d512d4d5f2bcac54ae2087f65cce3f9).
- Improved tracking of row cache memory usage, [122fd89](https://github.com/ytsaurus/ytsaurus/commit/122fd89ce22e6eb1f6cb48672463a0c264af9069).
- Histogram counters now use 64-bit values, [94fe6d3](https://github.com/ytsaurus/ytsaurus/commit/94fe6d36acebe32f2609ff1b7be7547dbeeaa446).
- Fixed propagation of `TLargeColumnarStatisticsExt` in MAW, [bee2ba3](https://github.com/ytsaurus/ytsaurus/commit/bee2ba31007789d05a6799a7e38add72dab6a8b7).
- Fixed handling of unexpected state in data node heartbeat logic, [2e16721](https://github.com/ytsaurus/ytsaurus/commit/2e16721f4f2aec16089ee6eae580e8bca03347ba).
- Properly handled peer disconnects in the NBD server, [b1f2acf](https://github.com/ytsaurus/ytsaurus/commit/b1f2acf7363a9e5dfc123f2cdf60483d3e9a37cb).
- NBD queue now uses a dedicated thread pool, configurable via dynamic config, [5e9c7fe](https://github.com/ytsaurus/ytsaurus/commit/5e9c7fe3167aa81403b0b4f8ced3cafd0ad544a2).



{% endcut %}


{% cut "**24.2.1**" %}

**Release date:** 2025-07-28


_To install YTsaurus Server 24.2.1 [update](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release%2F0.22.0) the k8s-operator to version 0.22.0._

### Proxy
Features:
- Support HTTP proxies in `discover_proxies` handler, [0dc02db](https://github.com/ytsaurus/ytsaurus/commit/0dc02db399a7e3e6255f8716102add5bf404bd39).
- Support full result table in YQL queries result, [2d7b0d3](https://github.com/ytsaurus/ytsaurus/commit/2d7b0d3e761361ff3157e7a36465ee79478ab3c7).

Fixes:
- Set attribute `treat_as_queue_producer=%true` during `queue_producer` creation, [1ee68e1](https://github.com/ytsaurus/ytsaurus/commit/1ee68e1fa7409d125605ba4047f655532f42f6ee).
- Fix `PartitionTables` handler for ordered mode, [100bdc4]( https://github.com/ytsaurus/ytsaurus/commit/100bdc425c787902212c041822ef9557ebb7b932).

### Master
Fixes:
- Fix a crash on `GetIteratorOrCrash` in `TChunkMerger::HydraFinalizeChunkMergeSessions`, [5eda095]( https://github.com/ytsaurus/ytsaurus/commit/5eda0952c2a3453d853a83c24b16b3a5d7f31d49).
- Fix for Object Service hang ups, [31e2dfd](https://github.com/ytsaurus/ytsaurus/commit/31e2dfd7dbe7ad001f11d30fa1c59bc7a7a9ca21).
- Fix a data race on cache cookies in Object Service, [1d79d8d](https://github.com/ytsaurus/ytsaurus/commit/1d79d8d37d81438389a5280c595e8254cfc6f678).

### Queue Agent
Features:
- Add a flag to select queue export implementation, [3ec2e69](https://github.com/ytsaurus/ytsaurus/commit/3ec2e6945cf0af0f64d16e94ee5aa66e902abbf1).
Fixes:

Fixes:
- Fix `init_queue_agent_state` script to handle restart cases properly, [ae43add](https://github.com/ytsaurus/ytsaurus/commit/ae43add3ecd560c1f49fc41b40a2b0bd6b1f402f).
- Fix potential data loss in case of multiple exports per queue due to incorrect merging of queue export progresses, [c44e13b](https://github.com/ytsaurus/ytsaurus/commit/c44e13b5271ff91c5860c91448fbd704924ddf35).

### Tablet Balancer
Fixes:
- Fix deviation recalculation during parameterized merge right index search, [b7df500](https://github.com/ytsaurus/ytsaurus/commit/b7df5009926f4d0386fa924902da46e71e63bb54)

### Other
Feature:
- Support `ytprof` for all components.

{% endcut %}


{% cut "**24.2.0**" %}

**Release date:** 2025-03-19


_To install YTsaurus Server 24.2.0 [update](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release%2F0.22.0) the k8s-operator to version 0.22.0._

---
### Known Issue
- Expanding a cluster with new master cells is temporarily disabled due to a bug. This issue will be resolved in the upcoming 25.1 version.

---

### Scheduler and GPU
New Features & Changes:
- Added support for ACO-based access control in operations.
- Introduced `get_job_trace` method in the jobs API.
- Added an option to fail an operation if started in a non-existent pool.
- Enriched configuration options for offloading operations to pool trees.

Fixes & Optimizations:
- Fixed scheduling issues with new allocations immediately after operation suspension.
- Optimized fair share updates within the control thread.

### Queue Agent
New Features & Changes:
- Queue exports are now considered when trimming replicated and chaos replicated table queues.
- Introduced sum aggregation for lag metrics in consumer partitions.
- Refactor queue exports: add retries and rate limiting.

Fixes & Optimizations:
- Fixed a possible queue controller suspension by adding a timeout to `GetOrderedTabletSafeTrimRowCount` requests.
- Corrected lock options when acquiring a shared lock for the export directory.
- Resolved expiration issues of clients for chaos queues and consumers when the cluster connection changes.

### Proxy
New Features & Changes:
- Supported YAML format for structured data. See details in the RFC: [YAML-format support](https://github.com/ytsaurus/ytsaurus/wiki/%5BRFC%5D-YAML-format-support).
- Introduced create_user_if_not_exists config flag to disable automatic user creation during OAuth authentication. [Issue](https://github.com/ytsaurus/ytsaurus/issues/930).
- Added `cache_key_mode` parameter for controlling credential caching granularity.
- Implemented a new handler for retrieving job trace events.

Fixes & Optimizations:
- The `discover_proxies` handler now returns an error when the proxy type is `http`.
- Fixed a heap buffer overflow in the Arrow parser.
- If insufficient memory is available to handle RPC responses, a retryable `Unavailable` error will now be returned instead of the non-retryable `MemoryPressure` error.
- Optimized the `concatenate` method.

### Kafka proxy
Introduce a new component: the Kafka Proxy. This MVP version enables integration with YTsaurus queues using the Kafka protocol. It currently supports a minimal API for writing to queues and reading with load balancing through consumer groups.

### Dynamic Tables
New Features & Changes:
- Introduced Versioned Remote Copy for tables with hunks.

Fixes & Optimizations:
- Fixed issues with secondary indices in multi-cell clusters (especially large clusters).
- Improved stability and performance of chaos replication.
  
### MapReduce
New Features & Changes:
- Disallowed cluster_connection in remote copy operations.
- Introduced single-chunk teleportation for auto-merge operations.
- Merging of tables with compatible (not necessarily matching) schemas is supported.
- Refactored code in preparation for gang operation introduction.
- Refactored code to enable job-level allocation reuse.
- Improved per-job directory logging in job-proxy mode.

Fixes & Optimizations:
- Optimized job resource acquisition in exec nodes.
- Fixed cases of lost metrics in exec nodes.

### Master Server
New Features & Changes:
- Added an option to fetch input/output table schemas from an external cell using a schema ID.
- Deprecated the list node type; its creation is now forbidden.
- Introduced a new write target allocation strategy using the “two random choices” algorithm.
- Implemented an automatic mechanism to disable replication to data nodes in failing data centers. This can be configured in `//sys/@config/chunk_manager/data_center_failure_detector`.
- Introduced pessimistic validation for resource usage increases when changing the primary medium.
- Forbidden certain erasure codecs in remote copy operations.
- Added node groups attribute for node

Fixes & Optimizations:
- Fixed a race condition between transaction coordinator commit and cell unref for exported objects, [8d6721a](https://github.com/ytsaurus/ytsaurus/commit/8d6721a16bb6a1bc26c9f0d1dc5506f32635e6b6).
- Fixed manual Cypress node merging for Scheduler transactions, [f87a2ad](https://github.com/ytsaurus/ytsaurus/commit/f87a2ad466c2352be9ba7bfee6e7d93796a9eb6a).
- Fixed master crash when setting a YSON dictionary with duplicate keys in a custom attribute, [0cfad80](https://github.com/ytsaurus/ytsaurus/commit/0cfad80f415c23233ca748e345cd9af91169f4c3).
- Fixed row comparison in shallow merge validation to prevent job failures, [3c282d4](https://github.com/ytsaurus/ytsaurus/commit/3c282d4e9f50aa00d861b7a6f1ca388fea18e51d).
- Fixed a crash when reading `@local_scan_flags` attribute, [5b4c954](https://github.com/ytsaurus/ytsaurus/commit/5b4c954c09ac6e1adc55aa6a5d7baff8f894fb61).
- Fixed non-deterministic YSON struct field loading that could cause a “state hashes differ” alert due to inconsistent error messages when multiple required fields were missing, [0553e21](https://github.com/ytsaurus/ytsaurus/commit/0553e2182a0df502592abdd1fcd8ac3c6afd64ad).
- Fixed an issue where nodes held by transactions interfered with cleanup triggered by `expiration_time` attribute.
- Fixed a bug that caused account metrics to break when adding a new account.
- Fixed a bug in attribute-based access control that caused the first entry to always be evaluated.
- Fixed an issue where Hydra followers could become indefinitely stuck after a lost mutation.
- Limited chunk list count per chunk merger session to prevent master overload.
- Fixed an incorrect check for the state of a node during the removal process.
- Improved incremental heartbeat reporting to prevent chunks from getting stuck in the destroyed queue.
- Optimized chunk merger by reducing unnecessary requisition updates.



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
