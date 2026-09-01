## Kubernetes operator

Распространяется в виде helm-чартов на [GitHub Packages](https://github.com/ytsaurus/ytsaurus-k8s-operator/pkgs/container/ytop-chart).

**Релизы:**

{% cut "**v0.32.1**" %}

**Дата релиза:** 2026-06-04

**Страница релиза:** [v0.32.1](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/v0.32.1)

**Helm-чарт:** [0.32.1](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/968345595?tag=0.32.1)

#### Что изменилось
* Использование configmap для конфигурации timbertruck и исправление процесса обновления при включенном TT от @savnadya в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/812
* Исправление ручного перезапуска init-джобов от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/810
* Удаление небезопасных пароля и токена администратора по умолчанию от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/814

**Полный список изменений**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/v0.32.0...v0.32.1

{% endcut %}

{% cut "**v0.32.0**" %}

**Дата релиза:** 2026-05-29

**Страница релиза:** [v0.32.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/v0.32.0)

**Helm-чарт:** [0.32.0](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/902118332?tag=0.32.0)

#### Что изменилось
* config/samples: добавлены yt-scripts для cgroup-v2 от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/762
* Удалены EnableFullUpdate и UpdateSelector от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/765
* Более подробная диагностика количества подов от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/789
* API: добавлена опция imagePullSecret для образов джобов в CRI-O от @Copilot в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/792
* Поддержка METAX GPU от @futujaos в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/793
* Ожидание фактического получения пользователем прав суперпользователя от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/795
* Использование json для логирования по умолчанию от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/799
* Добавлена опция YQL-агента для включения движка DQ от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/804
* Санитизация операций с токенами от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/806
* Добавлена поддержка PodMonitor от @kruftik в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/807

#### Обновление Rolling / OnDelete
* Добавлена стратегия onDelete для таблет-нод от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/761
* Добавлена логика пропуска удаления таблет-селлов, если tnd использует стратегию onDelete от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/769
* Добавлен статус runsMasterSafetySteps в componentManager для пропуска шагов безопасности в режиме onDelete от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/781
* Обновление мастеров перед запуском других компонентов от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/790
* Исправлена ошибка, из-за которой сбой предварительной проверки вызывал экспоненциальную задержку реконсилера от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/797

#### Вторичные мастер-селлы
* API: добавлено техническое обслуживание кластера от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/747
* API: подготовка к вторичным мастер-селлам [1] от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/782
* API: очистка спеки мастер-кэшей [2] от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/785
* Рефакторинг проверки кворума мастеров и возможности обновления [3] от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/774
* Инициализация вторичных мастеров [4] от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/791

#### Исправления
* Рефакторинг init-джобов от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/758
* Исправление применения глобальных опций подов для UI и strawberry от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/737
* API: minReadyInstanceCount должен быть int32 от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/746
* Исправлена ошибка прокси с отсутствием TLS-томов во время rolling-обновления от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/751
* Исправлена логика удаления подов и проверки готовности от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/756
* Улучшена функция arePodsUpdatedToNewRevision для логики onDelete от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/759
* Исправлена ошибка rollingUpdate для exec-нод и добавлен e2e-тест от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/773
* Создание логгера клиента ytsaurus из логгера контроллера от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/800

#### Тестирование
* Очистка теста для устаревшего EnableAntiAffinity от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/745
* test: добавлен релиз 25.3 от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/752
* Обновление helm в workflows от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/733
* test/r8r: исправлена гонка при сборе событий от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/757
* Обновление canondata от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/767
* Добавлен навык github-actions-run-errors для claude от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/777
* Добавлена опция DEBUG-сборки с санитайзерами от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/750
* Замена BeTrue/BeFalse на BeTrueBecause/BeFalseBecause в тестах от @Copilot в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/771
* Запуск интеграционных тестов для всех версий от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/779
* test/e2e: более подробное ожидание статуса операции от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/784
* Использование образов chyt/query-tracker без debuginfo в примерах и compat-тесте от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/778
* test: загрузка снапшота логов как артефакта от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/783
* Добавлены workflows для проверки актуальности ветки PR от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/794
* Исправление e2e-тестов от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/805

**Полный список изменений**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/v0.31.0...v0.32.0

{% endcut %}

{% cut "**v0.31.0**" %}

**Дата релиза:** 2026-03-12

**Страница релиза:** [v0.31.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/v0.31.0)

**Helm-чарт:** [0.31.0](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/732908203?tag=0.31.0)

#### Что изменилось
* API: вынос API в отдельный модуль от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/675
* Реализован компонент ImageHeater от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/693
* Реализован imageHeater как шаг 0 при создании кластера от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/698
* helm: очистка env в values и установка GOMAXPROCS=1 от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/706
* Добавление клиентской конфигурации в мастер-контейнеры от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/715
* Реализована логика rollingUpdate для hp/rp-прокси от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/708
* Подготовка data-нод к покадровому rollingUpdate от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/720
* Подготовка execNodes к rollingUpdate от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/729
* Добавлена опция ограничения количества одновременно обновляемых групп инстансов от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/722
* feat(ui): добавлено поле urls в UISpec для пользовательских иконок кластеров от @masterbpro в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/726
* Добавлены состояния кластера «Preparing» и «UpdateBlocked» от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/727
* Установлена минимальная требуемая версия Kubernetes 1.29+ и Helm 3.18+ от @Copilot в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/732

#### Исправления
* spyt: исправлено окружение корневого CA-бандла в init-джобе от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/686
* Рефакторинг и исправление обновления компонентов от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/690
* Исправлена ошибка в ImageHeater при повторном запуске от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/696
* В CRD Spyt добавлен флаг offline от @Intention-man в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/687
* Очистка и документирование опций обновления кластера от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/699
* Обновление примера манифеста SPYT с актуальным образом и новыми полями от @Intention-man в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/701
* Удален флаг из примера от @Intention-man в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/702
* Удаление устаревших файлов от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/695
* Добавлен адрес привязки pprof от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/694
* Рефакторинг машины состояний обновления ytsaurus от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/707
* Добавлен тип сервиса NodePort для rpc-прокси в примере локального кластера от @DanilSabirov в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/719
* Использование golang-сборщика из mirror.gcr.io/library/golang от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/712
* Создание пользователей hydra uploader и timbertruck только при необходимости от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/717
* Очистка логики определения необходимости обновления компонента от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/710
* Исправление зависания обновления Ytsaurus в ожидании условия `WaitingForTabletCellsRemoved` от @aapurii в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/714
* Переработка конструкторов статусов компонентов от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/721
* Добавлена опция EnableAnchorProfiling от @Gufran в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/650
* Исправление вызовов ComponentStatusBlocked от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/723
* Создан помощник dispatchComponentUpdate для опций rolling-обновления от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/724
* Очистка image heater от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/703
* Добавлены ссылки на соответствующую документацию в примеры конфигураций от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/731
* Исправлено ожидание image heater при инициализации от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/734
* Исправление EnableAnchorProfiling от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/735
* Исправление селектора нод и tolerations по умолчанию в image heater от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/736

#### Тестирование
* test/e2e: синхронизация удаления namespace и логирование событий от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/683
* test/r8r: цензурирование хэшей от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/691
* test/r8r: исправление цензурирования хэшей для списков объектов от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/697
* Общая очистка и r8r-тест для remote-нод, chyt, spyt от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/692
* Конвертация update_flow_steps_test.go в Ginkgo/Gomega от @Copilot в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/700
* Рефакторинг тестовых образов YTsaurus от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/635
* Отключение восстановления после паники в тестах от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/709
* test/e2e: включение mTLS для 25.2 от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/711
* Отключение параллелизма ginkgo для DEBUG=1 от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/716
* Исправление kube-rbac-proxy в compat-тесте от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/725

#### Новые участники
* @Copilot сделал свой первый вклад в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/700
* @DanilSabirov сделал свой первый вклад в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/719
* @aapurii сделал свой первый вклад в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/714
* @masterbpro сделал свой первый вклад в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/726

**Полный список изменений**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/v0.30.0...v0.31.0

{% endcut %}

{% cut "**v0.30.0**" %}

**Дата релиза:** 2026-02-02

**Страница релиза:** [v0.30.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/v0.30.0)

**Helm-чарт:** [0.30.0](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/667678795?tag=0.30.0)

#### Что изменилось
* Удаление kube-rbac-proxy от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/637
* Создание пула по умолчанию только при его отсутствии от @Gufran в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/646
* Установка @cluster_name в init-джобе от @Gufran в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/645
* Установка ресурсов data- и таблет-нод в конфигурации от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/619
* Добавлено разрешение на использование ch_public для всех от @Gufran в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/647
* Добавлена опция items для FileObjectReference от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/654
* Извлечение json-schema из CRD от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/657
* config/samples: добавлены аннотации yaml-language-server от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/659
* Возможность указания любой доступной версии spark в SPYT CRD для k8s от @Intention-man в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/658
* Обновление настройки корневого CA-бандла для Java от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/667
* Добавлен фича-флаг для защиты подключений к кластеру от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/621
* Исправление аннотаций подов от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/673
* Автоматическая генерация динамической конфигурации для query tracker с версиями spark и spyt от @Intention-man в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/665
* Добавлена возможность привязки YT-спеки к версии оператора от @Gufran в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/653
* Рефакторинг и исправление общих опций подов от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/671
* Обновление списка инстансов YQL-агента с помощью cypress-патча от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/680
* Переход на стандартную схему golang для тегов релизов от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/681

#### Стратегии обновления (в процессе разработки)
* Добавление мастеров в новый режим rolling-обновления от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/630
* Проверка согласованности плана обновления от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/603
* Добавлена prometheus-метрика ytop_strategy_on_delete_waiting_time_seconds от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/641
* Исправлена ошибка в UpdatePreCheck планировщика от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/660
* Улучшена функция arePodsUpdatedToNewRevision для логики onDelete от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/661
* Добавлены контрольная сумма конфигурации и аннотация пода, обновлен e2e-тест onDelete от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/652
* Добавлены ca, ds и msc в новую стратегию обновления от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/662
* Изменено возвращаемое значение для некоторых компонентов с ComponentStatusReadyAfter от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/663
* Добавлены http- и rpc-прокси в новую стратегию обновления от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/666
* Добавлены tnd, end и dnd в новую стратегию обновления bulkMode от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/669
* Сохранение поколений и хэшей для обнаружения необходимых обновлений от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/580

#### Тестирование
* test/e2e: сбор и передача контекста тестовой спеки в репортеры от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/639
* test/e2e: исправление getOperatorMetricsURL от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/643
* test/e2e: исправление checkClusterHealth от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/644
* test/e2e: определение ресурсов нод от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/599
* Настройка правил golangci от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/649
* test: таймаут по умолчанию 1 час от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/668
* Очистка управления config map в BuildCypressPatch от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/676
* test/e2e: использование флага обслуживания вместо клонов мастеров от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/677
* Переключение devel-версии на 0.0.0-devel от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/678

#### Новые участники
* @Intention-man сделал свой первый вклад в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/658

**Полный список изменений**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.29.0...v0.30.0

{% endcut %}

{% cut "**0.29.0**" %}

**Дата релиза:** 2025-12-19


**Страница релиза:** [0.29.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.29.0)


**Helm chart:** [0.29.0](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/617929635?tag=0.29.0)


#### Новые возможности
* Добавлен tablet balancer в оператор от @ifsmirnov в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/634
* Добавлен CA Root Bundle от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/617
* Поддержка API только по HTTPS от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/596
* Обработка ошибок при генерации и переопределении конфигурации от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/633

#### Второстепенные изменения / Исправления
* Переименование имени бинарного файла OffshoreDataGateway от @pavel-bash в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/622
* components/yql_agent: исправление окружения CA Root Bundle от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/623
* Добавлен забытый prometheus_cluster_role.yaml для интеграции с Prometheus от @Kontakter в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/626
* test/e2e: перемещение объекта chyt в yt builder от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/627
* Добавлен обходной путь для ошибки в seccomp-профиле CRI-O в привилегированных подах от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/631
* test/e2e: переименование и перегруппировка тестов от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/636

#### Стратегии обновления (в процессе разработки)
* Добавлен режим bulkUpdate в updatePlan. Создан интерфейс preChecks,… от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/604
* Добавлен режим onDelete rollingUpdate для планировщика от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/625
* test/e2e: использование более надёжного метода ожидания переключения sts на onDelete от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/632

**Полный список изменений**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.28.0...release/0.29.0

{% endcut %}


{% cut "**0.28.0**" %}

**Дата релиза:** 2025-12-09


**Страница релиза:** [0.28.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.28.0)


**Helm chart:** [0.28.0](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/604937890?tag=0.28.0)


#### Что изменилось
* Использование сокращённой версии спецификации Volume из core/v1 от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/571
* Установка лимита памяти для пользовательских джобов в конфигурации exec-ноды от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/584
* Обнуление запросов ресурсов контейнера джоба без выделенных ресурсов для джобов от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/616

#### Новые возможности
* Добавлен компонент OffshoreDataGateway от @pavel-bash в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/567
* Добавлена возможность настройки ключа solomon_exporter от @kmalov в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/576
* Отслеживание версии оператора от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/579
* Установка CRD стала опциональной от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/589
* Добавлен выбор ресурсов по метке экземпляра оператора от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/590
* Добавлен NVIDIA container runtime для CRI-O от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/586

#### Второстепенные изменения и исправления в тестах
* Откат "test: skip case broken for macos" от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/532
* Добавлен YTOP_ENABLE_E2E для включения e2e-тестов от @l0kix2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/565
* test/e2e: проверка TLS для CHYT/YQL/QueryTracker для различных версий от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/563
* Исправление: сбой sidecar при инициализации приводит к невозможности инициализации подов [другая реализация] от @ilyaibraev в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/570
* controllers: очистка component manager от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/545
* Обновление сгенерированных файлов для OffshoreDataGateway от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/583
* Обновление README.md от @AMRivkin в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/585
* Обновление образов до последних версий в cluster_v1_local.yaml от @savnadya в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/592
* Обновление контейнерных образов в cluster_v1_demo.yaml от @savnadya в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/591
* [timbertruck] Улучшения инициализации от @ilyaibraev в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/594
* test/e2e: обновление образов ytsaurus от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/595
* Валидация настроек `structuredLoggers` при включённом `timbertruck` от @ilyaibraev в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/597
* Очистка и исправление теста совместимости оператора от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/593
* Добавлен хелпер handleUpdatingClusterState для компонентов qa,qt,yqla от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/601
* Исправление монтирования configmap для CRI-O sidecar от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/600
* test/e2e: исправление тестов http api от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/605
* Исправление make install от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/602
* test/e2e: повышение уровня логов yt client до info от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/608
* test/e2e: увеличение таймаутов запросов http api от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/611
* Выделение fillJobEnvironmentCRI из fillJobEnvironment от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/614
* test/r8r: добавление mtls https cri от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/612
* test/webhooks: очистка от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/613
* Очистка компонентов CA bundle от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/615
* test/e2e: исправление использования spec-wide context от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/618
* Откат "Added handleUpdatingClusterState helper for qa,qt,yqla compon… от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/609
* test/e2e: проверка обновления query tracker от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/620

#### Новые участники
* @pavel-bash внесли первый вклад в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/567

**Полный список изменений**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.27.0...release/0.28.0

{% endcut %}


{% cut "**0.27.0**" %}

**Дата релиза:** 2025-09-22


**Страница релиза:** [0.27.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.27.0)


**Helm chart:** [0.27.0](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/522515753?tag=0.27.0)


#### Основные изменения
* Добавлена поддержка CRI-O от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/525
* Введены патчи Кипариса от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/531
* Поддержка GPU Nvidia: nvidia-container-runtime и точка входа для контейнера джобов с GpuAgent от @futujaos в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/547
* Добавлены discovery-серверы в конфигурацию мастера от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/549
* Исправлен конвейер обновления мастера от @savnadya в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/552
* Поддержка сервера CHYT в http-прокси от @epsilond1 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/553
* strawberry: добавлена опция для записи логов в stderr от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/555
* Добавлены опции для конкурентности реконсилера от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/560
* Добавлен bundle controller в оператор от @ifsmirnov в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/562

#### Второстепенные изменения
* Использование golang 1.24 от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/538
* Исправление выполнения go tool из других директорий от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/539
* Обновление инструментов от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/540
* Обновление канонизированных файлов от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/541
* Добавление недостающих файлов canondata от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/542
* Получение или определение домена кластера только один раз при запуске от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/543
* Исправление логики обновления патчей Кипариса от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/546
* Обновление примеров конфигураций от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/548
* Обновление ginkgo до v2.25.3 от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/551
* test/e2e: проверка map-операции от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/550
* test/e2e: проверка ротации сертификатов native transport от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/554
* Добавлен canon-тест timbertruck от @ilyaibraev в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/557
* Обновление зависимостей от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/558
* Удалена устаревшая опция спецификации операций от @Kontakter в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/559
* По умолчанию выполняется до 1 реконсиляции одновременно от @savnadya в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/561

#### Новые участники
* @epsilond1 внесли первый вклад в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/553
* @Kontakter внесли первый вклад в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/559
* @ifsmirnov внесли первый вклад в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/562

**Полный список изменений**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.26.0...release/0.27.0

{% endcut %}


{% cut "**0.26.0**" %}

**Дата релиза:** 2025-08-20


**Страница релиза:** [0.26.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.26.0)


**Helm chart:** [0.26.0](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/491862233?tag=0.26.0)


#### Возможности
* Представлен новый компонент Timbertruck для доставки логов в Кипарис от @ilyaibraev в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/509
* Добавлены прокси Кипариса от @koloshmet в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/519

#### Второстепенные изменения
* Передача переменных окружения metadata в контейнеры подов сервера от @Gufran в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/520
* Включение окружения по умолчанию в CRI sidecar от @Gufran в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/522
* Добавлены метрики сервиса CRI от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/523
* Исправлена опечатка в имени ENV от @kruftik в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/528
* Обновление до golang 1.23.12 от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/529
* Обновление cluster_v1_local.yaml от @ogorbacheva в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/463
* Разрешён TLS для native transport без mTLS от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/537


#### Новые участники
* @Gufran внесли первый вклад в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/520
* @koloshmet внесли первый вклад в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/519
* @ogorbacheva внесли первый вклад в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/463

**Полный список изменений**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.25.0...release/0.26.0

{% endcut %}


{% cut "**0.25.0**" %}

**Дата релиза:** 2025-07-23


**Страница релиза:** [0.25.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.25.0)


**Helm chart:** [0.25.0](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/471953965?tag=0.25.0)


#### Возможности
* Поддержка [YTsaurus Server 25.1.0](https://github.com/ytsaurus/ytsaurus/releases/tag/docker%2Fytsaurus%2F25.1.0)
* Native RPC mTLS от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/493
* Добавлена возможность переопределения портов http-прокси от @imakunin в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/505
* Поддержка нового формата конфигурации exe-нод от @k-pogorelov в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/511
* Поддержка HydraPersistenceUploader sidecar от @ilyaibraev в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/489

#### Второстепенные изменения
* Синхронизация exec-ноды при изменении конфигурации containerd от @sanchosancho в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/480
* Исправление атомарного обновления статуса в логике update plan от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/496
* Исправление неидемпотентного CreateUser от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/515


#### Новые участники
* @sanchosancho внесли первый вклад в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/480
* @kirillgrachoff внесли первый вклад в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/492
* @ilyaibraev внесли первый вклад в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/489
* @k-pogorelov внесли первый вклад в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/511

**Полный список изменений**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.24.0...release/0.25.0

{% endcut %}


{% cut "**0.24.0**" %}

**Дата релиза:** 2025-05-20


**Страница релиза:** [0.24.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.24.0)


**Helm chart:** [0.24.0](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/419797949?tag=0.24.0)


#### Второстепенные изменения
* Повторный запуск скрипта init queue agent при обновлении QA от @savnadya в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/479

**Полный список изменений**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.23.1...release/0.24.0

{% endcut %}

{% cut "**0.23.1**" %}

**Дата релиза:** 2025-04-04


**Страница релиза:** [0.23.1](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.23.1)


**Helm-чарт:** [0.23.1](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/388993495?tag=0.23.1)


#### Прочее
* Откат коммита «Disable stockpile by default» в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/477

Изменение, добавленное в 0.23.0, было откачено, так как при обновлении оператора оно приводило к обновлению всех компонентов всех существующих кластеров, при этом само изменение не является важным. Мы рассмотрим возможность сделать его опциональным в следующих релизах.


{% endcut %}


{% cut "**0.23.0**" %}

**Дата релиза:** 2025-04-02


**Страница релиза:** [0.23.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.23.0)


**Helm-чарт:** [0.23.0](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/387207486?tag=0.23.0)


#### Прочее
* Исправлена ошибка в колонке заблокированных компонентов от @l0kix2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/464
* Удалён `stderr`-логгер для JobProxy от @imakunin в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/465
* Установлена начальная квота для пользовательских медиумов от @futujaos в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/466
* Отключен stockpile по умолчанию от @imakunin в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/467
* Фильтр тегов таблет-нод для бандлов при начальной загрузке от @futujaos в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/468
* Создан ROADMAP.md от @AMRivkin в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/473
* Настройка yqla mrjob syslibs V2 от @Krisha11 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/475

#### Примечания к релизу
* Поле `configureMrJobSystemLibs` было удалено https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/475, теперь системные библиотеки для YQL-агента добавляются безусловно.
* Параметр stockpile/thread_count установлен в ноль в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/467, чтобы убрать нерелевантные предупреждения в логах; недостатком является то, что это вызовет обновление всех компонентов.

#### Новые участники
* @futujaos сделал свой первый вклад в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/466
* @AMRivkin сделал свой первый вклад в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/473

**Полный список изменений**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.22.0...release/0.23.0

{% endcut %}


{% cut "**0.22.0**" %}

**Дата релиза:** 2025-03-07


**Страница релиза:** [0.22.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.22.0)


**Helm-чарт:** [0.22.0](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/369818188?tag=0.22.0)


#### Новые возможности
* Поддерживается обновление до YTsaurus 24.2

#### Прочее
* Добавлены потерянные VolumeMounts для CA-набора и TLS-секретов в контейнер джобов от @imakunin в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/449
* Добавлена конфигурация bus-клиента от @imakunin в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/450

#### Экспериментально
* Добавлены множественные селекторы обновления от @wilwell в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/383
* Добавлена колонка заблокированных компонентов в вывод kubectl от @l0kix2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/459

#### Новые участники
* @imakunin сделал свой первый вклад в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/449

**Полный список изменений**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.21.0...release/0.22.0

{% endcut %}


{% cut "**0.21.0**" %}

**Дата релиза:** 2025-02-10


**Страница релиза:** [0.21.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.21.0)


**Helm-чарт:** [0.21.0](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/361365252?tag=0.21.0)


#### Новые возможности
* Добавлена возможность развертывания Kafka-прокси от @savnadya в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/407

#### Прочее
* Добавлена конфигурация для kind с журналом аудита от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/441

#### Исправления ошибок
* Сохранение финализаторов объектов от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/440
* Установка квоты и min_disk_space для локаций от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/445
* Исправлен нулевой порт, если не настроен порт мониторинга от @l0kix2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/447


{% endcut %}


{% cut "**0.20.0**" %}

**Дата релиза:** 2025-01-20


**Страница релиза:** [0.20.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.20.0)


**Helm-чарт:** [0.20.0](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/342298010?tag=0.20.0)


#### Прочее
* Добавлена возможность не создавать несуществующих пользователей от @l0kix2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/416
* Добавлен DNSConfig в Instance и YTsaurusSpec от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/420
* Включен джоб для real chunks от @l0kix2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/412
* Добавлен log_manager_template для job-прокси от @l0kix2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/428

#### Примечания к релизу
Этот релиз делает yt-оператор совместимым с ytsaurus 24.2. 
Обновление до этой версии запустит джоб для установки правильных значений enable_real_chunks_value в Кипарисе, а exec-ноды будут обновлены с новой конфигурацией.

**Полный список изменений**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.19.0...release/0.20.0

{% endcut %}


{% cut "**0.19.0**" %}

**Дата релиза:** 2025-01-09


**Страница релиза:** [0.19.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.19.0)


**Helm-чарт:** [0.19.0](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/333778227?tag=0.19.0)


#### Прочее
* Настройка yqla mrjob syslibs от @Krisha11 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/409
#### Исправления ошибок
* Добавлен джоб обновления yqla от @Krisha11 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/387

**Полный список изменений**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.18.1...release/0.19.0

{% endcut %}


{% cut "**0.18.1**" %}

**Дата релиза:** 2024-12-13


**Страница релиза:** [0.18.1](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.18.1)


**Helm-чарт:** [0.18.1](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/322387466?tag=0.18.1)


#### Прочее
* Больше проверок от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/393
#### Исправления ошибок
* Исправлены обновления для именованных компонентов кластера от @l0kix2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/401

**Полный список изменений**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.18.0...release/0.18.1

{% endcut %}


{% cut "**0.18.0**" %}

**Дата релиза:** 2024-11-26


**Страница релиза:** [0.18.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.18.0)


**Helm-чарт:** [0.18.0](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/312306352?tag=0.18.0)


#### Предупреждение
В этом релизе есть известная ошибка, которая нарушала обновление компонентов YTsaurus с непустыми именами (имена можно задавать для data/tablet/exec-нод) и ролями (можно задавать для прокси).
Ошибка исправлена в [0.18.1](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release%2F0.18.1).

#### Новые возможности
* Реализован API RemoteTabletNodes от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/372

#### Прочее
* Обновлен пример конфигурации для кластера с TLS от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/369
* Удалены DataNodes из обновления StatelessOnly от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/371
* Добавлено значение namespacedScope в helm-чарт от @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/376
* Обновлен crd-ref-docs от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/379
* Добавлено observed generation для удаленных нод от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/382
* Поддержка разных семейств контроллеров в конфигурации strawberry от @dmi-feo в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/355
* kata-compat: монтирование TLS-файлов в отдельную директорию от @kruftik в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/388
* Поддержка трансформаций OAuth-логина от @l0kix2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/397
* Добавлен diff для случая обновления статической конфигурации от @l0kix2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/398

#### Исправления ошибок
* Исправлено observed generation от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/373
* Исправлено создание динамической конфигурации YQL-агента от @savnadya в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/377
* Исправлено журналирование в chyt_controller от @dmi-feo в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/370
* Исправлено имя контейнера strawberry от @dmi-feo в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/375
* Используется ожидаемое количество инстансов как значение по умолчанию для минимального количества готовых от @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/395

#### Новые участники
* @dmi-feo сделал свой первый вклад в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/370

**Полный список изменений**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.17.0...release/0.18.0

{% endcut %}


{% cut "**0.17.0**" %}

**Дата релиза:** 2024-10-11


**Страница релиза:** [0.17.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.17.0)


**Helm-чарт:** [0.17.0](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/293977708?tag=0.17.0)


#### Прочее
* Разделение опций инициализации CHYT на makeDefault и createPublicClique от @achulkov2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/347
#### Исправления ошибок
* Исправлено использование скрипта инициализации queue-агента для 24.* от @l0kix2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/356


{% endcut %}

{% cut "**0.16.2**" %}

**Дата релиза:** 2024-09-13


**Страница релиза:** [0.16.2](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.16.2)


**Helm-чарт:** [0.16.2](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/273430407?tag=0.16.2)


#### Исправление ошибок
* Исправлен образ strawberry-контроллера для второго джоба: @l0kix2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/345


{% endcut %}


{% cut "**0.16.1**" %}

**Дата релиза:** 2024-09-13


**Страница релиза:** [0.16.1](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.16.1)


**Helm-чарт:** [0.16.1](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/273375484?tag=0.16.1)


#### Предупреждение
В этом релизе есть ошибка, если включены компоненты Strawberry.
Используйте версию 0.16.2.

#### Исправление ошибок
* Откат переопределения образа джоба для UI/strawberry: @l0kix2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/344 — ошибка была внесена в версии 0.16.0


{% endcut %}


{% cut "**0.16.0**" %}

**Дата релиза:** 2024-09-12


**Страница релиза:** [0.16.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.16.0)


**Helm-чарт:** [0.16.0](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/272705267?tag=0.16.0)


#### Предупреждение
В этом релизе есть ошибка для конфигурации, в которой включены компоненты UI или Strawberry и для некоторых из них были переопределены образы (k8s init-джобы для таких компонентов будут завершаться ошибкой).
Используйте версию 0.16.2.

#### Изменения
* Добавлено поле observedGeneration в YtsaurusStatus: @wilwell в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/333
* Установлена статистика для алертов о низком использовании CPU джобами: @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/335
* Добавлен nodeSelector для UI и Strawberry: @l0kix2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/338
* Init-джоб создается из образа InstanceSpec, если он указан: @wilwell в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/336
* Добавлены tolerations и nodeSelectors для джобов: @l0kix2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/342

#### Новые участники
* @wilwell внес первый вклад в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/333


{% endcut %}


{% cut "**0.15.0**" %}

**Дата релиза:** 2024-09-04


**Страница релиза:** [0.15.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.15.0)


**Helm-чарт:** [0.15.0](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/268358441?tag=0.15.0)


#### Обратно несовместимые изменения
1. Метки подов компонентов были рефакторизованы в #326, изменения:
- Метка `app.kubernetes.io/instance` удалена
- Метка `app.kubernetes.io/name` раньше содержала значение Ytsaurus, теперь она содержит тип компонента
- Метка `app.kubernetes.io/managed-by` теперь имеет значение `"ytsaurus-k8s-operator"` вместо `"Ytsaurus-k8s-operator"`

2. Устаревшее поле `chyt` в основном [спецификации YTsaurus](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/docs/api.md#ytsaurusspec) удалено, вместо него используйте поле `strawberry` с той же схемой.

#### Изменения
* Добавлены tolerations для Strawberry: @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/328
* Рефакторинг названий меток для компонентов: @achulkov2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/326

#### Экспериментальные возможности
* RemoteDataNodes: @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/330


{% endcut %}


{% cut "**0.14.0**" %}

**Дата релиза:** 2024-08-22


**Страница релиза:** [0.14.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.14.0)


**Helm-чарт:** [0.14.0](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/261892792?tag=0.14.0)


#### Обратно несовместимые изменения
До этого релиза `StrawberryController` безусловно настраивался с `{address_resolver={enable_ipv4=%true;enable_ipv6=%true}}` в своем статическом конфиге. Теперь он учитывает общие поля `useIpv6` и `useIpv4`, которые можно задать в [YtsaurusSpec](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/docs/api.md#ytsaurusspec).
Если по какой-то причине требуется конфигурация, отличная от
```yaml
useIpv6: true
useIpv4: true
```
для основной спецификации Ytsaurus и одновременно `enable_ipv4=%true;enable_ipv6=%true` для `StrawberryController`, это можно сделать с помощью ConfigMap `configOverrides`:
```yaml
data:
    strawberry-controller.yson: |
    {
      controllers = {
        chyt = {
          address_resolver = {
            enable_ipv4 = %true;
            enable_ipv6 = %true;
          };
        };
      };
    }
``` 

#### Изменения
* Добавлена валидация не более одной спецификации ytsaurus на namespace: @qurname2 в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/305
* Добавлены strategy, nodeSelector, affinity, tolerations: @sgburtsev в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/321
* Добавлены опции forceTcp и keepSocket: @leo-astorsky в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/324

#### Исправления ошибок
* Исправлен пустой массив volumes в примере конфигурации: @koct9i в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/318

#### Новые участники
* @leo-astorsky внес первый вклад в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/324


{% endcut %}


{% cut "**0.13.1**" %}

**Дата релиза:** 2024-07-30


**Страница релиза:** [0.13.1](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.13.1)


**Helm-чарт:** [0.13.1](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/250876401?tag=0.13.1)


#### Исправления ошибок
* Откат объявления `useInsecureCookies` устаревшим в #310: @sgburtsev в https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/317

В предыдущем релизе поле `useInsecureCookies` было объявлено устаревшим необратимым образом, этот релиз исправляет это. Теперь можно независимо настраивать безопасность cookie UI (через поле `useInsecureCookies`) и безопасность взаимодействия UI и HTTP-прокси (через поле `secure`).


{% endcut %}


{% cut "**0.13.0**" %}

**Дата релиза:** 2024-07-23


**Страница релиза:** [0.13.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.13.0)


**Helm-чарт:** [0.13.0](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/247522829?tag=0.13.0)


#### Новые возможности
* Добавлен параметр terminationGracePeriodSeconds для каждого компонента: @koct9i в https://github.com/ytsaurus/yt-k8s-operator/pull/304
* Добавлен параметр externalProxy для UI: @sgburtsev в https://github.com/ytsaurus/yt-k8s-operator/pull/308
* Размер как Quantity в LogRotationPolicy: @sgburtsev в https://github.com/ytsaurus/yt-k8s-operator/pull/309
* Используйте `secure` вместо `useInsecureCookies`, передавайте caBundle в UI: @sgburtsev в https://github.com/ytsaurus/yt-k8s-operator/pull/310

#### Изменения 
* Все CRD YTsaurus добавлены в категории "ytsaurus-all" "yt-all": @koct9i в https://github.com/ytsaurus/yt-k8s-operator/pull/311

#### Исправления ошибок
* Оператор должен обнаруживать обновления configOverrides: @l0kix2 в https://github.com/ytsaurus/yt-k8s-operator/pull/314



{% endcut %}


{% cut "**0.12.0**" %}

**Дата релиза:** 2024-06-28


**Страница релиза:** [0.12.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.12.0)


**Helm-чарт:** [0.12.0](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/236432165?tag=0.12.0)


#### Новые возможности
* Больше опций для store locations: @sgburtsev в https://github.com/ytsaurus/yt-k8s-operator/pull/294
  * верхний предел `low_watermark` для data-нод увеличен с 5 до 25 ГиБ;
  * `trash_cleanup_watermark` data-нод будет устанавливаться равным значению `lowWatermark` из спецификации
  * `max_trash_ttl` можно настроить в спецификации
* Добавлена поддержка directDownload в спецификации UI: @kozubaeff в https://github.com/ytsaurus/yt-k8s-operator/pull/257
  * `directDownload` для UI теперь можно настроить в спецификации. Если параметр опущен или установлен в `true`, UI будет использовать текущее поведение по умолчанию (использовать прокси для загрузки), если установлен в `false` — для загрузки будет использоваться бэкенд UI.

#### Новые участники
* @sgburtsev внес первый вклад в https://github.com/ytsaurus/yt-k8s-operator/pull/294


{% endcut %}

{% cut "**0.11.0**" %}

**Дата релиза:** 2024-06-27


**Страница релиза:** [0.11.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.11.0)


**Helm-чарт:** [0.11.0](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/236035237?tag=0.11.0)


#### Новые возможности
* Опция SetHostnameAsFQDN добавлена для всех компонентов. Значение по умолчанию — true, автор @qurname2, https://github.com/ytsaurus/yt-k8s-operator/pull/302
* Добавлена опция hostNetwork для каждого компонента, автор @koct9i, https://github.com/ytsaurus/yt-k8s-operator/pull/287

#### Прочее
* Добавлена опция для квоты дискового пространства для каждого location, автор @koct9i, https://github.com/ytsaurus/yt-k8s-operator/pull/279
* Добавлены переменные окружения для CRI-инструментов в поды exec-нод, автор @koct9i, https://github.com/ytsaurus/yt-k8s-operator/pull/283
* Добавлены podLabels и podAnnotations для каждой группы инстансов, автор @koct9i, https://github.com/ytsaurus/yt-k8s-operator/pull/289
* Сортировка status conditions для улучшения читаемости, автор @koct9i, https://github.com/ytsaurus/yt-k8s-operator/pull/290
* Добавлены init-контейнеры для exec-нод, автор @koct9i, https://github.com/ytsaurus/yt-k8s-operator/pull/288
* Добавлен уровень логирования "warning", автор @koct9i, https://github.com/ytsaurus/yt-k8s-operator/pull/292
* Удалены мутирующие webhook'и, автор @koct9i, https://github.com/ytsaurus/yt-k8s-operator/pull/296

#### Исправления
* Исправлен расчет ресурсов exec-нод в неизолированном CRI-окружении для джобов, автор @kruftik, https://github.com/ytsaurus/yt-k8s-operator/pull/277



{% endcut %}


{% cut "**0.10.0**" %}

**Дата релиза:** 2024-06-07


**Страница релиза:** [0.10.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.10.0)


**Helm-чарт:** [0.10.0](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/229520332?tag=0.10.0)


#### Новые возможности
#### Прочее
 - Добавлен ACO everyone-share для QT, автор @Krisha11, #272
 - Добавлен канал в конфигурацию qt, автор @Krisha11, #273
 - Добавлена опция для квоты дискового пространства для каждого location #279
#### Исправления
- Исправлен расчет ресурсов exec-нод в неизолированном CRI-окружении для джобов #277



{% endcut %}


{% cut "**0.9.1**" %}

**Дата релиза:** 2024-05-30


**Страница релиза:** [0.9.1](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.9.1)


**Helm-чарт:** [0.9.1](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/222950367?tag=0.9.1)


#### Новые возможности
#### Прочее
 - Добавлен 'physical_host' в cypress_annotations для совместимости с CMS и UI #252
 - Добавлены переменная окружения WATCH_NAMESPACE и LeaderElectionNamespace #168
 - Добавлена конфигурация для экспортера solomon: указание хоста и некоторых тегов инстансов #258
 - Добавлена поддержка sidecar-контейнеров для контейнеров первичных мастеров #259
 - Добавлена опция для пути к конфигурации containerd registry #264
#### Исправления
 - Исправлено CRI-окружение для джобов на удаленных exec-нодах #261


{% endcut %}


{% cut "**0.9.0**" %}

**Дата релиза:** 2024-04-23


**Страница релиза:** [0.9.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.9.0)


**Helm-чарт:** [0.9.0](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/222974140?tag=0.9.0)


#### Новые возможности
- Добавлено экспериментальное поле UpdateSelector #211 (поведение может измениться) для обновления компонентов по отдельности
#### Прочее
- Включение TmpFS, когда это возможно #235
- Отключение дисковой квоты для slot locations #236
- Проброс переменных окружения docker-образа в пользовательский джоб #248
#### Исправления
- Исправлен флаг doNotSetUserId #243


{% endcut %}


{% cut "**0.8.0**" %}

**Дата релиза:** 2024-04-12


**Страница релиза:** [0.8.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.8.0)


**Helm-чарт:** [0.8.0](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/222976310?tag=0.8.0)


#### Новые возможности
#### Прочее
- Увеличено значение по умолчанию для MaxSnapshotCountToKeep и MaxChangelogCountToKeep
- Изменен стандартный replication factor для бандлов #210
- Установлено EnableServiceLinks=false для всех подов #218
#### Исправления
- Исправлена конфигурация аутентификации для RPC Proxy #207
- Скрипт джоба обновляется при перезапуске #224
- Использование безопасного генератора случайных чисел и base64 для токенов #202
- Исправлен запуск джобов с пользовательским docker_image, когда образ для джобов по умолчанию не задан #217

{% endcut %}


{% cut "**0.7.0**" %}

**Дата релиза:** 2024-04-04


**Страница релиза:** [0.7.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.7.0)


**Helm-чарт:** [0.7.0](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/222976364?tag=0.7.0)


#### Новые возможности
  * Добавлена поддержка удаленных exec-нод #75
  * Добавлена поддержка MasterCaches #122
  * Включено автоматическое обновление TLS-сертификатов для HTTP-прокси #167
  * CRI containerd окружение для джобов #105

#### Прочее
  * Поддержка RuntimeClassName в InstanceSpec
  * Настраиваемый порт мониторинга #146
  * Обновление дата-нод не запускает полное обновление
  * Добавлен ALLOW_PASSWORD_AUTH в UI #162
  * Проверки готовности для strawberry и UI
  * Медиум теперь называется domestic medium #88
  * Настройка начального replication factor для таблетных ченджлогов и слепков в зависимости от количества дата-нод #185
  * Генерация markdown-документации по API
  * Переименование архива операций #116
  * Настройка кластера для использования jupyt #149
  * Исправлено создание QT ACO при обновлении кластера #176
  * Установка ACL для QT ACO, добавлен ACO everyone-use #181
  * Включен rpc proxy в job proxy #197
  * Добавлен файл токена yqla в контейнер #140

#### Исправления
  * Замена стандартного порта мониторинга YQL Agent 10029 -> 10019


{% endcut %}


{% cut "**0.6.0**" %}

**Дата релиза:** 2024-02-26


**Страница релиза:** [0.6.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.6.0)


**Helm-чарт:** [0.6.0](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/222976440?tag=0.6.0)


#### Новые возможности
- Добавлена поддержка обновления мастеров версий 23.2
- Добавлена возможность привязки мастеров к набору узлов по hostname узлов.
- Добавлена возможность настройки количества хранимых слепков и ченджлогов в спецификации мастера
- Добавлена возможность создания объектов контроля доступа для пользователей
- Добавлена поддержка монтирования томов с mountPropagation = Bidirectional в execNodes
- Добавлены namespace "queries" и объект "nobody" для контроля доступа. Они необходимы для query_tracker версии 0.0.5 и выше.
- Добавлена поддержка нового UI для Cliques CHYT.
- Добавлено создание группы для администраторов (admins).
- Добавлены readiness-пробы в спецификации statefulset компонентов

#### Исправления
- Улучшены ACL на схемах мастеров
- Init-джобы мастера и планировщика больше не перезаписывают существующие динамические конфигурации.

#### Тесты
- Добавлен процесс запуска тестов на ресурсах Github
- Добавлен e2e-тест для проверки обновления с 23.1 до 23.2
- Добавлены тесты генератора конфигураций для всех компонентов
- Добавлено использование переменной окружения KIND_CLUSTER_NAME в e2e-тестах
- Поддержка локального проброса портов k8s в e2e

#### Обратно несовместимые изменения
- `exec_agent` переименован в `exec_node` в конфигурации exec-ноды. Если в ваших спецификациях есть `configOverrides`, переименуйте поля соответствующим образом.



{% endcut %}


{% cut "**0.5.0**" %}

**Дата релиза:** 2023-11-29


**Страница релиза:** [0.5.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.5.0)


**Helm-чарт:** [0.5.0](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/222976486?tag=0.5.0)


**Новые возможности**
- Добавлен `minReadyInstanceCount` в компоненты Ytsaurus, который позволяет не ждать готовности всех подов.
- Поддержка queue agent.
- Добавлена постобработка сгенерированных статических конфигураций.
- Добавлена отдельная опция UseIPv4 для поддержки dualstack-конфигураций.
- Поддержка мастеров в режиме host network.
- Добавлен движок spyt в query tracker по умолчанию.
- Включены ipv4 и ipv6 по умолчанию в chyt-контроллерах.
- Стандартный CHYT-клик создается как tracked, а не untracked.
- Проверка полного обновления не выполняется, если полное обновление не включено (флаг `enable_full_update` в спецификации).
- Алгоритм обновления кластера улучшен. Если полное обновление необходимо для уже запущенных компонентов и были добавлены новые компоненты, оператор сначала запустит новые компоненты и только затем начнет полное обновление. Ранее такая реконфигурация не поддерживалась.
- Добавлена опциональная поддержка TLS для native-rpc соединений.
- Добавлена возможность настройки логгеров job proxy.
- Изменен способ расчета лимитов ресурсов нод из `resourceLimits` и `resourceRequests`.
- Включены debug-логи YTsaurus go client для пода контроллера.
- Поддержка dualstack-кластеров в YQL agent.
- Поддержка нового формата конфигурации YQL agent.
- Поддержка спецификации `NodePort` для HTTP proxy (http, https), UI (http) и RPC proxy (rpc port). Для TCP proxy NodePorts используются неявно при выборе сервиса NodePort. Размер диапазона портов и minPort теперь настраиваются.

**Исправления**
- Исправлена работа YQL agents в кластерах только с ipv6.
- Исправлена взаимоблокировка в случае ручного удаления UI-деплоймента.

**Тесты**
- Исправлены e2e-тесты.
- Добавлен e2e-тест для совместимости версий оператора.

{% endcut %}

{% cut "**0.4.1**" %}

**Дата релиза:** 2023-10-03

**Страница релиза:** [0.4.1](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.4.1)

**Helm-чарт:** [0.4.1](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/222976536?tag=0.4.1)

**Новые возможности**
- Поддержка переопределения конфигурации для отдельной группы инстансов.
- Поддержка TLS для RPC-прокси.

**Исправления**
- Исправлена ошибка при создании стандартного клика `CHYT` (`ch_public`).

{% endcut %}


{% cut "**0.4.0**" %}

**Дата релиза:** 2023-09-26

**Страница релиза:** [0.4.0](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.4.0)

**Helm-чарт:** [0.4.0](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/222976570?tag=0.4.0)

**Новые возможности**

- Архив операций будет обновляться при изменении образа планировщика.
- Возможность указывать разные образы для разных компонентов.
- Поддержка обновления кластера без полного простоя для компонентов без состояния.
- Поддержка обновления конфигов статических компонентов при необходимости.
- Улучшен SPYT-контроллер. Добавлен статус инициализации (`ReleaseStatus`).
- Добавлен CHYT-контроллер и возможность загружать несколько разных версий на один кластер YTsaurus.
- Добавлена возможность указывать формат журнала (`yson`, `json` или `plain_text`), а также возможность включить запись структурированных журналов.
- Добавлена дополнительная диагностика о развертывании компонентов кластера в статусе `Ytsaurus`.
- Добавлена возможность отключить запуск полного обновления (поле `enableFullUpdate` в спецификации `Ytsaurus`).
- Поле `chyt` в спецификации переименовано в `strawberry`. Для обратной совместимости оно остается в `crd`, но рекомендуется переименовать его.
- Размер поля `description` в `crd` теперь ограничен 80 символами, что значительно уменьшает размер `crd`.
- Статусные таблицы `Query Tracker` теперь автоматически мигрируют при его обновлении.
- Добавлена возможность установить привилегированный режим для контейнеров `exec node`.
- Добавлен `TCP proxy`.
- Добавлена дополнительная валидация спецификации: проверка, что пути в locations принадлежат одному из volumes, а также проверка, что для каждого указанного компонента присутствуют все компоненты, необходимые для его успешной работы.
- `strawberry controller` и `ui` также можно обновлять.
- Добавлена возможность развертывания `http-proxy` с TLS.
- Адрес сервиса Odin для UI можно указать в спецификации.
- Добавлена возможность настраивать `tags` и `rack` для узлов.
- Поддержка конфигурации OAuth-сервиса в спецификации.
- Добавлена возможность передавать дополнительные переменные окружения в UI, а также задавать тему и окружение (`testing`, `production` и т. д.) для UI.
- Медиумы для locations data-узлов создаются автоматически при первичном развертывании кластера.



{% endcut %}


{% cut "**0.3.1**" %}

**Дата релиза:** 2023-08-14

**Страница релиза:** [0.3.1](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release/0.3.1)

**Helm-чарт:** [0.3.1](https://github.com/orgs/ytsaurus/packages/container/ytop-chart/222976640?tag=0.3.1)

**Новые возможности**

- Добавлена возможность настройки автоматической ротации журналов.
- `toleration` и `nodeSelector` можно указывать в спецификациях инстансов компонентов.
- Типы генерируемых объектов указываются в конфигурации контроллера, поэтому оператор реагирует на изменения генерируемых объектов путем согласования.
- Config maps хранят данные в текстовом виде вместо бинарного, чтобы можно было просматривать содержимое конфигов через `kubectl describe configmap <configmap-name>`.
- Добавлены расчет и установка `disk_usage_watermark` и `disk_quota` для exec node.
- Добавлен SPYT-контроллер и возможность загружать необходимое для SPYT в Кипарис с помощью отдельного ресурса, что позволяет иметь несколько версий SPYT на одном кластере.

**Исправления**

- Исправлена ошибка в именовании поля `medium_name` в статических конфигах.



{% endcut %}
