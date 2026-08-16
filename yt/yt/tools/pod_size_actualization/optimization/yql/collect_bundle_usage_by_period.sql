-- Подготовка данных потребления для кубика assign_pod_sizes.
--
-- Что делает:
--   1. Склеивает per-host витрину коллег с картой host -> bundle на тот же день
--      и берёт поточечный MAX по хостам бандла. Максимум обязан считаться до
--      квантили: max по хостам с квантилью по времени не коммутирует.
--   2. Режет окно на периоды (по умолчанию 3 недели по 7 дней) и считает
--      квантиль по времени внутри каждого периода.
--   3. Отбрасывает периоды с coverage ниже 50% или конфигурацией, отличной от
--      последней доступной, и оставляет хвостовой отрезок валидных периодов.
--
-- Кластеры не фильтруются: бандлы разбирает по группам assign_pod_sizes.
--
-- Выходы:
--   $metrics_0 / $metrics_1 / $metrics_2 — по одной таблице на период
--     (период 0 — самый свежий), строка на (cluster, bundle). Лишние выходы
--     при periods < 3 остаются пустыми: число выходов у кубика статично.
--   $node_specs / $rpc_specs — каталог типов из последних доступных spec бандлов.
--
-- Метрики невалидных периодов и типов инстансов приходят NULL:
-- data.py пропускает инстанс-тип, у которого *_cpu_total_p75 или
-- *_anon_memory_p75 пустые, и не выдаёт по нему рекомендацию.
--
-- Параметры и пути выходов приходят через DECLARE: YQL-процессор Нирваны
-- превращает каждую опцию блока и каждое имя MR-выхода в параметр запроса.
-- Для ручного запуска есть run_collect_bundle_usage_by_period.py — он подставляет
-- значения вместо DECLARE.

PRAGMA AnsiInForEmptyOrNullableItemsCollections;
USE hahn;

-- ==================== параметры ====================
DECLARE $end_date    AS String;   -- последний день окна, YYYY-MM-DD; "" = вчера (UTC)
DECLARE $period_days AS Int32;    -- длина одного периода в днях
DECLARE $periods     AS Int32;    -- сколько периодов брать, 1..3 (по числу выходов)
DECLARE $quantile    AS Double;   -- квантиль по времени внутри периода

DECLARE $usage_nodes   AS String; -- витрина потребления tablet nodes, per-host
DECLARE $usage_proxies AS String; -- витрина потребления rpc proxy, per-host
DECLARE $nodes_spec   AS String; -- карта host -> bundle для нод
DECLARE $proxies_spec AS String; -- карта host -> role для проксей
DECLARE $bundle_spec   AS String; -- спек-лог бандлов (конфигурация и гарантии)

DECLARE $metrics_0  AS String;
DECLARE $metrics_1  AS String;
DECLARE $metrics_2  AS String;
DECLARE $node_specs AS String;
DECLARE $rpc_specs  AS String;

-- ============================== окно ================================
-- Выходов под метрики три, больше периодов не унести.
$n_periods = Ensure($periods, $periods >= 1 AND $periods <= 3,
                    "periods must be between 1 and 3: the block has 3 metric outputs");

$end_d = Unwrap(
    IF($end_date == "", CurrentUtcDate() - DateTime::IntervalFromDays(1), CAST($end_date AS Date)),
    "end_date must be a date in YYYY-MM-DD form (or empty for yesterday)");
$end_s   = CAST($end_d AS String);
$start_s = CAST(Unwrap($end_d - DateTime::IntervalFromDays($period_days * $n_periods - 1)) AS String);

-- День -> номер периода. Период 0 — последние $period_days дней окна, дальше
-- вглубь. Считаем словарь один раз: арифметика дат внутри лямбды, вызываемой
-- на каждой строке, обходится дороже и хуже выводится по типам.
-- Unwrap и CAST не для красоты: ключ словаря обязан быть неопциональным
-- String, иначе поиск по колонке дня не сойдётся по типам.
$day_period = ToDict(ListMap(ListFromRange(0, $period_days * $n_periods), ($i) -> {
    $day = CAST($i AS Int32);
    RETURN AsTuple(
        CAST(Unwrap($end_d - DateTime::IntervalFromDays($day)) AS String),
        $day / $period_days)
}));

-- ============================== хелперы ================================
-- День вне окна получает -1 и отфильтровывается вместе с прочим мусором.
$period_of = ($day_str) -> { RETURN COALESCE($day_period[$day_str], -1) };

-- Monium отдаёт cpu в сотых долях ядра, витрина хранит его как есть.
$cpu_usage_divisor = 100.0;
$min_period_coverage = 0.5;
$min_full_confidence_coverage = 0.7;
$usage_points_per_day = 2880;

-- NaN и бесконечности в витрине встречаются; NULL их не ломает.
-- NaN не равен сам себе, бесконечности отсекаются границами.
$finite = ($x) -> { RETURN IF($x == $x AND $x > -1.0e300 AND $x < 1.0e300, $x, NULL) };

-- Точная квантиль с линейной интерполяцией — та же, что у numpy.percentile,
-- чтобы результат сходился с текущим сбором из Solomon.
$percentile = ($values, $q) -> {
    $sorted = ListSort($values);
    $n = ListLength($sorted);
    $pos = ($q / 100.0) * CAST($n - 1 AS Double);
    $lo = CAST($pos AS Uint64);
    $hi = IF(CAST($lo AS Double) < $pos, $lo + 1, $lo);
    RETURN IF($n == 0, NULL,
           IF($lo == $hi,
              $sorted[$lo],
              $sorted[$lo] * (CAST($hi AS Double) - $pos)
              + $sorted[$hi] * ($pos - CAST($lo AS Double))));
};

-- Отпечаток конфигурации. Имя типа не определяет размер (у tiny может стать
-- 5 ядер вместо 4), поэтому в отпечаток входят и сами гарантии.
$fingerprint = ($count, $type, $vcpu, $memory, $net) -> {
    RETURN CAST(COALESCE($count, 0) AS String) || "|" || COALESCE($type, "")
        || "|" || CAST(COALESCE($vcpu, 0) AS String)
        || "|" || CAST(COALESCE($memory, 0) AS String)
        || "|" || CAST(COALESCE($net, 0) AS String);
};

-- Причины формируются здесь, рядом с порогами, которые их и порождают. Python
-- только переносит готовые строки в итоговую таблицу.
--
-- Отбрасывание периода и понижение confidence — разные события с разными
-- порогами, поэтому и причины разные. Первая объясняет, почему оборвался хвост
-- валидных периодов: она относится к граничному периоду, его номер и стоит в
-- префиксе.
$period_invalidation_reason = ($boundary_period, $spec_bad, $usage_bad, $config_bad) -> {
    $prefix = "period_" || CAST($boundary_period AS String) || ":";
    RETURN COALESCE(ListConcat(ListNotNull(AsList(
        IF($spec_bad, $prefix || "insufficient_spec_coverage"),
        IF($usage_bad, $prefix || "insufficient_usage_coverage"),
        IF($config_bad, $prefix || "recent_configuration_change")
    )), ","), "");
};

-- Вторая объясняет, почему confidence не full: часть периодов отброшена и/или
-- среднее coverage по вошедшим ниже порога full.
$confidence_reason = ($valid_periods, $spec_bad, $usage_bad) -> {
    RETURN COALESCE(ListConcat(ListNotNull(AsList(
        IF($valid_periods == 0, "no_valid_period"),
        IF($valid_periods > 0 AND $valid_periods < $n_periods, "dropped_periods"),
        IF($spec_bad, "insufficient_spec_coverage"),
        IF($usage_bad, "insufficient_usage_coverage")
    )), ","), "");
};

-- Гарантии и отпечатки конфигурации по дням: нужны и шагу 1, и шагу 3.
$spec_by_day = (
    SELECT
        TableName() AS day, cluster AS cluster, bundle_name AS bundle,
        abc_service_slug,
        abc_service_path,
        value_stream_slug,
        value_stream_name_ru,
        business_unit_slug,
        business_unit_name_ru,
        business_group_slug,
        business_group_name_ru,
        tablet_node_count AS node_count,
        tablet_node_type_guarantee AS node_type,
        tablet_node_vcpu_guarantee AS node_vcpu,
        tablet_node_memory_guarantee AS node_memory,
        tablet_node_net_bytes_guarantee AS node_net,
        rpc_proxy_count AS rpc_count,
        rpc_proxy_type_guarantee AS rpc_type,
        rpc_proxy_vcpu_guarantee AS proxy_vcpu,
        rpc_proxy_memory_guarantee AS proxy_memory,
        rpc_proxy_net_bytes_guarantee AS proxy_net,
        $fingerprint(tablet_node_count, tablet_node_type_guarantee, tablet_node_vcpu_guarantee,
                     tablet_node_memory_guarantee, tablet_node_net_bytes_guarantee) AS node_fp,
        $fingerprint(rpc_proxy_count, rpc_proxy_type_guarantee, rpc_proxy_vcpu_guarantee,
                     rpc_proxy_memory_guarantee, rpc_proxy_net_bytes_guarantee) AS proxy_fp
    FROM RANGE($bundle_spec, $start_s, $end_s)
    WHERE bundle_name IS NOT NULL
);

-- ================= шаг 1: MAX по хостам бандла в каждой точке =================

-- Хост размера не по гарантии бандла — временно назначенный spare или под старой
-- конфигурации; его потребление в максимум бандла не берём. По сети сравниваем
-- только когда лимит известен с обеих сторон: там, где он не включён, спек-лог
-- отдаёт NULL (ноды hahn и arnold) или ноль (их же прокси).
$net_known = ($net) -> { RETURN COALESCE($net, 0) != 0 };
$same_size = ($h_vcpu, $h_mem, $h_net, $g_vcpu, $g_mem, $g_net) -> {
    RETURN $h_vcpu == $g_vcpu AND $h_mem == $g_mem
        AND (NOT $net_known($h_net) OR NOT $net_known($g_net) OR $h_net == $g_net);
};

$node_hosts = (
    SELECT h.day AS day, h.cluster AS cluster, h.bundle AS bundle, h.host AS host
    FROM (
        SELECT TableName() AS day, cluster AS cluster, bundle AS bundle, host AS host,
               tablet_node_vcpu AS vcpu, tablet_node_memory AS memory,
               tablet_node_net_bytes AS net
        FROM RANGE($nodes_spec, $start_s, $end_s)
        WHERE nanny_service_id IS NOT NULL AND host IS NOT NULL
    ) AS h
    INNER JOIN $spec_by_day AS s
        ON h.day == s.day AND h.cluster == s.cluster AND h.bundle == s.bundle
    WHERE $same_size(h.vcpu, h.memory, h.net, s.node_vcpu, s.node_memory, s.node_net)
);

-- Прокси привязываем по bundle (allocated_for_bundle), а не по роли: на cross-dc
-- кластерах у прокси неактивного ДЦ роль <bundle>_released, и по роли она выпала
-- бы из бандла на все сутки, хотя до переключения ДЦ несла нагрузку. Простой в
-- максимум по хостам не мешает. У spare-проксей bundle = "spare" даже когда они
-- выданы бандлу, так что они по-прежнему не учитываются.
$proxy_hosts = (
    SELECT h.day AS day, h.cluster AS cluster, h.bundle AS bundle, h.host AS host
    FROM (
        -- bundle в спек-логе проксей — String, а дальше по конвейеру всё Utf8.
        SELECT TableName() AS day, cluster AS cluster, CAST(bundle AS Utf8) AS bundle,
               host AS host, rpc_proxy_vcpu AS vcpu, rpc_proxy_memory AS memory,
               rpc_proxy_net_bytes AS net
        FROM RANGE($proxies_spec, $start_s, $end_s)
        WHERE nanny_service_id IS NOT NULL
          AND allocated AND bundle IS NOT NULL AND bundle != "" AND host IS NOT NULL
    ) AS h
    INNER JOIN $spec_by_day AS s
        ON h.day == s.day AND h.cluster == s.cluster AND h.bundle == s.bundle
    WHERE $same_size(h.vcpu, h.memory, h.net, s.proxy_vcpu, s.proxy_memory, s.proxy_net)
);

INSERT INTO @usage_max
SELECT
    "node" AS instance_type,
    h.cluster AS cluster, h.bundle AS bundle, u.day AS day, u.ts AS ts,
    MAX($finite(u.vcpu_usage))        AS cpu_total,
    MAX($finite(u.memory_anon_usage)) AS anon_memory,
    MAX($finite(u.net_tx))            AS net_tx,
    MAX($finite(u.net_rx))            AS net_rx
FROM (
    SELECT TableName() AS day, host, ts, memory_anon_usage, net_tx, net_rx,
           vcpu_usage / $cpu_usage_divisor AS vcpu_usage
    FROM RANGE($usage_nodes, $start_s, $end_s)
    WHERE ts IS NOT NULL AND host IS NOT NULL
) AS u
INNER JOIN $node_hosts AS h ON u.day == h.day AND u.host == h.host
GROUP BY h.cluster AS cluster, h.bundle AS bundle, u.day AS day, u.ts AS ts;

INSERT INTO @usage_max
SELECT
    "proxy" AS instance_type,
    h.cluster AS cluster, h.bundle AS bundle, u.day AS day, u.ts AS ts,
    MAX($finite(u.vcpu_usage))        AS cpu_total,
    MAX($finite(u.memory_anon_usage)) AS anon_memory,
    MAX($finite(u.net_tx))            AS net_tx,
    MAX($finite(u.net_rx))            AS net_rx
FROM (
    SELECT TableName() AS day, host, ts, memory_anon_usage, net_tx, net_rx,
           vcpu_usage / $cpu_usage_divisor AS vcpu_usage
    FROM RANGE($usage_proxies, $start_s, $end_s)
    WHERE ts IS NOT NULL AND host IS NOT NULL
) AS u
INNER JOIN $proxy_hosts AS h ON u.day == h.day AND u.host == h.host
GROUP BY h.cluster AS cluster, h.bundle AS bundle, u.day AS day, u.ts AS ts;

COMMIT;

-- ================= шаг 2: квантиль по времени внутри периода =================

$stats_raw = (
    SELECT
        instance_type, cluster, bundle, period,
        $percentile(ListNotNull(AGGREGATE_LIST(cpu_total)), $quantile)
            AS cpu_total_p75,
        $percentile(ListNotNull(AGGREGATE_LIST(anon_memory)), $quantile)
            AS anon_memory_p75,
        $percentile(ListNotNull(AGGREGATE_LIST(net_tx)), $quantile)
            AS net_tx_p75,
        $percentile(ListNotNull(AGGREGATE_LIST(net_rx)), $quantile)
            AS net_rx_p75,
        COUNT_IF(cpu_total IS NOT NULL) AS cpu_n_points,
        COUNT_IF(anon_memory IS NOT NULL) AS memory_n_points,
        COUNT_IF(net_tx IS NOT NULL) AS net_tx_n_points,
        COUNT_IF(net_rx IS NOT NULL) AS net_rx_n_points
    FROM (SELECT u.*, $period_of(u.day) AS period FROM @usage_max AS u)
    WHERE period >= 0 AND period < $n_periods
    GROUP BY instance_type AS instance_type, cluster AS cluster,
             bundle AS bundle, period AS period
);

-- Контракт витрины — 2880 точек в сутки. Проверяем его отдельно для каждого
-- кластера и типа: смена частоты должна уронить запрос, а не молча изменить
-- coverage. Сам coverage абсолютный, поэтому общий провал нескольких дней будет
-- виден, даже если затронул все бандлы и кластеры.
$usage_points_by_bundle_day = (
    SELECT instance_type, cluster, bundle, day, COUNT(*) AS n_points
    FROM @usage_max
    GROUP BY instance_type AS instance_type, cluster AS cluster,
             bundle AS bundle, day AS day
);

$expected_usage_points = (
    SELECT
        instance_type, cluster,
        Ensure(
            MAX(n_points),
            MAX(n_points) == $usage_points_per_day,
            "unexpected daily usage point count: expected 2880"
        ) * $period_days AS expected_n_points
    FROM $usage_points_by_bundle_day
    GROUP BY instance_type AS instance_type, cluster AS cluster
);

-- Сохраняем отдельно, чтобы проверка cadence не встраивалась в map join ниже
-- вместе с исходными суточными метриками.
INSERT INTO @expected_usage_points
SELECT * FROM $expected_usage_points;

COMMIT;

$stats = (
    SELECT
        s.*,
        CAST(MIN_OF(s.cpu_n_points, s.memory_n_points,
                    s.net_tx_n_points, s.net_rx_n_points) AS Double) AS n_points,
        IF(e.expected_n_points > 0,
           CAST(MIN_OF(s.cpu_n_points, s.memory_n_points,
                       s.net_tx_n_points, s.net_rx_n_points) AS Double)
               / CAST(e.expected_n_points AS Double),
           0.0) AS usage_coverage
    FROM $stats_raw AS s
    INNER JOIN @expected_usage_points AS e
        ON s.instance_type == e.instance_type AND s.cluster == e.cluster
);

-- Не даём оптимизатору встроить расчёт usage в последующий map join: без
-- барьера промежуточный словарь строится по исходным метрикам и не помещается
-- в память джоба. После агрегации таблица небольшая.
INSERT INTO @stats
SELECT * FROM $stats;

COMMIT;

-- Зоны доступности кластера: у cross-dc кластеров инстансы бандла разложены по
-- ДЦ и активны не все сразу, у обычных бандл-контроллер отдаёт один "default".
$zones_by_cluster_day = (
    SELECT cluster, day, COUNT(DISTINCT data_center) AS availability_zones
    FROM (
        SELECT cluster, TableName() AS day, data_center
        FROM RANGE($nodes_spec, $start_s, $end_s)
        WHERE data_center IS NOT NULL
    ) AS daily_zones
    GROUP BY cluster AS cluster, day AS day
);

$zones_by_cluster = (
    SELECT cluster, MAX(availability_zones) AS availability_zones
    FROM $zones_by_cluster_day
    GROUP BY cluster AS cluster
);

-- ================= шаг 3: эпохи конфигурации =================

-- Последняя доступная конфигурация выбирается отдельно для каждого бандла: один
-- недоступный кластер или частично собранный последний день не должны удалять его
-- из результата.
$current_ranked = (
    SELECT
        s.*,
        ROW_NUMBER() OVER (
            PARTITION BY cluster, bundle
            ORDER BY day DESC
        ) AS recency_rank
    FROM $spec_by_day AS s
);

$current = (
    SELECT
        cluster, bundle,
        abc_service_slug, abc_service_path,
        value_stream_slug, value_stream_name_ru,
        business_unit_slug, business_unit_name_ru,
        business_group_slug, business_group_name_ru,
        node_count, node_type, node_vcpu, node_memory, node_net,
        rpc_count, rpc_type, proxy_vcpu AS rpc_vcpu, proxy_memory AS rpc_memory,
        proxy_net AS rpc_net,
        node_fp AS node_fp_cur,
        proxy_fp AS proxy_fp_cur,
        day AS bundle_spec_loaded_at
    FROM $current_ranked
    WHERE recency_rank == 1
);

-- Все пары (бандл, период): так отсутствие свежих данных становится нулевым
-- coverage, а не исчезновением бандла из результата.
$grid = (
    SELECT c.cluster AS cluster, c.bundle AS bundle, p AS period
    FROM $current AS c
    CROSS JOIN (SELECT * FROM AS_TABLE(ListMap(ListFromRange(0, $n_periods),
                                               ($i) -> { RETURN AsStruct($i AS p) }))) AS periods
);

-- Покрытие spec считается по дням. Пропуски допустимы, но все присутствующие
-- fingerprints обязаны совпадать с последней известной конфигурацией.
$period_flags = (
    SELECT
        g.cluster AS cluster, g.bundle AS bundle, g.period AS period,
        COUNT(s.day) AS days_present,
        COALESCE(SUM(IF(s.node_fp == c.node_fp_cur, 1, 0)), 0) AS node_match,
        COALESCE(SUM(IF(s.proxy_fp == c.proxy_fp_cur, 1, 0)), 0) AS proxy_match,
        MAX(IF(s.node_fp != c.node_fp_cur, s.day, NULL)) AS node_last_diff,
        MAX(IF(s.proxy_fp != c.proxy_fp_cur, s.day, NULL)) AS proxy_last_diff
    FROM $grid AS g
    INNER JOIN $current AS c ON g.cluster == c.cluster AND g.bundle == c.bundle
    LEFT JOIN (SELECT d.*, $period_of(d.day) AS period FROM $spec_by_day AS d) AS s
           ON g.cluster == s.cluster AND g.bundle == s.bundle AND g.period == s.period
    GROUP BY g.cluster AS cluster, g.bundle AS bundle, g.period AS period
);

$period_coverage = (
    SELECT
        f.*,
        CAST(f.days_present AS Double) / CAST($period_days AS Double) AS node_spec_coverage,
        CAST(f.days_present AS Double) / CAST($period_days AS Double) AS proxy_spec_coverage,
        f.node_match == f.days_present AS node_config_matches,
        f.proxy_match == f.days_present AS proxy_config_matches,
        COALESCE(n.usage_coverage, 0.0) AS node_usage_coverage,
        COALESCE(p.usage_coverage, 0.0) AS proxy_usage_coverage,
        n.cpu_total_p75 AS node_cpu_total_p75,
        n.anon_memory_p75 AS node_anon_memory_p75,
        n.net_tx_p75 AS node_net_tx_p75,
        n.net_rx_p75 AS node_net_rx_p75,
        n.n_points AS node_n_points,
        p.cpu_total_p75 AS proxy_cpu_total_p75,
        p.anon_memory_p75 AS proxy_anon_memory_p75,
        p.net_tx_p75 AS proxy_net_tx_p75,
        p.net_rx_p75 AS proxy_net_rx_p75,
        p.n_points AS proxy_n_points
    FROM $period_flags AS f
    LEFT JOIN (SELECT * FROM @stats WHERE instance_type == "node") AS n
        ON f.cluster == n.cluster AND f.bundle == n.bundle AND f.period == n.period
    LEFT JOIN (SELECT * FROM @stats WHERE instance_type == "proxy") AS p
        ON f.cluster == p.cluster AND f.bundle == p.bundle AND f.period == p.period
);

-- Хвостовой отрезок: сколько периодов подряд, начиная с самого свежего, валидны.
-- MIN(номер первого невалидного) и есть их количество. При невалидном period_0
-- valid_periods == 0, но диагностическая строка бандла остаётся в @out.
$valid_counts = (
    SELECT
        cluster, bundle,
        MIN(IF(node_spec_coverage >= $min_period_coverage
                   AND node_config_matches
                   AND node_usage_coverage >= $min_period_coverage,
               $n_periods, period)) AS node_valid_periods,
        MIN(IF(proxy_spec_coverage >= $min_period_coverage
                   AND proxy_config_matches
                   AND proxy_usage_coverage >= $min_period_coverage,
               $n_periods, period)) AS proxy_valid_periods,
        MAX(node_last_diff)  AS node_last_config_change,
        MAX(proxy_last_diff) AS proxy_last_config_change
    FROM $period_coverage
    GROUP BY cluster AS cluster, bundle AS bundle
);

-- Итоговая диагностика одинакова во всех периодных выходах. Coverage усредняем
-- по периодам, реально вошедшим в рекомендацию; если period_0 невалиден, берём
-- его самого, чтобы объяснить отсутствие рекомендации.
$bundle_diagnostics_raw = (
    SELECT
        q.cluster AS cluster, q.bundle AS bundle,
        v.node_valid_periods AS node_valid_periods,
        v.proxy_valid_periods AS proxy_valid_periods,
        v.node_last_config_change AS node_last_config_change,
        v.proxy_last_config_change AS proxy_last_config_change,
        AVG(IF(q.period < v.node_valid_periods
                   OR (v.node_valid_periods == 0 AND q.period == 0),
               q.node_spec_coverage, NULL)) AS node_spec_coverage,
        AVG(IF(q.period < v.proxy_valid_periods
                   OR (v.proxy_valid_periods == 0 AND q.period == 0),
               q.proxy_spec_coverage, NULL)) AS proxy_spec_coverage,
        AVG(IF(q.period < v.node_valid_periods
                   OR (v.node_valid_periods == 0 AND q.period == 0),
               q.node_usage_coverage, NULL)) AS node_usage_coverage,
        AVG(IF(q.period < v.proxy_valid_periods
                   OR (v.proxy_valid_periods == 0 AND q.period == 0),
               q.proxy_usage_coverage, NULL)) AS proxy_usage_coverage,
        MAX(IF(q.period == v.node_valid_periods
                   AND v.node_valid_periods < $n_periods
                   AND q.node_spec_coverage < $min_period_coverage, 1, 0))
            AS node_boundary_spec_bad,
        MAX(IF(q.period == v.proxy_valid_periods
                   AND v.proxy_valid_periods < $n_periods
                   AND q.proxy_spec_coverage < $min_period_coverage, 1, 0))
            AS proxy_boundary_spec_bad,
        MAX(IF(q.period == v.node_valid_periods
                   AND v.node_valid_periods < $n_periods
                   AND q.node_usage_coverage < $min_period_coverage, 1, 0))
            AS node_boundary_usage_bad,
        MAX(IF(q.period == v.proxy_valid_periods
                   AND v.proxy_valid_periods < $n_periods
                   AND q.proxy_usage_coverage < $min_period_coverage, 1, 0))
            AS proxy_boundary_usage_bad,
        MAX(IF(q.period == v.node_valid_periods
                   AND v.node_valid_periods < $n_periods
                   AND NOT q.node_config_matches, 1, 0))
            AS node_boundary_config_bad,
        MAX(IF(q.period == v.proxy_valid_periods
                   AND v.proxy_valid_periods < $n_periods
                   AND NOT q.proxy_config_matches, 1, 0))
            AS proxy_boundary_config_bad
    FROM $period_coverage AS q
    INNER JOIN $valid_counts AS v
        ON q.cluster == v.cluster AND q.bundle == v.bundle
    GROUP BY
        q.cluster AS cluster, q.bundle AS bundle,
        v.node_valid_periods AS node_valid_periods,
        v.proxy_valid_periods AS proxy_valid_periods,
        v.node_last_config_change AS node_last_config_change,
        v.proxy_last_config_change AS proxy_last_config_change
);

$bundle_diagnostics = (
    SELECT
        d.*,
        IF(d.node_valid_periods == 0, "none",
           IF(d.node_valid_periods >= $n_periods
                  AND d.node_spec_coverage >= $min_full_confidence_coverage
                  AND d.node_usage_coverage >= $min_full_confidence_coverage,
              "full", "low")) AS node_confidence,
        IF(d.proxy_valid_periods == 0, "none",
           IF(d.proxy_valid_periods >= $n_periods
                  AND d.proxy_spec_coverage >= $min_full_confidence_coverage
                  AND d.proxy_usage_coverage >= $min_full_confidence_coverage,
              "full", "low")) AS proxy_confidence,
        $period_invalidation_reason(
            d.node_valid_periods,
            d.node_boundary_spec_bad > 0,
            d.node_boundary_usage_bad > 0,
            d.node_boundary_config_bad > 0) AS node_period_invalidation_reason,
        $period_invalidation_reason(
            d.proxy_valid_periods,
            d.proxy_boundary_spec_bad > 0,
            d.proxy_boundary_usage_bad > 0,
            d.proxy_boundary_config_bad > 0) AS proxy_period_invalidation_reason,
        $confidence_reason(
            d.node_valid_periods,
            d.node_spec_coverage < $min_full_confidence_coverage,
            d.node_usage_coverage < $min_full_confidence_coverage) AS node_confidence_reason,
        $confidence_reason(
            d.proxy_valid_periods,
            d.proxy_spec_coverage < $min_full_confidence_coverage,
            d.proxy_usage_coverage < $min_full_confidence_coverage) AS proxy_confidence_reason
    FROM $bundle_diagnostics_raw AS d
);

INSERT INTO @out
SELECT
    q.period AS period,
    q.bundle AS bundle,
    q.cluster AS cluster,
    CAST($period_days AS String) || "d" AS time_period,
    "period_" || CAST(q.period AS String) AS method_name,
    c.node_type AS node_type,
    c.node_count AS node_count,
    c.rpc_type AS rpc_type,
    c.rpc_count AS rpc_count,
    c.bundle_spec_loaded_at AS bundle_spec_loaded_at,
    c.abc_service_slug AS abc_service_slug,
    c.abc_service_path AS abc_service_path,
    c.value_stream_slug AS value_stream_slug,
    c.value_stream_name_ru AS value_stream_name_ru,
    c.business_unit_slug AS business_unit_slug,
    c.business_unit_name_ru AS business_unit_name_ru,
    c.business_group_slug AS business_group_slug,
    c.business_group_name_ru AS business_group_name_ru,
    $n_periods AS periods_total,

    IF(q.period < d.node_valid_periods, q.node_cpu_total_p75)    AS node_cpu_total_p75,
    IF(q.period < d.node_valid_periods, q.node_anon_memory_p75)  AS node_anon_memory_p75,
    IF(q.period < d.node_valid_periods, q.node_net_tx_p75)       AS node_net_tx_p75,
    IF(q.period < d.node_valid_periods, q.node_net_rx_p75)       AS node_net_rx_p75,

    IF(q.period < d.proxy_valid_periods, q.proxy_cpu_total_p75)   AS proxy_cpu_total_p75,
    IF(q.period < d.proxy_valid_periods, q.proxy_anon_memory_p75) AS proxy_anon_memory_p75,
    IF(q.period < d.proxy_valid_periods, q.proxy_net_tx_p75)      AS proxy_net_tx_p75,
    IF(q.period < d.proxy_valid_periods, q.proxy_net_rx_p75)      AS proxy_net_rx_p75,

    -- Служебные поля: data.py их игнорирует, приджойнит кубик аннотации.
    d.node_valid_periods AS node_valid_periods,
    d.proxy_valid_periods AS proxy_valid_periods,
    d.node_last_config_change AS node_last_config_change,
    d.proxy_last_config_change AS proxy_last_config_change,
    q.node_n_points AS node_n_points,
    q.proxy_n_points AS proxy_n_points,
    -- Coverage периода этой строки. Усреднённое по вошедшим периодам наружу не
    -- отдаём: оно нужно только порогу confidence и восстанавливается по
    -- периодным значениям и valid_periods.
    q.node_spec_coverage AS node_spec_coverage,
    q.proxy_spec_coverage AS proxy_spec_coverage,
    q.node_usage_coverage AS node_usage_coverage,
    q.proxy_usage_coverage AS proxy_usage_coverage,
    d.node_confidence AS node_confidence,
    d.proxy_confidence AS proxy_confidence,
    d.node_period_invalidation_reason AS node_period_invalidation_reason,
    d.proxy_period_invalidation_reason AS proxy_period_invalidation_reason,
    d.node_confidence_reason AS node_confidence_reason,
    d.proxy_confidence_reason AS proxy_confidence_reason,
    COALESCE(z.availability_zones, 1) AS availability_zones
FROM $period_coverage AS q
INNER JOIN $current AS c ON q.cluster == c.cluster AND q.bundle == c.bundle
INNER JOIN $bundle_diagnostics AS d
    ON q.cluster == d.cluster AND q.bundle == d.bundle
LEFT JOIN $zones_by_cluster AS z ON q.cluster == z.cluster;

COMMIT;

-- ================= выходы =================
-- Период = метод: assign_pod_sizes берёт по каждому бандлу максимум по методам.

INSERT INTO $metrics_0 WITH TRUNCATE
SELECT * WITHOUT period FROM @out WHERE period == 0 ORDER BY cluster, bundle;

INSERT INTO $metrics_1 WITH TRUNCATE
SELECT * WITHOUT period FROM @out WHERE period == 1 ORDER BY cluster, bundle;

INSERT INTO $metrics_2 WITH TRUNCATE
SELECT * WITHOUT period FROM @out WHERE period == 2 ORDER BY cluster, bundle;

-- Каталог типов контейнеров. Один и тот же тип в разных кластерах может иметь
-- разные гарантии (proxy heavy: 28 ядер в sas/klg, 30 в vla), поэтому кластер
-- входит в ключ. Внутри кластера коллизий сейчас нет, MIN лишь делает
-- результат детерминированным.

-- Для каталога берём единый последний доступный день каждого кластера. Иначе
-- per-bundle fallback в $current смешал бы старые и новые гарантии одного типа.
$latest_cluster_spec_day = (
    SELECT cluster, MAX(day) AS day
    FROM $spec_by_day
    GROUP BY cluster AS cluster
);

$latest_cluster_specs = (
    SELECT s.*
    FROM $spec_by_day AS s
    INNER JOIN $latest_cluster_spec_day AS d
        ON s.cluster == d.cluster AND s.day == d.day
);

INSERT INTO $node_specs WITH TRUNCATE
SELECT
    cluster,
    node_type AS container_type,
    CAST(MIN(node_vcpu) AS Double) / 1000.0 AS cpu_cores,
    MIN(node_memory) AS memory_bytes,
    MIN(node_net) AS net_bytes,
    CAST(MIN(node_memory) AS Double) / 1073741824.0 AS memory_gb
FROM $latest_cluster_specs
WHERE node_count > 0 AND node_type IS NOT NULL AND node_type != ""
GROUP BY cluster, node_type
ORDER BY cluster, container_type;

INSERT INTO $rpc_specs WITH TRUNCATE
SELECT
    cluster,
    rpc_type AS container_type,
    CAST(MIN(proxy_vcpu) AS Double) / 1000.0 AS cpu_cores,
    MIN(proxy_memory) AS memory_bytes,
    MIN(proxy_net) AS net_bytes,
    CAST(MIN(proxy_memory) AS Double) / 1073741824.0 AS memory_gb
FROM $latest_cluster_specs
WHERE rpc_count > 0 AND rpc_type IS NOT NULL AND rpc_type != ""
GROUP BY cluster, rpc_type
ORDER BY cluster, container_type;
