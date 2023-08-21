# Мониторинг (настройка, дашборды)

## Настройка prometheus
### Пререквизиты
- prometheus-operator: можно установить по инструкции https://github.com/prometheus-operator/prometheus-operator#quickstart

### Запуск
Сервисы для мониторинга создаются оператором YTsaurus автоматически, чтобы собирать метрики надо
- Создать ServiceMonitor: https://github.com/ytsaurus/yt-k8s-operator/blob/main/config/samples/prometheus/prometheus_service_monitor.yaml
- Создать сервисный аккаунт: https://github.com/ytsaurus/yt-k8s-operator/blob/main/config/samples/prometheus/prometheus_service_account.yaml
- Выдать созданному аккаунту роль: https://github.com/ytsaurus/yt-k8s-operator/blob/main/config/samples/prometheus/prometheus_role_binding.yaml
- Создать Prometheus: https://github.com/ytsaurus/yt-k8s-operator/blob/main/config/samples/prometheus/prometheus.yaml
