# Мониторинг

## Настройка prometheus
### Пререквизиты
- установить prometheus-operator [по инструкции](https://github.com/prometheus-operator/prometheus-operator#quickstart).

### Запуск
Сервисы для мониторинга создаются оператором {{product-name}} автоматически. Для сбора метрик необходимо:
1. Создать [ServiceMonitor](https://github.com/ytsaurus/yt-k8s-operator/blob/main/config/samples/prometheus/prometheus_service_monitor.yaml).
2. Создать [сервисный аккаунт](https://github.com/ytsaurus/yt-k8s-operator/blob/main/config/samples/prometheus/prometheus_service_account.yaml).
3. Выдать созданному аккаунту [роль](https://github.com/ytsaurus/yt-k8s-operator/blob/main/config/samples/prometheus/prometheus_role_binding.yaml).
4. [Создать Prometheus](https://github.com/ytsaurus/yt-k8s-operator/blob/main/config/samples/prometheus/prometheus.yaml).
