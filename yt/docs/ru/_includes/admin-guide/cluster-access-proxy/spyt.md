# Как настроить доступ к SPYT {#spyt-access}

Для работы SPYT в режиме [Standalone](../../../user-guide/data-processing/spyt/cluster/cluster-desc.md#spark-standalone) внешний драйвер Spark должен соединяться напрямую со Spark-воркерами. Эти воркеры запускаются не как отдельные поды Kubernetes, а как процессы внутри [Vanilla-джобов](../../../user-guide/data-processing/operations/vanilla.md) {{product-name}}. Стандартные сетевые абстракции Kubernetes (Services) не умеют адресовать трафик внутрь таких процессов.

Для проксирования таких соединений используется `tcp_proxy`. Это компонент {{product-name}}, который знает, на какой ноде и на каком порту запустился конкретный процесс воркера, и умеет пробросить туда трафик снаружи &mdash; на основе таблиц в Кипарисе `//sys/tcp_proxies/routes`.

Чтобы настроить `tcp_proxy`:

1. Убедитесь, что порты, на которых работает `tcp_proxy` (по умолчанию 32000–32019), открыты через Kubernetes-сервисы (NodePort или LoadBalancer).
2. Пропишите внешние адреса этих портов в атрибут `//sys/tcp_proxies/routes/<proxy_role>/@external_addresses` в Кипарисе.

Это позволит драйверу Spark использовать `tcp_proxy` как посредника для связи с внутренними воркерами.

Пример:

```bash
yt set //sys/tcp_proxies/routes/default/@external_addresses '["node1.example.com:32000"; "node2.example.com:32000"]'
```
