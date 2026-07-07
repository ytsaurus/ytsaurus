# Как решить проблему сетевой изоляции

Если вы настроили Ingress, но при попытке записать данные получаете ошибки, несмотря на работающие команды `list` или `create`, — вы столкнулись с проблемой изоляции.

Решить проблему можно несколькими способами:

- [Использовать прямую маршрутизацию](#use-flat-network)
- [Использовать Host Network](#use-host-network)
- [Настроить Kubernetes Services и подмену адресов](#k8s-services)

## Использовать прямую маршрутизацию {#use-flat-network}

Если ваша сетевая инфраструктура позволяет обращаться к подам напрямую (например, используется CNI Calico с маршрутизацией BGP или AWS VPC CNI), дополнительная настройка {{product-name}} не требуется. Механизм [Discovery](../../../admin-guide/cluster-access-proxy/index.md#discovery) вернёт внутренние FQDN подов (например, `hp-0.http-proxies.default.svc.cluster.local`).

Однако внешний клиент должен иметь возможность:
- Разрешить DNS-имена подов. Внутренние FQDN вида `*.svc.cluster.local` по умолчанию известны только DNS-серверу внутри кластера Kubernetes &mdash; внешний клиент не сможет их разрешить через обычные публичные DNS-серверы.
- Установить сетевое соединение. Даже если имя разрешилось в IP-адрес, клиент должен иметь сетевую связность с этим IP (маршрутизацию).

### Настройка для AWS

В AWS самый простой способ получить доступ к подам, которые уже имеют сетевую связность в dual stack режиме, — это настроить CoreDNS для разрешения имён подов. Для этого необходимо:

1. Открыть доступ к CoreDNS в кластере для внешних клиентов.
2. Настроить CoreDNS отвечать на запросы вида `*.cluster.domain.name`, где `cluster.domain.name` — это доменное имя вашего кластера.

{% cut "Пример настройки CoreDNS ConfigMap" %}

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: coredns
  namespace: kube-system
data:
  Corefile: |
    .:53 {
        errors
        health {
           lameduck 5s
        }
        ready
        kubernetes cluster.local in-addr.arpa ip6.arpa {
           pods insecure
           fallthrough in-addr.arpa ip6.arpa
           ttl 30
        }
        # ВНИМАНИЕ: Замените 'cluster.domain.name' на реальный домен вашего кластера.
        # Этот шаблон позволяет внешним клиентам находить IP-адреса подов по их именам.
        template IN A cluster.domain.name {
            match "^([^.]+)\.http-proxies\.default\.svc\.cluster\.domain\.name\.$"
            answer "{{ .Name }} 60 IN A {{ .Group 1 | replace \"-\" \".\" }}"
        }
        prometheus :9153
        forward . /etc/resolv.conf {
           max_concurrent 1000
        }
        cache 30
        loop
        reload
        loadbalance
    }
```

После этого внешние клиенты смогут разрешать FQDN подов в их реальные IP-адреса и устанавливать прямые соединения.

{% endcut %}


## Использовать Host Network {#use-host-network}

В этом режиме поды прокси не получают выделенного IP-адреса на под из внутренней сети кластера K8s, а используют сетевой интерфейс физического сервера (ноды), на котором они запущены.

Чтобы включить режим, добавьте поле `hostNetwork: true` в корневой уровень спецификации [оператора](../../../admin-guide/install-ytsaurus#operator):

```yaml
apiVersion: cluster.ytsaurus.tech/v1
kind: Ytsaurus
metadata:
  name: my-cluster
spec:
  # Включаем использование сети хоста для всех компонентов.
  hostNetwork: true

  httpProxies:
    - serviceType: ClusterIP # При hostNetwork внешний сервис K8s не обязателен.
      instanceCount: 1
  # ... остальные настройки ...
```

В этом случае прокси будут регистрироваться в Кипарисе под именами нод кластера Kubernetes. Они будут отдавать именно эти имена K8s-нод клиентам при Discovery. Далее внешним клиентам необходимо самостоятельно разрешать DNS-имена этих нод.

{% note warning %}

В режиме `hostNetwork` прокси будут занимать порты (по умолчанию 80 и 443) непосредственно на нодах кластера. Убедитесь, что на этих узлах не запущены другие конфликтующие веб-сервисы, либо измените порты в конфигурации.

{% endnote %}

## Настроить K8S Services и подмену адресов {#k8s-services}

Этот способ наиболее универсален для облачных сред. Прокси публикуются через сервисы (NodePort или LoadBalancer), а {{product-name}} конфигурируется так, чтобы механизм [Discovery](../../../admin-guide/cluster-access-proxy/index.md#discovery) возвращал клиентам именно эти внешние адреса, а не внутренние имена (FQDN) подов.

Этот подход требует двух шагов:

1. [Открыть порты](#external-access-step) через K8s-сервисы (NodePort или LoadBalancer).
1. [Настроить подмену адресов](#set-discovery-step) (Advertised Addresses).

### Шаг 1: Открыть порты {#external-access-step}

В спецификации оператора укажите тип сервиса для нужных групп прокси. Например:

```yaml
spec:
  # HTTP прокси
  httpProxies:
    - role: control
      serviceType: LoadBalancer   # Входная точка для лёгких запросов
      instanceCount: 1
    - role: default
      serviceType: NodePort       # Точки входа для тяжёлых запросов (Data)
      instanceCount: 3

  # RPC прокси
  rpcProxies:
    - role: project-a
      serviceType: LoadBalancer
      instanceCount: 2
```

[Пример полной спецификации](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/config/samples/cluster_v1_local.yaml)

### Шаг 2: Настроить подмены адресов (Discovery) {#set-discovery-step}

Даже если сервисы созданы, команда `discover_proxies` всё ещё будет возвращать внутренние FQDN подов. Необходимо настроить {{product-name}} так, чтобы он возвращал внешние адреса, которые ведут на созданные сервисы.

Для этого задайте конфигурацию атрибутам `//sys/http_proxies/@balancers` и `//sys/rpc_proxies/@balancers`:

```yson
{
    "<proxy_role>" = {
        "<address_type>" = {
            "<network_name>" = ["<external_addr_1>"; "<external_addr_2>"]
        }
    }
}
```

- `proxy_role` &mdash; роль прокси (например, `default` или `project-a`).
- `address_type` &mdash; тип протокола. Используйте `http` для HTTP-прокси и `internal_rpc` для RPC-прокси.
- `network_name` &mdash; имя сети. В серверных компонентах {{product-name}} есть поддержка нескольких интерфейсов (исторически для разделения real-time и bulk трафика). В Kubernetes у пода обычно один сетевой интерфейс, поэтому стандартное значение &mdash; `default`.

{% note warning %}

Значение `internal_rpc` у параметра `address_type` — историческое. Оно относится к внутренней реализации кода и не означает, что адреса должны быть внутренними. В этот блок нужно вписывать **внешние** адреса (или FQDN), доступные клиентам.

Обратите внимание, что формат конфигурации — YSON, поэтому элементы списков разделяются не запятой, а точкой с запятой (`;`).

{% endnote %}

{% note info %}

Настройку адресов необходимо задавать для каждой роли отдельно.

{% endnote %}

### Пример настройки

Предположим, в кластере сконфигурированы две роли прокси:

- Контрольные прокси: доступны через общий LoadBalancer (или Ingress) по адресу `yt.example.com`.
- Data-прокси: вы запустили три инстанса и открыли к ним прямой доступ через NodePort. При использовании NodePort порт одинаков для всех узлов K8s, поэтому адреса будут вида: `node1.example.com:30001`, `node2.example.com:30001` и `node3.example.com:30001`.

Чтобы Discovery корректно возвращал эти адреса для каждой роли, выполните команды:

```bash
# 1. Настройка адресов для Data-прокси (роль default)
# Эти адреса будут использоваться для тяжёлых операций (чтение/запись).
$ yt set //sys/http_proxies/@balancers/default \
  '{"http"={"default"=[
      "node1.example.com:30001";
      "node2.example.com:30001";
      "node3.example.com:30001"
  ]}}'

# 2. Настройка адреса для контрольных прокси (роль control)
# Этот адрес будет возвращаться, если клиент явно запросит дискавери для контрольной группы.
$ yt set //sys/http_proxies/@balancers/control \
  '{"http"={"default"=[
      "yt.example.com"
  ]}}'
```

{% note tip %}

В примере команда `set` применяется к конкретным путям (`@balancers/default` и `@balancers/control`), а не к корневому атрибуту `@balancers`. Такой подход безопаснее: каждая роль конфигурируется независимо, без риска случайно перезаписать конфигурацию других ролей.

{% endnote %}
