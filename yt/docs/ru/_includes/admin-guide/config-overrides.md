# Переопределение конфигурации

Оператор {{product-name}} автоматически генерирует статические конфигурационные файлы для всех компонентов кластера &mdash; на основе [спецификации](../../admin-guide/prepare-spec.md) `Ytsaurus`. Если необходимо точечно изменить параметры, которые не вынесены в поля этой спецификации (например, чтобы включить debug-логирование для диагностики или настроить RPC-таймауты), используйте поле `configOverrides`. Этот механизм позволяет наложить свои настройки поверх конфигурации, сгенерированной оператором.

## Как это работает {#how-it-works}

Механизм переопределения работает по принципу наложения изменений (патчинга). Вы создаёте отдельный [Kubernetes ConfigMap](https://kubernetes.io/docs/concepts/configuration/configmap/), в котором описываете только те параметры, которые хотите изменить или добавить. В спецификации вашего {{product-name}}-кластера в поле `configOverrides` вы указываете имя этого ConfigMap. Оператор считывает эти настройки и «вливает» их в автоматически сгенерированную конфигурацию перед запуском компонентов.

{% note info %}

При использовании `configOverrides` заранее убедитесь, что переопределяемая опция присутствует в конфигурации нужного компонента и что она поддерживается в текущей версии {{product-name}} &mdash; оператор такой проверки не выполняет. Если опция в оверрайде указана неверно, изменения будут проигнорированы.

{% endnote %}

## Формат конфигурации {#configuration-format}

ConfigMap должен содержать фрагмент конфигурации в формате YSON.

## Структура ConfigMap {#configuration-structure}

При формировании ConfigMap используйте следующую структуру:

- Каждый ключ &mdash; это имя конфигурационного файла компонента. Например, `ytserver-http-proxy.yson` для HTTP-прокси или `ytserver-master.yson` для мастера.
- Каждое значение &mdash; это фрагмент YSON-конфигурации с переопределяемыми параметрами.

## Пример {#example}

Ниже приведён пример использования `configOverrides` для настройки домена cookie для HTTP прокси:

**Шаг 1:** Создайте ConfigMap с переопределениями конфигурации:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: overrides
data:
  ytserver-http-proxy.yson: |
    {
        "auth" = {
            "cypress_cookie_manager" = {
                "cookie_generator" = {
                    "domain" = ".yt-cluster.my-domain.com";
                }
            };
        };
    }
```

**Шаг 2:** Укажите ссылку на ConfigMap в спецификации `Ytsaurus`:

```yaml
apiVersion: cluster.ytsaurus.tech/v1
kind: Ytsaurus
metadata:
  name: ytdemo
spec:
  configOverrides:
    name: overrides

  # ... Остальная часть спецификации
```

## Доступные конфигурационные файлы {#available-configuration-files}

Ниже перечислены компоненты кластера, для которых можно переопределить конфигурацию &mdash; используя соответствующее имя конфигурационного файла:

- `ytserver-master.yson` &mdash; мастер-серверы
- `ytserver-http-proxy.yson` &mdash; HTTP-прокси
- `ytserver-rpc-proxy.yson` &mdash; RPC-прокси
- `ytserver-data-node.yson` &mdash; ноды данных
- `ytserver-exec-node.yson` &mdash; Exec ноды
- `ytserver-tablet-node.yson` &mdash; таблет-ноды
- `ytserver-scheduler.yson` &mdash; планировщик
- `ytserver-controller-agent.yson` &mdash; контроллер-агенты
- `ytserver-discovery.yson` &mdash; сервис обнаружения прокси
