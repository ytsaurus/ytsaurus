# Изменение конфигурации кластера

## Переопределение конфигурации { #config-overrides }

Оператор {{product-name}} автоматически генерирует статические конфигурационные файлы для всех компонентов кластера на основе спецификации `Ytsaurus`. В редких случаях может потребоваться переопределить определенные параметры конфигурации, которые не доступны через поля спецификации. Для этого можно использовать поле `configOverrides`.

### Как это работает

Поле `configOverrides` ссылается на Kubernetes ConfigMap, который содержит фрагменты конфигурации в формате YSON для конкретных компонентов. Эти фрагменты объединяются с конфигурацией, сгенерированной оператором.

### Формат конфигурации

Создайте ConfigMap, где:
- Каждый ключ представляет имя конфигурационного файла компонента (например, `ytserver-http-proxy.yson`)
- Каждое значение содержит фрагмент конфигурации в формате YSON для переопределения или расширения сгенерированной конфигурации

### Пример

Вот пример использования `configOverrides` для настройки домена cookie для HTTP прокси:

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
  
  # ... остальная часть спецификации
```

### Доступные конфигурационные файлы

Вы можете переопределить конфигурацию для любого компонента, используя соответствующее имя конфигурационного файла:

- `ytserver-master.yson` - Мастер-серверы
- `ytserver-http-proxy.yson` - HTTP прокси
- `ytserver-rpc-proxy.yson` - RPC прокси
- `ytserver-data-node.yson` - Ноды данных
- `ytserver-exec-node.yson` - Exec ноды
- `ytserver-tablet-node.yson` - Tablet ноды
- `ytserver-scheduler.yson` - Планировщики
- `ytserver-controller-agent.yson` - Агенты контроллера
- `ytserver-discovery.yson` - Сервис обнаружения


