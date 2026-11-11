#### Дополнительные зависимости (extras) { #extras }

Часть функциональности требует дополнительных зависимостей, которые не ставятся по умолчанию. Они объявлены в наборах extras пакета `ytsaurus-client` и устанавливаются указанием набора в квадратных скобках:

| Набор | Зависимости | Для чего нужен |
| --- | --- | --- |
| `recommended` | `brotli`, `cryptography` | Рекомендуемые опциональные зависимости |
| `admin` | `kubernetes`, `docker` | [Административные команды CLI](../../../admin-guide/cli-admin.md) (`yt admin logs k8s`, `yt admin metrics replay`) |

```bash
pip install "ytsaurus-client[recommended]"
pip install "ytsaurus-client[admin]"
```

