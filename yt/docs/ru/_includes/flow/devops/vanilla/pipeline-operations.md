# Базовые операции с пайплайном {{product-name}} Flow

После [первичного деплоя](../../../../flow/devops/vanilla/initial-deploy.md) пайплайном управляют{% if audience == "internal" %} через UI {{product-name}} или{% endif %} через [CLI](../../../../flow/tools/cli.md). Основные операции — запуск, остановка и пауза:

* `start-pipeline` — запустить пайплайн;
* `stop-pipeline` — остановить пайплайн через режим `draining` (полный сброс промежуточных буферов);
* `pause-pipeline` — остановить пайплайн немедленно.

Подробнее про состояния пайплайна — в [глоссарии](../../../../flow/concepts/glossary.md#start-stop-pause-pipeline).

Эти команды управляют состоянием пайплайна, а не самой Vanilla-операцией: остановка операции и её пересоздание при выкатке нового релиза описаны в разделе [Обновления и релизы](../../../../flow/devops/vanilla/releases.md).

## См. также

- [CLI {{product-name}} Flow](../../../../flow/tools/cli.md)
- [Первичный деплой](../../../../flow/devops/vanilla/initial-deploy.md)
- [Обновления и релизы](../../../../flow/devops/vanilla/releases.md)
- [Безопасность и доступы](../../../../flow/devops/vanilla/security.md)
- [Spec и DynamicSpec](../../../../flow/concepts/spec.md)
