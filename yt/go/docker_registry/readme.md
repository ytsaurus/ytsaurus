# Docker registry
## Общая схема
yt docker registry выполнен на базе https://github.com/distribution/distribution
Для работы с YT написано несколько плагинов:
- ytdriver - плагин реализует хранение данных в YT.
- ytauth - плагин аутентификации и авторизации.
- ytrepositorymiddleware - плагин для извлечения списка слоев из образа и сохранения этого списка в кипарисе в качестве подсказки для scheduler.

### ytdriver
Реализует протокол хранения данных Docker registry.
Все запросы выполняются от отдельного пользователя, токен которого берется из переменой окружения YT_PROXY, путь для хранения данных выбирается из переменной DOCKER_REGISTRY_YT_HOME. Кластер выбирается из заголовков HTTP запроса.

### ytauth
Плагин выполнен на базе basic auth.
Пользователь выполняет команду ```docker login registry.CLUSTER_NAME.yt.yandex-team.ru```, передает свой логи и YT токен.

Токен проверяется в blackbox на предмет валидности.
Далее проходит авторизация, для этого проверяется, есть ли у пользователя права на запись в определенную map_node.
map_node определяется из названия образа, например для команды:
```
docker pull registry.hume.yt.yandex-team.ru/sys/admin/images/ubuntu:18.04
```
map_node будет //sys/admin/images/ubuntu.
Мы хотим, чтобы документы с подсказками пользователи хранили у себя в домашках,  потому названия образов будут такими /home/nirvana/images/ubuntu.


### ytrepositorymiddleware
Плагин вызывается после ytauth и ytdriver, он создает документ-подсказку для scheduler, в котором указаны слои, входящие в образ.
Список этих слоев используется scheduler для задания layer_paths при старте операции.
Пример такого документа:
```
{
"22.04" = [
"//sys/admin/registry/docker/registry/v2/blobs/sha256/e9/e96e057aae67380a4ddb16c337c5c3669d97fdff69ec…"
];
"20.04" = [
"//sys/admin/registry/docker/registry/v2/blobs/sha256/d7/d7bfe07ed8476565a440c2113cc64d7c0409dba8ef76…"
];
"18.04" = [
"//sys/admin/registry/docker/registry/v2/blobs/sha256/a4/a404e54162968593b8d92b571f3cdd673e4c9eab5d9b…"
]
}
```

### Выбор кластера
Сервис выбирает имя YT кластера их HTTP заголовка HOST.
Заголовок должен быть в формате registry.CLUSTER_NAME.yt.yandex-team.ru.
Кроме того CLUSTER_NAME проверяется в переменной окружения DOCKER_REGISTRY_YT_CLUSTERS.

## Сервис yt_docker_registry_test
https://nanny.yandex-team.ru/ui/#/services/catalog/yt_docker_registry_test/

### Переменные окружения
- REGISTRY_HTTP_SECRET - переменная окружения docker registry, нужна для корректной работы нескольких инстансов и должна быть одинаковой на всех инстансах.
- DOCKER_REGISTRY_YT_HOME - путь для хранения данных registry.
- DOCKER_REGISTRY_YT_CLUSTERS - список поддерживаемых кластеров.
- QLOUD_TVM_TOKEN - tvm токен для работы с blackbox.
- YT_TOKEN - токен пользователя, под которым хранятся данные registry.
- TVMTOOL_CONFIG - конфиг tvmtool.

## l7 балансер
https://nanny.yandex-team.ru/ui/#/awacs/namespaces/list/yt-slb-test.yandex.net/upstreams/list/yt_docker_registry_test/show

## API docker registry
https://github.com/distribution/distribution/blob/main/docs/spec/api.md
