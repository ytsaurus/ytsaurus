Запуск операций на кластре через Python SDK может быть затруднителен, т.к. на кластер доставляется код вместе с зависимостями среды запуска скрипта (локальной машаины). И если расхождения в версии/типу OS, Python значительны код не запустится.

Один из подходов решения этой проблемы - обеспечить идентичность OS/Python в точке запуска (локальная машина) и в точке исполнения (на кластере) запуская локальный код внутри Docker контейнера с правильным окружением.
Этот подход позволяет запускать скрипты с разных OS, не требует поддержки cri на кластере и не требует пушить docker образы в какой-либо registry.

Для этого:


#### 0. Настроим Docker

- Для ARM платформ (mac m1/m2) нужно запустить Docker в x86 режиме
```
# установите Rozetta для поддержки x86
softwareupdate --install-rosetta --agree-to-license

# если используется Colima - ее нужно перезапустить:
colima stop
colima start --arch x86_64 --vm-type=vz --vz-rosetta
```

- Если ваш кластер исползьуем ipv6 адреса необходимо настроить их роутинг из контейнера
см. https://docs.docker.com/engine/daemon/ipv6/
NB: Colima не поддерживает ipv6.


#### 1. Создаем Docker образ, соотвествующий настройкам кластера:

```(bash)
# Соберем docker образ для целевого кластера (OS и версия Python в образе будут идентичны кластеру)
yt devtools image prepare --proxy <my_cluster.fqdn> --with-modules ytsaurus-client ytsaurus-yson
```

Если версия pyhon/OS на кластере слишком старая и мешает установке нужных зависимостей, можно указать нужную версию (требует поддержки cri на кластере)
```(bash)
# Узнаем версию OS/Python на кластере
yt devtools image get-cluster-env --proxy <my_cluster.fqdn>
# Соберем Docker образ с нужной OS (к примеру Ubuntu trusty) и Python
yt devtools image prepare --proxy <my_cluster.fqdn> --base-image docker.io/library/ubuntu:trusty --python 3.13
```

Важно указывать полностью одинаковый `proxy` для всех команд, из него неявно выводится результирующее имя образа (`cluster_env_<current_user>_<cluster_name>:latest`).
Хотя это поведение всегда можно переопределить параметром `--image-name <my_specific_image_name>`

Список всех образов можно посмотреть командой
```(bash)
yt devtools image list
```


#### 2. Доустанавливаем пакеты в образ

В образ, в любой момент, можно доустановить недостающие пакеты.

```(bash)
# установить pip пакеты
yt devtools image install --proxy <my_cluster.fqdn> --pip <package_1> <package_2>
# или выполнить произвольную команду
yt devtools image install --proxy <my_cluster.fqdn> --bash '<my_command_with_spaces>'
```

#### 3. Запускаем скрипт

Если необходимо, устанавливаем переменные окружения
```
export YT_TOKEN=...
export YT_TOKEN_PATH=...
export YT_PROXY=my_cluster.fqdn
```

И запускаем скрипт
```
yt devtools image run --proxy <my_cluster.fqdn> <path_to_my_script.py>

# или вручную:
# docker run --platform linux/amd64 --rm -e USER=$USER -e YT_TOKEN_PATH="${YT_TOKEN_PATH:-$HOME/.yt/token}" -e YT_TOKEN="$YT_TOKEN" -e YT_PROXY="$YT_PROXY" -v $HOME:$HOME -it my_cluster_image:latest bash -ic "python $PWD/my_script.py"
```

