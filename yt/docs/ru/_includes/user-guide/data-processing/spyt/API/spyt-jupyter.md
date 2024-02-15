# SPYT в Jupyter

##  Подготовка { #prepare }

Перед тем как использовать Spark в Jupyter, необходимо создать [кластер](../../../../../user-guide/data-processing/spyt/cluster/cluster-start.md). Работа со Spark из Jupyter ноутбуков в настоящий момент возможна только с использованием standalone кластера.

При наличии готового кластера для работы с ним необходимо узнать значение `proxy` и `discovery_path`.

##  Настройка Jupyter { #custom }

1. Получите сетевые доступы с машины с Jupyter до SPYT кластера, порты `27000-27200`.
2. Получите сетевые доступы из SPYT кластера до машины с Jupyter, порты `27000-27200`.
3. Поставьте  deb-пакет с java:
    ```bash
    sudo apt-get update
    sudo apt-get install openjdk-11-jdk

    ```
4. Поставьте pip-пакет:

    ```bash
    pip install ytsaurus-spyt

    ```
5. Положите токен для {{product-name}} в `~/.yt/token`:
    ```bash
    mkdir ~/.yt
    cat <<EOT > ~/.yt/token
    $YOUR_YT_TOKEN
    EOT
    ```
6. Положите в домашнюю директорию файл `~/spyt.yaml` с координатами кластера Spark:

    ```bash
    cat <<EOT > ~/spyt.yaml
    yt_proxy: "cluster_name"
    discovery_path: "$YOUR_DISCOVERY_DIR"
    EOT
    ```

## Обновление клиента в Jupyter { #new-client }

Обновите `ytsaurus-spyt` в Jupyter:

```bash
pip install ytsaurus-spyt
```
Если вторая компонента в версии `ytsaurus-spyt` больше, чем в версии вашего кластера, новая функциональность может не работать. Обновите кластер согласно инструкции.




