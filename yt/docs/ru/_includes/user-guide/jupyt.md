---
title: Работа с Jupyter Notebooks | {{product-name}}
---

# Работа с Jupyter Notebooks

{{product-name}} позволяет запустить Jupyter Notebooks на вычислительных мощностях кластера. Функциональность доступна только для кластеров, использующих CRI job environment.

Для развёртывания ноутбука необходимо выполнить следующие действия.

1. Установить переменную окружения `JUPYT_CTL_ADDRESS` в адрес strawberry controller для JUPYT.
```bash
export JUPYT_CTL_ADDRESS=jupyt.test.yt.mycloud.net
```

1. Выбрать docker-образ jupyter-ноутбука. В качестве образа или основы для образа можно выбрать [jupyter stacks](https://jupyter-docker-stacks.readthedocs.io/en/latest/), например, [minimal-notebook](quay.io/jupyter/minimal-notebook).

1. Создать ноутбук. Для создания ноутбука необходимо указать docker-образ с ноутбуком и пул, в котором будет запущена операция.
```bash
yt jupyt ctl create --speclet-options '{jupyter_docker_image="quay.io/jupyter/minimal-notebook"; pool=my-pool}' test-jupyt
```

1. Запустить ноутбук.
```bash
yt jupyt ctl start test-jupyt
```

1. Дождаться запуска ноутбука и получить ссылку на ноутбук.
```bash
foo@bar:~$ yt jupyt ctl get-endpoint test-jupyt
{
    "address" = "http://some-node.yt.mycloud.net:27042";
    "operation_id" = "60b8ec86-6f123a7d-134403e8-ee6b88de";
    "job_id" = "16f76ac9-2a20ab96-13440384-14e";
}
```

{% note warning "Внимание" %}

Содержимое ноутбуков и файлы, созданные на файловой системе теряются при перезапуске джоба с ноутбуком. Для надёжного хранения файлов необходимо использовать Кипарис.

{% endnote %}
