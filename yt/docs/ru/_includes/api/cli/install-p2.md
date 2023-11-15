## Автодополнение { #autocompletion }

{% if audience == "internal" %}
В комплекте с Deb-пакетом также поставляется скрипт для bash autocompletion, который при установке автоматически добавляется в `/etc/bash_completion.d/`.
{% endif %}

В случае pip-пакета включить autocompletion можно с помощью утилиты `register-python-argcomplete`, которая поставляется вместе с пакетом argcomplete.
```
register-python-argcomplete yt | sudo tee /etc/bash_completion.d/yt >/dev/null
```

Проверить работоспособность автодополнения можно, набрав в консоли:

```bash
$ yt <TAB><TAB>
abort-job
abort-op
abort-tx
add-member
alter-table
alter-table-replica
check-permission
...
```

Автодополнение также умеет работать с путями в Кипарисе. Для этого необходимо, чтобы в переменных окружения был указан кластер, с которым вы работаете. Указать его можно командой `$ export YT_PROXY=<cluster_name>`. Пример:

```bash
$ export YT_PROXY=<cluster-name>
$ yt list /<TAB>/<TAB><TAB>
//@               //home/           //porto_layers/   //statbox/        //test_q_roc_auc  //trash           //userfeat/       //userstats/
//cooked_logs/    //logs            //projects/       //sys             //tmp/            //userdata/       //user_sessions/
```

Если вместо названий команд или путей Кипариса вы видите что-то иное (либо ничего, либо пути в текущей директории), значит, autocompletion не включился. Проверьте выполнение следующих условий:

- Должен быть установлен основной пакет bash_completion. Под Ubuntu-подобными системами этого можно достичь посредством запуска команды `$ sudo apt-get install bash-completion`.
- bash-completion должен инициироваться при запуске bash-сессии. Например, в Ubuntu по умолчанию в .bashrc есть строка `. /etc/bash_completion`, если у вас такой не видно, то стоит ее прописать себе в .bashrc.
{% if audience == "internal" %}
- Если вы устанавливали deb-пакет, то в `/etc/bash_completion.d` должен попасть файл `yt_completion`, настраивающий автодополнение для команды yt. Если под вашей системой все содержимое `/etc/bash_completion.d` по каким-то причинам не запускается автоматически, можно самостоятельно запустить этот скрипт в конце вашего .bashrc.{% endif %}

