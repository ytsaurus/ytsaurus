# Автодополнение { #autocompletion }

В комплекте с Deb-пакетом также поставляется скрипт для bash autocompletion, который при установке автоматически добавляется в `/etc/bash_completion.d/`.

В случае pip-пакета включить autocompletion можно с помощью утилиты `register-python-argcomplete`, которая поставляется вместе с пакетом argcomplete.
```
register-python-argcomplete yt | sudo tee /etc/bash_completion.d/yt >/dev/null
```

Проверить работоспособность автодополнения можно набрав в консоли:

```bash
yt <TAB><TAB>
abort-job
abort-op
abort-tx
add-member
alter-table
alter-table-replica
check-permission
...
```

Главным преимуществом этой функции является возможность дополнять пути в Кипарисе. Для работы этой функции необходимо, чтобы в переменных окружения был указан кластер, с которым вы работаете, например, посредством команды `export YT_PROXY=<cluster_name>`. Пример:

```bash
export YT_PROXY=<cluster-name>
yt list /<TAB>/<TAB><TAB>
//@               //home/           //porto_layers/   //statbox/        //test_q_roc_auc  //trash           //userfeat/       //userstats/
//cooked_logs/    //logs            //projects/       //sys             //tmp/            //userdata/       //user_sessions/
```

Если вместо названий команд или путей Кипариса вы видите что-то иное (либо ничего, либо пути в текущей директории), значит autocompletion не включился. Проверьте выполнение следующих условий:

- Должен быть установлен основной пакет bash_completion. Под Ubuntu-подобными системами этого можно достичь посредством запуска команды `sudo apt-get install bash-completion`;
- bash-completion должен инициироваться при запуске bash-сессии. Например, в Ubuntu по умолчанию в .bashrc есть строка `. /etc/bash_completion`, если у вас такой не видно, то стоит ее прописать себе в .bashrc;
- В /etc/bash_completion.d должен попасть файл `yt_completion`, настраивающий автодополнение для команды yt. Если под вашей системой всё содержимое /etc/bash_completion.d по каким-то причинам не запускается автоматически, можно самостоятельно запустить этот скрипт в конце вашего .bashrc;

