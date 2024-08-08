# Анатомия запроса до и после движка ClickHouse

В данной статье описано, как устроен запрос в CHYT и откуда можно получить информацию об исполнении запроса.

## Идентификаторы запроса: `trace id`, `query id`, `datalens request id` { #id }

С любым запросом связаны несколько идентификаторов, которые позволяют постфактум восстановить всю необходимую информацию об исполнении запроса.

- `query id` — это идентификатор, назначаемый каждому запросу в ClickHouse. В отличие от оригинального ClickHouse, в CHYT этот идентификатор не контролируется пользователем и всегда имеет вид *{{product-name}} GUID* – четыре шестнадцатеричных uint32, разделённых дефисами.
- `trace id` — это идентификатор, который позволяет провязывать цепочки вызовов в распределённых системах, создавая «след» исполнения запроса. Это часть протокола трассировки [opentracing](https://opentracing.io/), а конкретно — его реализации под названием [Jaeger Tracing](https://www.jaegertracing.io/), использующейся в {{product-name}}. `trace id` также является {{product-name}} GUID, который в некоторых ситуациях совпадает с `query id`, а в некоторых не совпадает, о чём подробно будет написано ниже.

## Путь запроса от клиента до тяжёлых прокси { #way-to-proxy }

Чтобы лучше понимать, по какой логике существуют и назначаются эти идентификаторы, следует разобраться в том, через какие компоненты в каком порядке проходит запрос в CHYT. Единственным публичным API доступа к CHYT на текущий момент является HTTP API, поэтому дальнейшее описание относится именно к протоколу HTTP.

Первым важным пунктом на пути исполнения запроса являются так называемые _тяжёлые прокси_ {{product-name}}, сетевой доступ к которым есть у всех пользователей {{product-name}}. Тяжёлые прокси узнают, где сейчас живут инстансы клики с заданным алиасом, а затем отправляют запрос на случайно выбранный инстанс.

Ниже показаны три самых популярных способа доступа к {{product-name}}.

![](../../../../../../images/chyt_before_clique.png){ .center }

При обращении к CHYT напрямую из скрипта либо из командной строки посредством утилиты cURL в качестве endpoint используется SLB-балансер, например `http://$YT_PROXY`. За ним скрывается сложная конструкция из балансеров, которая направляет запрос на так называемые *контрольные прокси*, которые отвечают HTTP-редиректом на тяжёлые прокси, обслуживающие всю тяжёлую нагрузку в {{product-name}}. При таком интерфейсе доступа `query id` совпадает с `trace id`: их можно увидеть в заголовках `X-Yt-Trace-Id` и `X-ClickHouse-Query-Id`.

Ниже показан пример взаимодействия с CHYT через утилиту cURL, в котором отмечены наиболее интересные заголовки ответа.

{% note info "Примечание" %}

В версиях YT 24.1 и ранее алиас клики указывается со звёздочкой в начале, например, `*ch_public`. В текущей версии `*ch_public` и `ch_public` соответствуют одному алиасу.

При обращении к CHYT по устаревшему пути `/query` алиас передаётся через url-параметр `database`. При обращении по актуальному пути `/chyt` алиас берётся из `user` или из url-параметра `chyt.clique_alias`.

{% endnote %}

```bash
$ curl -v --location-trusted 'http://$YT_PROXY/chyt?chyt.clique_alias=ch_public' -d 'select max(a) from "//sys/clickhouse/sample_table"' -H "Authorization: OAuth `cat ~/.yt/token`"
*   Trying ip_address:80...
* Connected to $YT_PROXY (ip_address) port 80 (#0)
> POST /chyt?chyt.clique_alias=ch_public HTTP/1.1
> Host: $YT_PROXY
> User-Agent: curl/7.69.1-DEV
> Accept: */*
> Authorization: OAuth <i>...<my_token>...</i>
> Content-Length: 50
> Content-Type: application/x-www-form-urlencoded
>
* upload completely sent off: 50 out of 50 bytes
* Mark bundle as not supporting multiuse
<b> // Получаем redirect на тяжелую прокси.</b>
< HTTP/1.1 307 Temporary Redirect
< Content-Length: 0
< Location: http://sas4-9923-proxy-$YT_PROXY/chyt?chyt.clique_alias=ch_public
< X-Yt-Trace-Id: 8e9bcc43-5c2be9b4-56f18c4e-117ea314
<
* Connection #0 to host $YT_PROXY left intact
* Issue another request to this URL: 'http://sas4-9923-$YT_PROXY/chyt?chyt.clique_alias=ch_public'
*   Trying ip_address:80...
* Connected to sas4-9923-$YT_PROXY (ip_address) port 80 (#1)
> POST /chyt?chyt.clique_alias=ch_public HTTP/1.1
> Host: sas4-9923-$YT_PROXY
> User-Agent: curl/7.69.1-DEV
> Accept: */*
> Authorization: OAuth <i>...<my_token>...</i>
> Content-Length: 50
> Content-Type: application/x-www-form-urlencoded
>
* upload completely sent off: 50 out of 50 bytes
* Mark bundle as not supporting multiuse
< HTTP/1.1 200 OK
< Transfer-Encoding: chunked
<b>  // Обратите внимание, query id = trace id. </b>
< <b>X-ClickHouse-Query-Id:</b> 3fa9405e-15b29877-524e3e67-2be50e94
< <b>X-Yt-Trace-Id:</b> 3fa9405e-15b29877-524e3e67-2be50e94
<b>  // По техническим причинам X-Yt-Trace-Id встретится дважды.</b>
< X-Yt-Trace-Id: 3fa9405e-15b29877-524e3e67-2be50e94
< Keep-Alive: timeout=10
<b>  // Адрес инстанса-координатора запроса. </b>
< <b>X-ClickHouse-Server-Display-Name:</b> sas2-1374-node-$YT_PROXY
< X-Yt-Request-Id: 3fa9405d-26285349-db14531a-2a12b9f9
< Date: Sun, 05 Apr 2020 18:49:57 GMT
< Content-Type: text/tab-separated-values; charset=UTF-8
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
<b>  // Адрес тяжелой прокси, которая обслуживала запроса. </b>
< <b>X-Yt-Proxy:</b> sas4-9923-$YT_PROXY
<
1100
* Connection #1 to host sas4-9923-$YT_PROXY left intact
```
