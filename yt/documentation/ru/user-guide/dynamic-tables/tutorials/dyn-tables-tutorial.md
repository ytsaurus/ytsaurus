{% include [Пример использования](../../../_includes/user-guide/dynamic-tables/tutorials/dyn-tables-tutorial.md) %}

## Примеры запросов { #examples}
Для отправки запросов используется `curl`, для форматирования JSON вывода утилита `jq`.

{% cut "Листинг 22 — Установка утилит" %}

  ```bash
  sudo apt-get install curl
  sudo apt-get install jq
  ```

{% endcut %}

{% cut "Листинг 23 — Создание топиков с несколькими комментариями" %}

  ```bash
  # Первый топик:
  curl - s - X
  POST - d
  'user=abc&content=comment1' 'http://127.0.0.1:5000/post_comment/' | jq.
  {
    "comment_id": 0,
    "new_topic": true,
    "parent_path": "0",
    "topic_id": "d178dfb1-b721a596-4358abc9-ed93ae6b"
  }


  curl - s - X
  POST - d
  'user=abc&content=comment2&topic_id=d178dfb1-b721a596-4358abc9-ed93ae6b&parent_path=0' 'http://127.0.0.1:5000/post_comment/' | jq.
  {
    "comment_id": 1,
    "new_topic": false,
    "parent_path": "0/1"
  }


  curl - s - X
  POST - d
  'user=def&content=comment3&topic_id=d178dfb1-b721a596-4358abc9-ed93ae6b&parent_path=0' 'http://127.0.0.1:5000/post_comment/' | jq.
  {
    "comment_id": 2,
    "new_topic": false,
    "parent_path": "0/2"
  }


  # Второй топик:
  curl - s - X
  POST - d
  'user=def&content=comment4' 'http://127.0.0.1:5000/post_comment/' | jq.
  {
    "comment_id": 0,
    "new_topic": true,
    "parent_path": "0",
    "topic_id": "d9de3eac-fa020dab-4299d3b5-cb5fd5b8"
  }


  curl - s - X
  POST - d
  'user=abc&content=comment5&topic_id=d9de3eac-fa020dab-4299d3b5-cb5fd5b8&parent_path=0' 'http://127.0.0.1:5000/post_comment/' | jq.
  {
    "comment_id": 1,
    "new_topic": false,
    "parent_path": "0/1"
  }
  ```
{% endcut %}

{% cut "Листинг 24 — Редактирование комментария" %}

  ```bash
  # Редактирование второго комментария
  curl - s - X
  POST - d
  'topic_id=d178dfb1-b721a596-4358abc9-ed93ae6b&content=new_comment2&parent_path=0/1' 'http://127.0.0.1:5000/edit_comment/' | jq.
  ```

{% endcut %}

{% cut "Листинг 25 — Удаление комментария" %}

  ```bash
  # Удаление четвертого комментария (корневого во втором топике). Пятый комментарий при этом не удалится.
  curl - s - X
  POST - d
  'topic_id=d9de3eac-fa020dab-4299d3b5-cb5fd5b8&parent_path=0' 'http://127.0.0.1:5000/delete_comment/' | jq.
  ```
{% endcut %}

{% cut "Листинг 26 — Вывод комментариев" %}

  ```bash
  # Вывод двух последних комментариев пользователя abc
  curl - s - H @ headers
  'http://127.0.0.1:5000/user_comments/?user=abc&limit=2' | jq.
  [
    {
      "comment_id": 1,
      "content": "new_comment2",
      "topic_id": "d178dfb1-b721a596-4358abc9-ed93ae6b",
      "update_time": 1564581207,
      "user": "abc",
      "views_count": 0
    },
    {
      "comment_id": 1,
      "content": "comment5",
      "topic_id": "d9de3eac-fa020dab-4299d3b5-cb5fd5b8",
      "update_time": 1564581173,
      "user": "abc",
      "views_count": 0
    }
  ]
  ```

{% endcut %}

{% cut "Листинг 27 — Вывод поддерева комментариев" %}

  ```bash
  # Вывод всех комментариев в первом топике в поддереве второго комментария (куда входит только данный комментарий)
  curl - s - H @ headers
  'http://127.0.0.1:5000/topic_comments/?topic_id=d178dfb1-b721a596-4358abc9-ed93ae6b&parent_path=0/1' | jq.
  [
    {
      "comment_id": 1,
      "content": "new_comment2",
      "creation_time": 1564581113,
      "deleted": false,
      "parent_id": 0,
      "update_time": 1564581207,
      "user": "abc",
      "views_count": 1
    }
  ]
  ```

{% endcut %}

{% cut "Листинг 28 — Вывод списка топиков" %}

  ```bash
  # Вывод всех последних топиков
  curl - s - H @ headers
  'http://127.0.0.1:5000/last_topics/' | jq.
  [
    {
      "content": "comment4",
      "topic_id": "d9de3eac-fa020dab-4299d3b5-cb5fd5b8",
      "update_time": 1564582660,
      "user": "abc",
      "views_count": 0
    },
    {
      "content": "comment1",
      "topic_id": "d178dfb1-b721a596-4358abc9-ed93ae6b",
      "update_time": 1564581207,
      "user": "abc",
      "views_count": 0
    }
  ]
  ```
{% endcut %}

{% cut "Листинг 29 — Примеры неудачных запросов" %}

  ```bash
  # Случай указания неполного набора аргументов: не указан topic_id в запросе к topic_comments
  curl - s - H @ headers
  'http://127.0.0.1:5000/topic_comments/?parent_path=0' | jq.
  {
    "error": "Parameter topic_id must be specified"
  }
  ```

{% endcut %}

{% cut "Листинг 30 — Пример сообщения об ошибке" %}

  ```bash
  # В случае когда невозможно выполнить запрос к системе {{product-name}}, вернётся сообщение об ошибке вида:
  {
    "error": "Received driver response with error\n    Internal RPC call failed\n        Error getting mount info for _home/dev/username/comment_service/user_comments\n            Error communicating with master\n                Error resolving path #f0b5-5c916-3f401a9-dda0ef6f\n                    No such object f0b5-5c916-3f401a9-dda0ef6f\n\n***** Details:\nReceived driver response with error    \n    origin          user_host.domain.com in 2018-09-28T10:33:17.618617Z\nInternal RPC call failed    \n    origin          node001.cluster.domain.com in 2018-09-28T10:33:17.601953Z (pid 745355, tid 4359a68cbe5897dd, fid fffee7436fa6bd03)    \n    service         ApiService    \n    request_id      3dc-764489c-69ebdf66-942f638f    \n    connection_id   7-e996d4e8-7b3a20cb-a9093d41    \n    address         node001.cluster.domain.com:9013    \n    realm_id        0-0-0-0    \n    method          SelectRows\nError getting mount info for _home/dev/username/comment_service/user_comments    \n    origin          node001.cluster.domain.com in 2018-09-28T10:33:17.601569Z (pid 745355, tid 372673d539e8466f, fid fffee7436e8d8609)\nError communicating with master    \n    origin          node001.cluster.domain.com in 2018-09-28T10:33:17.601413Z (pid 745355, tid 372673d539e8466f, fid fffee7436e8d8609)\nError resolving path #f0b5-5c916-3f401a9-dda0ef6f    \n    code            500    \n    origin          m01.cluster.domain.com in 2018-09-28T10:33:17.602199Z (pid 471427, tid e8efa5c24fc65652, fid fffe806472536cdb)    \n    method          GetMountInfo\nNo such object f0b5-5c916-3f401a9-dda0ef6f    \n    code            500    \n    origin          m01.cluster.domain.com in 2018-09-28T10:33:17.602147Z (pid 471427, tid e8efa5c24fc65652, fid fffe806472536cdb)\n"
  }


  # Для того, чтобы вывести ошибку в более читаемом виде, можно заменить "jq ." на "jq -r .error"
  Received driver response with error
      Internal RPC call failed
          Error getting mount info for //home/dev/username/comment_service/user_comments
              Error communicating with master
                  Error resolving path #f0b5-5c916-3f401a9-dda0ef6f
                      No such object f0b5-5c916-3f401a9-dda0ef6f

  ***** Details:
  Received driver response with error
      origin          user_host.domain.com in 2018-09-28T10:33:31.449413Z
  Internal RPC call failed
      origin          node001.cluster.domain.com in 2018-09-28T10:33:31.434137Z (pid 745355, tid 4359a68cbe5897dd, fid fffee743122257c3)
      service         ApiService
      request_id      3df-add3dc38-3fd547ae-d9ab851f
      connection_id   7-e996d4e8-7b3a20cb-a9093d41
      address         node001.cluster.domain.com:9013
      realm_id        0-0-0-0
      method          SelectRows
  Error getting mount info for //home/dev/username/comment_service/user_comments
      origin          node001.cluster.domain.com in 2018-09-28T10:33:17.601569Z (pid 745355, tid 372673d539e8466f, fid fffee7436e8d8609)
  Error communicating with master
      origin          node001.cluster.domain.com in 2018-09-28T10:33:17.601413Z (pid 745355, tid 372673d539e8466f, fid fffee7436e8d8609)
  Error resolving path #f0b5-5c916-3f401a9-dda0ef6f
      code            500
      origin          m01.cluster.domain.com in 2018-09-28T10:33:17.602199Z (pid 471427, tid e8efa5c24fc65652, fid fffe806472536cdb)
      method          GetMountInfo
  No such object f0b5-5c916-3f401a9-dda0ef6f
      code            500
      origin          m01.cluster.domain.com in 2018-09-28T10:33:17.602147Z (pid 471427, tid e8efa5c24fc65652, fid fffe806472536cdb)
  ```

{% endcut %}
