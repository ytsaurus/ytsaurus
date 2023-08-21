# Создадим топиков с несколькими комментариями
# Первый топик:
$ curl -s -X POST -d 'user=abc&content=comment1' 'http://127.0.0.1:5000/post_comment/' | jq .
{
  "comment_id": 0,
  "new_topic": true,
  "parent_path": "0",
  "topic_id": "d178dfb1-b721a596-4358abc9-ed93ae6b"
}


$ curl -s -X POST -d 'user=abc&content=comment2&topic_id=d178dfb1-b721a596-4358abc9-ed93ae6b&parent_path=0' 'http://127.0.0.1:5000/post_comment/' | jq .
{
  "comment_id": 1,
  "new_topic": false,
  "parent_path": "0/1"
}


$ curl -s -X POST -d 'user=def&content=comment3&topic_id=d178dfb1-b721a596-4358abc9-ed93ae6b&parent_path=0' 'http://127.0.0.1:5000/post_comment/' | jq .
{
  "comment_id": 2,
  "new_topic": false,
  "parent_path": "0/2"
}


# Второй топик:
$ curl -s -X POST -d 'user=def&content=comment4' 'http://127.0.0.1:5000/post_comment/' | jq .
{
  "comment_id": 0,
  "new_topic": true,
  "parent_path": "0",
  "topic_id": "d9de3eac-fa020dab-4299d3b5-cb5fd5b8"
}


$ curl -s -X POST -d 'user=abc&content=comment5&topic_id=d9de3eac-fa020dab-4299d3b5-cb5fd5b8&parent_path=0' 'http://127.0.0.1:5000/post_comment/' | jq .
{
  "comment_id": 1,
  "new_topic": false,
  "parent_path": "0/1"
}


# Редактирование второго комментария
$ curl -s -X POST -d 'topic_id=d178dfb1-b721a596-4358abc9-ed93ae6b&content=new_comment2&parent_path=0/1' 'http://127.0.0.1:5000/edit_comment/' | jq .


# Удаление четвертого комментария (корневого во втором топике). Пятый комментарий при этом не удалится.
$ curl -s -X POST -d 'topic_id=d9de3eac-fa020dab-4299d3b5-cb5fd5b8&parent_path=0' 'http://127.0.0.1:5000/delete_comment/' | jq .


# Вывод двух последних комментариев пользователя abc
$ curl -s -H @headers 'http://127.0.0.1:5000/user_comments/?user=abc&limit=2' | jq .
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


# Вывод всех комментариев в первом топике в поддереве второго комментария (куда входит только данный комментарий)
$ curl -s -H @headers 'http://127.0.0.1:5000/topic_comments/?topic_id=d178dfb1-b721a596-4358abc9-ed93ae6b&parent_path=0/1' | jq .
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


# Вывод всех последних топиков
$ curl -s -H @headers 'http://127.0.0.1:5000/last_topics/' | jq .
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


# Случай указания неполного набора аргументов: не указан topic_id в запросе к topic_comments
$ curl -s -H @headers 'http://127.0.0.1:5000/topic_comments/?parent_path=0' | jq .
{
  "error": "Parameter topic_id must be specified"
}


# В случае когда невозможно выполнить запрос к системе YT, вернётся сообщение об ошибке вида:
{
  "error": "Received driver response with error\n    Internal RPC call failed\n        Error getting mount info for //home/dev/ivanashevi/comment_service/topic_comments\n            Error communicating with master\n                Error resolving path #f038-5561f-3f401a9-97a4933d\n                    No such object f038-5561f-3f401a9-97a4933d\n\n***** Details:\nReceived driver response with error    \n    origin          ivanashevi-l.yandex.net in 2018-09-26T12:50:16.213122Z\nInternal RPC call failed    \n    origin          n0671i.freud.yt.yandex.net in 2018-09-26T12:50:16.195609Z (pid 745355, tid 372673d539e8466f, fid fffef960aa514ef3)    \n    timeout         15000    \n    service         ApiService    \n    request_id      11-111ac99-d9094eb7-ac87ba7b    \n    connection_id   7-fa547bd-8b128a46-e9327f2c    \n    address         n0671i.freud.yt.yandex.net:9013    \n    realm_id        0-0-0-0    \n    method          GetTableMountInfo\nError getting mount info for //home/dev/ivanashevi/comment_service/topic_comments    \n    origin          n0671i.freud.yt.yandex.net in 2018-09-26T12:50:16.195496Z (pid 745355, tid 372673d539e8466f, fid fffef960aa514ef3)\nError communicating with master    \n    origin          n0671i.freud.yt.yandex.net in 2018-09-26T12:50:16.195340Z (pid 745355, tid 372673d539e8466f, fid fffef960aa514ef3)\nError resolving path #f038-5561f-3f401a9-97a4933d    \n    code            500    \n    origin          m12i.freud.yt.yandex.net in 2018-09-26T12:50:16.194079Z (pid 559431, tid 3c0975eeaf9db7de, fid fffd7720ee0fb080)    \n    method          GetMountInfo\nNo such object f038-5561f-3f401a9-97a4933d    \n    code            500    \n    origin          m12i.freud.yt.yandex.net in 2018-09-26T12:50:16.194029Z (pid 559431, tid 3c0975eeaf9db7de, fid fffd7720ee0fb080)\n"
}
# Для того, чтобы вывести ошибку в более читаемом виде, можно заменить "jq ." на "jq -r .error"
