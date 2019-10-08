YP ysons
============

Go библиотечка с YSON представлением протобафов YP. В качестве источника используются оригинальные .proto YP-мастера: [yp/client/api/proto](https://a.yandex-team.ru/arc/trunk/arcadia/yp/client/api/proto).

Все это добро работает за счет кастомного плагина к protoc, который генерит YSON-структурки из .proto: [internal/proto-yson-gen](internal/proto-yson-gen)
