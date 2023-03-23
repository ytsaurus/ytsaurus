# -*- coding: utf-8 -*-

import yt.wrapper
import yt.yson

import getpass


def mapper(rec):
    # В операциях всё работает так же, как и при чтении таблиц (см. ниже)
    assert not yt.yson.is_unicode(rec["y"])
    assert yt.yson.get_bytes(rec["y"]) == b"\xFF"
    yield {"x": int(rec["x"]) + 1000}


def main():
    client = yt.wrapper.YtClient(proxy="freud")

    path = "//tmp/{}-yson-string-proxy".format(getpass.getuser())
    client.create("map_node", path, ignore_existing=True)

    # Записываем в raw режиме, на вход - список бинарных строк.
    client.write_table(path + "/table", [br'{x=1;y="\xFF"};'], raw=True, format=yt.wrapper.YsonFormat())

    # Читаем в YSON формате по умолчанию, записи раскодируются автоматически.
    # Строки, которые не получилось раскодировать, превращаются в YsonStringProxy, их можно сравнивать
    # и хэшировать как bytes.
    rows = list(client.read_table(path + "/table"))
    assert rows == [{"x": 1, "y": b"\xFF"}]

    # Также можно проверять, смогла ли строка распарситься из указанной кодировки (по умолчанию UTF-8).
    value = rows[0]["y"]
    assert yt.yson.get_bytes(value) == b"\xFF"
    assert not yt.yson.is_unicode(value)

    # Записываем в YSON формате, юникодные строки будут закодированы в utf-8
    client.write_table(path + "/other", [{"y": 1}], format=yt.wrapper.YsonFormat())
    assert list(client.read_table(path + "/other")) == [{"y": 1}]

    # Хотим использовать байтовый ключ в записи с обычными ключами.
    client.write_table(
        path + "/mixed",
        [
            {
                "a": "a",
                "b": "b",
                yt.yson.make_byte_key(b"\xFF"): b"aaa\xFF",
            }
        ],
    )
    assert list(client.read_table(path + "/mixed")) == [{"a": "a", "b": "b", b"\xFF": b"aaa\xFF"}]

    # Хотим работать с чисто бинарными строками в записях, устанавливаем encoding=None.
    client.write_table(path + "/binary", [{b"x": b"aaa\xFF"}], format=yt.wrapper.YsonFormat(encoding=None))
    rows = list(client.read_table(path + "/binary", format=yt.wrapper.YsonFormat(encoding=None)))
    assert rows == [{b"x": b"aaa\xFF"}]

    # В операциях всё работает так же.
    client.run_map(mapper, path + "/table", path + "/output", format=yt.wrapper.YsonFormat())
    assert list(client.read_table(path + "/output")) == [{"x": 1001}]


if __name__ == "__main__":
    main()
