~/ytbuild/bin/rpc_proxy_sample --config config.yson --user <user> --token $(cat ~/.yt/token) 2> /dev/null

list //sys

get //sys

select timestamp, host, rack, utc_time, data FROM [//home/dev/andozer/autorestart_nodes_copy] LIMIT 10

ulookup //home/dev/andozer/autorestart_nodes_copy timestamp;host;rack;utc_time;data <id=0>1486113922563016;<id=1>"s04-sas.hahn.yt.yandex.net";<id=2>"SAS2.4.3-13" <id=0>1486113924172063;<id=1>"s04-sas.hahn.yt.yandex.net";<id=2>"SAS2.4.3-13" <id=0>1486113992045484;<id=1>"s04-sas.hahn.yt.yandex.net";<id=2>"SAS2.4.3-13" <id=0>1486113992591731;<id=1>"s04-sas.hahn.yt.yandex.net";<id=2>"SAS2.4.3-13" <id=0>1486113997734536;<id=1>"n4137-sas.hahn.yt.yandex.net";<id=2>"SAS2.4.3-13"

upsert //home/dev/andozer/autorestart_nodes_copy timestamp;host;rack;utc_time;data <id=0>123;<id=1>"host123";<id=2>"rack123";<id=3>"utc_time1";<id=4>"data1" <id=0>567;<id=1>"host567";<id=2>"rack567";<id=3>"utc_time2";<id=4>"data2"

delete //home/dev/andozer/autorestart_nodes_copy timestamp;host;rack <id=0>123;<id=1>"host123";<id=2>"rack123"

