#!/bin/sh

export YT_PROXY=barney.yt.yandex.net

./tabletize.py --input //home/hans/report_Multiproject_Totals_Totals_daily --output //tmp/totals \
    --schema '[{name=project;type=string};
               {name=region_sname;type=string};
               {name=fielddate;type=date};
               {name=visitors_per_host;type=double};
               {name=hosts;type=integer};
               {name=puids;type=integer};
               {name=luids;type=integer};
               {name=visitors;type=integer};
               {name=old_cookie_hits;type=integer};
               {name=hits;type=integer};
               {name=spuids;type=integer};
               {name=hits_per_visitor;type=double};
               {name=old_cookie_visitors;type=integer}]' \
    --key-columns '[project;region_sname;fielddate]' \
    --hash-columns '[project;region_sname]'

./tabletize.py --input //home/hans/report_Multiproject_Totals_Geography_daily --output //tmp/geography \
    --schema '[{name=projectid;type=string};
               {name=geoid;type=string};
               {name=fielddate;type=date};
               {name=hosts;type=integer};
               {name=hosts_all;type=integer};
               {name=old_visitors;type=integer};
               {name=visitors;type=integer};
               {name=visitors_all;type=integer};
               {name=hits;type=integer};
               {name=hits_all;type=integer};
               {name=old_visitors_all;type=integer};
               {name=old_visitors_hits;type=integer};
               {name=old_visitors_hits_all;type=integer}]' \
    --key-columns '[geoid;projectid;fielddate]' \
    --hash-columns '[geoid;projectid]'

