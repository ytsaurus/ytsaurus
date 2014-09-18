#!/bin/sh

export YT_PROXY=barney.yt.yandex.net

./tabletize.py --input //home/hans/report_Multiproject_Totals_Totals_daily --output //tmp/totals \
    --schema '[{name=project;type=string};
               {name=region_sname;type=string};
               {name=fielddate;type=date};
               {name=visitors_per_host;type=double};
               {name=hosts;type=int64};
               {name=puids;type=int64};
               {name=luids;type=int64};
               {name=visitors;type=int64};
               {name=old_cookie_hits;type=int64};
               {name=hits;type=int64};
               {name=spuids;type=int64};
               {name=hits_per_visitor;type=double};
               {name=old_cookie_visitors;type=int64}]' \
    --key-columns '[project;region_sname;fielddate]' \
    --hash-columns '[project;region_sname]'

./tabletize.py --input //home/hans/report_Multiproject_Totals_Geography_daily --output //tmp/geography \
    --schema '[{name=projectid;type=string};
               {name=geoid;type=string};
               {name=fielddate;type=date};
               {name=hosts;type=int64};
               {name=hosts_all;type=int64};
               {name=old_visitors;type=int64};
               {name=visitors;type=int64};
               {name=visitors_all;type=int64};
               {name=hits;type=int64};
               {name=hits_all;type=int64};
               {name=old_visitors_all;type=int64};
               {name=old_visitors_hits;type=int64};
               {name=old_visitors_hits_all;type=int64};
               {name=geoid__lvl;type=int64}]' \
    --key-columns '[projectid;geoid;fielddate]'

./tabletize.py --input //home/hans/report_Search_Mobile_All_Adhoc_AudienceMobileWebShort_daily --output //tmp/mobile_web \
    --schema '[{name=old_cookie;type=string};
               {name=platform_type;type=string};
               {name=fielddate;type=date};
               {name=searcher;type=string};
               {name=measure;type=string};
               {name=region_sname;type=string};
               {name=platform_type__lvl;type=string}]' \
    --key-columns '[old_cookie;platform_type;searcher;fielddate]' \
    --hash-columns '[old_cookie;platform_type;searcher]'

