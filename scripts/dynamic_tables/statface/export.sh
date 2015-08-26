#!/bin/sh
#"//home/hans/report_Multiproject_Totals_Geography_daily" "//home/hans/report_Multiproject_Totals_Totals_daily"
for table in "//home/hans/report_Search_Mobile_All_Adhoc_AudienceMobileWebShort_daily"; do  
    YT_PROXY=kant.yt.yandex.net export_to_yt.py --src "$table" --dst "$table"  --yt-proxy barney.yt.yandex.net --yt-token 0a8f523f14c3318bd3df7bd79af527fb
done
