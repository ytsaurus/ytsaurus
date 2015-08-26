#!/bin/sh

#4.98714 -- 12 -- 0.41559 -- SELECT "PROJECT", "REGION_SNAME",  TO_CHAR( "FIELDDATE", 'YYYY-MM-DD HH24:MI:SS' ) fielddate , "HOSTS", "VISITORS_PER_HOST", "PUIDS", "OLD_COOKIE_HITS", "VISITORS", "LUIDS", "HITS", "SPUIDS", "OLD_COOKIE_VISITORS", "HITS_PER_VISITOR" FROM "STATBOX"."CORE_HC_MULTIPROJE_FIWOTZRUD3" WHERE LOWER("PROJECT") in (LOWER('Search_Blogs')) AND "REGION_SNAME" in ('TOT') AND "FIELDDATE" between TO_DATE('2014-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') and TO_DATE('2014-07-01 23:59:59', 'YYYY-MM-DD HH24:MI:SS') ORDER BY "FIELDDATE"

export YT_PROXY=barney.yt.yandex.net

yt2 select 'project, region_sname, fielddate, hosts, visitors_per_host, puids, old_cookie_hits, visitors, luids, hits, spuids, old_cookie_visitors, hits_per_visitor from [//tmp/totals] WHERE hash=31065622513776491 AND project="Search_Blogs" AND region_sname="TOT"' --format yson
