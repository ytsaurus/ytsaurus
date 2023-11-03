# Use UNION instead PACKAGE to avoid artifact duplication; details in DEVTOOLSSUPPORT-15693
UNION()

   
BUNDLE(yt/yt/server/all NAME ytserver-all)
BUNDLE(yt/python/yt/local/bin/yt_local_make NAME yt_local)

PEERDIR(
    yt/python/yt/environment/bin/yt_env_watcher_make
)
    
IF (NOT OPENSOURCE)
    INCLUDE(ya_non_opensource.inc)
ENDIF()

END()
