UNION()

BUNDLE(
    yt/yt/server/all NAME ytserver-all
    yt/python/yt/environment/bin/yt_env_watcher_make NAME yt_env_watcher

    yt/yt/orm/example/server/bin NAME ytserver-orm-example
)

END()
