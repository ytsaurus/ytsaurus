YTMonitor = window.YTMonitor || {};

YTMonitor.config = {
    requestsTimeout: 30,
    clusters: [
        {
            name: "Тестовый",
            proxy: "n01-0400g.yt.yandex.net:80",
            masters: [
                "meta01-001g.yt.yandex.net:10010",
                "meta01-002g.yt.yandex.net:10010",
                "meta01-003g.yt.yandex.net:10010"
            ]
        },
        {
            name: "Разработческий",
            proxy: "n01-0400g.yt.yandex.net:81",
            masters: [
                "meta01-001g.yt.yandex.net:10000",
                "meta01-002g.yt.yandex.net:10000",
                "meta01-003g.yt.yandex.net:10000"
            ]
        }
   ]
}
