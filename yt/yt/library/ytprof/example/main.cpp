#include <yt/yt/core/concurrency/poller.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>
#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/http/server.h>
#include <yt/yt/library/ytprof/http/handler.h>

using namespace NYT;
using namespace NYT::NHttp;
using namespace NYT::NConcurrency;
using namespace NYT::NYTProf;

int main(int argc, char* argv[])
{
    try {
        if (argc != 2 && argc != 3) {
            throw yexception() << "usage: " << argv[0] << " PORT";
        }

        auto port = FromString<int>(argv[1]);
        auto poller = CreateThreadPoolPoller(1, "Example");
        auto server = CreateServer(port, poller);

        Register(server, "");
        server->Start();

        ui64 value = 0;
        while (true) {
            THash<TString> hasher;
            for (int i = 0; i < 10000000; i++) {
                value += hasher(ToString(i));
            }

            if (value == 1) {
                Sleep(TDuration::Seconds(1));
            }
        }
    } catch (const std::exception& ex) {
        Cerr << ex.what() << Endl;
        _exit(1);
    }

    return 0;
}
