#include <library/cpp/threading/cron/cron.h>

#include <util/stream/output.h>

int main() {
    auto handle = NCron::StartPeriodicJob([] {
        Cerr << "x" << Endl;
    },
                                          TDuration::Seconds(1));

    Sleep(TDuration::Seconds(10));
}
