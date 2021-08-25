#include <mapreduce/lib/all.h>
#include <util/system/env.h>

using namespace NMR;

class TFailingMap : public NMR::IMap {
public:
    TFailingMap() = default;

    TFailingMap(int sleepSeconds)
        : SleepSeconds(sleepSeconds)
    { }

    void Start(ui32, ui64, TUpdate&) override
    {
        Sleep(TDuration::Seconds(SleepSeconds));
        _exit(1); // error exit
    }

    void Do(TValue, TValue, TUpdate&) override
    { }

private:
    int SleepSeconds = 0;

private:
    OBJECT_METHODS(TFailingMap);
    SAVELOAD(SleepSeconds);
};

REGISTER_SAVELOAD_CLASS(0x73BC2B28, TFailingMap);

int main(int argc, const char** argv) {
    NMR::Initialize(argc, argv);

    TString ytProxy = GetEnv("YT_PROXY");
    int sleepSeconds = FromString<int>(GetEnv("SLEEP_SECONDS"));
    TString inputTable = GetEnv("INPUT_TABLE");
    TString outputTable = GetEnv("OUTPUT_TABLE");
    if (inputTable.StartsWith("//")) {
        inputTable = inputTable.substr(2);
    }
    if (outputTable.StartsWith("//")) {
        outputTable = outputTable.substr(2);
    }

    TServer server(ytProxy);
    {
        TClient client(server);
        TUpdate update(client, inputTable);
        update.Add("foo", "bar");
    }

    TMRParams params;
    params.AddInputTable(inputTable);
    params.AddOutputTable(outputTable);
    params.WorkingTimeLimit.SetMaxJobFails(1);

    THolder<TFailingMap> testMap(new TFailingMap(sleepSeconds));
    server.Map(params, testMap.Get());
}
