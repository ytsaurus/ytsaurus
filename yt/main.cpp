#include <dict/json/json.h>
#include <util/stream/file.h>
#include <quality/util/prog_options.h>
#include <junk/monster/yt/ytlib/logging/log.h>
#include <junk/monster/yt/ytlib/holder/chunk_holder_server.h>

using namespace NYT;

////////////////////////////////////////////////////////////////////////////////

void StartChunkHolder(ui16 port, Stroka configName) {
    //NLog::Start();

    Cout << "Chunk holder started on port " << port << Endl;

    TIFStream config(configName);
    TJsonReader reader(CODES_UTF8, &config);
    TJsonObject* root = reader.ReadAll();

    TChunkHolderConfig chunkHolderConfig;
    chunkHolderConfig.Read(root);

    TRpcChunkHolderServer server(port, chunkHolderConfig);
    server.Main();
}

void Usage()  {
    Cout << "Chunkholder for YT. Options:" << Endl;
    Cout << "\t-port N      -\t for chunkholder" << Endl;
    Cout << "\t-config file" << Endl;
    Cout << "\t-holder      -\t launch chunkholder" << Endl;
}

int EntryPoint(TProgramOptions& progOptions)
{
    TOptRes cfgOpt = progOptions.GetOption("config");
    Stroka config = cfgOpt.first ? cfgOpt.second : "config.txt";

//    if (progOptions.HasOption("log"))
//        NLog::SetStderrLevel(NLog::L_DEBUG);

    if (progOptions.HasOption("holder")) {
        TOptRes portOpt = progOptions.GetOption("port");
        ui16 port = portOpt.first ? FromString<ui16>(portOpt.second) : 8888;
        StartChunkHolder(port, config);
    } else
        Usage(); 

    return 0;
}

int main(int argc, const char *argv[])
{
    TProgramOptions progOptions("|port|+|config|+|holder|log|");
    return main_with_options_and_catch(progOptions, argc, argv, &EntryPoint, &Usage);
}

////////////////////////////////////////////////////////////////////////////////
