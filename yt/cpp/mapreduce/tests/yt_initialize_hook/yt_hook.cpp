#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/config.h>

void InitializeYt(int argc, char** argv)
{
    NYT::TConfig::Get()->LogLevel = "debug";
    NYT::Initialize(argc, argv);
}
