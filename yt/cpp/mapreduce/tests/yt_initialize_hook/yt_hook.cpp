#include <library/cpp/testing/hook/hook.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/config.h>

Y_TEST_HOOK_BEFORE_INIT(YtInitialize)
{
    NYT::TConfig::Get()->LogLevel = "debug";
    NYT::Initialize();
}
