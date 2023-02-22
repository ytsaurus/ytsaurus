#include <library/cpp/testing/hook/hook.h>

#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/interface/config.h>

Y_TEST_HOOK_BEFORE_INIT(YtInitialize)
{
    NYT::TConfig::Get()->LogLevel = "debug";
    NYT::Initialize();
}
