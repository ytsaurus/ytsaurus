#include <mapreduce/yt/interface/client.h>

#include <util/system/env.h>

namespace NYT {
namespace NTesting {

IClientPtr CreateTestClient()
{
    Stroka ytProxy = GetEnv("YT_PROXY");
    if (ytProxy.empty()) {
        ythrow yexception() << "YT_PROXY env variable must be set";
    }
    auto client = CreateClient(ytProxy);
    client->Remove("//testing", TRemoveOptions().Recursive(true).Force(true));
    client->Create("//testing", ENodeType::NT_MAP, TCreateOptions());
    return client;
}

} // namespace NTesting
} // namespace NYT
