#include "lib.h"

#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/interface/client.h>

#include <util/system/env.h>

////////////////////////////////////////////////////////////////////

template<>
void Out<NYT::TNode>(TOutputStream& s, const NYT::TNode& node)
{
    s << "TNode:" << NodeToYsonString(node);
}

template<>
void Out<TGUID>(TOutputStream& s, const TGUID& guid)
{
    s << GetGuidAsString(guid);
}

////////////////////////////////////////////////////////////////////

namespace NYT {
namespace NTesting {

////////////////////////////////////////////////////////////////////

IClientPtr CreateTestClient()
{
    TString ytProxy = GetEnv("YT_PROXY");
    if (ytProxy.empty()) {
        ythrow yexception() << "YT_PROXY env variable must be set";
    }
    auto client = CreateClient(ytProxy);
    client->Remove("//testing", TRemoveOptions().Recursive(true).Force(true));
    client->Create("//testing", ENodeType::NT_MAP, TCreateOptions());
    return client;
}

////////////////////////////////////////////////////////////////////

TZeroWaitLockPollIntervalGuard::TZeroWaitLockPollIntervalGuard()
    : OldWaitLockPollInterval_(TConfig::Get()->WaitLockPollInterval)
{
    TConfig::Get()->WaitLockPollInterval = TDuration::Zero();
}

TZeroWaitLockPollIntervalGuard::~TZeroWaitLockPollIntervalGuard()
{
    TConfig::Get()->WaitLockPollInterval = OldWaitLockPollInterval_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTesting
} // namespace NYT
