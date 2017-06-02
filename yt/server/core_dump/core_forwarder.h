#pragma once

#include <yt/core/misc/common.h>
#include <yt/core/misc/error.h>

namespace NYT {
namespace NCoreDump {

////////////////////////////////////////////////////////////////////////////////

class TCoreForwarder
{
public:
    TCoreForwarder();
    ~TCoreForwarder();

    // Returns true if an error happened, false otherwise.
    bool Main(const std::vector<TString>& args);

private:
    int ProcessId_ = 0;
    int UserId_ = 0;
    TString ExecutableName_;
    ui64 RLimitCore_ = 0;
    TString JobProxySocketNameDirectory_;
    TString FallbackPath_;

    void ParseArgs(const std::vector<TString>& args);

    void GuardedMain(const std::vector<TString>& args);

    void WriteCoreToDisk();
    void ForwardCore(const TString& SocketName);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCoreDump
} // namespace NYT
