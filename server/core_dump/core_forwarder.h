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
    bool Main(const std::vector<Stroka>& args);

private:
    int ProcessId_ = 0;
    int UserId_ = 0;
    Stroka ExecutableName_;
    ui64 RLimitCore_ = 0;
    Stroka JobProxySocketNameDirectory_;
    Stroka FallbackPath_;

    void ParseArgs(const std::vector<Stroka>& args);

    void GuardedMain(const std::vector<Stroka>& args);

    void WriteCoreToDisk();
    void ForwardCore(const Stroka& SocketName);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCoreDump
} // namespace NYT
