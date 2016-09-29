#pragma once

#include <yt/core/misc/common.h>

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
    int ProcessId_ = -1;
    int UserId_ = -1;
    Stroka ExecutableName_;
    i64 RlimitCore_ = -1;
    Stroka JobProxyUdsNameDirectory_;
    Stroka FallbackPath_;

    void ParseArgs(const std::vector<Stroka>& args);

    void GuardedMain(const std::vector<Stroka>& args);

    void WriteCoreToDisk();
    void ForwardCore(const Stroka& UdsName);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCoreDump
} // namespace NYT
