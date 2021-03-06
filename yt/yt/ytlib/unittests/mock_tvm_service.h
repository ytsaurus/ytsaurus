#include <yt/yt/ytlib/auth/tvm_service.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NAuth {

class TMockTvmService
    : public ITvmService
{
public:
    MOCK_METHOD1(GetServiceTicket, TString(const TString&));
    MOCK_METHOD1(ParseUserTicket, TParsedTicket(const TString&));
};

} // namespace NYT::NAuth
