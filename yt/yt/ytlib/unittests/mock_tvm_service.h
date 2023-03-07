#include <yt/ytlib/auth/tvm_service.h>

#include <yt/core/test_framework/framework.h>

namespace NYT::NAuth {

class TMockTvmService
    : public ITvmService
{
public:
    MOCK_METHOD1(GetTicket, TFuture<TString>(const TString&));
};

} // namespace NYT::NAuth
