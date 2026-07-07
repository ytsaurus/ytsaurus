#include "dummy_blackbox_client.h"

#include <yql/essentials/utils/yql_panic.h>

namespace NYql {

class TDummyBlackboxClient : public IBlackboxClient {
public:
    TString GetUserTicket(const TUserCredentials& /*credentials*/) override {
        YQL_ENSURE(false, "Blackbox client implementation is not present");
        return {};
    }
};

IBlackboxClient::TPtr CreateDummyBlackboxClient() {
    return MakeIntrusive<TDummyBlackboxClient>();
}

}; // namespace NYql
