#pragma once

#include <yt/yql/providers/yt/lib/tvm_client/tvm_client.h>

#include <yql/essentials/core/credentials/yql_credentials.h>

#include <util/generic/ptr.h>

namespace NYql {

class IBlackboxClient : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IBlackboxClient>;

    virtual TString GetUserTicket(const TUserCredentials& credentials) = 0;
};

}; // namespace NYql
