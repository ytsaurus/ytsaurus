#include "credentials.h"

#include <yql/tools/yqlworker/interface/proto/task.pb.h>

namespace NYql {

void ParseCredentials(TStringBuf authData, TCredentials& credentials) {
    NProto::TTaskAuthTokens authTokens;
    if (!authTokens.ParseFromString(authData) || authTokens.TokensSize() == 0) {
        return;
    }

    for (const auto& t : authTokens.GetTokens()) {
        if (t.HasAlias()) {
            credentials.AddCredential(t.GetAlias(), TCredential(t.GetCategory(), t.GetSubcategory(), t.GetContent()));
        }
    }
}

} // namespace NYql
