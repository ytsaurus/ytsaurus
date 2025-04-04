#include <contrib/ydb/library/aclib/aclib.h>
#include <contrib/ydb/library/login/login.h>
#include <contrib/ydb/library/login/protos/login.pb.h>

#include "local_user_token.h"

namespace NKikimr {

NACLib::TUserToken BuildLocalUserToken(const NLogin::TLoginProvider& loginProvider, const TString& user) {
    const auto providerGroups = loginProvider.GetGroupsMembership(user);
    const TVector<NACLib::TSID> groups(providerGroups.begin(), providerGroups.end());
    //NOTE: TVector vs std::vector incompatibility between TUserToken and TLoginProvider
    return NACLib::TUserToken(user, groups);
}

NACLib::TUserToken BuildLocalUserToken(const NLoginProto::TSecurityState& state, const TString& user) {
    NLogin::TLoginProvider loginProvider;
    loginProvider.UpdateSecurityState(state);
    return BuildLocalUserToken(loginProvider, user);
}

}
