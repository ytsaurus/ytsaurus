#pragma once

#include <yt/yt/ytlib/security_client/proto/acl.pb.h>

#include <yt/yt/client/security_client/acl.h>
#include <yt/yt/client/security_client/public.h>

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TRowLevelAccessControlEntry* protoRlAce, const TRowLevelAccessControlEntry& rlAce);
void FromProto(TRowLevelAccessControlEntry* rlAce, const NProto::TRowLevelAccessControlEntry& protoRlAce);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
