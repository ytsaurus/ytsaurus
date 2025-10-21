#pragma once

#include <yt/yt/ytlib/security_client/proto/acl.pb.h>

#include <yt/yt/client/security_client/acl.h>
#include <yt/yt/client/security_client/public.h>

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TRowLevelAccessControlEntry* protoRowLevelAce, const TRowLevelAccessControlEntry& rowLevelAce);
void FromProto(TRowLevelAccessControlEntry* rowLevelAce, const NProto::TRowLevelAccessControlEntry& protoRowLevelAce);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
