#pragma once

#include <yt/yt/ytlib/security_client/proto/acl.pb.h>

#include <yt/yt/client/security_client/acl.h>
#include <yt/yt/client/security_client/public.h>

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TRowLevelAccessControlEntry* protoRlace, const TRowLevelAccessControlEntry& rlace);
void FromProto(TRowLevelAccessControlEntry* rlace, const NProto::TRowLevelAccessControlEntry& protoRlace);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
