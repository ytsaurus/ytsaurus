#include "acl.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NSecurityClient {

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TRowLevelAccessControlEntry* protoRlAce, const TRowLevelAccessControlEntry& rlAce)
{
    protoRlAce->set_expression(rlAce.Expression);
    protoRlAce->set_inapplicable_expression_mode(ToProto(rlAce.InapplicableExpressionMode));
}

void FromProto(TRowLevelAccessControlEntry* rlAce, const NProto::TRowLevelAccessControlEntry& protoRlAce)
{
    rlAce->Expression = protoRlAce.expression();
    rlAce->InapplicableExpressionMode = FromProto<EInapplicableExpressionMode>(protoRlAce.inapplicable_expression_mode());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
