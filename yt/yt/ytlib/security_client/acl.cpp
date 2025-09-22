#include "acl.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NSecurityClient {

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TRowLevelAccessControlEntry* protoRowLevelAce, const TRowLevelAccessControlEntry& rowLevelAce)
{
    protoRowLevelAce->set_expression(rowLevelAce.Expression);
    protoRowLevelAce->set_inapplicable_expression_mode(ToProto(rowLevelAce.InapplicableExpressionMode));
}

void FromProto(TRowLevelAccessControlEntry* rowLevelAce, const NProto::TRowLevelAccessControlEntry& protoRowLevelAce)
{
    rowLevelAce->Expression = protoRowLevelAce.expression();
    rowLevelAce->InapplicableExpressionMode = FromProto<EInapplicableExpressionMode>(protoRowLevelAce.inapplicable_expression_mode());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
