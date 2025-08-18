#include "acl.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NSecurityClient {

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TRowLevelAccessControlEntry* protoRlace, const TRowLevelAccessControlEntry& rlace)
{
    protoRlace->set_expression(rlace.Expression);
    protoRlace->set_inapplicable_expression_mode(ToProto(rlace.InapplicableExpressionMode));
}

void FromProto(TRowLevelAccessControlEntry* rlace, const NProto::TRowLevelAccessControlEntry& protoRlace)
{
    rlace->Expression = protoRlace.expression();
    rlace->InapplicableExpressionMode = FromProto<EInapplicableExpressionMode>(protoRlace.inapplicable_expression_mode());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
