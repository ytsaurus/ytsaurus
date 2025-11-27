#include "acl.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NSecurityClient {

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TRowLevelAccessControlEntry* protoRowLevelAce, const TRowLevelAccessControlEntry& rowLevelAce)
{
    protoRowLevelAce->set_row_access_predicate(rowLevelAce.RowAccessPredicate);
    protoRowLevelAce->set_inapplicable_row_access_predicate_mode(ToProto(rowLevelAce.InapplicableRowAccessPredicateMode));
}

void FromProto(TRowLevelAccessControlEntry* rowLevelAce, const NProto::TRowLevelAccessControlEntry& protoRowLevelAce)
{
    rowLevelAce->RowAccessPredicate = protoRowLevelAce.row_access_predicate();
    rowLevelAce->InapplicableRowAccessPredicateMode = FromProto<EInapplicableRowAccessPredicateMode>(protoRowLevelAce.inapplicable_row_access_predicate_mode());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
