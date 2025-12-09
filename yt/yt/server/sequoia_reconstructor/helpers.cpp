#include "helpers.h"

namespace NYT::NSequoiaReconstructor {

////////////////////////////////////////////////////////////////////////////////

void TPathToNodeChangeRecord::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
    registrar.Parameter("node_id", &TThis::NodeId);
    registrar.Parameter("transaction_id", &TThis::TransactionId);
    registrar.Parameter("transaction_ancestors", &TThis::TransactionAncestors);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaReconstructor
