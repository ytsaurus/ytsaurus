#include "transaction_manager.h"

#include <yt/core/misc/serialize.h>

namespace NYT {
namespace NHiveServer {

////////////////////////////////////////////////////////////////////////////////

void TTransactionActionData::Persist(TStreamPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Type);
    Persist(context, Value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveServer
} // namespace NYT
