#include "object_lock.h"

#include <yt/yt/core/yson/pull_parser_deserialize.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/node.h>

namespace NYT::NClickHouseServer {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TObjectLock& lock, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("node_id").Value(lock.NodeId)
            .Item("revision").Value(lock.Revision)
            .OptionalItem("external_transaction_id", lock.ExternalTransactionId ? std::optional(lock.ExternalTransactionId) : std::nullopt)
        .EndMap();
}

void Deserialize(TObjectLock& lock, const INodePtr& node)
{
    if (node->GetType() != ENodeType::Map) {
        THROW_ERROR_EXCEPTION("Error parsing object lock: expected %Qlv node, actual %Qlv node",
            ENodeType::Map,
            node->GetType());
    }

    auto mapNode = node->AsMap();

    lock.NodeId = mapNode->GetChildValueOrThrow<NObjectClient::TObjectId>("node_id");
    lock.Revision = mapNode->GetChildValueOrThrow<NHydra::TRevision>("revision");
    lock.ExternalTransactionId = mapNode->GetChildValueOrDefault<NTransactionClient::TTransactionId>("external_transaction_id", NTransactionClient::NullTransactionId);
}

void Deserialize(TObjectLock& lock, TYsonPullParserCursor* cursor)
{
    Deserialize(lock, ExtractTo<INodePtr>(cursor));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
