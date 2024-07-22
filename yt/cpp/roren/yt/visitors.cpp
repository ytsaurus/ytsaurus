#include "visitors.h"
#include "plain_graph.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

void TraverseInTopologicalOrder(const TYtGraphV2::TPlainGraph& plainGraph, IPlainGraphVisitor* visitor)
{
    THashSet<TYtGraphV2::TTableNode*> visited;
    auto tryVisitTable = [&] (TYtGraphV2::TTableNode* table) {
        auto inserted = visited.insert(table).second;
        if (inserted) {
            visitor->OnTableNode(table);
        }
    };

    for (const auto& operation : plainGraph.Operations) {
        for (const auto& table : operation->InputTables) {
            tryVisitTable(table);
        }
        visitor->OnOperationNode(operation.get());
        for (const auto& [connector, table] : operation->OutputTables) {
            tryVisitTable(table);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
