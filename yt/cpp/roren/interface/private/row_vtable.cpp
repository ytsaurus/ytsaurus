#include "row_vtable.h"

#include <util/ysaveload.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

IRawCoderPtr CrashingCoderFactory()
{
    Y_ABORT("Internal error: CrashingCoderFactory is called");
}

TRowVtable CrashingGetVtableFactory()
{
    Y_ABORT("Internal error: CrashingGetVtableFactory is called");
}

////////////////////////////////////////////////////////////////////////////////


NYT::TNode SaveToNode(const std::vector<TRowVtable>& rowVtables)
{
    auto list = NYT::TNode::CreateList();
    for (const auto& rowVtable : rowVtables) {
        list.Add(SaveToNode(rowVtable));
    }
    return list;
}

NYT::TNode SaveToNode(const TRowVtable& rowVtable)
{
    TStringStream out;
    Save(&out, rowVtable);
    return out.Str();
}

std::vector<TRowVtable> LoadVtablesFromNode(const NYT::TNode& node) {
    std::vector<TRowVtable> rowVtables;

    auto list = node.AsList();
    rowVtables.reserve(list.size());

    for (const auto& item : list) {
        rowVtables.push_back(LoadVtableFromNode(item));
    }
    return rowVtables;
}

TRowVtable LoadVtableFromNode(const NYT::TNode& node)
{
    TStringInput in(node.AsString());
    TRowVtable result;
    Load(&in, result);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate

