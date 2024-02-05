#include "dot.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

void DOTPrologue(IOutputStream& out)
{
    out << "digraph {\n";
}

TString DOTTableName(int tableId)
{
    return "\"table-" + std::to_string(tableId) + "\"";
}

void DOTTable(IOutputStream& out, const TString& name, const TString& path)
{
    out << name << " [shape=\"square\", label=\"" << path << "\"]\n";
}

void DOTEdge(IOutputStream& out, const TString& from, const TString& to)
{
    out << from << "->" << to << "\n";
}

TString DOTOperationName(int operationId)
{
    return "\"op-" + std::to_string(operationId) + "\"";
}

void DOTOperation(IOutputStream& out, const TString& name, const TString& label)
{
    out << name << " [shape=\"circle\", label=\"" << label << "\"]\n";
}

void DOTEpilogue(IOutputStream& out)
{
    out << "}\n";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
