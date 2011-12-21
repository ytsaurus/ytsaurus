#include "stdafx.h"
#include "command.h"

namespace NYT {
namespace NDriver {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static void StreamSpecValidator(const INode::TPtr& node)
{
    if (!node)
        return;
    auto type = node->GetType();
    switch (type) {
        case ENodeType::String:
            if (node->GetValue<Stroka>().empty()) {
                ythrow yexception() << "Empty stream specification";
            }
            break;

        case ENodeType::Int64:
            if (node->GetValue<i64>() < 0) {
                ythrow yexception() << "Negative stream handles are forbidden";
            }
            break;

        default:
            ythrow yexception() << Sprintf("Invalid stream specification (Expected: String or Integer, Actual: %s)",
                ~type.ToString());
    }
}

IParamAction<const INode::TPtr&>::TPtr TRequestBase::StreamSpecIsValid = FromMethod(&StreamSpecValidator);

Stroka ToStreamSpec(INode::TPtr node)
{
    if (!node) {
        return "";
    }
    switch (node->GetType()) {
        case NYTree::ENodeType::String:
            return node->GetValue<Stroka>();
        case NYTree::ENodeType::Int64:
            return "&" + ToString(node->GetValue<i64>());
        default:
            YUNREACHABLE();
    }
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
