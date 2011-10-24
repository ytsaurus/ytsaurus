#include "stdafx.h"
#include "file_type_handler.h"
#include "file_node.h"

namespace NYT {
namespace NFileServer {

////////////////////////////////////////////////////////////////////////////////

ERuntimeNodeType TFileTypeHandler::GetRuntimeType()
{
    return ERuntimeNodeType::File;
}

Stroka TFileTypeHandler::GetTypeName()
{
    return FileTypeName;
}

TAutoPtr<ICypressNode> TFileTypeHandler::Create(
    const TNodeId& nodeId,
    const TTransactionId& transactionId,
    IMapNode::TPtr description)
{
    UNUSED(transactionId);
    UNUSED(description);
    return new TFileNode(TBranchedNodeId(nodeId, NullTransactionId));
}

TAutoPtr<ICypressNode> TFileTypeHandler::Load(TInputStream* stream)
{
    UNUSED(stream);
    YUNIMPLEMENTED();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

