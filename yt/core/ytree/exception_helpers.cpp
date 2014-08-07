#include "stdafx.h"
#include "exception_helpers.h"
#include "node.h"

#include <core/misc/error.h>

#include <core/ypath/token.h>

#include <core/ytree/attribute_helpers.h>

#include <core/rpc/public.h>

namespace NYT {
namespace NYTree {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

namespace {

Stroka GetNodePathHelper(IConstNodePtr node)
{
    auto path = node->GetPath();
    return path.empty() ? "Node" : Format("Node %v", path);
}

} // namespace

void ThrowInvalidNodeType(IConstNodePtr node, ENodeType expectedType, ENodeType actualType)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::ResolveError,
        "%v has invalid type: expected %Qv, actual %Qv",
        GetNodePathHelper(node),
        expectedType,
        actualType);
}

void ThrowNoSuchChildKey(IConstNodePtr node, const Stroka& key)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::ResolveError,
        "%v has no child with key %Qv",
        GetNodePathHelper(node),
        ToYPathLiteral(key));
}

void ThrowNoSuchChildIndex(IConstNodePtr node, int index)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::ResolveError,
        "%v has no child with index %v",
        GetNodePathHelper(node),
        index);
}

void ThrowNoSuchAttribute(const Stroka& key)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::ResolveError,
        "Attribute %Qv is not found",
        ToYPathLiteral(key));
}

void ThrowNoSuchCustomAttribute(const Stroka& key)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::ResolveError,
        "Custom attribute %Qv is not found",
        ToYPathLiteral(key));
}

void ThrowNoSuchBuiltinAttribute(const Stroka& key)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::ResolveError,
        "Builtin attribute %Qv is not found",
        ToYPathLiteral(key));
}

void ThrowMethodNotSupported(const Stroka& method, const TNullable<Stroka>& resolveType)
{
    auto error = TError(
        NRpc::EErrorCode::NoSuchMethod,
        "Method %v is not supported",
        method);
    if (resolveType) {
        error.Attributes().Set("resolve_type", *resolveType);
    }
    THROW_ERROR(error);
}

void ThrowCannotHaveChildren(IConstNodePtr node)
{
    THROW_ERROR_EXCEPTION("%v cannot have children",
        GetNodePathHelper(node));
}

void ThrowAlreadyExists(IConstNodePtr node)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::AlreadyExists,
        "%v already exists",
        GetNodePathHelper(node));
}

void ThrowCannotRemoveRoot()
{
    THROW_ERROR_EXCEPTION("Root node cannot be removed");
}
void ThrowCannotRemoveAttribute(const Stroka& key)

{
    THROW_ERROR_EXCEPTION("Attribute %Qv cannot be removed",
        ToYPathLiteral(key));
}

void ThrowCannotSetBuiltinAttribute(const Stroka& key)
{
    THROW_ERROR_EXCEPTION("Builtin attribute %Qv cannot be set",
        ToYPathLiteral(key));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
