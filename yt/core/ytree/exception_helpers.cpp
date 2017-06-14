#include "exception_helpers.h"
#include "node.h"

#include <yt/core/misc/error.h>

#include <yt/core/rpc/public.h>

#include <yt/core/ypath/token.h>

#include <yt/core/ytree/helpers.h>

namespace NYT {
namespace NYTree {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

namespace {

TString GetNodePathHelper(IConstNodePtr node)
{
    auto path = node->GetPath();
    return path.empty() ? "Node" : Format("Node %v", path);
}

} // namespace

void ThrowInvalidNodeType(IConstNodePtr node, ENodeType expectedType, ENodeType actualType)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::ResolveError,
        "%v has invalid type: expected %Qlv, actual %Qlv",
        GetNodePathHelper(node),
        expectedType,
        actualType);
}

void ThrowNoSuchChildKey(IConstNodePtr node, const TString& key)
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

void ThrowNoSuchAttribute(const TString& key)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::ResolveError,
        "Attribute %Qv is not found",
        ToYPathLiteral(key));
}

void ThrowNoSuchCustomAttribute(const TString& key)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::ResolveError,
        "Custom attribute %Qv is not found",
        ToYPathLiteral(key));
}

void ThrowNoSuchBuiltinAttribute(const TString& key)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::ResolveError,
        "Builtin attribute %Qv is not found",
        ToYPathLiteral(key));
}

void ThrowMethodNotSupported(const TString& method, const TNullable<TString>& resolveType)
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

void ThrowCannotReplaceRoot()
{
    THROW_ERROR_EXCEPTION("Root node cannot be replaced");
}

void ThrowCannotRemoveAttribute(const TString& key)
{
    THROW_ERROR_EXCEPTION("Attribute %Qv cannot be removed",
        ToYPathLiteral(key));
}

void ThrowCannotSetBuiltinAttribute(const TString& key)
{
    THROW_ERROR_EXCEPTION("Builtin attribute %Qv cannot be set",
        ToYPathLiteral(key));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
