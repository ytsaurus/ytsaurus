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
    return path.empty() ? "Node" : Sprintf("Node %s", ~path);
}

} // namespace

void ThrowInvalidNodeType(IConstNodePtr node, ENodeType expectedType, ENodeType actualType)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::ResolveError,
        "%s has invalid type: expected %s, actual %s",
        ~GetNodePathHelper(node),
        ~FormatEnum(expectedType).Quote(),
        ~FormatEnum(actualType).Quote());
}

void ThrowNoSuchChildKey(IConstNodePtr node, const Stroka& key)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::ResolveError,
        "%s has no child with key %s",
        ~GetNodePathHelper(node),
        ~ToYPathLiteral(key).Quote());
}

void ThrowNoSuchChildIndex(IConstNodePtr node, int index)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::ResolveError,
        "%s has no child with index %d",
        ~GetNodePathHelper(node),
        index);
}

void ThrowNoSuchAttribute(const Stroka& key)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::ResolveError,
        "Attribute %s is not found",
        ~ToYPathLiteral(key).Quote());
}

void ThrowNoSuchCustomAttribute(const Stroka& key)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::ResolveError,
        "User attribute %s is not found",
        ~ToYPathLiteral(key).Quote());
}

void ThrowNoSuchBuiltinAttribute(const Stroka& key)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::ResolveError,
        "System attribute %s is not found",
        ~ToYPathLiteral(key).Quote());
}

void ThrowVerbNotSuppored(const Stroka& verb, const TNullable<Stroka>& resolveType)
{
    auto error = TError(
        NRpc::EErrorCode::NoSuchVerb,
        "Verb %s is not supported",
        ~verb.Quote());
    if (resolveType) {
        error.Attributes().Set("resolve_type", *resolveType);
    }
    THROW_ERROR(error);
}

void ThrowCannotHaveChildren(IConstNodePtr node)
{
    THROW_ERROR_EXCEPTION("%s cannot have children",
        ~GetNodePathHelper(node));
}

void ThrowAlreadyExists(IConstNodePtr node)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::AlreadyExists,
        "%s already exists",
        ~GetNodePathHelper(node));
}

void ThrowCannotRemoveRoot()
{
    THROW_ERROR_EXCEPTION("Root node cannot be removed");
}
void ThrowCannotRemoveAttribute(const Stroka& key)

{
    THROW_ERROR_EXCEPTION("Attribute %s cannot be removed",
        ~ToYPathLiteral(key).Quote());
}

void ThrowCannotSetBuiltinAttribute(const Stroka& key)
{
    THROW_ERROR_EXCEPTION("System attribute %s cannot be set",
        ~ToYPathLiteral(key).Quote());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
