#include "stdafx.h"
#include "exception_helpers.h"
#include "node.h"

#include <ytlib/misc/error.h>

#include <ytlib/ypath/token.h>

#include <ytlib/ytree/attribute_helpers.h>

#include <ytlib/rpc/error.h>

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
    THROW_ERROR_EXCEPTION("%s has invalid type: expected %s, actual %s",
        ~GetNodePathHelper(node),
        ~expectedType.ToString(),
        ~actualType.ToString());
}

void ThrowNoSuchChildKey(IConstNodePtr node, const Stroka& key)
{
    THROW_ERROR_EXCEPTION("%s has no child with key: %s",
        ~GetNodePathHelper(node),
        ~ToYPathLiteral(key));
}

void ThrowNoSuchChildIndex(IConstNodePtr node, int index)
{
    THROW_ERROR_EXCEPTION("%s has no child with index: %d",
        ~GetNodePathHelper(node),
        index);
}

void ThrowNoSuchAttribute(const Stroka& key)
{
    THROW_ERROR_EXCEPTION("Attribute is not found: %s",
        ~ToYPathLiteral(key));
}

void ThrowNoSuchUserAttribute(const Stroka& key)
{
    THROW_ERROR_EXCEPTION("User attribute is not found: %s",
        ~ToYPathLiteral(key));
}

void ThrowNoSuchSystemAttribute(const Stroka& key)
{
    THROW_ERROR_EXCEPTION("System attribute is not found: %s",
        ~ToYPathLiteral(key));
}

void ThrowVerbNotSuppored(const Stroka& verb, const TNullable<Stroka>& resolveType)
{
    auto error = TError(
        NRpc::EErrorCode::NoSuchVerb,
        "Verb is not supported: %s",
        ~verb);
    if (resolveType) {
        error.Attributes().Set("resolve_type", resolveType.Get());
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
    THROW_ERROR_EXCEPTION("%s already exists",
        ~GetNodePathHelper(node));
}

void ThrowCannotRemoveAttribute(const Stroka& key)
{
    THROW_ERROR_EXCEPTION("Attribute cannot be removed: %s",
        ~NYPath::ToYPathLiteral(key));
}

void ThrowCannotSetSystemAttribute(const Stroka& key)
{
    THROW_ERROR_EXCEPTION("System attribute cannot be set: %s",
        ~NYPath::ToYPathLiteral(key));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
