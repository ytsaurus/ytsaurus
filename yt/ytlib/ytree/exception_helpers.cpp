#include "stdafx.h"
#include "exception_helpers.h"
#include "node.h"

#include <ytlib/misc/error.h>

#include <ytlib/ypath/token.h>

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

void ThrowVerbNotSuppored(IConstNodePtr node, const Stroka& verb)
{
    THROW_ERROR_EXCEPTION(
        NRpc::EErrorCode::NoSuchVerb,
        "%s does not support verb: %s",
        ~GetNodePathHelper(node),
        ~verb);
}

void ThrowVerbNotSuppored(const Stroka& verb)
{
    THROW_ERROR_EXCEPTION(
        NRpc::EErrorCode::NoSuchVerb,
        "Verb is not supported: %s",
        ~verb);
}

void ThrowCannotHaveChildren(IConstNodePtr node)
{
    THROW_ERROR_EXCEPTION("%s cannot have children",
        ~GetNodePathHelper(node));
}

void ThrowCannotRemoveAttribute(const Stroka& key)
{
    THROW_ERROR_EXCEPTION("Attribute cannot be removed: %s",
        ~NYPath::ToYPathLiteral(key));
}

void ThrowCannotRemoveAttribute()
{
    THROW_ERROR_EXCEPTION("The attribute cannot be removed");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
