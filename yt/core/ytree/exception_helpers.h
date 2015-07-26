#pragma once

#include "public.h"

#include <core/misc/nullable.h>

#include <core/yson/public.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

void ThrowInvalidNodeType(IConstNodePtr node, ENodeType expectedType, ENodeType actualType);
void ThrowNoSuchChildKey(IConstNodePtr node, const Stroka& key);
void ThrowNoSuchChildIndex(IConstNodePtr node, int index);
void ThrowNoSuchAttribute(const Stroka& key);
void ThrowNoSuchBuiltinAttribute(const Stroka& key);
void ThrowNoSuchCustomAttribute(const Stroka& key);
void ThrowMethodNotSupported(const Stroka& method, const TNullable<Stroka>& resolveType = Null);
void ThrowCannotHaveChildren(IConstNodePtr node);
void ThrowAlreadyExists(IConstNodePtr node);
void ThrowCannotRemoveRoot();
void ThrowCannotRemoveAttribute(const Stroka& key);
void ThrowCannotSetBuiltinAttribute(const Stroka& key);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
