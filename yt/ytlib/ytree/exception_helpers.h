#pragma once

#include "public.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

void ThrowInvalidNodeType(IConstNodePtr node, ENodeType expectedType, ENodeType actualType);
void ThrowNoSuchChildKey(IConstNodePtr node, const Stroka& key);
void ThrowNoSuchChildIndex(IConstNodePtr node, int index);
void ThrowNoSuchAttribute(const Stroka& key);
void ThrowNoSuchSystemAttribute(const Stroka& key);
void ThrowNoSuchUserAttribute(const Stroka& key);
void ThrowVerbNotSuppored(const Stroka& verb);
void ThrowVerbNotSuppored(IConstNodePtr node, const Stroka& verb);
void ThrowCannotHaveChildren(IConstNodePtr node);
void ThrowCannotRemoveAttribute(const Stroka& key);
void ThrowCannotRemoveAttribute();
void ThrowCannotSetSystemAttribute(const Stroka& key);
void ThrowCannotSetOpaqueAttribute(const Stroka& key);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
