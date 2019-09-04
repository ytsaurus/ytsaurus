#pragma once

#include "public.h"

#include <yt/core/misc/optional.h>

#include <yt/core/yson/public.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

void ThrowInvalidNodeType(const IConstNodePtr& node, ENodeType expectedType, ENodeType actualType);
void ValidateNodeType(
    const IConstNodePtr& node,
    const THashSet<ENodeType>& expectedTypes,
    const TString& expectedTypesStringRepresentation);
void ThrowNoSuchChildKey(const IConstNodePtr& node, const TString& key);
void ThrowNoSuchChildIndex(const IConstNodePtr& node, int index);
void ThrowNoSuchAttribute(const TString& key);
void ThrowNoSuchBuiltinAttribute(const TString& key);
void ThrowNoSuchCustomAttribute(const TString& key);
void ThrowMethodNotSupported(const TString& method, const std::optional<TString>& resolveType = std::nullopt);
void ThrowCannotHaveChildren(const IConstNodePtr& node);
void ThrowAlreadyExists(const IConstNodePtr& node);
void ThrowCannotRemoveNode(const IConstNodePtr& node);
void ThrowCannotReplaceNode(const IConstNodePtr& node);
void ThrowCannotRemoveAttribute(const TString& key);
void ThrowCannotSetBuiltinAttribute(const TString& key);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
