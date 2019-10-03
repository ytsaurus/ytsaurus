#pragma once

#include "public.h"

#include <yt/core/misc/optional.h>

#include <yt/core/yson/public.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

void ThrowInvalidNodeType(
    const IConstNodePtr& node,
    ENodeType expectedType,
    ENodeType actualType);
void ValidateNodeType(
    const IConstNodePtr& node,
    const THashSet<ENodeType>& expectedTypes,
    const TString& expectedTypesStringRepresentation);
void ThrowNoSuchChildKey(const IConstNodePtr& node, TStringBuf key);
void ThrowNoSuchChildIndex(const IConstNodePtr& node, int index);
void ThrowNoSuchAttribute(TStringBuf key);
void ThrowNoSuchBuiltinAttribute(TStringBuf key);
void ThrowNoSuchCustomAttribute(TStringBuf key);
void ThrowMethodNotSupported(
    TStringBuf method,
    const std::optional<TString>& resolveType = {});
void ThrowCannotHaveChildren(const IConstNodePtr& node);
void ThrowAlreadyExists(const IConstNodePtr& node);
void ThrowCannotRemoveNode(const IConstNodePtr& node);
void ThrowCannotReplaceNode(const IConstNodePtr& node);
void ThrowCannotRemoveAttribute(TStringBuf key);
void ThrowCannotSetBuiltinAttribute(TStringBuf key);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
