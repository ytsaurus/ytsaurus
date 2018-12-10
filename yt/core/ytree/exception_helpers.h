#pragma once

#include "public.h"

#include <yt/core/misc/optional.h>

#include <yt/core/yson/public.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

void ThrowInvalidNodeType(IConstNodePtr node, ENodeType expectedType, ENodeType actualType);
void ThrowNoSuchChildKey(IConstNodePtr node, const TString& key);
void ThrowNoSuchChildIndex(IConstNodePtr node, int index);
void ThrowNoSuchAttribute(const TString& key);
void ThrowNoSuchBuiltinAttribute(const TString& key);
void ThrowNoSuchCustomAttribute(const TString& key);
void ThrowMethodNotSupported(const TString& method, const std::optional<TString>& resolveType = std::nullopt);
void ThrowCannotHaveChildren(IConstNodePtr node);
void ThrowAlreadyExists(IConstNodePtr node);
void ThrowCannotRemoveRoot();
void ThrowCannotReplaceRoot();
void ThrowCannotRemoveAttribute(const TString& key);
void ThrowCannotSetBuiltinAttribute(const TString& key);
void ThrowCannotMoveFromAnotherTransaction();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
