#pragma once
#ifndef PERMISSION_VALIDATOR_INL_H_
#error "Direct inclusion of this file is not allowed, include permission_validator.h"
// For the sake of sane code completion.
#include "permission_validator.h"
#endif

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

template <class TObject>
THierarchicPermissionValidator<TObject>::THierarchicPermissionValidator(
    std::unique_ptr<IPermissionValidator> validator)
    : Underlying_(std::move(validator))
{ }

template <class TObject>
void THierarchicPermissionValidator<TObject>::ValidatePermission(
    TObject* object,
    NYTree::EPermissionCheckScope scope,
    NYTree::EPermission permission)
{
    if (Any(scope & NYTree::EPermissionCheckScope::This)) {
        Underlying_->ValidatePermission(object, permission);
    }

    if (Any(scope & NYTree::EPermissionCheckScope::Parent) && object->GetParent()) {
        Underlying_->ValidatePermission(object->GetParent(), permission);
    }

    if (Any(scope & NYTree::EPermissionCheckScope::Descendants)) {
        for (auto* descendant : ListDescendants(object)) {
            Underlying_->ValidatePermission(descendant, permission);
        }
    }
}

template <class TObject>
void THierarchicPermissionValidator<TObject>::ValidateCreatePermissions(
    bool replace,
    const NYTree::IAttributeDictionary* attributes)
{
    ValidateAddChildPermissions(replace);

    if (attributes && (attributes->Contains("acl") || attributes->Contains("inherit_acl"))) {
        Underlying_->ValidatePermission(
            replace ? NYTree::EPermissionCheckScope::Parent : NYTree::EPermissionCheckScope::This,
            NYTree::EPermission::Administer);
    }
}

template <class TObject>
void THierarchicPermissionValidator<TObject>::ValidateCopyPermissions(
    TObject* sourceImpl,
    NCypressClient::ENodeCloneMode mode,
    bool replace)
{
    ValidateCopyPermissionsSource(sourceImpl, mode);
    ValidateCopyPermissionsDestination(replace);
}

template <class TObject>
void THierarchicPermissionValidator<TObject>::ValidateCopyPermissionsSource(
    TObject* sourceImpl,
    NCypressClient::ENodeCloneMode mode)
{
    ValidatePermission(
        sourceImpl,
        NYTree::EPermissionCheckScope::This | NYTree::EPermissionCheckScope::Descendants,
        NYTree::EPermission::Read);

    if (mode == NCypressClient::ENodeCloneMode::Move) {
        ValidatePermission(
            sourceImpl,
            NYTree::EPermissionCheckScope::This | NYTree::EPermissionCheckScope::Descendants,
            NYTree::EPermission::Remove);
        ValidatePermission(
            sourceImpl,
            NYTree::EPermissionCheckScope::Parent,
            NYTree::EPermission::Write | NYTree::EPermission::ModifyChildren);
    }
}

template <class TObject>
void THierarchicPermissionValidator<TObject>::ValidateCopyPermissionsDestination(bool replace)
{
    ValidateAddChildPermissions(replace);
}

template <class TObject>
void THierarchicPermissionValidator<TObject>::ValidateAddChildPermissions(bool replace)
{
    if (replace) {
        Underlying_->ValidatePermission(
            NYTree::EPermissionCheckScope::This | NYTree::EPermissionCheckScope::Descendants,
            NYTree::EPermission::Remove);
        Underlying_->ValidatePermission(
            NYTree::EPermissionCheckScope::Parent,
            NYTree::EPermission::Write | NYTree::EPermission::ModifyChildren);
    } else {
        Underlying_->ValidatePermission(
            NYTree::EPermissionCheckScope::This,
            NYTree::EPermission::Write | NYTree::EPermission::ModifyChildren);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
