#ifndef PERMISSION_VALIDATOR_INL_H_
#error "Direct inclusion of this file is not allowed, include permission_validator.h"
// For the sake of sane code completion.
#include "permission_validator.h"
#endif

#include <yt/yt/core/ytree/attributes.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

template <class TObject, class TObjectBase>
THierarchicPermissionValidator<TObject, TObjectBase>::THierarchicPermissionValidator(
    std::unique_ptr<IPermissionValidator> validator)
    : Underlying_(std::move(validator))
{ }

template <class TObject, class TObjectBase>
void THierarchicPermissionValidator<TObject, TObjectBase>::ValidatePermission(
    TObject object,
    NYTree::EPermissionCheckScope scope,
    NYTree::EPermission permission)
{
    switch (scope) {
        case NYTree::EPermissionCheckScope::This:
            return Underlying_->ValidatePermission(object, permission);
        case NYTree::EPermissionCheckScope::Parent:
            if (auto parent = GetParentForPermissionValidation(object)) {
                return Underlying_->ValidatePermission(parent, permission);
            }
            return;
        case NYTree::EPermissionCheckScope::Descendants:
            return ValidatePermissionForSubtree(object, permission, /*descendantsOnly*/ true);
        case NYTree::EPermissionCheckScope::Subtree:
            return ValidatePermissionForSubtree(object, permission);
        default:
            YT_ABORT();
    }
}

template <class TObject, class TObjectBase>
void THierarchicPermissionValidator<TObject, TObjectBase>::ValidatePermissionForSubtree(
    TObject object,
    NYTree::EPermission permission,
    bool descendantsOnly)
{
    if (!descendantsOnly) {
        Underlying_->ValidatePermission(object, permission);
    }

    for (auto descendant : ListDescendantsForPermissionValidation(object)) {
        Underlying_->ValidatePermission(descendant, permission);
    }
}

template <class TObject, class TObjectBase>
void THierarchicPermissionValidator<TObject, TObjectBase>::ValidateCreatePermissions(
    TObject object,
    bool replace,
    const NYTree::IAttributeDictionary* attributes)
{
    bool validateAdminister = attributes && (attributes->Contains("acl") || attributes->Contains("inherit_acl"));
    ValidateAddChildPermissions(object, replace, validateAdminister);
}

template <class TObject, class TObjectBase>
void THierarchicPermissionValidator<TObject, TObjectBase>::ValidateCopyPermissions(
    TObject sourceObject,
    TObject thisObject,
    NCypressClient::ENodeCloneMode mode,
    bool replace,
    bool validateAdminister)
{
    ValidateCopyFromSourcePermissions(sourceObject, mode);
    ValidateCopyToThisDestinationPermissions(thisObject, replace, validateAdminister);
}

template <class TObject, class TObjectBase>
void THierarchicPermissionValidator<TObject, TObjectBase>::ValidateCopyFromSourcePermissions(
    TObject sourceObject,
    NCypressClient::ENodeCloneMode mode)
{
    ValidatePermission(
        sourceObject,
        NYTree::EPermissionCheckScope::Subtree,
        NYTree::EPermission::FullRead);

    if (mode == NCypressClient::ENodeCloneMode::Move) {
        // NB: passing a disjunction of check scopes to ValidatePermission makes it
        // check multiple scopes but doing the same for permissions checks that at least one
        // of them is valid.
        ValidatePermission(
            sourceObject,
            NYTree::EPermissionCheckScope::Subtree,
            NYTree::EPermission::Remove);
        ValidatePermission(
            sourceObject,
            NYTree::EPermissionCheckScope::Parent,
            NYTree::EPermission::Write | NYTree::EPermission::ModifyChildren);
    }
}

template <class TObject, class TObjectBase>
void THierarchicPermissionValidator<TObject, TObjectBase>::ValidateCopyToThisDestinationPermissions(
    TObject thisObject,
    bool replace,
    bool validateAdminister)
{
    ValidateAddChildPermissions(thisObject, replace, validateAdminister);
}

template <class TObject, class TObjectBase>
void THierarchicPermissionValidator<TObject, TObjectBase>::ValidateAddChildPermissions(
    TObject object,
    bool replace,
    bool validateAdminister)
{
    if (replace) {
        ValidatePermission(
            object,
            NYTree::EPermissionCheckScope::Parent,
            NYTree::EPermission::Write | NYTree::EPermission::ModifyChildren);
        ValidatePermission(
            object,
            NYTree::EPermissionCheckScope::Subtree,
            NYTree::EPermission::Remove);
        if (validateAdminister) {
            ValidatePermission(object, NYTree::EPermissionCheckScope::Parent, NYTree::EPermission::Administer);
        }
    } else {
        ValidatePermission(
            object,
            NYTree::EPermissionCheckScope::This,
            NYTree::EPermission::Write | NYTree::EPermission::ModifyChildren);
        if (validateAdminister) {
            ValidatePermission(object, NYTree::EPermissionCheckScope::This, NYTree::EPermission::Administer);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
