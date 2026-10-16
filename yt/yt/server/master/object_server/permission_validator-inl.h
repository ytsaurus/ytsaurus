#ifndef PERMISSION_VALIDATOR_INL_H_
#error "Direct inclusion of this file is not allowed, include permission_validator.h"
// For the sake of sane code completion.
#include "permission_validator.h"
#endif

#include <yt/yt/server/lib/object_server/helpers.h>

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
    switch (scope) {
        case NYTree::EPermissionCheckScope::This:
            Underlying_->ValidatePermission(object, permission);
            return;
        case NYTree::EPermissionCheckScope::Parent:
            if (auto parent = GetParentForPermissionValidation(object)) {
                Underlying_->ValidatePermission(parent, permission);
            }
            return;
        case NYTree::EPermissionCheckScope::Subtree:
            Underlying_->ValidatePermission(object, permission);
            [[fallthrough]];
        case NYTree::EPermissionCheckScope::Descendants:
            for (auto* descendant : ListDescendantsForPermissionValidation(object)) {
                Underlying_->ValidatePermission(descendant, permission);
            }
            return;
        default:
            YT_ABORT();
    }
}

template <class TObject>
void THierarchicPermissionValidator<TObject>::ValidateCreatePermissions(
    TObject* object,
    bool replace,
    const NYTree::IAttributeDictionary* attributes)
{
    ValidateAddChildPermissions(object, replace, IsAdministerValidationNeeded(attributes));
}

template <class TObject>
void THierarchicPermissionValidator<TObject>::ValidateCopyPermissions(
    TObject* sourceObject,
    TObject* thisObject,
    NCypressClient::ENodeCloneMode mode,
    bool replace,
    bool validateAdminister)
{
    ValidateCopyFromSourcePermissions(sourceObject, mode);
    ValidateCopyToThisDestinationPermissions(thisObject, replace, validateAdminister);
}

template <class TObject>
void THierarchicPermissionValidator<TObject>::ValidateCopyFromSourcePermissions(
    TObject* sourceObject,
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

template <class TObject>
void THierarchicPermissionValidator<TObject>::ValidateCopyToThisDestinationPermissions(
    TObject* thisObject,
    bool replace,
    bool validateAdminister)
{
    ValidateAddChildPermissions(thisObject, replace, validateAdminister);
}

template <class TObject>
void THierarchicPermissionValidator<TObject>::ValidateAddChildPermissions(
    TObject* object,
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
