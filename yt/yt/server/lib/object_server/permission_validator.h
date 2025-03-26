#pragma once

#include <yt/yt/ytlib/cypress_client/public.h>

#include <yt/yt/core/ytree/permission.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

// TODO(danilalexeev): Maybe better expressed as inheritance rather than callbacks.
template <class TObject>
struct IPermissionValidator
{
    virtual ~IPermissionValidator() = default;

    virtual void ValidatePermission(
        TObject object,
        NYTree::EPermission permission) = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Encapsulates common permission-related logic for hierarchic objects like
//! Cypress nodes and non-versioned map objects.
template <class TObject, class TObjectBase = TObject>
class THierarchicPermissionValidator
{
private:
    using IPermissionValidator = NObjectServer::IPermissionValidator<TObjectBase>;

public:
    THierarchicPermissionValidator(std::unique_ptr<IPermissionValidator> validator);

protected:
    void ValidatePermission(
        TObject object,
        NYTree::EPermissionCheckScope scope,
        NYTree::EPermission permission);

    // TODO(danilalexeev): Drop this in favor of |THierarchicPermissionValidator::ValidatePermissionForSubtree|.
    virtual TCompactVector<TObjectBase, 1> ListDescendantsForPermissionValidation(TObject object) = 0;
    virtual TObjectBase GetParentForPermissionValidation(TObject object) = 0;

    virtual void ValidatePermissionForSubtree(
        TObject object,
        NYTree::EPermission permission,
        bool descendantsOnly = false);

    void ValidateCreatePermissions(
        TObject object,
        bool replace,
        const NYTree::IAttributeDictionary* attributes);

    void ValidateCopyPermissions(
        TObject sourceImpl,
        TObject thisImpl,
        NCypressClient::ENodeCloneMode mode,
        bool replace,
        bool validateAdminister);
    void ValidateCopyFromSourcePermissions(TObject sourceObject, NCypressClient::ENodeCloneMode mode);
    void ValidateCopyToThisDestinationPermissions(TObject thisObject, bool replace, bool validateAdminister);

private:
    std::unique_ptr<IPermissionValidator> Underlying_;

    void ValidateAddChildPermissions(TObject object, bool replace, bool validateAdminister);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

#define PERMISSION_VALIDATOR_INL_H_
#include "permission_validator-inl.h"
#undef PERMISSION_VALIDATOR_INL_H_
