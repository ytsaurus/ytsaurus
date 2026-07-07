#pragma once

#include "object.h"

#include <yt/yt/ytlib/cypress_client/public.h>

#include <yt/yt/core/ytree/permission.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

struct IPermissionValidator
{
    virtual ~IPermissionValidator() = default;

    virtual void ValidatePermission(
        TObject* object,
        NYTree::EPermission permission) = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Encapsulates common permission-related logic for hierarchic objects like
//! Cypress nodes and non-versioned map objects.
template <class TObject>
class THierarchicPermissionValidator
{
public:
    explicit THierarchicPermissionValidator(std::unique_ptr<IPermissionValidator> validator);

protected:
    void ValidatePermission(
        TObject* object,
        NYTree::EPermissionCheckScope scope,
        NYTree::EPermission permission);

    // TODO(danilalexeev): Drop this in favor of |THierarchicPermissionValidator::ValidatePermissionForSubtree|.
    virtual TCompactVector<TObject*, 1> ListDescendantsForPermissionValidation(TObject* object) = 0;
    virtual TObject* GetParentForPermissionValidation(TObject* object) = 0;

    virtual void ValidatePermissionForSubtree(
        TObject* object,
        NYTree::EPermission permission,
        bool descendantsOnly = false);

    void ValidateCreatePermissions(
        TObject* object,
        bool replace,
        const NYTree::IAttributeDictionary* attributes);

    void ValidateCopyPermissions(
        TObject* sourceObject,
        TObject* thisObject,
        NCypressClient::ENodeCloneMode mode,
        bool replace,
        bool validateAdminister);
    void ValidateCopyFromSourcePermissions(TObject* sourceObject, NCypressClient::ENodeCloneMode mode);
    void ValidateCopyToThisDestinationPermissions(TObject* thisObject, bool replace, bool validateAdminister);

private:
    std::unique_ptr<IPermissionValidator> Underlying_;

    void ValidateAddChildPermissions(TObject* object, bool replace, bool validateAdminister);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

#define PERMISSION_VALIDATOR_INL_H_
#include "permission_validator-inl.h"
#undef PERMISSION_VALIDATOR_INL_H_
