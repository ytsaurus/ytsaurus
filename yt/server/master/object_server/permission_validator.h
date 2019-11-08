#pragma once

#include "object.h"

#include <yt/ytlib/cypress_client/public.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

struct IPermissionValidator
{
    virtual ~IPermissionValidator() = default;

    virtual void ValidatePermission(
        NYTree::EPermissionCheckScope scope,
        NYTree::EPermission permission,
        const TString& /* user */ = {}) = 0;

    virtual void ValidatePermission(
        TObject* object,
        NYTree::EPermission permission) = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Encapsulates common permission-related logic for hierarchic objects like
//! Cypress node and non-versioned map objects.
template <class TObject>
class THierarchicPermissionValidator
{
public:
    THierarchicPermissionValidator(std::unique_ptr<IPermissionValidator> validator);

protected:
    void ValidatePermission(
        TObject* object,
        NYTree::EPermissionCheckScope scope,
        NYTree::EPermission permission);

    virtual TSharedRange<TObject*> ListDescendants(TObject* object) = 0;

    void ValidateCreatePermissions(
        bool replace,
        const NYTree::IAttributeDictionary* attributes);

    void ValidateCopyPermissions(
        TObject* sourceImpl,
        NCypressClient::ENodeCloneMode mode,
        bool replace,
        bool validateAdminister);
    void ValidateCopyPermissionsSource(TObject* sourceImpl, NCypressClient::ENodeCloneMode mode);
    void ValidateCopyPermissionsDestination(bool replace, bool validateAdminister);

private:
    std::unique_ptr<IPermissionValidator> Underlying_;

    void ValidateAddChildPermissions(bool replace, bool validateAdminister);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

#define PERMISSION_VALIDATOR_INL_H_
#include "permission_validator-inl.h"
#undef PERMISSION_VALIDATOR_INL_H_
