#pragma once

#include "public.h"

namespace NYT::NOrm::NClient::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct TRawAccessControlPermission
{
    int Value;
    TString Name;
};

////////////////////////////////////////////////////////////////////////////////

struct IAccessControlRegistry
    : public TRefCounted
{
public:
    virtual TStringBuf GetPermissionNameOrThrow(int value) const = 0;
    virtual int GetPermissionValueOrThrow(TStringBuf name) const = 0;
    virtual const std::vector<TRawAccessControlPermission>& GetAllPermissions() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IAccessControlRegistry)

////////////////////////////////////////////////////////////////////////////////

IAccessControlRegistryPtr CreateAccessControlRegistry(std::vector<TRawAccessControlPermission> permissions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NObjects
