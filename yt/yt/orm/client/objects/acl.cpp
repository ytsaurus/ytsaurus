#include "acl.h"
#include "private.h"

#include <yt/yt/core/misc/collection_helpers.h>

#include <library/cpp/yt/string/format.h>

namespace NYT::NOrm::NClient::NObjects {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TAccessControlRegistry
    : public IAccessControlRegistry
{
public:
    TAccessControlRegistry(std::vector<TRawAccessControlPermission> aclPermissions)
        : RawPermissions_(std::move(aclPermissions))
    {
        for (const auto& permission : RawPermissions_) {
            if (NameToValue_.contains(permission.Name)) {
                YT_VERIFY(permission.Value == NameToValue_[permission.Name]);
                continue;
            }
            EmplaceOrCrash(ValueToName_, permission.Value, permission.Name);
            EmplaceOrCrash(NameToValue_, permission.Name, permission.Value);
        }
    }

    TStringBuf GetPermissionNameOrThrow(int value) const override
    {
        if (auto it = ValueToName_.find(value); it != ValueToName_.end()) {
            return it->second;
        }

        THROW_ERROR_EXCEPTION("Unknown ACL permission %Qv", value);
    }

    int GetPermissionValueOrThrow(TStringBuf name) const override
    {
        if (auto it = NameToValue_.find(name); it != NameToValue_.end()) {
            return it->second;
        }

        THROW_ERROR_EXCEPTION("Unknown ACL permission %Qv", name);
    }

    const std::vector<TRawAccessControlPermission>& GetAllPermissions() const override
    {
        return RawPermissions_;
    }

private:
    const std::vector<TRawAccessControlPermission> RawPermissions_;

    THashMap<int, TStringBuf> ValueToName_;
    THashMap<TStringBuf, int> NameToValue_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IAccessControlRegistryPtr CreateAccessControlRegistry(std::vector<TRawAccessControlPermission> permissions)
{
    return New<TAccessControlRegistry>(std::move(permissions));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NObjects
