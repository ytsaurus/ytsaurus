// AUTOMATICALLY GENERATED. DO NOT EDIT!

#include "acl.h"

#include <yt/yt/orm/client/objects/registry.h>

#include <mutex>

namespace NYT::NOrm::NExample::NClient::NObjects {

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::IAccessControlRegistryPtr CreateAccessControlRegistry()
{
    return NYT::NOrm::NClient::NObjects::CreateAccessControlRegistry(
        {
            { .Value = 0, .Name = "none" },
            { .Value = 1, .Name = "read" },
            { .Value = 2, .Name = "write" },
            { .Value = 3, .Name = "create" },
            { .Value = 6, .Name = "use" },
            { .Value = 9, .Name = "administer" },
        });
}

void EnsureAccessControlRegistryInitialized()
{
    // Prevent redundant initializations during unittests execution.
    static std::once_flag Initialized;
    std::call_once(Initialized, [] {
        NYT::NOrm::NClient::NObjects::SetGlobalAccessControlRegistry(CreateAccessControlRegistry());
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NClient::NObjects
