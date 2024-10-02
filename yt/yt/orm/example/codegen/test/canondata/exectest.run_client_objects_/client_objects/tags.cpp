// AUTOMATICALLY GENERATED. DO NOT EDIT!

#include "tags.h"

#include "type.h"

#include <yt/yt/orm/client/objects/registry.h>

#include <mutex>

namespace NYT::NOrm::NExample::NClient::NObjects {

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::ITagsRegistryPtr CreateTagsRegistry()
{
    return NYT::NOrm::NClient::NObjects::CreateTagsRegistry(
        {
            { .Value = 64, .Name = "public_data" },
            { .Value = 66, .Name = "digital_data" },
            { .Value = 67, .Name = "money" },
            { .Value = 68, .Name = "book_moderation" },
            { .Value = 69, .Name = "nexus_id" },
            { .Value = 70, .Name = "teal" },
            { .Value = 0, .Name = "system" },
            { .Value = 1, .Name = "computed" },
            { .Value = 2, .Name = "mandatory" },
            { .Value = 3, .Name = "opaque" },
            { .Value = 4, .Name = "readonly" },
            { .Value = 5, .Name = "touch" },
            { .Value = 6, .Name = "meta" },
            { .Value = 7, .Name = "spec" },
            { .Value = 8, .Name = "status" },
            { .Value = 9, .Name = "control" },
            { .Value = 10, .Name = "any" },
        });
}

void EnsureTagsRegistryInitialized()
{
    // Prevent redundant initializations during unittests execution.
    static std::once_flag Initialized;
    std::call_once(Initialized, [] {
        NYT::NOrm::NClient::NObjects::SetGlobalTagsRegistry(CreateTagsRegistry());
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NClient::NObjects
