// AUTOMATICALLY GENERATED. DO NOT EDIT!

#include "type.h"

#include <yt/yt/orm/client/objects/registry.h>

#include <mutex>

namespace NYT::NOrm::NExample::NClient::NObjects {

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::IObjectTypeRegistryPtr CreateObjectTypeRegistry()
{
    return NYT::NOrm::NClient::NObjects::CreateObjectTypeRegistry(
        {
            {
                .Value = 0,
                .Name = "null",
                .HumanReadableName = "null",
                .CapitalizedHumanReadableName = "Null",
            },
            {
                .Value = 2,
                .Name = "author",
                .HumanReadableName = "author",
                .CapitalizedHumanReadableName = "Author",
            },
            {
                .Value = 1,
                .Name = "book",
                .HumanReadableName = "book",
                .CapitalizedHumanReadableName = "Book",
            },
            {
                .Value = 203,
                .Name = "buffered_timestamp_id",
                .HumanReadableName = "buffered timestamp id",
                .CapitalizedHumanReadableName = "Buffered timestamp id",
            },
            {
                .Value = 260,
                .Name = "cat",
                .HumanReadableName = "cat",
                .CapitalizedHumanReadableName = "Cat",
            },
            {
                .Value = 4,
                .Name = "editor",
                .HumanReadableName = "editor",
                .CapitalizedHumanReadableName = "Editor",
            },
            {
                .Value = 207,
                .Name = "employer",
                .HumanReadableName = "employer",
                .CapitalizedHumanReadableName = "Employer",
            },
            {
                .Value = 103,
                .Name = "executor",
                .HumanReadableName = "executor",
                .CapitalizedHumanReadableName = "Executor",
            },
            {
                .Value = 7,
                .Name = "genre",
                .HumanReadableName = "genre",
                .CapitalizedHumanReadableName = "Genre",
            },
            {
                .Value = 10,
                .Name = "group",
                .HumanReadableName = "group",
                .CapitalizedHumanReadableName = "Group",
            },
            {
                .Value = 42,
                .Name = "hitchhiker",
                .HumanReadableName = "hitchhiker",
                .CapitalizedHumanReadableName = "Hitchhiker",
            },
            {
                .Value = 5,
                .Name = "illustrator",
                .HumanReadableName = "illustrator",
                .CapitalizedHumanReadableName = "Illustrator",
            },
            {
                .Value = 204,
                .Name = "indexed_increment_id",
                .HumanReadableName = "indexed increment id",
                .CapitalizedHumanReadableName = "Indexed increment id",
            },
            {
                .Value = 102,
                .Name = "interceptor",
                .HumanReadableName = "interceptor",
                .CapitalizedHumanReadableName = "Interceptor",
            },
            {
                .Value = 200,
                .Name = "manual_id",
                .HumanReadableName = "manual id",
                .CapitalizedHumanReadableName = "Manual id",
            },
            {
                .Value = 101,
                .Name = "mother_ship",
                .HumanReadableName = "mother ship",
                .CapitalizedHumanReadableName = "Mother ship",
            },
            {
                .Value = 206,
                .Name = "multipolicy_id",
                .HumanReadableName = "multipolicy id",
                .CapitalizedHumanReadableName = "Multipolicy id",
            },
            {
                .Value = 208,
                .Name = "nested_columns",
                .HumanReadableName = "nested columns",
                .CapitalizedHumanReadableName = "Nested columns",
            },
            {
                .Value = 100,
                .Name = "nexus",
                .HumanReadableName = "nexus",
                .CapitalizedHumanReadableName = "Nexus",
            },
            {
                .Value = 104,
                .Name = "nirvana_dm_process_instance",
                .HumanReadableName = "nirvana dm process instance",
                .CapitalizedHumanReadableName = "Nirvana dm process instance",
            },
            {
                .Value = 3,
                .Name = "publisher",
                .HumanReadableName = "publisher",
                .CapitalizedHumanReadableName = "Publisher",
            },
            {
                .Value = 201,
                .Name = "random_id",
                .HumanReadableName = "random id",
                .CapitalizedHumanReadableName = "Random id",
            },
            {
                .Value = 256,
                .Name = "schema",
                .HumanReadableName = "schema",
                .CapitalizedHumanReadableName = "Schema",
            },
            {
                .Value = 258,
                .Name = "semaphore",
                .HumanReadableName = "semaphore",
                .CapitalizedHumanReadableName = "Semaphore",
            },
            {
                .Value = 259,
                .Name = "semaphore_set",
                .HumanReadableName = "semaphore set",
                .CapitalizedHumanReadableName = "Semaphore set",
            },
            {
                .Value = 202,
                .Name = "timestamp_id",
                .HumanReadableName = "timestamp id",
                .CapitalizedHumanReadableName = "Timestamp id",
            },
            {
                .Value = 6,
                .Name = "typographer",
                .HumanReadableName = "typographer",
                .CapitalizedHumanReadableName = "Typographer",
            },
            {
                .Value = 9,
                .Name = "user",
                .HumanReadableName = "user",
                .CapitalizedHumanReadableName = "User",
            },
            {
                .Value = 257,
                .Name = "watch_log_consumer",
                .HumanReadableName = "watch log consumer",
                .CapitalizedHumanReadableName = "Watch log consumer",
            },
        });
}

////////////////////////////////////////////////////////////////////////////////

void EnsureObjectTypeRegistryInitialized()
{
    // Prevent redundant initializations during unittests execution.
    static std::once_flag Initialized;
    std::call_once(Initialized, [] {
        NYT::NOrm::NClient::NObjects::SetGlobalObjectTypeRegistry(CreateObjectTypeRegistry());
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NClient::NObjects
