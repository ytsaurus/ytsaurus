#include "yt_directory_last_file_source.h"

#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/cache/cache.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/ypath/helpers.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {

using namespace NApi;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

bool IsSupportedChildType(EObjectType type)
{
    return type == EObjectType::File ||
        type == EObjectType::Table;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TYTDirectoryLastFileSourceParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
}

void TYTDirectoryLastFileSourceDynamicParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("pinned_file_name", &TThis::PinnedFileName)
        .Default();
    registrar.Postprocessor([] (TThis* parameters) {
        if (parameters->PinnedFileName) {
            ValidateFileSourceName(*parameters->PinnedFileName);
        }
    });
}

TFuture<TFileSourceRevisionPtr> TYTDirectoryLastFileSource::Discover()
{
    auto directoryPath = GetParameters()->Path;
    auto pinnedFileName = GetDynamicParameters()->PinnedFileName;
    auto cluster = directoryPath.GetCluster()
        ? directoryPath.GetCluster()
        : GetContext()->PipelinePath.GetCluster();
    THROW_ERROR_EXCEPTION_UNLESS(
        cluster,
        "Pipeline path must have a cluster to resolve YT directory file source path %v",
        directoryPath);
    auto client = GetContext()->ClientsCache->GetClient(*cluster);

    TListNodeOptions options;
    options.Attributes = {"type"};
    auto list = ConvertToNode(WaitFor(client->ListNode(directoryPath.GetPath(), options))
            .ValueOrThrow());

    std::optional<std::string> selectedName;
    for (const auto& child : list->AsList()->GetChildren()) {
        auto name = ConvertTo<std::string>(child);
        auto type = child->Attributes().Get<EObjectType>("type");
        if (pinnedFileName && name == *pinnedFileName) {
            THROW_ERROR_EXCEPTION_UNLESS(
                IsSupportedChildType(type),
                "Pinned YT directory child %Qv must be a Cypress file or a BLOB table",
                name)
                .With("actual_type", type);
            selectedName = std::move(name);
            break;
        }
        if (!pinnedFileName &&
            IsSupportedChildType(type) &&
            (!selectedName || name > *selectedName))
        {
            selectedName = std::move(name);
        }
    }

    THROW_ERROR_EXCEPTION_IF(
        pinnedFileName && !selectedName,
        "Pinned YT directory child %Qv does not exist",
        *pinnedFileName);
    if (!selectedName) {
        return MakeFuture<TFileSourceRevisionPtr>(nullptr);
    }

    auto childPath = directoryPath;
    childPath.SetCluster(*cluster);
    childPath.SetPath(YPathJoin(directoryPath.GetPath(), *selectedName));
    return DiscoverYTFileSource(
        GetContext(),
        TypeName<TYTDirectoryLastFileSource>(),
        childPath);
}

TFuture<void> TYTDirectoryLastFileSource::Download(
    const TFileSourceRevisionPtr& revision,
    const std::string& stagingDirectory)
{
    return DownloadYTFile(GetContext(), revision, stagingDirectory);
}

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_FILE_SOURCE(TYTDirectoryLastFileSource);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
