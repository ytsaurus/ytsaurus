#include "yt_directory_last_file_source.h"

#include "yt_file_source.h"

#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/cache/cache.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/ypath/helpers.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {

using namespace NApi;
using namespace NConcurrency;
using namespace NHydra;
using namespace NObjectClient;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TYTDirectoryLastFileSourceParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
}

TFuture<TFileSourceRevisionPtr> TYTDirectoryLastFileSource::Discover()
{
    auto directoryPath = GetParameters()->Path;
    auto cluster = directoryPath.GetCluster()
        ? directoryPath.GetCluster()
        : GetContext()->PipelinePath.GetCluster();
    THROW_ERROR_EXCEPTION_UNLESS(
        cluster,
        "Pipeline path must have a cluster to resolve YT directory file source path %v",
        directoryPath);
    auto client = GetContext()->ClientsCache->GetClient(*cluster);

    TListNodeOptions options;
    options.Attributes = {"id", "type", "revision", "uncompressed_data_size"};
    return client->ListNode(directoryPath.GetPath(), options)
        .Apply(BIND([directoryPath = std::move(directoryPath), cluster = *cluster] (const NYson::TYsonString& listYson) {
            auto list = ConvertToNode(listYson);

            std::pair<std::string, INodePtr> selected;
            for (const auto& child : list->AsList()->GetChildren()) {
                if (child->Attributes().Get<EObjectType>("type") != EObjectType::File) {
                    continue;
                }
                auto name = ConvertTo<std::string>(child);
                if (!selected.second || name > selected.first) {
                    selected = {std::move(name), child};
                }
            }
            if (!selected.second) {
                return TFileSourceRevisionPtr{};
            }

            auto childPath = directoryPath;
            childPath.SetCluster(cluster);
            childPath.SetPath(YPathJoin(directoryPath.GetPath(), selected.first));
            return MakeYTFileSourceRevision(
                TypeName<TYTDirectoryLastFileSource>(),
                childPath,
                cluster,
                selected.second->Attributes().Get<TObjectId>("id"),
                selected.second->Attributes().Get<TRevision>("revision"),
                selected.second->Attributes().Get<i64>("uncompressed_data_size"),
                selected.first);
        }));
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
