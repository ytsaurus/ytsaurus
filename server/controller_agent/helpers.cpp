#include "helpers.h"
#include "config.h"

#include "serialize.h"
#include "table.h"

#include <yt/server/scheduler/config.h>

#include <yt/ytlib/chunk_client/data_source.h>
#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/ytlib/scheduler/proto/output_result.pb.h>
#include <yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/hive/cluster_directory.h>

#include <yt/client/object_client/helpers.h>

#include <yt/client/table_client/row_buffer.h>

#include <yt/core/ytree/helpers.h>

namespace NYT {
namespace NControllerAgent {

using namespace NObjectClient;
using namespace NChunkClient;
using namespace NChunkPools;
using namespace NScheduler;
using namespace NTableClient;
using namespace NYTree;
using namespace NApi;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TString TrimCommandForBriefSpec(const TString& command)
{
    const int MaxBriefSpecCommandLength = 256;
    return
        command.length() <= MaxBriefSpecCommandLength
        ? command
        : command.substr(0, MaxBriefSpecCommandLength) + "...";
}

////////////////////////////////////////////////////////////////////////////////

NYTree::INodePtr UpdateSpec(NYTree::INodePtr templateSpec, NYTree::INodePtr originalSpec)
{
    if (!templateSpec) {
        return originalSpec;
    }
    return PatchNode(templateSpec, originalSpec);
}

////////////////////////////////////////////////////////////////////////////////

TString TLockedUserObject::GetPath() const
{
    return FromObjectId(ObjectId);
}

////////////////////////////////////////////////////////////////////////////////

void TUserFile::Persist(const TPersistenceContext& context)
{
    TUserObject::Persist(context);

    using NYT::Persist;
    Persist<TAttributeDictionaryRefSerializer>(context, Attributes);
    Persist(context, FileName);
    Persist(context, ChunkSpecs);
    Persist(context, ChunkCount);
    Persist(context, Type);
    Persist(context, Executable);
    Persist(context, Format);
    Persist(context, Schema);
    Persist(context, IsDynamic);
    if (context.GetVersion() >= 202000) {
        Persist(context, IsLayer);
    }
}

////////////////////////////////////////////////////////////////////////////////

TBoundaryKeys BuildBoundaryKeysFromOutputResult(
    const NScheduler::NProto::TOutputResult& boundaryKeys,
    const TEdgeDescriptor& edgeDescriptor,
    const TRowBufferPtr& rowBuffer)
{
    YCHECK(!boundaryKeys.empty());
    YCHECK(boundaryKeys.sorted());
    YCHECK(!edgeDescriptor.TableWriterOptions->ValidateUniqueKeys || boundaryKeys.unique_keys());

    auto trimAndCaptureKey = [&] (const TOwningKey& key) {
        int limit = edgeDescriptor.TableUploadOptions.TableSchema.GetKeyColumnCount();
        if (key.GetCount() > limit) {
            // NB: This can happen for a teleported chunk from a table with a wider key in sorted (but not unique_keys) mode.
            YCHECK(!edgeDescriptor.TableWriterOptions->ValidateUniqueKeys);
            return rowBuffer->Capture(key.Begin(), limit);
        } else {
            return rowBuffer->Capture(key.Begin(), key.GetCount());
        }
    };

    return TBoundaryKeys {
        trimAndCaptureKey(FromProto<TOwningKey>(boundaryKeys.min())),
        trimAndCaptureKey(FromProto<TOwningKey>(boundaryKeys.max())),
    };
}

void BuildFileSpecs(NScheduler::NProto::TUserJobSpec* jobSpec, const std::vector<TUserFile>& files)
{
    for (const auto& file : files) {
        auto* descriptor = file.IsLayer
            ? jobSpec->add_layers()
            : jobSpec->add_files();

        ToProto(descriptor->mutable_chunk_specs(), file.ChunkSpecs);

        if (file.Type == EObjectType::Table && file.IsDynamic && file.Schema.IsSorted()) {
            auto dataSource = MakeVersionedDataSource(
                file.GetPath(),
                file.Schema,
                file.Path.GetColumns(),
                file.Path.GetTimestamp().Get(AsyncLastCommittedTimestamp),
                file.Path.GetColumnRenameDescriptors().Get({}));

            ToProto(descriptor->mutable_data_source(), dataSource);
        } else {
            auto dataSource = file.Type == EObjectType::File
                ? MakeFileDataSource(file.GetPath())
                : MakeUnversionedDataSource(
                    file.GetPath(),
                    file.Schema,
                    file.Path.GetColumns(),
                    file.Path.GetColumnRenameDescriptors().Get({}));

            ToProto(descriptor->mutable_data_source(), dataSource);
        }

        if (!file.IsLayer) {
            descriptor->set_file_name(file.FileName);
            switch (file.Type) {
                case EObjectType::File:
                    descriptor->set_executable(file.Executable);
                    break;
                case EObjectType::Table:
                    descriptor->set_format(file.Format.GetData());
                    break;
                default:
                    Y_UNREACHABLE();
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

NNative::IConnectionPtr FindRemoteConnection(
    const NNative::IConnectionPtr& connection,
    TCellTag cellTag)
{
    if (cellTag == connection->GetCellTag()) {
        return connection;
    }

    auto remoteConnection = connection->GetClusterDirectory()->FindConnection(cellTag);
    if (!remoteConnection) {
        return nullptr;
    }

    return dynamic_cast<NNative::IConnection*>(remoteConnection.Get());
}

NNative::IConnectionPtr GetRemoteConnectionOrThrow(
    const NNative::IConnectionPtr& connection,
    TCellTag cellTag)
{
    auto remoteConnection = FindRemoteConnection(connection, cellTag);
    if (!remoteConnection) {
        THROW_ERROR_EXCEPTION("Cannot find cluster with cell tag %v", cellTag);
    }
    return remoteConnection;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT

