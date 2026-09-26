#include "client_impl.h"
#include "config.h"
#include "connection.h"
#include "file_reader.h"
#include "file_writer.h"

#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/file_client/file_chunk_reader.h>
#include <yt/yt/ytlib/file_client/helpers.h>
#include <yt/yt/ytlib/file_client/partition_utils.h>
#include <yt/yt/ytlib/file_client/proto/file_partition_cookie.pb.h>

#include <yt/yt/ytlib/node_tracker_client/node_directory_builder.h>

#include <yt/yt/client/api/file_reader.h>

#include <yt/yt/client/chunk_client/helpers.h>
#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/signature/generator.h>
#include <yt/yt/client/signature/signature.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NApi::NNative {

using namespace NChunkClient;
using namespace NCypressClient;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NYPath;

using NChunkClient::NProto::TChunkSpec;
using NChunkClient::NProto::TMiscExt;

////////////////////////////////////////////////////////////////////////////////

TFuture<IFileReaderPtr> TClient::CreateFileReader(
    const TYPath& path,
    const TFileReaderOptions& options)
{
    return NNative::CreateFileReader(this, path, options, HeavyRequestMemoryUsageTracker_);
}

IFileWriterPtr TClient::CreateFileWriter(
    const TRichYPath& path,
    const TFileWriterOptions& options)
{
    return NNative::CreateFileWriter(this, path, options, HeavyRequestMemoryUsageTracker_);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

void FillFilePartitionNodeDirectory(
    const TNodeDirectoryPtr& connectionNodeDirectory,
    NFileClient::NProto::TFilePartitionCookie* protoCookie)
{
    TNodeDirectoryBuilder nodeDirectoryBuilder(
        connectionNodeDirectory,
        protoCookie->mutable_node_directory());
    for (const auto& chunkSpec : protoCookie->chunk_specs()) {
        nodeDirectoryBuilder.Add(GetReplicasFromChunkSpec(chunkSpec));
    }
}

class TFilePartitionReader
    : public IFileReader
{
public:
    TFilePartitionReader(
        NFileClient::IFileReaderPtr reader,
        TObjectId id,
        NHydra::TRevision revision)
        : Reader_(std::move(reader))
        , Id_(id)
        , Revision_(revision)
    { }

    TFuture<TSharedRef> Read() override
    {
        TBlock block;
        if (!Reader_->ReadBlock(&block)) {
            return MakeFuture(TSharedRef());
        }

        if (block.Data) {
            return MakeFuture(block.Data);
        }

        return Reader_->GetReadyEvent().Apply(
            BIND(&TFilePartitionReader::Read, MakeStrong(this)));
    }

    TObjectId GetId() const override
    {
        return Id_;
    }

    NHydra::TRevision GetRevision() const override
    {
        return Revision_;
    }

private:
    const NFileClient::IFileReaderPtr Reader_;
    const TObjectId Id_;
    const NHydra::TRevision Revision_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

TFilePartitions TClient::DoPartitionFile(
    const TYPath& path,
    const std::vector<TFileReadRange>& ranges,
    const TPartitionFileOptions& options)
{
    const auto& config = Connection_->GetConfig();

    if (ranges.empty()) {
        THROW_ERROR_EXCEPTION("At least one file read range must be provided");
    }

    if (std::ssize(ranges) > config->MaxFilePartitionCount) {
        THROW_ERROR_EXCEPTION("Too many file partitions requested")
            .With("partition_count", std::ssize(ranges))
            .With("max_partition_count", config->MaxFilePartitionCount);
    }

    auto fileInfo = NFileClient::FetchFileObjectInfo(
        MakeStrong(this),
        path,
        options.TransactionId,
        NFileClient::TFetchFileObjectInfoOptions{
            .SuppressAccessTracking = options.SuppressAccessTracking,
            .SuppressExpirationTimeoutRenewal = options.SuppressExpirationTimeoutRenewal,
        },
        Logger);
    auto& userObject = fileInfo.UserObject;

    auto fetchChunkSpecConfig = options.FetchChunkSpecConfig
        ? options.FetchChunkSpecConfig
        : New<TFetchChunkSpecConfig>();

    auto chunkSpecs = FetchChunkSpecs(
        MakeStrong(this),
        Connection_->GetNodeDirectory(),
        userObject,
        {TReadRange()},
        fileInfo.ChunkCount,
        fetchChunkSpecConfig->MaxChunksPerFetch,
        fetchChunkSpecConfig->MaxChunksPerLocateRequest,
        [&] (const TChunkOwnerYPathProxy::TReqFetchPtr& req) {
            req->add_extension_tags(TProtoExtensionTag<TMiscExt>::Value);
            req->set_fetch_all_meta_extensions(false);
            NCypressClient::SetTransactionId(req, userObject.ExternalTransactionId);
            NCypressClient::SetSuppressAccessTracking(req, options.SuppressAccessTracking);
            NCypressClient::SetSuppressExpirationTimeoutRenewal(req, options.SuppressExpirationTimeoutRenewal);
        },
        Logger);

    auto cumulativeChunkSizes = NFileClient::BuildCumulativeChunkSizes(chunkSpecs);
    auto fileLength = cumulativeChunkSizes.back();

    auto dataSource = MakeFileDataSource(userObject.Path.GetPath());
    dataSource->SetObjectId(userObject.ObjectId);
    dataSource->SetAccount(userObject.Account);

    const auto& signatureGenerator = Connection_->GetSignatureGenerator();

    NFileClient::NProto::TFilePartitionCookie baseCookie;
    baseCookie.set_user(Options_.GetAuthenticatedUser());
    ToProto(baseCookie.mutable_object_id(), userObject.ObjectId);
    baseCookie.set_revision(ToProto(fileInfo.Revision));
    ToProto(baseCookie.mutable_data_source(), dataSource);

    TFilePartitions result;
    result.Partitions.reserve(ranges.size());

    for (const auto& range : ranges) {
        auto begin = range.Begin;
        auto end = std::min(range.End.value_or(fileLength), fileLength);

        if (begin < 0 || (range.End && *range.End < begin)) {
            THROW_ERROR_EXCEPTION("Invalid file read range")
                .With("begin", begin)
                .With("end", range.End);
        }
        if (begin > fileLength) {
            THROW_ERROR_EXCEPTION("File read range begin is past the end of file")
                .With("begin", begin)
                .With("file_length", fileLength);
        }

        auto cookieProto = baseCookie;

        auto slicedSpecs = NFileClient::SliceFileChunkSpecs(
            chunkSpecs,
            cumulativeChunkSizes,
            begin,
            end);
        for (auto& spec : slicedSpecs) {
            cookieProto.add_chunk_specs()->Swap(&spec);
        }

        if (cookieProto.chunk_specs_size() > config->MaxChunkSpecsPerFilePartition) {
            THROW_ERROR_EXCEPTION("File partition covers too many chunks")
                .With("begin", begin)
                .With("end", end)
                .With("chunk_spec_count", cookieProto.chunk_specs_size())
                .With("max_chunk_spec_count", config->MaxChunkSpecsPerFilePartition);
        }

        if (options.FetchCookieNodeDescriptors) {
            FillFilePartitionNodeDirectory(Connection_->GetNodeDirectory(), &cookieProto);
        }

        auto& partition = result.Partitions.emplace_back();
        partition.Length = end - begin;
        partition.Cookie = TFilePartitionCookiePtr(
            signatureGenerator->Sign(SerializeProtoToString(cookieProto)));
    }

    YT_TLOG_DEBUG("File partitioned")
        .With("Path", path)
        .With("FileLength", fileLength)
        .With("ChunkCount", chunkSpecs.size())
        .With("PartitionCount", result.Partitions.size());

    return result;
}

TFuture<IFileReaderPtr> TClient::CreateFilePartitionReader(
    const TFilePartitionCookiePtr& cookie,
    const TReadFilePartitionOptions& options)
{
    YT_VERIFY(cookie);

    return BIND([=, this, this_ = MakeStrong(this)] () -> IFileReaderPtr {
        NFileClient::NProto::TFilePartitionCookie cookieProto;
        if (!cookieProto.ParseFromString(cookie.Underlying()->Payload())) {
            THROW_ERROR_EXCEPTION("Failed to parse file partition cookie");
        }

        auto validateFieldPresent = [] (bool present, TStringBuf fieldName) {
            if (!present) {
                THROW_ERROR_EXCEPTION("Malformed file partition cookie")
                    .With("missing_field", fieldName);
            }
        };
        validateFieldPresent(cookieProto.has_user(), "user");
        validateFieldPresent(cookieProto.has_object_id(), "object_id");
        validateFieldPresent(cookieProto.has_revision(), "revision");
        validateFieldPresent(cookieProto.has_data_source(), "data_source");

        if (cookieProto.user() != Options_.GetAuthenticatedUser()) {
            THROW_ERROR_EXCEPTION("Partition must be read by the same user who created it")
                .With("read_partition_user", Options_.GetAuthenticatedUser())
                .With("partition_file_user", cookieProto.user());
        }

        if (cookieProto.has_node_directory()) {
            Connection_->GetNodeDirectory()->MergeFrom(cookieProto.node_directory());
        }

        TDataSourcePtr dataSource;
        FromProto(&dataSource, cookieProto.data_source());

        std::vector<TChunkSpec> chunkSpecs(cookieProto.chunk_specs().begin(), cookieProto.chunk_specs().end());

        auto config = options.Config ? options.Config : New<TFileReaderConfig>();

        TClientChunkReadOptions chunkReadOptions{
            .WorkloadDescriptor = config->WorkloadDescriptor,
            .ReadSessionId = TReadSessionId::Create(),
            .ChunkReaderStatistics = New<TChunkReaderStatistics>(),
            .MemoryUsageTracker = HeavyRequestMemoryUsageTracker_,
        };

        YT_TLOG_DEBUG("Creating file partition reader")
            .With("ChunkSpecCount", chunkSpecs.size())
            .With("ReadSessionId", chunkReadOptions.ReadSessionId);

        auto reader = NFileClient::CreateFileMultiChunkReader(
            config,
            New<TMultiChunkReaderOptions>(),
            New<TChunkReaderHost>(MakeStrong(this)),
            chunkReadOptions,
            std::move(chunkSpecs),
            dataSource);

        return New<TFilePartitionReader>(
            std::move(reader),
            FromProto<TObjectId>(cookieProto.object_id()),
            FromProto<NHydra::TRevision>(cookieProto.revision()));
    })
        .AsyncVia(NChunkClient::TDispatcher::Get()->GetReaderInvoker())
        .Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
