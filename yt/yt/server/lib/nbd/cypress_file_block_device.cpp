#include "cypress_file_block_device.h"
#include "block_device.h"
#include "profiler.h"
#include "random_access_file_reader.h"

#include <yt/yt/server/lib/nbd/private.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_spec_fetcher.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/replication_reader.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/file_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/file_client/file_ypath_proxy.h>

#include <yt/yt/library/profiling/tagged_counters.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

namespace NYT::NNbd {

using namespace NYTree;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

static TString GetFilesystem(
    const TUserObject& userObject,
    NApi::NNative::IClientPtr client,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

class TCypressFileBlockDevice
    : public IBlockDevice
{
public:
    TCypressFileBlockDevice(
        TString exportId,
        TCypressFileBlockDeviceConfigPtr config,
        NApi::NNative::IClientPtr client,
        IInvokerPtr invoker,
        const NLogging::TLogger& logger)
        : ExportId_(std::move(exportId))
        , Config_(std::move(config))
        , InThrottler_(GetUnlimitedThrottler())
        , OutRpsThrottler_(GetUnlimitedThrottler())
        , Client_(std::move(client))
        , Invoker_(std::move(invoker))
        , Logger(logger.WithTag("ExportId: %v", ExportId_))
        , TagSet_(TNbdProfilerCounters::MakeTagSet(Config_->Path))
    {
        TNbdProfilerCounters::Get()->GetGauge(TagSet_, "/device/count")
            .Update(FileBlockDeviceCount().Increment(TagSet_));
        TNbdProfilerCounters::Get()->GetCounter(TagSet_, "/device/created")
            .Increment(1);
    }

    TCypressFileBlockDevice(
        TString exportId,
        const ::google::protobuf::RepeatedPtrField<NChunkClient::NProto::TChunkSpec>& chunkSpecs,
        TCypressFileBlockDeviceConfigPtr config,
        IThroughputThrottlerPtr inThrottler,
        IThroughputThrottlerPtr outRpsThrottler,
        NApi::NNative::IClientPtr client,
        IInvokerPtr invoker,
        const NLogging::TLogger& logger)
        : ExportId_(std::move(exportId))
        , ChunkSpecs_(chunkSpecs)
        , Config_(std::move(config))
        , InThrottler_(std::move(inThrottler))
        , OutRpsThrottler_(std::move(outRpsThrottler))
        , Client_(std::move(client))
        , Invoker_(std::move(invoker))
        , Logger(logger.WithTag("ExportId: %v", ExportId_))
        , TagSet_(TNbdProfilerCounters::MakeTagSet(Config_->Path))
    {
        TNbdProfilerCounters::Get()->GetGauge(TagSet_, "/device/count")
            .Update(FileBlockDeviceCount().Increment(TagSet_));
        TNbdProfilerCounters::Get()->GetCounter(TagSet_, "/device/created")
            .Increment(1);
    }

    ~TCypressFileBlockDevice()
    {
        TNbdProfilerCounters::Get()->GetGauge(TagSet_, "/device/count")
            .Update(FileBlockDeviceCount().Decrement(TagSet_));
        TNbdProfilerCounters::Get()->GetCounter(TagSet_, "/device/removed")
            .Increment(1);

        auto statistics = Reader_->GetStatistics();
        TNbdProfilerCounters::Get()->GetCounter(TagSet_, "/device/read_block_bytes_from_cache")
            .Increment(statistics.ReadBlockBytesFromCache);
        TNbdProfilerCounters::Get()->GetCounter(TagSet_, "/device/read_block_bytes_from_disk")
            .Increment(statistics.ReadBlockBytesFromDisk);
        TNbdProfilerCounters::Get()->GetCounter(TagSet_, "/device/read_block_meta_bytes_from_disk")
            .Increment(statistics.ReadBlockMetaBytesFromDisk);

        YT_LOG_INFO("Destroying Cypress file block device (Path: %v, ReadBytes: %v, ReadBlockBytesFromCache: %v, ReadBlockBytesFromDisk: %v, ReadBlockMetaBytesFromDisk: %v)",
            Config_->Path,
            statistics.ReadBytes,
            statistics.ReadBlockBytesFromCache,
            statistics.ReadBlockBytesFromDisk,
            statistics.ReadBlockMetaBytesFromDisk);
    }

    virtual i64 GetTotalSize() const override
    {
        return Reader_->GetSize();
    }

    virtual bool IsReadOnly() const override
    {
        return true;
    }

    virtual TString DebugString() const override
    {
        return Format("{CypressPath: %v}", Config_->Path);
    }

    virtual TString GetProfileSensorTag() const override
    {
        return Config_->Path;
    }

    virtual TFuture<TSharedRef> Read(
        i64 offset,
        i64 length) override
    {
        auto traceContext = GetOrCreateTraceContext("ReadCypressFileBlockDevice");
        auto guard = TCurrentTraceContextGuard(std::move(traceContext));

        TNbdProfilerCounters::Get()->GetCounter(TagSet_, "/device/read_count").Increment(1);
        TNbdProfilerCounters::Get()->GetCounter(TagSet_, "/device/read_bytes").Increment(length);
        NProfiling::TEventTimerGuard readTimeGuard(TNbdProfilerCounters::Get()->GetTimer(TagSet_, "/device/read_time"));

        if (Config_->TestSleepBeforeRead != TDuration::Zero()) {
            YT_LOG_DEBUG("Sleep for testing purposes prior to starting a read (Offset: %v, Length: %v, Duration: %v)",
                offset,
                length,
                Config_->TestSleepBeforeRead);

            TDelayedExecutor::WaitForDuration(Config_->TestSleepBeforeRead);
        }

        return Reader_->Read(offset, length)
            .Apply(BIND([readTimeGuard = std::move(readTimeGuard), tagSet = TagSet_] (const TErrorOr<TSharedRef>& result) {
                if (!result.IsOK()) {
                    TNbdProfilerCounters::Get()->GetCounter(tagSet, "/device/read_errors").Increment(1);
                }

                return result.ValueOrThrow();
            }));
    }

    virtual TFuture<void> Write(
        i64 /*offset*/,
        const TSharedRef& /*data*/,
        const TWriteOptions& /*options*/) override
    {
        return MakeFuture(TError("Writes are not supported"));
    }

    virtual TFuture<void> Flush() override
    {
        return VoidFuture;
    }

    virtual TFuture<void> Initialize() override
    {
        return BIND(&TCypressFileBlockDevice::DoInitialize, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run();
    }

private:

    void DoInitialize()
    {
        YT_LOG_INFO("Initializing Cypress file block divice (Path: %v)", Config_->Path);

        NYPath::TRichYPath richPath{Config_->Path};

        if (ChunkSpecs_) {
            Reader_ = CreateRandomAccessFileReader(
                *ChunkSpecs_,
                Config_->Path,
                Client_,
                InThrottler_,
                OutRpsThrottler_,
                Logger);
        } else {
            auto userObject = GetUserObject(richPath, Client_, Logger);
            if (userObject.Type != NCypressClient::EObjectType::File) {
                THROW_ERROR_EXCEPTION("Invalid type of file %Qlv, expected %Qlv, but got %Qlv",
                    userObject.GetPath(),
                    NCypressClient::EObjectType::File,
                    userObject.Type);
            }

            auto filesystem = GetFilesystem(userObject, Client_, Logger);
            if (filesystem != "ext4" && filesystem != "squashfs") {
                THROW_ERROR_EXCEPTION("Invalid filesystem attribute %Qv of file %v",
                    filesystem,
                    userObject.GetPath());
            }

            Reader_ = CreateRandomAccessFileReader(
                userObject,
                Config_->Path,
                Client_,
                InThrottler_,
                OutRpsThrottler_,
                Logger);
        }

        YT_LOG_INFO("Initialized Cypress file block device (Path: %v)", Config_->Path);
    }

private:
    static NProfiling::TTaggedCounters<int>& FileBlockDeviceCount()
    {
        static NProfiling::TTaggedCounters<int> result;
        return result;
    }

    const TString ExportId_;
    const std::optional<::google::protobuf::RepeatedPtrField<NChunkClient::NProto::TChunkSpec>> ChunkSpecs_;
    const TCypressFileBlockDeviceConfigPtr Config_;
    const IThroughputThrottlerPtr InThrottler_;
    const IThroughputThrottlerPtr OutRpsThrottler_;
    const NApi::NNative::IClientPtr Client_;
    const IInvokerPtr Invoker_;
    const NLogging::TLogger Logger;
    const NProfiling::TTagSet TagSet_;
    IRandomAccessFileReaderPtr Reader_;
};

////////////////////////////////////////////////////////////////////////////////

//! Fetch object's filesystem attribute from Cypress.
static TString GetFilesystem(const TUserObject& userObject, NApi::NNative::IClientPtr client, const NLogging::TLogger& Logger)
{
    YT_LOG_INFO("Fetching file filesystem attribute (File: %v)", userObject.GetPath());

    auto proxy = NObjectClient::CreateObjectServiceReadProxy(
        client,
        NApi::EMasterChannelKind::Follower,
        userObject.ExternalCellTag);

    auto req = TYPathProxy::Get(userObject.GetObjectIdPath() + "/@");
    ToProto(req->mutable_attributes()->mutable_keys(), std::vector<TString>{
        "filesystem",
    });

    NObjectClient::AddCellTagToSyncWith(req, userObject.ObjectId);
    NCypressClient::SetTransactionId(req, NCypressClient::NullTransactionId);

    auto rspOrError = WaitFor(proxy.Execute(req));
    THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error requesting extended attributes for file %Qlv", userObject.GetPath());

    const auto& rsp = rspOrError.Value();
    auto attributes = ConvertToAttributes(NYson::TYsonString(rsp->value()));
    auto filesystem = attributes->Get<TString>("filesystem", "");

    YT_LOG_INFO("Fetched file filesystem attribute (File: %v, Filesystem: %v)", userObject.GetPath(), filesystem);

    return filesystem;
}

////////////////////////////////////////////////////////////////////////////////

IBlockDevicePtr CreateCypressFileBlockDevice(
    TString exportId,
    TCypressFileBlockDeviceConfigPtr exportConfig,
    NApi::NNative::IClientPtr client,
    IInvokerPtr invoker,
    NLogging::TLogger logger)
{
    return New<TCypressFileBlockDevice>(
        std::move(exportId),
        std::move(exportConfig),
        std::move(client),
        std::move(invoker),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

IBlockDevicePtr CreateCypressFileBlockDevice(
    TString exportId,
    const ::google::protobuf::RepeatedPtrField<NChunkClient::NProto::TChunkSpec>& chunkSpecs,
    TCypressFileBlockDeviceConfigPtr exportConfig,
    IThroughputThrottlerPtr inThrottler,
    IThroughputThrottlerPtr outRpsThrottler,
    NApi::NNative::IClientPtr client,
    IInvokerPtr invoker,
    NLogging::TLogger logger)
{
    return New<TCypressFileBlockDevice>(
        std::move(exportId),
        chunkSpecs,
        std::move(exportConfig),
        std::move(inThrottler),
        std::move(outRpsThrottler),
        std::move(client),
        std::move(invoker),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
