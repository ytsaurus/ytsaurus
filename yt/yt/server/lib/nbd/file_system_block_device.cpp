#include "file_system_block_device.h"
#include "block_device.h"
#include "config.h"
#include "image_reader.h"
#include "profiler.h"

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

class TFileSystemBlockDevice
    : public IBlockDevice
{
public:
    TFileSystemBlockDevice(
        TString exportId,
        TFileSystemBlockDeviceConfigPtr config,
        IImageReaderPtr reader,
        IInvokerPtr invoker,
        const NLogging::TLogger& logger)
        : ExportId_(std::move(exportId))
        , Config_(std::move(config))
        , Reader_(std::move(reader))
        , Invoker_(std::move(invoker))
        , Logger(logger.WithTag("ExportId: %v", ExportId_))
        , TagSet_(TNbdProfilerCounters::MakeTagSet(Reader_->GetPath()))
        , TraceContext_(GetOrCreateTraceContext("FileSystemBlockDevice"))
    {
        TNbdProfilerCounters::Get()->GetGauge(TagSet_, "/device/count")
            .Update(FileBlockDeviceCount().Increment(TagSet_));
        TNbdProfilerCounters::Get()->GetCounter(TagSet_, "/device/created")
            .Increment(1);
    }

    ~TFileSystemBlockDevice()
    {
        TNbdProfilerCounters::Get()->GetGauge(TagSet_, "/device/count")
            .Update(FileBlockDeviceCount().Decrement(TagSet_));
        TNbdProfilerCounters::Get()->GetCounter(TagSet_, "/device/removed")
            .Increment(1);

        auto statistics = Reader_->GetStatistics();
        TNbdProfilerCounters::Get()->GetCounter(TagSet_, "/device/read_block_bytes_from_cache")
            .Increment(statistics.DataBytesReadFromCache);
        TNbdProfilerCounters::Get()->GetCounter(TagSet_, "/device/read_block_bytes_from_disk")
            .Increment(statistics.DataBytesReadFromDisk);
        TNbdProfilerCounters::Get()->GetCounter(TagSet_, "/device/read_block_meta_bytes_from_disk")
            .Increment(statistics.MetaBytesReadFromDisk);

        auto guard = TCurrentTraceContextGuard(TraceContext_);
        YT_LOG_INFO("Destroying file system block device (Path: %v, ReadBytes: %v, DataBytesReadFromCache: %v, DataBytesReadFromDisk: %v, MetaBytesReadFromDisk: %v)",
            Reader_->GetPath(),
            statistics.ReadBytes,
            statistics.DataBytesReadFromCache,
            statistics.DataBytesReadFromDisk,
            statistics.MetaBytesReadFromDisk);
    }

    i64 GetTotalSize() const override
    {
        return Reader_->GetSize();
    }

    bool IsReadOnly() const override
    {
        return true;
    }

    TString DebugString() const override
    {
        return Format("{CypressPath: %v}", Reader_->GetPath());
    }

    TString GetProfileSensorTag() const override
    {
        return Reader_->GetPath();
    }

    virtual TFuture<TSharedRef> Read(
        i64 offset,
        i64 length) override
    {
        auto guard = TCurrentTraceContextGuard(TraceContext_);

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

    TFuture<void> Flush() override
    {
        return VoidFuture;
    }

    TFuture<void> Initialize() override
    {
        return BIND(&TFileSystemBlockDevice::DoInitialize, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run();
    }

private:
    const TString ExportId_;
    const TFileSystemBlockDeviceConfigPtr Config_;
    const IImageReaderPtr Reader_;
    const IInvokerPtr Invoker_;
    const NLogging::TLogger Logger;
    const NProfiling::TTagSet TagSet_;
    const TTraceContextPtr TraceContext_;

    void DoInitialize()
    {
        YT_LOG_INFO("Initializing File system block divice (Path: %v)", Reader_->GetPath());

        Reader_->Initialize();

        YT_LOG_INFO("Initialized File system block device (Path: %v)", Reader_->GetPath());
    }

    static NProfiling::TTaggedCounters<int>& FileBlockDeviceCount()
    {
        static NProfiling::TTaggedCounters<int> result;
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

IBlockDevicePtr CreateFileSystemBlockDevice(
    TString exportId,
    TFileSystemBlockDeviceConfigPtr config,
    IImageReaderPtr reader,
    IInvokerPtr invoker,
    NLogging::TLogger logger)
{
    return New<TFileSystemBlockDevice>(
        std::move(exportId),
        std::move(config),
        std::move(reader),
        std::move(invoker),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
