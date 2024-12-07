#include <yt/yt/server/lib/misc/cluster_connection.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/chunk_client/chunk_spec_fetcher.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>
#include <yt/yt/ytlib/chunk_client/proto/chunk_owner_ypath.pb.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/file_client/file_chunk_reader.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/library/program/config.h>
#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/helpers.h>

#include <yt/yt/client/chunk_client/read_limit.h>
#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/ema_counter.h>

#include <random>

namespace NYT {

using namespace NNodeTrackerClient;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NTransactionClient;
using namespace NObjectClient;
using namespace NYTree;
using namespace NNet;
using namespace NFileClient;
using namespace NProfiling;

static NLogging::TLogger Logger("Downloader");

////////////////////////////////////////////////////////////////////////////////

class TNativeFileDownloader
    : public TProgram
{
public:
    TNativeFileDownloader()
    {
        Opts_.AddLongOption("src-ypath").StoreResult(&SrcYPath_);
        Opts_.AddLongOption("dst-path").StoreResult(&DstPath_);
        Opts_.AddLongOption("cluster-proxy").StoreResult(&ClusterProxy_);
        Opts_.AddLongOption("user").StoreResult(&User_);
        Opts_.AddLongOption("repeat").StoreResult(&Repeat_);
    }

protected:
    void DoRun(const NLastGetopt::TOptsParseResult& /*parseResult*/) override
    {
        ConfigureUids();
        ConfigureIgnoreSigpipe();
        ConfigureCrashHandler();
        ConfigureExitZeroOnSigterm();
        auto singletonsConfig = New<TSingletonsConfig>();
        singletonsConfig->Stockpile->ThreadCount = 0;
        ConfigureSingletons(singletonsConfig);

        ActionQueue_ = New<TActionQueue>();

        WaitFor(BIND([&] {
            SetupClient();
            FetchChunkSpecs();
            do {
                DownloadFile();
            } while (Repeat_);
        })
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run())
            .ThrowOnError();
    }

    void SetupClient()
    {
        auto clusterConnection = DownloadClusterConnection(ClusterProxy_, Logger);
        auto config = ConvertTo<NApi::NNative::TConnectionCompoundConfigPtr>(clusterConnection);

        Connection_ = NApi::NNative::CreateConnection(config);
        Client_ = NApi::NNative::CreateClient(Connection_, NApi::TClientOptions{.User = User_});
    }

    void FetchChunkSpecs()
    {
        TUserObject file(SrcYPath_);

        YT_LOG_INFO("Getting basic attributes");

        GetUserObjectBasicAttributes(
            Client_,
            {&file},
            NullTransactionId,
            Logger,
            EPermission::Read);

        YT_LOG_INFO("Basic attributes collected (ObjectId: %v)", file.ObjectId);

        YT_VERIFY(file.Type == EObjectType::File);

        YT_LOG_INFO("Fetching chunk specs");

        auto chunkSpecFetcher = New<TMasterChunkSpecFetcher>(
            Client_,
            NApi::TMasterReadOptions{},
            Connection_->GetNodeDirectory(),
            GetCurrentInvoker(),
            /*maxChunksPerFetch*/ 100'000,
            /*maxChunksPerLocateRequest*/ 10'000,
            [&] (const TChunkOwnerYPathProxy::TReqFetchPtr& req, int /*fileIndex*/) {
                req->set_fetch_all_meta_extensions(false);
                req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
            },
            Logger);

        chunkSpecFetcher->Add(
            file.ObjectId,
            file.ExternalCellTag,
            file.ChunkCount,
            /*fileIndex*/ 0,
            {TReadRange()});

        WaitFor(chunkSpecFetcher->Fetch())
            .ThrowOnError();

        ChunkSpecs_ = std::move(chunkSpecFetcher->ChunkSpecs());

        FileLength_ = 0;
        for (const auto& chunkSpec : ChunkSpecs_) {
            auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(chunkSpec.chunk_meta().extensions());
            FileLength_ += miscExt.uncompressed_data_size();
        }

        YT_LOG_INFO(
            "Chunk specs fetched (FileLength: %v, ChunkCount: %v)",
            FileLength_,
            chunkSpecFetcher->ChunkSpecs().size());
    }

    void DownloadFile()
    {
        auto readerOptions = New<TMultiChunkReaderOptions>();

        auto chunkReaderHost = New<TChunkReaderHost>(
            Client_,
            TNodeDescriptor(std::string(GetLocalHostName())),
            GetNullBlockCache(),
            /*chunkMetaCache*/ nullptr,
            /*nodeStatusDirectory*/ nullptr,
            /*bandwidthThrottler*/ GetUnlimitedThrottler(),
            /*rpsThrottler*/ GetUnlimitedThrottler(),
            /*mediumThrottler*/ GetUnlimitedThrottler(),
            /*trafficMeter*/ nullptr);

        auto readerConfig = New<TMultiChunkReaderConfig>();
        TClientChunkReadOptions chunkReadOptions{
            .WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::Idle, 0, TInstant::Zero(), {"Download file"}),
            .ReadSessionId = TReadSessionId::Create(),
        };
        auto reader = CreateFileMultiChunkReader(
            readerConfig,
            readerOptions,
            std::move(chunkReaderHost),
            chunkReadOptions,
            ChunkSpecs_,
            MakeFileDataSource(std::nullopt));

        TFile file(DstPath_, CreateAlways | WrOnly | Seq);
        TUnbufferedFileOutput fileOutput(file);

        i64 writtenBytes = 0;

        TWallTimer timer;
        TBlock block;

        TEmaCounter<i64> speedCounter({TDuration::Seconds(5)});

        while (reader->ReadBlock(&block)) {
            if (block.Data.Empty()) {
                auto error = WaitFor(reader->GetReadyEvent());
                error.ThrowOnError();
            } else {
                fileOutput.Write(block.Data.Begin(), block.Size());
                file.FlushData();
                writtenBytes += block.Size();
                speedCounter.Update(writtenBytes);
                YT_LOG_INFO(
                    "Download progress (Bytes: %v/%v, Fraction: %.2v, Speed: %v MiB/sec)",
                    writtenBytes,
                    FileLength_,
                    double(writtenBytes) / FileLength_,
                    speedCounter.GetRate(0).value_or(0) / 1024.0 / 1024.0);
            }
        }
        fileOutput.Flush();
        fileOutput.Finish();
        file.Flush();
        YT_LOG_INFO(
            "Download complete (Bytes: %v, Time: %v sec, Speed: %v MiB/sec)",
            writtenBytes,
            timer.GetElapsedTime().SecondsFloat(),
            speedCounter.GetRate(0).value_or(0) / 1024.0 / 1024.0);
    }

private:
    TString SrcYPath_;
    TString DstPath_;
    TString ClusterProxy_;
    TString User_;
    bool Repeat_ = false;

    TActionQueuePtr ActionQueue_;

    NApi::NNative::IConnectionPtr Connection_;
    NApi::NNative::IClientPtr Client_;

    std::vector<NChunkClient::NProto::TChunkSpec> ChunkSpecs_;

    i64 FileLength_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

int main(int argc, const char** argv)
{
    return NYT::TNativeFileDownloader().Run(argc, argv);
}
