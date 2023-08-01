#pragma once

#include "public.h"

#include <yt/yt/server/lib/job_proxy/public.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/controller_agent/helpers.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/core/concurrency/async_rw_lock.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/net/public.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct TCoreResult
{
    NControllerAgent::TCoreInfos CoreInfos;
    NControllerAgent::NProto::TOutputResult BoundaryKeys;

    TCoreResult();
};

////////////////////////////////////////////////////////////////////////////////

class TGpuCoreReader
    : public TRefCounted
{
public:
    explicit TGpuCoreReader(const TString& corePipePath);

    //! Returns number of bytes available to read.
    i64 GetBytesAvailable() const;

    NNet::IConnectionReaderPtr CreateAsyncReader();

private:
    const TString Path_;
    int Fd_;
};

DEFINE_REFCOUNTED_TYPE(TGpuCoreReader)

////////////////////////////////////////////////////////////////////////////////

//! This class looks for cores appearing in `coreDirectoryPath' directory.
//! Each core with name `name' is delivered using two files:
//! "coreDirectoryPath/name.pipe" is a pipe with a core content.
//! "coreDirectoryPath/name.info" contains EOL-separated core attributes.
class TCoreWatcher
    : public TRefCounted
{
public:
    TCoreWatcher(
        TCoreWatcherConfigPtr config,
        TString coreDirectoryPath,
        IJobHostPtr jobHost,
        IInvokerPtr controlInvoker,
        NTableClient::TBlobTableWriterConfigPtr blobTableWriterConfig,
        NTableClient::TTableWriterOptionsPtr tableWriterOptions,
        NCypressClient::TTransactionId transaction,
        NChunkClient::TChunkListId chunkList);

    //! Should be called after job completion to obtain core watcher result.
    //! If `finalizationTimeout' is set, core watcher will wait for at least one core
    //! to appear for at most `finalizationTimeout'. If no core appeared in time, dummy core
    //! info will be created.
    TCoreResult Finalize(std::optional<TDuration> finalizationTimeout);

private:
    TFuture<void> GetCoreAppearedEvent() const;

    TCoreWatcherConfigPtr Config_;

    IInvokerPtr ControlInvoker_;
    IInvokerPtr IOInvoker_;

    NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;

    TString CoreDirectoryPath_;

    const IJobHostPtr JobHost_;
    const NTableClient::TBlobTableWriterConfigPtr BlobTableWriterConfig_;
    const NTableClient::TTableWriterOptionsPtr TableWriterOptions_;
    const NCypressClient::TTransactionId Transaction_;
    const NChunkClient::TChunkListId ChunkList_;

    THashSet<TString> SeenCoreNames_;

    int NextCoreIndex_ = 0;

    std::vector<TFuture<void>> CoreFutures_;

    TPromise<void> CoreAppearedPromise_ = NewPromise<void>();

    NConcurrency::TAsyncReaderWriterLock WriterLock_;

    TCoreResult CoreResult_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, CoreInfosLock_);

    TGpuCoreReaderPtr GpuCoreReader_;

    NLogging::TLogger Logger;

    void DoWatchCores();

    void DoProcessLinuxCore(const TString& coreName, int coreIndex);
    void DoProcessGpuCore(NConcurrency::IAsyncInputStreamPtr coreStream, int coreIndex);
    i64 DoReadCore(const NConcurrency::IAsyncInputStreamPtr& coreStream, const TString& coreName, int coreIndex);
    void DoAddCoreInfo(const NControllerAgent::NProto::TCoreInfo& coreInfo);
};


DEFINE_REFCOUNTED_TYPE(TCoreWatcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
