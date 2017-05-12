#pragma once

#include "public.h"

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/formats/public.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/table_client/public.h>
#include <yt/ytlib/table_client/schemaful_reader_adapter.h>

#include <yt/core/actions/public.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/blob.h>
#include <yt/core/misc/common.h>

#include <vector>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NScheduler {
namespace NProto {

class TQuerySpec;

} // namespace NProto
} // namespace NScheduler

namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TUserJobReadController
    : public TRefCounted
{
public:
    TUserJobReadController(
        IJobSpecHelperPtr jobSpecHelper,
        NApi::INativeClientPtr client,
        IInvokerPtr invoker,
        NNodeTrackerClient::TNodeDescriptor nodeDescriptor,
        TClosure onNetworkRelease,
        IUserJobIOFactoryPtr userJobIOFactory,
        TNullable<TString> udfDirectory);

    //! Returns closure that launches data transfer to given async output.
    TCallback<TFuture<void>()> PrepareJobInputTransfer(const NConcurrency::IAsyncOutputStreamPtr& asyncOutput);

    double GetProgress() const;
    TFuture<std::vector<TBlob>> GetInputContext() const;
    std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const;
    TNullable<NChunkClient::NProto::TDataStatistics> GetDataStatistics() const;
    void InterruptReader();
    std::vector<NChunkClient::TDataSliceDescriptor> GetUnreadDataSliceDescriptors() const;

private:
    TCallback<TFuture<void>()> PrepareInputActionsPassthrough(
        const NFormats::TFormat& format,
        const NConcurrency::IAsyncOutputStreamPtr& asyncOutput);

    TCallback<TFuture<void>()> PrepareInputActionsQuery(
        const NScheduler::NProto::TQuerySpec& querySpec,
        const NFormats::TFormat& format,
        const NConcurrency::IAsyncOutputStreamPtr& asyncOutput);

    void InitializeReader();
    void InitializeReader(NTableClient::TNameTablePtr nameTable, const NTableClient::TColumnFilter& columnFilter);

private:
    const IJobSpecHelperPtr JobSpecHelper_;
    const NApi::INativeClientPtr Client_;
    const IInvokerPtr SerializedInvoker_;
    const NNodeTrackerClient::TNodeDescriptor NodeDescriptor_;
    const TClosure OnNetworkRelease_;
    const IUserJobIOFactoryPtr UserJobIOFactory_;
    NTableClient::ISchemalessMultiChunkReaderPtr Reader_;
    std::vector<NFormats::ISchemalessFormatWriterPtr> FormatWriters_;
    TNullable<TString> UdfDirectory_;
    std::atomic<bool> Initialized_ = {false};
    std::atomic<bool> Interrupted_ = {false};

    NLogging::TLogger Logger;
};

DEFINE_REFCOUNTED_TYPE(TUserJobReadController)

////////////////////////////////////////////////////////////////////////////////

TUserJobReadControllerPtr CreateUserJobReadController(
        IJobSpecHelperPtr jobSpecHelper,
        NApi::INativeClientPtr client,
        IInvokerPtr invoker,
        NNodeTrackerClient::TNodeDescriptor nodeDescriptor,
        TClosure onNetworkRelease,
        TNullable<TString> udfDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
