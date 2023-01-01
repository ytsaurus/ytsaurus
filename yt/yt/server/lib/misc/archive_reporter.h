#pragma once

#include "public.h"
#include "config.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/table_client/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A primitive for getting an archive version.
//! It allows the uploader to update the format version on the go.
class TArchiveVersionHolder final
{
public:
    int Get() const;
    void Set(int version);

private:
    std::atomic<int> Version_ = -1;
};

DEFINE_REFCOUNTED_TYPE(TArchiveVersionHolder)

////////////////////////////////////////////////////////////////////////////////

struct IArchiveRowlet
{
    virtual size_t EstimateSize() const = 0;

    //! Converts an inner representation of data to a row in the specified archive version.
    //! Returns null-row if data cannot be represented as a row in the specified archive version.
    virtual NTableClient::TUnversionedOwningRow ToRow(int archiveVersion) const = 0;

    virtual ~IArchiveRowlet() = default;
};

////////////////////////////////////////////////////////////////////////////////

//! This class batches and uploads data to a dynamic table in the background.
//! Rows can be dropped if the specified limit is exceeded.
struct IArchiveReporter
    : public TRefCounted
{
    virtual void Enqueue(std::unique_ptr<IArchiveRowlet> row) = 0;
    virtual void SetEnabled(bool enable) = 0;
    virtual int ExtractWriteFailuresCount() = 0;
    virtual bool IsQueueTooLarge() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IArchiveReporter)

IArchiveReporterPtr CreateArchiveReporter(
    TArchiveVersionHolderPtr version,
    TArchiveReporterConfigPtr reporterConfig,
    TArchiveHandlerConfigPtr handlerConfig,
    NTableClient::TNameTablePtr nameTable,
    TString reporterName,
    NApi::NNative::IClientPtr client,
    IInvokerPtr invoker,
    NProfiling::TProfiler profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
