#pragma once

#include "public.h"
#include "config.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/table_client/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////
 
//! A primitive for getting an archive version.
//! It allows the uploader to update the format version on the go.
class TArchiveVersionHolder
    : public TRefCounted
{
public:
    void Set(int version);

    int Get() const;

private:
    std::atomic<int> Version_ = {-1};
};

DEFINE_REFCOUNTED_TYPE(TArchiveVersionHolder)

////////////////////////////////////////////////////////////////////////////////

class IArchiveRowlet
{
public:
    virtual size_t EstimateSize() const = 0;
    
    //! Converts an inner representation of data to a row in the specified archive version.
    //! Returns null-row if data cannot be represented as a row in the specified archive version.
    virtual NTableClient::TUnversionedOwningRow ToRow(int archiveVersion) const = 0;

    virtual ~IArchiveRowlet() = default;
};

////////////////////////////////////////////////////////////////////////////////

//! This class batches and uploads data to a dynamic table in the background.
//! Rows can be dropped if the specified limit is exceeded.
class TArchiveReporter
    : public TRefCounted
{
public:
    TArchiveReporter(
        TArchiveVersionHolderPtr version,
        TArchiveReporterConfigPtr config,
        NTableClient::TNameTablePtr nameTable,
        TString reporterName,
        NApi::NNative::IClientPtr client,
        IInvokerPtr invoker,
        const NProfiling::TProfiler& profiler);

    ~TArchiveReporter();

    void Enqueue(std::unique_ptr<IArchiveRowlet> row);

    void SetEnabled(bool enable);
    int ExtractWriteFailuresCount();
    bool QueueIsTooLarge() const;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TArchiveReporter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
