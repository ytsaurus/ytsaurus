#pragma once

#include <yt/yt/server/lib/job_agent/public.h>

#include <yt/yt/ytlib/table_client/public.h>
#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/api/operation_archive_schema.h>

#include <yt/yt/core/misc/optional.h>

#include <yt/yt/core/yson/string.h>

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

using NYT::NApi::TJobTableDescriptor;
using NYT::NApi::TJobSpecTableDescriptor;

////////////////////////////////////////////////////////////////////////////////

//! Periodically reports job statistics to the dynamic table.
class TJobReporter
    : public TRefCounted
{
public:
    TJobReporter(
        TJobReporterConfigPtr reporterConfig,
        const NApi::NNative::IConnectionPtr& masterConnection,
        std::optional<TString> localAddress = std::nullopt);

    ~TJobReporter();

    void HandleJobReport(TJobReport&& statistics);
    void SetEnabled(bool enable);
    void SetSpecEnabled(bool enable);
    void SetStderrEnabled(bool enable);
    void SetProfileEnabled(bool enable);
    void SetFailContextEnabled(bool enable);
    void SetOperationArchiveVersion(int version);
    int ExtractWriteFailuresCount();
    bool GetQueueIsTooLarge();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TJobReporter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
