#pragma once

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/transaction.h>
#include <yt/yt/ytlib/api/native/type_handler.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/library/auth/auth.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

class TQueueExporter
    : public TRefCounted
{
public:
    TQueueExporter() = default;

    explicit TQueueExporter(
        NApi::NNative::IClientPtr client,
        IInvokerPtr invoker,
        const NLogging::TLogger& logger);

    TFuture<void> RunExportIteration(
        NYPath::TYPath queue,
        NYPath::TYPath exportDirectory,
        TDuration exportPeriod);

private:
    const NApi::NNative::IClientPtr Client_;
    const IInvokerPtr Invoker_;

    const NLogging::TLogger Logger;
};

DEFINE_REFCOUNTED_TYPE(TQueueExporter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
