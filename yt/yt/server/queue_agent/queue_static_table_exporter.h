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

struct TQueueExportOptions
    : public NApi::TTransactionalOptions
{
    //! Every chunk with max_timestamp from misc ext laying between LowerExportTimestamp and UpperExportTimestamp includely will be exported.
    NTransactionClient::TTimestamp LowerExportTimestamp;
    NTransactionClient::TTimestamp UpperExportTimestamp;
};

////////////////////////////////////////////////////////////////////////////////

class TQueueExporter
    : public TRefCounted
{
public:
    TQueueExporter() = default;

    explicit TQueueExporter(
        NApi::NNative::IConnectionPtr connection,
        NApi::NNative::IClientPtr client,
        IInvokerPtr invoker,
        NLogging::TLogger logger);

    TFuture<void> ExportToStaticTable(
        const NYPath::TRichYPath& queuePath,
        const NYPath::TRichYPath& destinationPath,
        TQueueExportOptions options) const;

private:
    NApi::NNative::IConnectionPtr Connection_;
    NApi::NNative::IClientPtr Client_;
    IInvokerPtr Invoker_;

    NLogging::TLogger Logger;
};

DEFINE_REFCOUNTED_TYPE(TQueueExporter);

////////////////////////////////////////////////////////////////////////////////

TString GenerateStaticTableName(const NYPath::TRichYPath& queuePath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
