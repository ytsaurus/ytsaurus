#include "client_writer.h"

#include "retryful_writer.h"
#include "retryless_writer.h"

#include <mapreduce/yt/interface/io.h>
#include <mapreduce/yt/common/helpers.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TClientWriter::TClientWriter(
    const TRichYPath& path,
    IClientRetryPolicyPtr clientRetryPolicy,
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TMaybe<TFormat>& format,
    const TTableWriterOptions& options)
    : BUFFER_SIZE(options.BufferSize_)
{
    if (options.SingleHttpRequest_) {
        RawWriter_.Reset(new TRetrylessWriter(
            auth,
            transactionId,
            GetWriteTableCommand(),
            format,
            path,
            BUFFER_SIZE,
            options));
    } else {
        RawWriter_.Reset(new TRetryfulWriter(
            std::move(clientRetryPolicy),
            auth,
            transactionId,
            GetWriteTableCommand(),
            format,
            path,
            options));
    }
}

size_t TClientWriter::GetStreamCount() const
{
    return 1;
}

IOutputStream* TClientWriter::GetStream(size_t tableIndex) const
{
    Y_UNUSED(tableIndex);
    return RawWriter_.Get();
}

void TClientWriter::OnRowFinished(size_t)
{
    RawWriter_->NotifyRowEnd();
}

void TClientWriter::Abort()
{
    RawWriter_->Abort();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
