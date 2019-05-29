#include "file_writer.h"

#include <mapreduce/yt/io/helpers.h>
#include <mapreduce/yt/interface/finish_or_die.h>

#include <mapreduce/yt/common/helpers.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TFileWriter::TFileWriter(
    const TRichYPath& path,
    const IClientRetryPolicyPtr clientRetryPolicy,
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TFileWriterOptions& options)
    : RetryfulWriter_(
        std::move(clientRetryPolicy),
        auth,
        transactionId,
        GetWriteFileCommand(),
        TMaybe<TFormat>(),
        path,
        BUFFER_SIZE,
        options)
{ }

TFileWriter::~TFileWriter()
{
    NDetail::FinishOrDie(this, "TFileWriter");
}

void TFileWriter::DoWrite(const void* buf, size_t len)
{
    RetryfulWriter_.Write(buf, len);
    RetryfulWriter_.NotifyRowEnd();
}

void TFileWriter::DoFinish()
{
    RetryfulWriter_.Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
