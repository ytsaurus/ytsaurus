#include "file_reader.h"

#include "helpers.h"

#include <mapreduce/yt/common/log.h>
#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/common/config.h>

#include <mapreduce/yt/http/error.h>
#include <mapreduce/yt/http/http.h>
#include <mapreduce/yt/http/requests.h>
#include <mapreduce/yt/http/transaction.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static TMaybe<ui64> GetEndOffset(const TFileReaderOptions& options) {
    if (options.Length_) {
        return options.Offset_.GetOrElse(0) + *options.Length_;
    } else {
        return Nothing();
    }
}

////////////////////////////////////////////////////////////////////////////////

TFileReader::TFileReader(
    const TRichYPath& path,
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TFileReaderOptions& options)
    : Path_(path)
    , Auth_(auth)
    , FileReaderOptions_(options)
    , ReadTransaction_(new TPingableTransaction(auth, transactionId))
    , CurrentOffset_(FileReaderOptions_.Offset_.GetOrElse(0))
    , EndOffset_(GetEndOffset(FileReaderOptions_))
{
    Lock(Auth_, ReadTransaction_->GetId(), path.Path_, "snapshot");

    DoRead(nullptr, 0);
}

TFileReader::~TFileReader()
{ }

void TFileReader::CreateRequest()
{
    TString proxyName = GetProxyForHeavyRequest(Auth_);

    THttpHeader header("GET", GetReadFileCommand());
    header.SetToken(Auth_.Token);
    header.AddTransactionId(ReadTransaction_->GetId());
    header.SetDataStreamFormat(DSF_BYTES);

    if (EndOffset_) {
        Y_VERIFY(*EndOffset_ >= CurrentOffset_);
        FileReaderOptions_.Length(*EndOffset_ - CurrentOffset_);
    }
    FileReaderOptions_.Offset(CurrentOffset_);
    header.SetParameters(FormIORequestParameters(Path_, FileReaderOptions_));

    header.SetResponseCompression(TConfig::Get()->AcceptEncoding);

    Request_.Reset(new THttpRequest(proxyName));

    Request_->Connect();
    Request_->StartRequest(header);
    Request_->FinishRequest();

    Input_ = Request_->GetResponseStream();

    LOG_DEBUG("RSP %s - file stream", ~Request_->GetRequestId());
}

TString TFileReader::GetActiveRequestId() const
{
    if (Request_) {
        return Request_->GetRequestId();
    } else {
        return "<no-active-request>";
    }
}

size_t TFileReader::DoRead(void* buf, size_t len)
{
    const int retryCount = TConfig::Get()->ReadRetryCount;
    for (int attempt = 1; attempt <= retryCount; ++attempt) {
        try {
            if (!Input_) {
                CreateRequest();
            }
            if (len == 0) {
                return 0;
            }
            const size_t read = Input_->Read(buf, len);
            CurrentOffset_ += read;
            return read;
        } catch (TErrorResponse& e) {
            LOG_ERROR("RSP %s - failed: %s (attempt %d of %d)", ~GetActiveRequestId(), e.what(), attempt, retryCount);
            if (!e.IsRetriable() || attempt == retryCount) {
                throw;
            }
            Sleep(e.GetRetryInterval());
        } catch (yexception& e) {
            LOG_ERROR("RSP %s - failed: %s (attempt %d of %d)", ~GetActiveRequestId(), e.what(), attempt, retryCount);
            if (Request_) {
                Request_->InvalidateConnection();
            }
            if (attempt == retryCount) {
                throw;
            }
            Sleep(TConfig::Get()->RetryInterval);
        }
        Input_ = nullptr;
    }
    Y_UNREACHABLE(); // we should either return or throw from loop above
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
