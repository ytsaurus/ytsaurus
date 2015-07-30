#include "client_reader.h"

#include <mapreduce/yt/common/log.h>
#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/serialize.h>
#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/http/requests.h>
#include <mapreduce/yt/http/error.h>
#include <mapreduce/yt/http/transaction.h>
#include <mapreduce/yt/yson/json_writer.h>

#include <library/json/json_writer.h>
#include <library/json/json_reader.h>

#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/stream/str.h>
#include <util/random/random.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {

int GetRowIndexFromHeaders(THttpInput* httpInput)
{
    const THttpHeaders& headers = httpInput->Headers();
    Stroka responseParameters;
    for (auto h = headers.Begin(); h != headers.End(); ++h) {
        if (h->Name() == "X-YT-Response-Parameters")
            responseParameters = h->Value();
    }

    TStringInput stream(responseParameters);
    NJson::TJsonValue value;
    NJson::ReadJsonTree(&stream, &value);
    auto& jsonMap = value.GetMap();
    auto it = jsonMap.find("start_row_index");
    if (it == jsonMap.end()) {
        LOG_FATAL("Cannot find start_row_index in header");
    } else {
        return it->Second().GetInteger();
    }
}

}

////////////////////////////////////////////////////////////////////////////////

TClientReader::TClientReader(
    const TRichYPath& path,
    const Stroka& serverName,
    const TTransactionId& transactionId,
    EDataStreamFormat format)
    : Path_(AddPathPrefix(path))
    , ServerName_(serverName)
    , TransactionId_(transactionId)
    , Format_(format)
    , ReadTransaction_(new TPingableTransaction(serverName, transactionId))
    , RetriesLeft_(TConfig::Get()->RetryCount)
{
    CreateRequest(true);
}

bool TClientReader::OnStreamError(const yexception& ex)
{
    LOG_ERROR("RSP %s - %s",
        ~Request_->GetRequestId(), ex.what());

    if (--RetriesLeft_ == 0) {
        return false;
    }
    CreateRequest(false);
    return true;
}

void TClientReader::OnRowFetched()
{
    ++RowIndex_;
}

size_t TClientReader::DoRead(void* buf, size_t len)
{
    size_t bytes = Input_->Read(buf, len);
    return bytes;
}

void TClientReader::CreateRequest(bool initial)
{
    const int retryCount = TConfig::Get()->RetryCount;

    for (int attempt = 0; attempt < retryCount; ++attempt) {
        Stroka requestId;
        try {
            Stroka proxyName = GetProxyForHeavyRequest(ServerName_);

            THttpHeader header("GET", "read");
            header.AddTransactionId(ReadTransaction_->GetId());
            header.SetDataStreamFormat(Format_);

            // for now assume we always use only the first range
            if (initial) {
                header.SetParameters(YPathToJsonString(Path_));
            } else {
                TRichYPath path = Path_;
                TReadRange range;
                if (!path.Ranges_.empty()) {
                    path.Ranges_.clear();
                    range = Path_.Ranges_[0];
                }
                range.LowerLimit(TReadLimit().RowIndex(RowIndex_));
                path.Ranges_.push_back(range);
                header.SetParameters(YPathToJsonString(path));
            }

            Request_.Reset(new THttpRequest(proxyName));
            requestId = Request_->GetRequestId();

            Request_->Connect();
            Request_->StartRequest(header);
            Request_->FinishRequest();

            THttpInput* httpInput = Request_->GetResponseStream();
            Input_ = httpInput;

            LOG_DEBUG("RSP %s - table stream", ~requestId);

            RowIndex_ = GetRowIndexFromHeaders(httpInput);

        } catch (TErrorResponse& e) {
            LOG_ERROR("RSP %s - attempt %d failed",
                ~requestId, attempt);

            if (!e.IsRetriable() || attempt + 1 == retryCount) {
                throw;
            }
            Sleep(e.GetRetryInterval());
            continue;

        } catch (yexception& e) {
            LOG_ERROR("RSP %s - %s - attempt %d failed",
                ~requestId, e.what(), attempt);

            if (attempt + 1 == retryCount) {
                throw;
            }
            Sleep(TConfig::Get()->RetryInterval);
            continue;
        }

        return;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
