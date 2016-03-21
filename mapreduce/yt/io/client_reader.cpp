#include "client_reader.h"

#include "helpers.h"
#include "yamr_table_reader.h"

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

TClientReader::TClientReader(
    const TRichYPath& path,
    const TAuth& auth,
    const TTransactionId& transactionId,
    EDataStreamFormat format,
    const TTableReaderOptions& options)
    : Path_(path)
    , Auth_(auth)
    , TransactionId_(transactionId)
    , Format_(format)
    , Options_(options)
    , ReadTransaction_(new TPingableTransaction(auth, transactionId))
    , RetriesLeft_(TConfig::Get()->RetryCount)
{
    CreateRequest(true);
}

bool TClientReader::OnStreamError(const yexception& e, ui32 rangeIndex, ui64 rowIndex)
{
    LOG_ERROR("RSP %s - %s",
        ~Request_->GetRequestId(), e.what());

    if (--RetriesLeft_ == 0) {
        return false;
    }

    CreateRequest(false, rangeIndex, rowIndex);
    return true;
}

size_t TClientReader::DoRead(void* buf, size_t len)
{
    return Input_->Read(buf, len);
}

void TClientReader::CreateRequest(bool initial, ui32 rangeIndex, ui64 rowIndex)
{
    const int retryCount = TConfig::Get()->RetryCount;

    for (int attempt = 0; attempt < retryCount; ++attempt) {
        Stroka requestId;
        try {
            Stroka proxyName = GetProxyForHeavyRequest(Auth_);

            THttpHeader header("GET", GetReadTableCommand());
            header.SetToken(Auth_.Token);
            header.AddTransactionId(ReadTransaction_->GetId());
            header.AddParam("control_attributes[enable_row_index]", true);
            header.AddParam("control_attributes[enable_range_index]", true);
            header.SetDataStreamFormat(Format_);

            if (Format_ == DSF_YAMR_LENVAL) {
                auto format = GetTableFormat(Auth_, TransactionId_, Path_);
                if (format) {
                    header.SetFormat(NodeToYsonString(format.GetRef()));
                }
            }

            if (!initial) {
                auto& ranges = Path_.Ranges_;
                if (ranges.empty()) {
                    ranges.push_back(TReadRange());
                } else {
                    if (rangeIndex >= ranges.size()) {
                        LOG_FATAL("Range index %" PRIu32 " is out of range, input ranges count is %" PRISZT,
                            rangeIndex, ranges.size());
                    }
                    ranges.erase(ranges.begin(), ranges.begin() + rangeIndex);
                }
                ranges.begin()->LowerLimit(TReadLimit().RowIndex(rowIndex));
            }

            header.SetParameters(FormIORequestParameters(Path_, Options_));

            Request_.Reset(new THttpRequest(proxyName));
            requestId = Request_->GetRequestId();

            Request_->Connect();
            Request_->StartRequest(header);
            Request_->FinishRequest();

            THttpInput* httpInput = Request_->GetResponseStream();
            Input_ = httpInput;

            LOG_DEBUG("RSP %s - table stream", ~requestId);

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
