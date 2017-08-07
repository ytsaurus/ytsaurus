#include "client_reader.h"

#include "helpers.h"
#include "yamr_table_reader.h"

#include <mapreduce/yt/common/log.h>
#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/serialize.h>
#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/http/requests.h>
#include <mapreduce/yt/http/error.h>
#include <mapreduce/yt/http/retry_request.h>
#include <mapreduce/yt/http/transaction.h>
#include <library/yson/json_writer.h>

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
    const TString& formatConfig,
    const TTableReaderOptions& options)
    : Path_(path)
    , Auth_(auth)
    , TransactionId_(transactionId)
    , Format_(format)
    , FormatConfig_(formatConfig)
    , Options_(options)
    , ReadTransaction_(new TPingableTransaction(auth, transactionId))
    , RetriesLeft_(TConfig::Get()->RetryCount)
{
    Lock(Auth_, ReadTransaction_->GetId(), path.Path_, "snapshot");
    TransformYPath();
    CreateRequest();
}

bool TClientReader::Retry(
    const TMaybe<ui32>& rangeIndex,
    const TMaybe<ui64>& rowIndex)
{
    if (--RetriesLeft_ == 0) {
        return false;
    }

    CreateRequest(rangeIndex, rowIndex);
    return true;
}

size_t TClientReader::DoRead(void* buf, size_t len)
{
    return Input_->Read(buf, len);
}

void TClientReader::TransformYPath()
{
    for (auto& range : Path_.Ranges_) {
        auto& exact = range.Exact_;
        if (IsTrivial(exact)) {
            continue;
        }

        if (exact.RowIndex_) {
            range.LowerLimit(TReadLimit().RowIndex(*exact.RowIndex_));
            range.UpperLimit(TReadLimit().RowIndex(*exact.RowIndex_ + 1));
            exact.RowIndex_.Clear();

        } else if (exact.Key_) {
            range.LowerLimit(TReadLimit().Key(*exact.Key_));

            auto lastPart = TNode::CreateEntity();
            lastPart.Attributes() = TNode()("type", "max");
            exact.Key_->Parts_.push_back(lastPart);

            range.UpperLimit(TReadLimit().Key(*exact.Key_));
            exact.Key_.Clear();
        }
    }
}

void TClientReader::CreateRequest(const TMaybe<ui32>& rangeIndex, const TMaybe<ui64>& rowIndex)
{
    const int lastAttempt = TConfig::Get()->ReadRetryCount - 1;

    for (int attempt = 0; attempt <= lastAttempt; ++attempt) {
        TString requestId;
        try {
            TString proxyName = GetProxyForHeavyRequest(Auth_);

            THttpHeader header("GET", GetReadTableCommand());
            header.SetToken(Auth_.Token);
            header.AddTransactionId(ReadTransaction_->GetId());
            header.AddParam("control_attributes[enable_row_index]", true);
            header.AddParam("control_attributes[enable_range_index]", true);
            header.SetDataStreamFormat(Format_);

            header.SetResponseCompression(TConfig::Get()->AcceptEncoding);

            if (Format_ == DSF_YAMR_LENVAL) {
                auto format = GetTableFormat(Auth_, TransactionId_, Path_);
                if (format) {
                    header.SetOutputFormat(NodeToYsonString(format.GetRef()));
                }
            } else if (Format_ == DSF_PROTO) {
                header.SetOutputFormat(FormatConfig_);
            }

            if (rowIndex.Defined()) {
                auto& ranges = Path_.Ranges_;
                if (ranges.empty()) {
                    ranges.push_back(TReadRange());
                } else {
                    if (rangeIndex.GetOrElse(0) >= ranges.size()) {
                        ythrow yexception()
                            << "range index " << rangeIndex.GetOrElse(0)
                            << " is out of range, input range count is " << ranges.size();
                    }
                    ranges.erase(ranges.begin(), ranges.begin() + rangeIndex.GetOrElse(0));
                }
                ranges.begin()->LowerLimit(TReadLimit().RowIndex(*rowIndex));
            }

            header.SetParameters(FormIORequestParameters(Path_, Options_));

            Request_.Reset(new THttpRequest(proxyName));
            requestId = Request_->GetRequestId();

            Request_->Connect();
            Request_->StartRequest(header);
            Request_->FinishRequest();

            Input_ = Request_->GetResponseStream();

            LOG_DEBUG("RSP %s - table stream", ~requestId);

        } catch (TErrorResponse& e) {
            LOG_ERROR("RSP %s - attempt %d failed",
                ~requestId, attempt);

            if (!NDetail::IsRetriable(e) || attempt == lastAttempt) {
                throw;
            }
            Sleep(NDetail::GetRetryInterval(e));
            continue;
        } catch (yexception& e) {
            LOG_ERROR("RSP %s - %s - attempt %d failed",
                ~requestId, e.what(), attempt);

            if (Request_) {
                Request_->InvalidateConnection();
            }
            if (attempt == lastAttempt) {
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
