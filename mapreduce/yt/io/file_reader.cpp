#include "file_reader.h"

#include <mapreduce/yt/common/log.h>
#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/http/http.h>
#include <mapreduce/yt/http/requests.h>
#include <mapreduce/yt/http/error.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TFileReader::TFileReader(
    const TRichYPath& path,
    const Stroka& serverName,
    const TTransactionId& transactionId)
    : Path_(AddPathPrefix(path))
    , ServerName_(serverName)
    , TransactionId_(transactionId)
{
    Stroka requestId;
    try {
        Stroka proxyName = GetProxyForHeavyRequest(ServerName_);

        THttpHeader header("GET", "download");
        header.AddTransactionId(TransactionId_);
        header.SetDataStreamFormat(DSF_BYTES);
        header.SetParameters(YPathToJsonString(Path_));

        Request_.Reset(new THttpRequest(proxyName));
        requestId = Request_->GetRequestId();

        Request_->Connect();
        Request_->StartRequest(header);
        Request_->FinishRequest();

        Input_ = Request_->GetResponseStream();

        LOG_DEBUG("RSP %s - file stream", ~requestId);

    } catch (TErrorResponse& e) {
        LOG_ERROR("RSP %s - failed", ~requestId);
        throw;

    } catch (yexception& e) {
        LOG_ERROR("RSP %s - %s - failed", ~requestId, e.what());
        throw;
    }
}

size_t TFileReader::DoRead(void* buf, size_t len)
{
    return Input_->Read(buf, len);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
