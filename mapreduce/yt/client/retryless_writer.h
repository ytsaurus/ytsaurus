#pragma once

#include "transaction.h"

#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/io/helpers.h>
#include <mapreduce/yt/interface/common.h>
#include <mapreduce/yt/interface/io.h>
#include <mapreduce/yt/http/http.h>
#include <mapreduce/yt/raw_client/raw_requests.h>

#include <util/stream/buffered.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TRetrylessWriter
    : public TRawTableWriter
{
public:
    template <class TWriterOptions>
    TRetrylessWriter(
        const TAuth& auth,
        const TTransactionId& parentId,
        const TString& command,
        const TMaybe<TFormat>& format,
        const TRichYPath& path,
        size_t bufferSize,
        const TWriterOptions& options)
        : Request_()
    {
        THttpHeader header("PUT", command);
        header.SetInputFormat(format);
        header.MergeParameters(FormIORequestParameters(path, options));
        header.AddTransactionId(parentId);
        header.SetRequestCompression(ToString(TConfig::Get()->ContentEncoding));
        header.SetToken(auth.Token);

        Request_.Connect(GetProxyForHeavyRequest(auth));
        auto* outputStream = Request_.StartRequest(header);
        BufferedOutput_.Reset(new TBufferedOutput(outputStream, bufferSize));
    }

    ~TRetrylessWriter() override;
    void NotifyRowEnd() override;
    void Abort() override;

protected:
    void DoWrite(const void* buf, size_t len) override;
    void DoFinish() override;

private:
    bool Running_ = true;
    THttpRequest Request_;
    THolder<TBufferedOutput> BufferedOutput_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
