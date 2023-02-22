#pragma once

#include <mapreduce/yt/common/fwd.h>

#include <mapreduce/yt/interface/io.h>

#include <mapreduce/yt/http/context.h>
#include <mapreduce/yt/http/requests.h>
#include <mapreduce/yt/http/http.h>
#include <mapreduce/yt/http/http_client.h>

namespace NYT {

class TPingableTransaction;

////////////////////////////////////////////////////////////////////////////////

class TClientReader
    : public TRawTableReader
{
public:
    TClientReader(
        const TRichYPath& path,
        IClientRetryPolicyPtr clientRetryPolicy,
        ITransactionPingerPtr transactionPinger,
        const TClientContext& context,
        const TTransactionId& transactionId,
        const TFormat& format,
        const TTableReaderOptions& options,
        bool useFormatFromTableAttributes);

    bool Retry(
        const TMaybe<ui32>& rangeIndex,
        const TMaybe<ui64>& rowIndex) override;

    void ResetRetries() override;

    bool HasRangeIndices() const override { return true; }

protected:
    size_t DoRead(void* buf, size_t len) override;

private:
    TRichYPath Path_;
    const IClientRetryPolicyPtr ClientRetryPolicy_;
    const TClientContext Context_;
    TTransactionId ParentTransactionId_;
    TMaybe<TFormat> Format_;
    TTableReaderOptions Options_;

    THolder<TPingableTransaction> ReadTransaction_;

    NHttpClient::IHttpResponsePtr Response_;
    IInputStream* Input_;

    IRequestRetryPolicyPtr CurrentRequestRetryPolicy_;

private:
    void TransformYPath();
    void CreateRequest(const TMaybe<ui32>& rangeIndex = Nothing(), const TMaybe<ui64>& rowIndex = Nothing());
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
