#pragma once

#include <mapreduce/yt/interface/io.h>

#include <mapreduce/yt/http/requests.h>
#include <mapreduce/yt/http/http.h>

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
        const TAuth& auth,
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
    const TAuth Auth_;
    TTransactionId ParentTransactionId_;
    TMaybe<TFormat> Format_;
    TTableReaderOptions Options_;

    THolder<TPingableTransaction> ReadTransaction_;

    THolder<THttpRequest> Request_;
    THttpResponse* Input_;

    int InitialRetryCount_;
    int RetriesLeft_;

private:
    void TransformYPath();
    void CreateRequest(const TMaybe<ui32>& rangeIndex = Nothing(), const TMaybe<ui64>& rowIndex = Nothing());
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
