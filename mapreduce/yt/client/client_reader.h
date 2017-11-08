#pragma once

#include <mapreduce/yt/io/proxy_input.h>

#include <mapreduce/yt/interface/io.h>

#include <mapreduce/yt/http/requests.h>
#include <mapreduce/yt/http/http.h>

namespace NYT {

class TPingableTransaction;

////////////////////////////////////////////////////////////////////////////////

class TClientReader
    : public TProxyInput
{
public:
    TClientReader(
        const TRichYPath& path,
        const TAuth& auth,
        const TTransactionId& transactionId,
        const TMaybe<TFormat>& format,
        const TTableReaderOptions& options);

    bool Retry(
        const TMaybe<ui32>& rangeIndex,
        const TMaybe<ui64>& rowIndex) override;

    bool HasRangeIndices() const override { return true; }

protected:
    size_t DoRead(void* buf, size_t len) override;

private:
    TRichYPath Path_;
    TAuth Auth_;
    TTransactionId ParentTransactionId_;
    TMaybe<TFormat> Format_;
    TTableReaderOptions Options_;

    THolder<TPingableTransaction> ReadTransaction_;

    THolder<THttpRequest> Request_;
    THttpResponse* Input_;

    int RetriesLeft_;

private:
    void TransformYPath();
    void CreateRequest(const TMaybe<ui32>& rangeIndex = Nothing(), const TMaybe<ui64>& rowIndex = Nothing());
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
