#pragma once

#include "proxy_input.h"

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
        EDataStreamFormat format,
        const TTableReaderOptions& options);

    bool OnStreamError(const yexception& e, ui32 rangeIndex, ui64 rowIndex) override;
    bool HasRangeIndices() const override { return true; }

protected:
    size_t DoRead(void* buf, size_t len) override;

private:
    TRichYPath Path_;
    TAuth Auth_;
    TTransactionId TransactionId_;
    EDataStreamFormat Format_;
    TTableReaderOptions Options_;

    THolder<TPingableTransaction> ReadTransaction_;

    THolder<THttpRequest> Request_;
    TInputStream* Input_;

    int RetriesLeft_;

private:
    void CreateRequest(bool initial, ui32 rangeIndex = 0ul, ui64 rowIndex = 0ull);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
