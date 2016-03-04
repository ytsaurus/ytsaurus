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

    virtual bool OnStreamError(const yexception& ex) override;
    virtual void OnRowFetched() override;

protected:
    virtual size_t DoRead(void* buf, size_t len) override;

private:
    TRichYPath Path_;
    TAuth Auth_;
    TTransactionId TransactionId_;
    EDataStreamFormat Format_;
    TTableReaderOptions Options_;

    THolder<TPingableTransaction> ReadTransaction_;

    THolder<THttpRequest> Request_;
    TInputStream* Input_;
    int RowIndex_;

    int RetriesLeft_;

private:
    void CreateRequest(bool initial);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
