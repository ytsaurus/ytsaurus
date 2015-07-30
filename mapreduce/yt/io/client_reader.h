#pragma once

#include "proxy_input.h"

#include <mapreduce/yt/interface/common.h>
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
        const Stroka& serverName,
        const TTransactionId& transactionId,
        EDataStreamFormat format);

    virtual bool OnStreamError(const yexception& ex) override;
    virtual void OnRowFetched() override;

protected:
    virtual size_t DoRead(void* buf, size_t len) override;

private:
    TRichYPath Path_;
    Stroka ServerName_;
    TTransactionId TransactionId_;
    EDataStreamFormat Format_;

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
