#pragma once

#include <mapreduce/yt/interface/io.h>
#include <mapreduce/yt/http/requests.h>

class TInputStream;

namespace NYT {

class THttpRequest;

////////////////////////////////////////////////////////////////////////////////

class TFileReader
    : public IFileReader
{
public:
    TFileReader(
        const TRichYPath& path,
        const TAuth& auth,
        const TTransactionId& transactionId);

protected:
    virtual size_t DoRead(void* buf, size_t len) override;

private:
    TRichYPath Path_;
    TAuth Auth_;
    TTransactionId TransactionId_;

    THolder<THttpRequest> Request_;
    TInputStream* Input_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
