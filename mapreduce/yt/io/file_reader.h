#pragma once

#include <mapreduce/yt/interface/io.h>
#include <mapreduce/yt/http/requests.h>

class IInputStream;

namespace NYT {

class THttpRequest;
class TPingableTransaction;

////////////////////////////////////////////////////////////////////////////////

class TFileReader
    : public IFileReader
{
public:
    TFileReader(
        const TRichYPath& path,
        const TAuth& auth,
        const TTransactionId& transactionId,
        const TFileReaderOptions& options = TFileReaderOptions());

    ~TFileReader();

protected:
    size_t DoRead(void* buf, size_t len) override;

private:
    void CreateRequest();
    TString GetActiveRequestId() const;

private:
    const TRichYPath Path_;
    const TAuth Auth_;
    TFileReaderOptions FileReaderOptions_;

    THolder<THttpRequest> Request_;
    IInputStream* Input_ = nullptr;

    THolder<TPingableTransaction> ReadTransaction_;

    ui64 CurrentOffset_;
    const TMaybe<ui64> EndOffset_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
