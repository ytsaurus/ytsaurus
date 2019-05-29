#pragma once

#include <mapreduce/yt/interface/io.h>
#include <mapreduce/yt/http/requests.h>

class IInputStream;

namespace NYT {

class THttpRequest;
class TPingableTransaction;

namespace NDetail {
////////////////////////////////////////////////////////////////////////////////

class TStreamReaderBase
    : public IFileReader
{
public:
    TStreamReaderBase(
        IClientRetryPolicyPtr clientRetryPolicy,
        const TAuth& auth,
        const TTransactionId& transactionId);

    ~TStreamReaderBase();

protected:
    TYPath Snapshot(const TYPath& path);

private:
    size_t DoRead(void* buf, size_t len) override;
    virtual THolder<THttpRequest> CreateRequest(const TAuth& auth, const TTransactionId& transactionId, ui64 readBytes) = 0;
    TString GetActiveRequestId() const;

private:
    const IClientRetryPolicyPtr ClientRetryPolicy_;
    const TAuth Auth_;
    TFileReaderOptions FileReaderOptions_;

    THolder<THttpRequest> Request_;
    IInputStream* Input_ = nullptr;

    THolder<TPingableTransaction> ReadTransaction_;

    ui64 CurrentOffset_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TFileReader
    : public TStreamReaderBase
{
public:
    TFileReader(
        const TRichYPath& path,
        IClientRetryPolicyPtr clientRetryPolicy,
        const TAuth& auth,
        const TTransactionId& transactionId,
        const TFileReaderOptions& options = TFileReaderOptions());

private:
    THolder<THttpRequest> CreateRequest(const TAuth& auth, const TTransactionId& transactionId, ui64 readBytes) override;

private:
    TFileReaderOptions FileReaderOptions_;

    TRichYPath Path_;
    const ui64 StartOffset_;
    const TMaybe<ui64> EndOffset_;
};

////////////////////////////////////////////////////////////////////////////////

class TBlobTableReader
    : public TStreamReaderBase
{
public:
    TBlobTableReader(
        const TYPath& path,
        const TKey& key,
        IClientRetryPolicyPtr clientRetryPolicy,
        const TAuth& auth,
        const TTransactionId& transactionId,
        const TBlobTableReaderOptions& options);

private:
    THolder<THttpRequest> CreateRequest(const TAuth& auth, const TTransactionId& transactionId, ui64 readBytes) override;

private:
    const TKey Key_;
    const TBlobTableReaderOptions Options_;
    TYPath Path_;
};

} // namespace NDetail
} // namespace NYT
