#pragma once

#include "transaction.h"

#include <mapreduce/yt/io/helpers.h>

#include <mapreduce/yt/interface/common.h>
#include <mapreduce/yt/interface/io.h>
#include <mapreduce/yt/http/http.h>
#include <mapreduce/yt/raw_client/raw_requests.h>

#include <library/cpp/threading/blocking_queue/blocking_queue.h>

#include <util/stream/output.h>
#include <util/generic/buffer.h>
#include <util/stream/buffer.h>
#include <util/system/thread.h>
#include <util/system/event.h>

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TRetryfulWriter
    : public TRawTableWriter
{
public:
    template <class TWriterOptions>
    TRetryfulWriter(
        IClientRetryPolicyPtr clientRetryPolicy,
        const TAuth& auth,
        const TTransactionId& parentId,
        const TString& command,
        const TMaybe<TFormat>& format,
        const TRichYPath& path,
        const TWriterOptions& options)
        : ClientRetryPolicy_(std::move(clientRetryPolicy))
        , Auth_(auth)
        , Command_(command)
        , Format_(format)
        , BufferSize_(GetBufferSize(options.WriterOptions_))
        , ParentTransactionId_(parentId)
        , WriteTransaction_()
        , FilledBuffers_(2)
        , EmptyBuffers_(2)
        , Buffer_(BufferSize_ * 2)
        , Thread_(TThread::TParams{SendThread, this}.SetName("retryful_writer"))
    {
        Parameters_ = FormIORequestParameters(path, options);

        auto secondaryPath = path;
        secondaryPath.Append_ = true;
        secondaryPath.Schema_.Clear();
        secondaryPath.CompressionCodec_.Clear();
        secondaryPath.ErasureCodec_.Clear();
        secondaryPath.OptimizeFor_.Clear();
        SecondaryParameters_ = FormIORequestParameters(secondaryPath, options);

        if (options.CreateTransaction_) {
            WriteTransaction_.ConstructInPlace(ClientRetryPolicy_, auth, parentId, TStartTransactionOptions());
            auto append = path.Append_.GetOrElse(false);
            auto lockMode = (append  ? LM_SHARED : LM_EXCLUSIVE);
            NDetail::NRawClient::Lock(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, WriteTransaction_->GetId(), path.Path_, lockMode);
        }

        EmptyBuffers_.Push(TBuffer(BufferSize_ * 2));
    }

    ~TRetryfulWriter() override;
    void NotifyRowEnd() override;
    void Abort() override;
    
    size_t GetRetryBlockRemainingSize() const
    {
      return (BufferSize_ > Buffer_.size()) ? (BufferSize_ - Buffer_.size()) : 0;
    }

protected:
    void DoWrite(const void* buf, size_t len) override;
    void DoFinish() override;

private:
    static size_t GetBufferSize(const TMaybe<TWriterOptions>& writerOptions);

private:
    const IClientRetryPolicyPtr ClientRetryPolicy_;
    const TAuth Auth_;
    TString Command_;
    TMaybe<TFormat> Format_;
    const size_t BufferSize_;

    TNode Parameters_;
    TNode SecondaryParameters_;

    TTransactionId ParentTransactionId_;
    TMaybe<TPingableTransaction> WriteTransaction_;

    NThreading::TBlockingQueue<TBuffer> FilledBuffers_;
    NThreading::TBlockingQueue<TBuffer> EmptyBuffers_;

    TBuffer Buffer_;

    TThread Thread_;
    bool Started_ = false;
    std::exception_ptr Exception_ = nullptr;

    enum EWriterState {
        Ok,
        Completed,
        Error,
    } WriterState_ = Ok;

private:
    void FlushBuffer(bool lastBlock);
    void Send(const TBuffer& buffer);
    void CheckWriterState();

    void SendThread();
    static void* SendThread(void* opaque);
};

////////////////////////////////////////////////////////////////////////////////

}
