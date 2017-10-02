#pragma once

#include "helpers.h"

#include <mapreduce/yt/interface/common.h>
#include <mapreduce/yt/interface/io.h>
#include <mapreduce/yt/http/http.h>
#include <mapreduce/yt/http/transaction.h>
#include <mapreduce/yt/raw_client/raw_requests.h>

#include <util/stream/output.h>
#include <util/generic/buffer.h>
#include <util/stream/buffer.h>
#include <util/system/thread.h>
#include <util/system/event.h>

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TBlockWriter
    : public TRawTableWriter
{
public:
    template <class TWriterOptions>
    TBlockWriter(
        const TAuth& auth,
        const TTransactionId& parentId,
        const TString& command,
        EDataStreamFormat format,
        const TString& formatConfig,
        const TRichYPath& path,
        size_t bufferSize,
        const TWriterOptions& options)
        : Auth_(auth)
        , Command_(command)
        , Format_(format)
        , FormatConfig_(formatConfig)
        , BufferSize_(bufferSize)
        , ParentTransactionId_(parentId)
        , WriteTransaction_()
        , Buffer_(BufferSize_ * 2)
        , BufferOutput_(Buffer_)
        , Thread_(TThread::TParams{SendThread, this}.SetName("block_writer"))
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
            WriteTransaction_.ConstructInPlace(auth, parentId);
            auto append = path.Append_.GetOrElse(false);
            auto lockMode = (append  ? LM_SHARED : LM_EXCLUSIVE);
            NDetail::Lock(Auth_, WriteTransaction_.GetRef().GetId(), path.Path_, lockMode, TLockOptions());
        }
    }

    ~TBlockWriter();

    void NotifyRowEnd() override;

protected:
    void DoWrite(const void* buf, size_t len) override;
    void DoFinish() override;

private:
    TAuth Auth_;
    TString Command_;
    EDataStreamFormat Format_;
    TString FormatConfig_;
    const size_t BufferSize_;

    TString Parameters_;
    TString SecondaryParameters_;

    TTransactionId ParentTransactionId_;
    TMaybe<TPingableTransaction> WriteTransaction_;

    TBuffer Buffer_;
    TBufferOutput BufferOutput_;
    TBuffer SecondaryBuffer_;

    TThread Thread_;
    std::atomic<bool> Started_{false};
    std::atomic<bool> Stopped_{false};
    std::exception_ptr Exception_ = nullptr;

    TAutoEvent HasData_;
    TAutoEvent CanWrite_;

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
