#pragma once

#include "helpers.h"

#include <mapreduce/yt/interface/common.h>
#include <mapreduce/yt/interface/io.h>
#include <mapreduce/yt/http/http.h>
#include <mapreduce/yt/http/transaction.h>

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
        , WriteTransaction_(auth, parentId)
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
    }

    void NotifyRowEnd() override;

protected:
    void DoWrite(const void* buf, size_t len) override;
    void DoFinish() override;

private:
    TAuth Auth_;
    TString Command_;
    EDataStreamFormat Format_;
    TString FormatConfig_;
    size_t BufferSize_;

    TString Parameters_;
    TString SecondaryParameters_;

    TPingableTransaction WriteTransaction_;

    TBuffer Buffer_;
    TBufferOutput BufferOutput_;
    TBuffer SecondaryBuffer_;

    TThread Thread_;
    std::atomic<bool> Started_{false};
    std::atomic<bool> Stopped_{false};
    bool Finished_ = false;
    TMaybe<yexception> Exception_;

    TAutoEvent HasData_;
    TAutoEvent CanWrite_;

private:
    void FlushBuffer(bool lastBlock);
    void Send(const TBuffer& buffer);

    void SendThread();
    static void* SendThread(void* opaque);
};

////////////////////////////////////////////////////////////////////////////////

}
