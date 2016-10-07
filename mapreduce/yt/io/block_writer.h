#pragma once

#include "helpers.h"

#include <mapreduce/yt/interface/common.h>
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
    : public TOutputStream
{
public:
    template <class TWriterOptions>
    TBlockWriter(
        const TAuth& auth,
        const TTransactionId& parentId,
        const Stroka& command,
        EDataStreamFormat format,
        const Stroka& formatConfig,
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
        , Thread_(SendThread, this)
    {
        Parameters_ = FormIORequestParameters(path, options);

        auto secondaryPath = path;
        secondaryPath.Append_ = true;
        secondaryPath.Schema_.Clear();
        SecondaryParameters_ = FormIORequestParameters(secondaryPath, options);
    }

    void DoFlushIfNeeded();

protected:
    void DoWrite(const void* buf, size_t len) override;
    void DoFinish() override;

private:
    TAuth Auth_;
    Stroka Command_;
    EDataStreamFormat Format_;
    Stroka FormatConfig_;
    size_t BufferSize_;

    Stroka Parameters_;
    Stroka SecondaryParameters_;

    TPingableTransaction WriteTransaction_;

    TBuffer Buffer_;
    TBufferOutput BufferOutput_;
    TBuffer SecondaryBuffer_;

    TThread Thread_;
    std::atomic<bool> Started_{false};
    std::atomic<bool> Stopped_{false};
    bool Finished_ = false;

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
