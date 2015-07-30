#pragma once

#include <mapreduce/yt/interface/common.h>
#include <mapreduce/yt/http/http.h>
#include <mapreduce/yt/http/transaction.h>

#include <util/stream/output.h>
#include <util/generic/buffer.h>
#include <util/stream/buffer.h>
#include <util/system/thread.h>
#include <util/system/event.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TBlockWriter
    : public TOutputStream
{
public:
    TBlockWriter(
        const Stroka& serverName,
        const TTransactionId& parentId,
        const Stroka& command,
        EDataStreamFormat format,
        const TRichYPath& path,
        size_t bufferSize);

    void DoFlushIfNeeded();

protected:
    virtual void DoWrite(const void* buf, size_t len) override;
    virtual void DoFinish() override;

private:
    Stroka ServerName_;
    Stroka Command_;
    EDataStreamFormat Format_;
    TRichYPath Path_;
    size_t BufferSize_;

    TPingableTransaction WriteTransaction_;

    TBuffer Buffer_;
    TBufferOutput BufferOutput_;
    TBuffer SecondaryBuffer_;

    TThread Thread_;
    volatile bool Started_;
    volatile bool Stopped_;
    bool Finished_;

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
