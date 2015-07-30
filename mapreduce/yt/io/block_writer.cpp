#include "block_writer.h"

#include <mapreduce/yt/http/requests.h>

#include <mapreduce/yt/common/helpers.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TBlockWriter::TBlockWriter(
    const Stroka& serverName,
    const TTransactionId& parentId,
    const Stroka& command,
    EDataStreamFormat format,
    const TRichYPath& path,
    size_t bufferSize)
    : ServerName_(serverName)
    , Command_(command)
    , Format_(format)
    , Path_(AddPathPrefix(path))
    , BufferSize_(bufferSize)
    , WriteTransaction_(serverName, parentId)
    , Buffer_(BufferSize_ * 2)
    , BufferOutput_(Buffer_)
    , Thread_(SendThread, this)
    , Started_(false)
    , Stopped_(false)
    , Finished_(false)
{ }

void TBlockWriter::DoFlushIfNeeded()
{
    if (Buffer_.Size() >= BufferSize_) {
        FlushBuffer(false);
    }
}

void TBlockWriter::DoWrite(const void* buf, size_t len)
{
    while (Buffer_.Size() + len > Buffer_.Capacity()) {
        Buffer_.Reserve(Buffer_.Capacity() * 2);
    }
    BufferOutput_.Write(buf, len);
}

void TBlockWriter::DoFinish()
{
    if (Finished_) {
        return;
    }
    Finished_ = true;
    FlushBuffer(true);
    if (Started_) {
        Thread_.Join();
    }
    WriteTransaction_.Commit();
}

void TBlockWriter::FlushBuffer(bool lastBlock)
{
    if (!Started_) {
        if (lastBlock) {
            Send(Buffer_);
            return;
        } else {
            Started_ = true;
            SecondaryBuffer_.Reserve(Buffer_.Capacity());
            Thread_.Start();
        }
    }

    CanWrite_.Wait();
    SecondaryBuffer_.Swap(Buffer_);
    Stopped_ = lastBlock;
    HasData_.Signal();
}

void TBlockWriter::Send(const TBuffer& buffer)
{
    if (buffer.Empty()) {
        return;
    }

    THttpHeader header("PUT", Command_);
    header.SetParameters(YPathToJsonString(Path_));
    header.SetChunkedEncoding();
    header.SetDataStreamFormat(Format_);

    RetryHeavyWriteRequest(ServerName_, WriteTransaction_.GetId(), header, buffer);

    Path_.Append_ = true; // all blocks except the first one are appended
}

void TBlockWriter::SendThread()
{
    while (!Stopped_) {
        CanWrite_.Signal();
        HasData_.Wait();
        Send(SecondaryBuffer_);
        SecondaryBuffer_.Clear();
    }
}

void* TBlockWriter::SendThread(void* opaque)
{
    static_cast<TBlockWriter*>(opaque)->SendThread();
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
