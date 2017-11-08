#include "block_writer.h"


#include "retry_heavy_write_request.h"

#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/common/wait_proxy.h>

#include <mapreduce/yt/http/requests.h>

#include <mapreduce/yt/interface/errors.h>
#include <mapreduce/yt/interface/finish_or_die.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TBlockWriter::~TBlockWriter()
{
    NDetail::FinishOrDie(this, "TBlockWriter");
}

void TBlockWriter::CheckWriterState()
{
    switch (WriterState_) {
        case Ok:
            break;
        case Completed:
            ythrow TApiUsageError() << "Cannot use table writer that is finished";
        case Error:
            ythrow TApiUsageError() << "Cannot use table writer that finished with error";
    }
}

void TBlockWriter::NotifyRowEnd()
{
    CheckWriterState();
    if (Buffer_.Size() >= BufferSize_) {
        FlushBuffer(false);
    }
}

void TBlockWriter::DoWrite(const void* buf, size_t len)
{
    CheckWriterState();
    while (Buffer_.Size() + len > Buffer_.Capacity()) {
        Buffer_.Reserve(Buffer_.Capacity() * 2);
    }
    BufferOutput_.Write(buf, len);
}

void TBlockWriter::DoFinish()
{
    if (WriterState_ != Ok) {
        return;
    }
    FlushBuffer(true);
    if (Started_) {
        Thread_.Join();
    }
    if (WriteTransaction_) {
        WriteTransaction_->Commit();
    }

    if (Exception_) {
        WriterState_ = Error;
        std::rethrow_exception(Exception_);
    } else if (WriterState_ == Ok) {
        WriterState_ = Completed;
    }
}

void TBlockWriter::FlushBuffer(bool lastBlock)
{
    if (!Started_) {
        if (lastBlock) {
            try {
                Send(Buffer_);
            } catch (...) {
                WriterState_ = Error;
                throw;
            }
            return;
        } else {
            Started_ = true;
            SecondaryBuffer_.Reserve(Buffer_.Capacity());
            Thread_.Start();
        }
    }

    NDetail::TWaitProxy::WaitEvent(CanWrite_);
    if (Exception_) {
        WriterState_ = Error;
        std::rethrow_exception(Exception_);
    }

    SecondaryBuffer_.Swap(Buffer_);
    Stopped_ = lastBlock;
    HasData_.Signal();
}

void TBlockWriter::Send(const TBuffer& buffer)
{
    THttpHeader header("PUT", Command_);
    header.SetInputFormat(Format_);
    header.SetParameters(Parameters_);

    auto streamMaker = [&buffer] () {
        return new TBufferInput(buffer);
    };

    auto transactionId = (WriteTransaction_ ? WriteTransaction_.GetRef().GetId() : ParentTransactionId_);
    RetryHeavyWriteRequest(Auth_, transactionId, header, streamMaker);

    Parameters_ = SecondaryParameters_; // all blocks except the first one are appended
}

void TBlockWriter::SendThread()
{
    while (!Stopped_) {
        try {
            CanWrite_.Signal();
            NDetail::TWaitProxy::WaitEvent(HasData_);
            Send(SecondaryBuffer_);
            SecondaryBuffer_.Clear();
        } catch (const yexception&) {
            Exception_ = std::current_exception();
            CanWrite_.Signal();
            break;
        }
    }
}

void* TBlockWriter::SendThread(void* opaque)
{
    static_cast<TBlockWriter*>(opaque)->SendThread();
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
