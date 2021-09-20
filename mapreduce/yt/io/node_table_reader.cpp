#include "node_table_reader.h"

#include <mapreduce/yt/common/node_builder.h>
#include <mapreduce/yt/common/wait_proxy.h>

#include <mapreduce/yt/interface/logging/log.h>

#include <library/cpp/yson/parser.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TStopException
    : public yexception
{ };

////////////////////////////////////////////////////////////////////////////////

TRowQueue::TRowQueue(size_t sizeLimit)
    : SizeLimit_(sizeLimit)
{ }

void TRowQueue::Enqueue(TRowElement&& row)
{
    EnqueueSize_ += row.Size;
    EnqueueSize_ += sizeof(TRowElement);
    EnqueueBuffer_.push_back(std::move(row));

    if (row.Type == TRowElement::Row && EnqueueSize_ < SizeLimit_) {
        return;
    }

    NDetail::TWaitProxy::Get()->WaitEvent(DequeueEvent_);

    if (Stopped_) {
        throw TStopException();
    }

    EnqueueBuffer_.swap(DequeueBuffer_);
    EnqueueSize_ = 0;
    DequeueIndex_ = 0;

    EnqueueEvent_.Signal();
}

TRowElement TRowQueue::Dequeue()
{
    while (true) {
        if (DequeueIndex_ < DequeueBuffer_.size()) {
            return std::move(DequeueBuffer_[DequeueIndex_++]);
        }
        DequeueBuffer_.clear();
        DequeueEvent_.Signal();
        NDetail::TWaitProxy::Get()->WaitEvent(EnqueueEvent_);
    }
}

void TRowQueue::Clear()
{
    EnqueueBuffer_.clear();
    DequeueBuffer_.clear();
    EnqueueSize_ = 0;
    DequeueIndex_ = 0;
}

void TRowQueue::Stop()
{
    Stopped_ = true;
    DequeueEvent_.Signal();
}

////////////////////////////////////////////////////////////////////////////////

class TRowBuilder
    : public TYsonConsumerBase
{
public:
    explicit TRowBuilder(TRowQueue* queue);

    void OnStringScalar(const TStringBuf& value) override;
    void OnInt64Scalar(i64 value) override;
    void OnUint64Scalar(ui64 value) override;
    void OnDoubleScalar(double value) override;
    void OnBooleanScalar(bool value) override;
    void OnBeginList() override;
    void OnEntity() override;
    void OnListItem() override;
    void OnEndList() override;
    void OnBeginMap() override;
    void OnKeyedItem(const TStringBuf& key) override;
    void OnEndMap() override;
    void OnBeginAttributes() override;
    void OnEndAttributes() override;

    void Stop();
    void OnStreamError();
    void Finalize();

private:
    THolder<TNodeBuilder> Builder_;
    TRowElement Row_;
    int Depth_ = 0;
    bool Started_ = false;
    std::atomic<bool> Stopped_{false};
    TRowQueue* RowQueue_;

    void EnqueueRow();
};

TRowBuilder::TRowBuilder(TRowQueue* queue)
    : RowQueue_(queue)
{ }

void TRowBuilder::OnStringScalar(const TStringBuf& value)
{
    Row_.Size += sizeof(TNode) + sizeof(TString) + value.size();
    Builder_->OnStringScalar(value);
}

void TRowBuilder::OnInt64Scalar(i64 value)
{
    Row_.Size += sizeof(TNode);
    Builder_->OnInt64Scalar(value);
}

void TRowBuilder::OnUint64Scalar(ui64 value)
{
    Row_.Size += sizeof(TNode);
    Builder_->OnUint64Scalar(value);
}

void TRowBuilder::OnDoubleScalar(double value)
{
    Row_.Size += sizeof(TNode);
    Builder_->OnDoubleScalar(value);
}

void TRowBuilder::OnBooleanScalar(bool value)
{
    Row_.Size += sizeof(TNode);
    Builder_->OnBooleanScalar(value);
}

void TRowBuilder::OnBeginList()
{
    ++Depth_;
    Builder_->OnBeginList();
}

void TRowBuilder::OnEntity()
{
    Row_.Size += sizeof(TNode);
    Builder_->OnEntity();
}

void TRowBuilder::OnListItem()
{
    if (Depth_ == 0) {
        EnqueueRow();
    } else {
        Builder_->OnListItem();
    }
}

void TRowBuilder::OnEndList()
{
    --Depth_;
    Builder_->OnEndList();
}

void TRowBuilder::OnBeginMap()
{
    ++Depth_;
    Builder_->OnBeginMap();
}

void TRowBuilder::OnKeyedItem(const TStringBuf& key)
{
    Row_.Size += sizeof(TString) + key.size();
    Builder_->OnKeyedItem(key);
}

void TRowBuilder::OnEndMap()
{
    --Depth_;
    Builder_->OnEndMap();
}

void TRowBuilder::OnBeginAttributes()
{
    ++Depth_;
    Builder_->OnBeginAttributes();
}

void TRowBuilder::OnEndAttributes()
{
    --Depth_;
    Builder_->OnEndAttributes();
}

void TRowBuilder::EnqueueRow()
{
    if (!Started_) {
        Started_ = true;
    } else {
        RowQueue_->Enqueue(std::move(Row_));
    }
    Row_.Reset();
    Builder_.Reset(new TNodeBuilder(&Row_.Node));
}

void TRowBuilder::Stop()
{
    Stopped_ = true;
    RowQueue_->Stop();
}

void TRowBuilder::OnStreamError()
{
    Row_.Reset(TRowElement::Error);
    RowQueue_->Enqueue(std::move(Row_));
}

void TRowBuilder::Finalize()
{
    if (Started_) {
        RowQueue_->Enqueue(std::move(Row_));
    }
    Row_.Reset(TRowElement::Finish);
    RowQueue_->Enqueue(std::move(Row_));
}

////////////////////////////////////////////////////////////////////////////////

TNodeTableReader::TNodeTableReader(::TIntrusivePtr<TRawTableReader> input, size_t sizeLimit)
    : Input_(std::move(input))
    , RowQueue_(sizeLimit)
{
    PrepareParsing();

    Running_ = true;
    Thread_.Reset(new TThread(TThread::TParams(FetchThread, this).SetName("node_reader")));
    Thread_->Start();

    Next();
}

TNodeTableReader::~TNodeTableReader()
{
    if (Running_) {
        Running_ = false;
        Builder_->Stop();
        RetryPrepared_.Signal();
        Thread_->Join();
    }
}

const TNode& TNodeTableReader::GetRow() const
{
    CheckValidity();
    if (!Row_) {
        ythrow yexception() << "Row is moved";
    }
    return Row_->Node;
}

void TNodeTableReader::MoveRow(TNode* result)
{
    CheckValidity();
    if (!Row_) {
        ythrow yexception() << "Row is moved";
    }
    *result = std::move(Row_->Node);
    Row_.Clear();
}

bool TNodeTableReader::IsValid() const
{
    return Valid_;
}

void TNodeTableReader::Next()
{
    try {
        NextImpl();
    } catch (const yexception& ex) {
        LOG_ERROR("TNodeTableReader::Next failed: %s", ex.what());
        throw;
    }
}

void TNodeTableReader::NextImpl()
{
    CheckValidity();

    if (RowIndex_) {
        ++*RowIndex_;
    }

    while (true) {
        Row_ = RowQueue_.Dequeue();

        if (Row_->Type == TRowElement::Row) {
            // We successfully parsed one more row from the stream,
            // so reset retry count to their initial value.
            Input_.ResetRetries();

            if (!Row_->Node.IsEntity()) {
                AtStart_ = false;
                break;
            }

            for (auto& entry : Row_->Node.GetAttributes().AsMap()) {
                if (entry.first == "key_switch") {
                    if (!AtStart_) {
                        Valid_ = false;
                    }
                } else if (entry.first == "table_index") {
                    TableIndex_ = static_cast<ui32>(entry.second.AsInt64());
                } else if (entry.first == "row_index") {
                    RowIndex_ = static_cast<ui64>(entry.second.AsInt64());
                } else if (entry.first == "range_index") {
                    RangeIndex_ = static_cast<ui32>(entry.second.AsInt64());
                } else if (entry.first == "tablet_index") {
                    TabletIndex_ = entry.second.AsInt64();
                } else if (entry.first == "end_of_stream") {
                    IsEndOfStream_ = true;
                }
            }

            if (!Valid_) {
                break;
            }

        } else if (Row_->Type == TRowElement::Finish) {
            Finished_ = true;
            Valid_ = false;
            Running_ = false;
            Thread_->Join();
            break;

        } else if (Row_->Type == TRowElement::Error) {
            OnStreamError();
        } else {
            Y_FAIL("Unexpected row type: %d", Row_->Type);
        }
    }
}

ui32 TNodeTableReader::GetTableIndex() const
{
    CheckValidity();
    return TableIndex_;
}

ui32 TNodeTableReader::GetRangeIndex() const
{
    CheckValidity();
    return RangeIndex_.GetOrElse(0);
}

ui64 TNodeTableReader::GetRowIndex() const
{
    CheckValidity();
    return RowIndex_.GetOrElse(0UL);
}

i64 TNodeTableReader::GetTabletIndex() const
{
    CheckValidity();
    return TabletIndex_.GetOrElse(0L);
}

void TNodeTableReader::NextKey()
{
    while (Valid_) {
        Next();
    }

    if (Finished_) {
        return;
    }

    Valid_ = true;

    if (RowIndex_) {
        --*RowIndex_;
    }
}

TMaybe<size_t> TNodeTableReader::GetReadByteCount() const
{
    return Input_.GetReadByteCount();
}

bool TNodeTableReader::IsEndOfStream() const
{
    return IsEndOfStream_;
}

bool TNodeTableReader::IsRawReaderExhausted() const
{
    return Finished_;
}

////////////////////////////////////////////////////////////////////////////////

void TNodeTableReader::PrepareParsing()
{
    RowQueue_.Clear();
    Builder_.Reset(new TRowBuilder(&RowQueue_));
    Parser_.Reset(new NYson::TYsonParser(Builder_.Get(), &Input_, YT_LIST_FRAGMENT));
}

void TNodeTableReader::OnStreamError()
{
    LOG_ERROR("Read error: %s", Exception_.what());
    if (Input_.Retry(RangeIndex_, RowIndex_))
    {
        RowIndex_.Clear();
        RangeIndex_.Clear();
        PrepareParsing();
        RetryPrepared_.Signal();
    } else {
        Running_ = false;
        RetryPrepared_.Signal();
        Thread_->Join();
        throw Exception_;
    }
}

void TNodeTableReader::CheckValidity() const
{
    if (!Valid_) {
        ythrow yexception() << "Iterator is not valid";
    }
}

void TNodeTableReader::FetchThread()
{
    while (Running_) {
        try {
            Parser_->Parse();
            Builder_->Finalize();
            break;
        } catch (const TStopException&) {
            break;
        } catch (yexception& e) {
            Exception_ = e;
            try {
                Builder_->OnStreamError();
            } catch (const TStopException&) {
                break;
            }
            NDetail::TWaitProxy::Get()->WaitEvent(RetryPrepared_);
        }
    }
}

void* TNodeTableReader::FetchThread(void* opaque)
{
    static_cast<TNodeTableReader*>(opaque)->FetchThread();
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
