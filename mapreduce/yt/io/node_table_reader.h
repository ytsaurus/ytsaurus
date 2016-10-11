#pragma once

#include <mapreduce/yt/interface/io.h>

#include <util/stream/input.h>
#include <util/generic/buffer.h>
#include <util/system/event.h>
#include <util/system/thread.h>

#include <atomic>

namespace NYT {

class TProxyInput;
class TRowBuilder;
class TYsonParser;

////////////////////////////////////////////////////////////////////////////////

struct TRowElement
{
    TNode Node;
    size_t Size = 0;
    enum EType {
        Row,
        Error,
        Finish
    } Type = Row;

    void Reset(EType type = Row)
    {
        Node = TNode();
        Size = 0;
        Type = type;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRowQueue
{
public:
    TRowQueue();

    void Enqueue(TRowElement&& row);
    TRowElement Dequeue();

    void Clear();
    void Stop();

private:
    yvector<TRowElement> EnqueueBuffer_;
    size_t EnqueueSize_ = 0;

    yvector<TRowElement> DequeueBuffer_;
    size_t DequeueIndex_ = 0;

    static constexpr size_t SizeLimit_ = 4 << 20;

    TAutoEvent EnqueueEvent_;
    TAutoEvent DequeueEvent_;
    std::atomic<bool> Stopped_{false};
};

////////////////////////////////////////////////////////////////////////////////

class TNodeTableReader
    : public INodeReaderImpl
{
public:
    explicit TNodeTableReader(THolder<TProxyInput> input);
    ~TNodeTableReader() override;

    const TNode& GetRow() const override;

    bool IsValid() const override;
    void Next() override;
    ui32 GetTableIndex() const override;
    ui64 GetRowIndex() const override;
    void NextKey() override;

private:
    void OnStreamError();
    void CheckValidity() const;
    void PrepareParsing();

    void FetchThread();
    static void* FetchThread(void* opaque);

private:
    THolder<TProxyInput> Input_;

    bool Valid_ = true;
    bool Finished_ = false;
    ui32 TableIndex_ = 0;
    TMaybe<ui64> RowIndex_;
    TMaybe<ui32> RangeIndex_;
    bool AtStart_ = true;

    TRowElement Row_;
    TRowQueue RowQueue_;

    THolder<TRowBuilder> Builder_;
    THolder<TYsonParser> Parser_;

    std::atomic<bool> Running_{false};
    TAutoEvent RetryPrepared_;
    THolder<TThread> Thread_;
    yexception Exception_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
