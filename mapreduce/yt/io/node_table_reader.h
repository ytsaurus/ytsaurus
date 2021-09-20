#pragma once

#include "counting_raw_reader.h"

#include <mapreduce/yt/interface/io.h>

#include <library/cpp/yson/public.h>

#include <util/stream/input.h>
#include <util/generic/buffer.h>
#include <util/system/event.h>
#include <util/system/thread.h>

#include <atomic>

namespace NYT {

class TRawTableReader;
class TRowBuilder;

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
    explicit TRowQueue(size_t sizeLimit = 4 << 20);

    void Enqueue(TRowElement&& row);
    TRowElement Dequeue();

    void Clear();
    void Stop();

private:
    TVector<TRowElement> EnqueueBuffer_;
    size_t EnqueueSize_ = 0;

    TVector<TRowElement> DequeueBuffer_;
    size_t DequeueIndex_ = 0;

    TAutoEvent EnqueueEvent_;
    TAutoEvent DequeueEvent_;
    std::atomic<bool> Stopped_{false};
    size_t SizeLimit_;
};

////////////////////////////////////////////////////////////////////////////////

class TNodeTableReader
    : public INodeReaderImpl
{
public:
    explicit TNodeTableReader(::TIntrusivePtr<TRawTableReader> input, size_t sizeLimit = 4 << 20);
    ~TNodeTableReader() override;

    const TNode& GetRow() const override;
    void MoveRow(TNode* result) override;

    bool IsValid() const override;
    void Next() override;
    ui32 GetTableIndex() const override;
    ui32 GetRangeIndex() const override;
    ui64 GetRowIndex() const override;
    i64 GetTabletIndex() const override;
    void NextKey() override;
    TMaybe<size_t> GetReadByteCount() const override;
    bool IsEndOfStream() const override;
    bool IsRawReaderExhausted() const override;

private:
    void NextImpl();
    void OnStreamError();
    void CheckValidity() const;
    void PrepareParsing();

    void FetchThread();
    static void* FetchThread(void* opaque);

private:
    NDetail::TCountingRawTableReader Input_;

    bool Valid_ = true;
    bool Finished_ = false;
    ui32 TableIndex_ = 0;
    TMaybe<ui64> RowIndex_;
    TMaybe<ui32> RangeIndex_;
    TMaybe<i64> TabletIndex_;
    bool IsEndOfStream_ = false;
    bool AtStart_ = true;

    TMaybe<TRowElement> Row_;
    TRowQueue RowQueue_;

    THolder<TRowBuilder> Builder_;
    THolder<NYson::TYsonParser> Parser_;

    std::atomic<bool> Running_{false};
    TAutoEvent RetryPrepared_;
    THolder<TThread> Thread_;
    yexception Exception_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
