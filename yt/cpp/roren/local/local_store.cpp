#include "local_store.h"

#include <util/string/hex.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TLocalStoreOutputStream
    : public IZeroCopyOutput
{
public:
    explicit TLocalStoreOutputStream(TLocalStorePtr localStore)
        : LocalStore_(std::move(localStore))
    { }

protected:
    size_t DoNext(void** ptr) override
    {
        auto* lastChunk = LocalStore_->LastChunk();
        if (!lastChunk || lastChunk->Size() < lastChunk->Capacity()) {
            lastChunk = &LocalStore_->AddChunk();
        }

        *ptr = lastChunk->Pos();

        const auto avail = lastChunk->Avail();
        lastChunk->Resize(lastChunk->Capacity());
        return avail;
    }

    void DoUndo(size_t len) override
    {
        auto* lastChunk = LocalStore_->LastChunk();
        Y_VERIFY(lastChunk);
        lastChunk->Resize(lastChunk->Size() - len);
    }

private:
    TLocalStorePtr LocalStore_;
};

////////////////////////////////////////////////////////////////////////////////

class TLocalStoreInputStream
    : public IZeroCopyInput
{
public:
    TLocalStoreInputStream(TLocalStorePtr localStore)
        : LocalStore_(std::move(localStore))
        , Iterator_(LocalStore_->begin())
    {
        ResetCurrentStream();
    }

protected:
    size_t DoNext(const void** ptr, size_t len) override
    {
        if (CurrentStream_.Exhausted()) {
            if (!Exhausted_) {
                ++Iterator_;
                ResetCurrentStream();
            }
        }
        if (Exhausted_) {
            return 0;
        } else {
            return CurrentStream_.Next(ptr, len);
        }
    }

private:
    void ResetCurrentStream()
    {
        if (Iterator_ == LocalStore_->end()) {
            Exhausted_ = true;
            CurrentStream_.Reset(nullptr, 0);
        }
        CurrentStream_.Reset(Iterator_->Data(), Iterator_->Size());
    }

private:
    TLocalStorePtr LocalStore_;
    TLocalStore::TConstIterator Iterator_;
    TMemoryInput CurrentStream_;
    bool Exhausted_ = false;
};

////////////////////////////////////////////////////////////////////////////////

class TStreamOutput
    : public IRawOutput
{
public:
    TStreamOutput(const TRowVtable& rowVtable, std::unique_ptr<IOutputStream> output)
        : Coder_(rowVtable.RawCoderFactory())
        , Output_(std::move(output))
        , RowSize_(rowVtable.DataSize)
    { }

    void AddRaw(const void* rows, ssize_t count) override
    {
        auto current = static_cast<const std::byte*>(rows);

        for (ssize_t i = 0; i < count; ++i, current += RowSize_) {
            TmpBuffer_.clear();
            TStringOutput out(TmpBuffer_);
            Coder_->EncodeRow(&out, current);

            ::Save(Output_.get(), true);
            ::Save(Output_.get(), TmpBuffer_);
        }
    }

    void Close() override
    {
        ::Save(Output_.get(), false);
        Output_->Flush();
    }

private:
    IRawCoderPtr Coder_;
    std::unique_ptr<IOutputStream> Output_;
    TString TmpBuffer_;
    ssize_t RowSize_;
};

////////////////////////////////////////////////////////////////////////////////

class TStreamInput
    : public IRawInput
{
public:
    TStreamInput(const TRowVtable& rowVtable, std::unique_ptr<IInputStream> input)
        : Coder_(rowVtable.RawCoderFactory())
        , Input_(std::move(input))
        , RowHolder_(rowVtable)
    { }

    const void* NextRaw() override
    {
        bool hasMore;
        ::Load(Input_.get(), hasMore);
        if (!hasMore) {
            return nullptr;
        }
        ::Load(Input_.get(), TmpBuffer_);
        Coder_->DecodeRow(TmpBuffer_, RowHolder_.GetData());
        return RowHolder_.GetData();
    }

private:
    IRawCoderPtr Coder_;
    std::unique_ptr<IInputStream> Input_;
    TRawRowHolder RowHolder_;
    TString TmpBuffer_;
};

////////////////////////////////////////////////////////////////////////////////

IRawInputPtr MakeLocalStoreInput(TLocalStorePtr localStore)
{
    auto stream = std::make_unique<TLocalStoreInputStream>(localStore);
    return MakeIntrusive<TStreamInput>(localStore->GetRowVtable(), std::move(stream));
}

std::pair<TLocalStorePtr, IRawOutputPtr> MakeLocalStore(TRowVtable vtable)
{
    auto localStore = MakeIntrusive<TLocalStore>(vtable);
    auto outputStream = std::make_unique<TLocalStoreOutputStream>(localStore);
    auto output = MakeIntrusive<TStreamOutput>(vtable, std::move(outputStream));
    return {localStore, output};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
