#pragma once

#include <yt/cpp/roren/interface/private/raw_data_flow.h>
#include <yt/cpp/roren/interface/private/row_vtable.h>

#include <util/generic/size_literals.h>

#include <list>
#include <utility>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TLocalStore;
using TLocalStorePtr = ::TIntrusivePtr<TLocalStore>;

IRawInputPtr MakeLocalStoreInput(TLocalStorePtr localStore);
std::pair<TLocalStorePtr, IRawOutputPtr> MakeLocalStore(TRowVtable vtable);

////////////////////////////////////////////////////////////////////////////////

class TLocalStore
    : public TThrRefBase
{
public:
    using TConstIterator = std::list<TBuffer>::const_iterator;

    TLocalStore(TRowVtable rowVtable)
        : RowVtable_(rowVtable)
    { }

    const TRowVtable& GetRowVtable() const
    {
        return RowVtable_;
    }

    TConstIterator begin() const
    {
        return Data_.begin();
    }

    TConstIterator end() const
    {
        return Data_.end();
    }

    TBuffer& AddChunk()
    {
        auto& added = Data_.emplace_back(DefaultDataSize_);
        return added;
    }

    TBuffer* LastChunk()
    {
        return Data_.empty() ? nullptr : &Data_.back();
    }

private:
    static constexpr size_t DefaultDataSize_ = 1_MB;

    std::list<TBuffer> Data_;
    TRowVtable RowVtable_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
