#pragma once

#include "public.h"
#include "atomic_ptr.h"
#include "lock_free_hash_table.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TConcurrentCache
{
private:
    using THashTable = TLockFreeHashTable<T>;

    struct TLookupTable;

    TIntrusivePtr<TLookupTable> RenewTable(const TIntrusivePtr<TLookupTable>& head);

public:
    using TValuePtr = TIntrusivePtr<T>;

    explicit TConcurrentCache(size_t maxElementCount);

    ~TConcurrentCache();

    struct TCachedItemRef
        : public THashTable::TItemRef
    {
        TCachedItemRef() = default;

        TCachedItemRef(typename THashTable::TItemRef ref, TLookupTable* origin)
            : THashTable::TItemRef(ref)
            , Origin(origin)
        { }

        TLookupTable* const Origin = nullptr;
    };

    class TLookuper
    {
    public:
        TLookuper() = default;

        TLookuper(TLookuper&& other) = default;

        TLookuper& operator= (TLookuper&& other)
        {
            Parent_ = std::move(other.Parent_);
            Primary_ = std::move(other.Primary_);
            Secondary_ = std::move(other.Secondary_);

            return *this;
        }

        TLookuper(
            TConcurrentCache* parent,
            TIntrusivePtr<TLookupTable> primary,
            TIntrusivePtr<TLookupTable> secondary)
            : Parent_(parent)
            , Primary_(std::move(primary))
            , Secondary_(std::move(secondary))
        { }

        template <class TKey>
        TCachedItemRef operator() (const TKey& key)
        {
            auto fingerprint = THash<T>()(key);

            // Use fixed lookup tables. No need to read head.

            if (auto item = Primary_->FindRef(fingerprint, key)) {
                return TCachedItemRef(item, Primary_.Get());
            }

            if (!Secondary_) {
                return TCachedItemRef();
            }

            return TCachedItemRef(Secondary_->FindRef(fingerprint, key), Secondary_.Get());
        }

        explicit operator bool ()
        {
            return Parent_;
        }

    private:
        TConcurrentCache* Parent_ = nullptr;
        TIntrusivePtr<TLookupTable> Primary_;
        TIntrusivePtr<TLookupTable> Secondary_;
    };

    TLookuper GetLookuper()
    {
        auto primary = Head_.Acquire();
        auto secondary = primary ? primary->Next.Acquire() : nullptr;

        return TLookuper(this, std::move(primary), std::move(secondary));
    }

    class TInserter
    {
    public:
        TInserter() = default;

        TInserter(TInserter&& other) = default;

        TInserter& operator= (TInserter&& other)
        {
            Parent_ = std::move(other.Parent_);
            Primary_ = std::move(other.Primary_);

            return *this;
        }

        TInserter(
            TConcurrentCache* parent,
            TIntrusivePtr<TLookupTable> primary)
            : Parent_(parent)
            , Primary_(std::move(primary))
        { }

        TLookupTable* GetTable()
        {
            if (Primary_->Size >= Parent_->Capacity_) {
                Primary_ = Parent_->RenewTable(Primary_);
            }

            return Primary_.Get();
        }

    private:
        TConcurrentCache* Parent_ = nullptr;
        TIntrusivePtr<TLookupTable> Primary_;
    };

    TInserter GetInserter()
    {
        auto primary = Head_.Acquire();
        return TInserter(this, std::move(primary));
    }

public:
    const size_t Capacity_;
    TAtomicPtr<TLookupTable> Head_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define CONCURRENT_CACHE_INL_H_
#include "concurrent_cache-inl.h"
#undef CONCURRENT_CACHE_INL_H_
