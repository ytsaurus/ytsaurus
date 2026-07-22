#ifndef INCLUDE_THREAD_LOCAL_LIST_INL_H
    #error "include thread_local_list.h instead"
    #include "thread_local_list.h"
#endif

#include <library/cpp/threading/hazard_pointer/utils/fast_pointer_hash.h>
#include <library/cpp/threading/hazard_pointer/utils/static_bucket_traits.h>

namespace NHp::NStructs {

    // ============================================================================================
    // TThreadLocalListNode
    // ============================================================================================

    template <class TValue>
    TThreadLocalListNode<TValue>::TThreadLocalListNode(const TThreadLocalListNode&)
        : TThreadLocalListNode()
    {}

    template <class TValue>
    TThreadLocalListNode<TValue>& TThreadLocalListNode<TValue>::operator=(const TThreadLocalListNode&) {
        return *this;
    }

    template <class TValue>
    void TThreadLocalListNode<TValue>::DoAttach() {
        constexpr auto hasAttach = requires(TValue* value) {
            value->OnAttach();
        };
        if constexpr (hasAttach) {
            static_cast<TValue*>(this)->OnAttach();
        }
    }

    template <class TValue>
    void TThreadLocalListNode<TValue>::DoDetach() {
        constexpr auto hasDetach = requires(TValue* value) {
            value->OnDetach();
        };
        if constexpr (hasDetach) {
            static_cast<TValue*>(this)->OnDetach();
        }
    }

    // ============================================================================================
    // TThreadLocalList::TThreadLocalOwner
    // ============================================================================================

    template <class TValue>
    class TThreadLocalList<TValue>::TThreadLocalOwner {
        struct TKeyOfValue {
            const TThreadLocalList* operator()(const TValue& value) const noexcept {
                auto& node = static_cast<const TThreadLocalListNode<TValue>&>(value);
                return static_cast<const TThreadLocalList*>(node.Key_);
            }
        };

        static constexpr std::size_t NumOfBuckets = 8;

        using TBucketTraits = NUtil::TStaticBucketTraits<
            NumOfBuckets,
            NIntrusive::TUnorderedBucketType<
                NOptions::TBaseHook<
                    NIntrusive::TUnorderedSetBaseHook<NOptions::TIsAutoUnlink<false>>>>>;

        using TUnorderedSet = NIntrusive::TUnorderedSet<
            TValue,
            NOptions::TKeyOfValue<TKeyOfValue>,
            NOptions::TIsPower2Buckets<true>,
            NOptions::THasher<NUtil::TFastPointerHash>,
            NOptions::TBucketTraits<TBucketTraits>>;

    public:
        TThreadLocalOwner() = default;

        ~TThreadLocalOwner() {
            OwnerAlive_ = false;
            auto current = EntrySet_.begin();
            while (current != EntrySet_.end()) {
                auto prev = current++;
                DetachImpl(*prev);
            }
        }

        TPointer GetEntry(const TThreadLocalList* key) noexcept {
            if (auto found = EntrySet_.find(key); found != EntrySet_.end()) {
                return std::addressof(*found);
            }
            return nullptr;
        }

        void Attach(TReference value) {
            EntrySet_.insert(value);
            value.DoAttach();
        }

        void Detach(const TThreadLocalList* key) {
            if (auto found = EntrySet_.find(key); found != EntrySet_.end()) {
                DetachImpl(*found);
            }
        }

    private:
        void DetachImpl(TReference value) {
            value.DoDetach();
            EntrySet_.erase(EntrySet_.iterator_to(value));
            value.Release();
        }

    private:
        TUnorderedSet EntrySet_{};
    };

    // ============================================================================================
    // TThreadLocalList
    // ============================================================================================

    template <class TValue>
    template <NPrivate::CCallable<typename TThreadLocalList<TValue>::TFactorySig> TFactory>
    TThreadLocalList<TValue>::TThreadLocalList(TFactory&& factory)
        : Factory_(std::forward<TFactory>(factory))
        , Deleter_(std::default_delete<TValue>{})
    {}

    template <class TValue>
    template <
        NPrivate::CCallable<typename TThreadLocalList<TValue>::TFactorySig> TFactory,
        NPrivate::CCallable<typename TThreadLocalList<TValue>::TDeleterSig> TDeleter>
    TThreadLocalList<TValue>::TThreadLocalList(TFactory&& factory, TDeleter&& deleter)
        : Factory_(std::forward<TFactory>(factory))
        , Deleter_(std::forward<TDeleter>(deleter))
    {}

    template <class TValue>
    TThreadLocalList<TValue>::~TThreadLocalList() {
        auto current = List_.begin();
        while (current != List_.end()) {
            auto prev = current++;
            bool acquired = prev->IsAcquired();
            Y_ABORT_IF(acquired, "Can't clear while all threads aren't detached");
            Deleter_(std::addressof(*prev));
        }
    }

    template <class TValue>
    TThreadLocalList<TValue>::TReference TThreadLocalList<TValue>::GetThreadLocal() {
        auto result = Owner_.GetEntry(this);
        if (!result) [[unlikely]] {
            result = FindOrCreate();
            Owner_.Attach(*result);
        }
        return *result;
    }

    template <class TValue>
    void TThreadLocalList<TValue>::AttachThread() {
        auto result = Owner_.GetEntry(this);
        if (!result) [[likely]] {
            auto newItem = FindOrCreate();
            Owner_.Attach(*newItem);
        }
    }

    template <class TValue>
    void TThreadLocalList<TValue>::DetachThread() {
        if (Y_UNLIKELY(!OwnerAlive_)) {
            // Owner_'s destructor already ran and detached the thread
            return;
        }
        Owner_.Detach(this);
    }

    template <class TValue>
    TThreadLocalList<TValue>::iterator TThreadLocalList<TValue>::begin() noexcept {
        return List_.begin();
    }

    template <class TValue>
    TThreadLocalList<TValue>::iterator TThreadLocalList<TValue>::end() noexcept {
        return List_.end();
    }

    template <class TValue>
    TThreadLocalList<TValue>::const_iterator TThreadLocalList<TValue>::begin() const noexcept {
        return List_.begin();
    }

    template <class TValue>
    TThreadLocalList<TValue>::const_iterator TThreadLocalList<TValue>::end() const noexcept {
        return List_.end();
    }

    template <class TValue>
    TThreadLocalList<TValue>::TPointer TThreadLocalList<TValue>::FindOrCreate() {
        auto found = List_.AcquireFree();
        if (found == List_.end()) {
            auto newItem = Factory_();
            auto newNode = static_cast<TThreadLocalListNode<TValue>*>(newItem);

            newNode->Key_ = this;
            List_.Push(*newItem);
            return newItem;
        }
        return std::addressof(*found);
    }

} // namespace NHp::NStructs
