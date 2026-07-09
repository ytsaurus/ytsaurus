#ifndef INCLUDE_HAZARD_POINTER_INL_H
    #error "include hazard_pointer.h instead"
    #include "hazard_pointer.h"
#endif

#include <library/cpp/threading/hazard_pointer/intrusive/options.h>
#include <library/cpp/threading/hazard_pointer/structs/free_list.h>
#include <library/cpp/threading/hazard_pointer/utils/fast_pointer_hash.h>
#include <library/cpp/threading/hazard_pointer/utils/macro.h>

#include <atomic>
#include <bit>
#include <memory>

namespace NHp::NPrivate {

    // ============================================================================================
    // THazardObject
    // ============================================================================================

    inline THazardObject::THazardObject(const void* key, TReclaimSig* reclaim) noexcept
        : Key_(key)
        , ReclaimFunc_(reclaim)
    {}

    inline THazardObject::~THazardObject() {
        Y_ASSERT(!this->IsLinked());
    }

    inline void THazardObject::Reclaim() {
        ReclaimFunc_(this);
    }

    inline const void* THazardObject::GetKey() const noexcept {
        return Key_.Get();
    }

    inline bool THazardObject::IsProtected() const noexcept {
        return Key_.IsMarked();
    }

    inline void THazardObject::SetProtection(bool value) noexcept {
        Key_.SetMark(value);
    }

    // ============================================================================================
    // THazardRetires
    // ============================================================================================

    struct THazardKeyOfValue {
        const void* operator()(const THazardObject& value) const noexcept {
            return value.GetKey();
        }
    };

    struct THazardHash {
        std::size_t operator()(const void* ptr) const noexcept {
            return NUtil::TFastPointerHash()(static_cast<const THazardObject*>(ptr));
        }
    };

    using THazardRetiresSet = NIntrusive::TUnorderedSet<
        THazardObject,
        NOptions::TBaseHook<THazardRetiresHook>,
        NOptions::TIsPower2Buckets<true>,
        NOptions::TKeyOfValue<THazardKeyOfValue>,
        NOptions::THasher<THazardHash>>;

    class THazardRetires: public THazardRetiresSet {
        using TBase = THazardRetiresSet;

        using TBucketType = typename TBase::TBucketType;
        using TBucketTraits = typename TBase::TBucketTraits;

    public:
        using TResource = TArrayRef<TBucketType>;

    public:
        THazardRetires(TResource buckets) noexcept
            : TBase(TBucketTraits(buckets.data(), buckets.size()))
        {
            for (std::size_t i = 0; i < buckets.size(); ++i) {
                ::new (buckets.data() + i) TBucketType();
            }
        }
    };

    // ============================================================================================
    // THazardRecords
    // ============================================================================================

    class THazardRecords;

    class THazardRecord: public NStructs::TFreeListNode<THazardRecord> {
    public:
        explicit THazardRecord(THazardRecords* owner) noexcept
            : Owner_(owner)
        {}

        THazardRecord(const THazardRecord&) = delete;
        THazardRecord& operator=(const THazardRecord&) = delete;

        inline void Reset(const void* ptr = nullptr) {
            Protected_.store(ptr, std::memory_order_release);
        }

        inline const void* Get() const noexcept {
            return Protected_.load(std::memory_order_acquire);
        }

        inline THazardRecords* GetOwner() const noexcept {
            return Owner_;
        }

    private:
        std::atomic<const void*> Protected_{};
        THazardRecords* Owner_;
    };

    class THazardRecords: public NStructs::TFreeList<THazardRecord> {
        using TBase = NStructs::TFreeList<THazardRecord>;

    public:
        using TResource = TArrayRef<THazardRecord>;

        using iterator = typename TResource::iterator;
        using const_iterator = typename TResource::const_iterator;

    public:
        THazardRecords(TResource data) noexcept
            : Data_(data)
        {
            for (std::size_t i = 0; i < Data_.size(); ++i) {
                ::new (Data_.data() + i) THazardRecord(this);
                TBase::PushToLocal(Data_[i]);
            }
        }

        THazardRecords(const THazardRecords&) = delete;
        THazardRecords& operator=(const THazardRecords&) = delete;

    public:
        iterator begin() noexcept {
            return Data_.data();
        }

        iterator end() noexcept {
            return Data_.data() + Data_.size();
        }

        const_iterator begin() const noexcept {
            return Data_.data();
        }

        const_iterator end() const noexcept {
            return Data_.data() + Data_.size();
        }

    private:
        TResource Data_;
    };

} // namespace NHp::NPrivate

namespace NHp {

    // ============================================================================================
    // THazardPointerDomain::THazardThreadData
    // ============================================================================================

    class THazardPointerDomain::THazardThreadData: public NStructs::TThreadLocalListNode<THazardThreadData> {
    public:
        using TRecordsResource = typename NPrivate::THazardRecords::TResource;
        using TRetiresResource = typename NPrivate::THazardRetires::TResource;

        THazardThreadData(
            THazardPointerDomain* domain,
            TRecordsResource recordsResource,
            TRetiresResource retiresResource,
            std::size_t scanThreshold
        ) noexcept
            : Domain_(domain)
            , Records_(recordsResource)
            , Retires_(retiresResource)
            , ScanThreshold_(scanThreshold)
        {}

        THazardThreadData(const THazardThreadData&) = delete;
        THazardThreadData& operator=(const THazardThreadData&) = delete;

        ~THazardThreadData() {
            auto current = Retires_.begin();
            while (current != Retires_.end()) {
                auto prev = current++;
                Reclaim(std::addressof(*prev));
            }
        }

        void Reclaim(NPrivate::THazardObject* retired) {
            NumOfReclaimed.fetch_add(1, std::memory_order_relaxed);
            Retires_.erase(Retires_.iterator_to(*retired));
            retired->Reclaim();
        }

        void Retire(NPrivate::THazardObject* retired) noexcept {
            NumOfRetired.fetch_add(1, std::memory_order_relaxed);
            auto [_, isUnique] = Retires_.insert(*retired);
            Y_ASSERT(isUnique && "Double retire is not allowed" );
            if (Retires_.size() >= ScanThreshold_) [[unlikely]] {
                Scan();
            }
        }

        NPrivate::THazardRecord* AcquireRecord() noexcept {
            return Records_.Pop();
        }

        void ReleaseRecord(NPrivate::THazardRecord* record) noexcept {
            auto owner = record->GetOwner();
            if (owner == &Records_) {
                owner->PushToLocal(*record);
            } else {
                owner->PushToGlobal(*record);
            }
        }

        void Scan() {
            std::atomic_thread_fence(std::memory_order_seq_cst);
            for (auto current = Domain_->List_.begin(); current != Domain_->List_.end(); ++current) {
                auto& records = current->Records_;
                for (auto record = records.begin(); record != records.end(); ++record) {
                    auto found = Retires_.find(record->Get());
                    if (found != Retires_.end()) {
                        found->SetProtection(true);
                    }
                }
            }

            auto current = Retires_.begin();
            while (current != Retires_.end()) {
                auto prev = current++;
                if (prev->IsProtected()) {
                    prev->SetProtection(false);
                } else {
                    Reclaim(std::addressof(*prev));
                }
            }
        }

        void HelpScan() {
            for (auto current = Domain_->List_.begin(); current != Domain_->List_.end(); ++current) {
                if (current->TryAcquire()) {
                    Retires_.merge(current->Retires_);
                    current->Release();
                }
            }
            Scan();
        }

        void OnDetach() {
            HelpScan();
        }

    public:
        CACHE_LINE_ALIGNAS std::atomic<std::size_t> NumOfRetired;
        CACHE_LINE_ALIGNAS std::atomic<std::size_t> NumOfReclaimed;

    private:
        THazardPointerDomain* Domain_;

        NPrivate::THazardRecords Records_;
        NPrivate::THazardRetires Retires_;
        std::size_t ScanThreshold_;
    };

    // ============================================================================================
    // THazardPointerDomain
    // ============================================================================================

    class THazardPointerDomain::THazardThreadDataFactory {
    public:
        explicit THazardThreadDataFactory(THazardPointerDomain* self) noexcept
            : Self_(self)
        {}

        THazardThreadData* operator()() const {
            using TRecordsResource = typename THazardThreadData::TRecordsResource;
            using TRecordsElement = typename TRecordsResource::value_type;

            using TRetiresResource = typename THazardThreadData::TRetiresResource;
            using TRetiresElement = typename TRetiresResource::value_type;

            auto numOfRecords = Self_->NumOfRecords_;
            auto numOfRetires = std::bit_ceil(Self_->NumOfRetires_);

            auto headerSize = sizeof(THazardThreadData);
            auto recordsResourceSize = sizeof(TRecordsElement) * numOfRecords;
            auto retiresResourceSize = sizeof(TRetiresElement) * numOfRetires;

            auto size = headerSize + recordsResourceSize + retiresResourceSize;

            auto blob = static_cast<std::uint8_t*>(operator new(size, std::align_val_t{alignof(THazardThreadData)}));
            auto records = reinterpret_cast<TRecordsElement*>(blob + headerSize);
            auto retires = reinterpret_cast<TRetiresElement*>(blob + headerSize + recordsResourceSize);

            auto recordsResource = TRecordsResource(records, numOfRecords);
            auto retiresResource = TRetiresResource(retires, numOfRetires);

            ::new (blob) THazardThreadData(
                Self_,
                std::move(recordsResource),
                std::move(retiresResource),
                Self_->ScanThreshold_
            );

            return reinterpret_cast<THazardThreadData*>(blob);
        }

    private:
        THazardPointerDomain* const Self_;
    };

    class THazardPointerDomain::THazardThreadDataDeleter {
    public:
        void operator()(THazardThreadData* threadData) const {
            threadData->~THazardThreadData();
            operator delete(threadData, std::align_val_t{alignof(THazardThreadData)});
        }
    };

    inline THazardPointerDomain::THazardPointerDomain(
        std::size_t numOfRecords,
        std::size_t numOfRetires,
        std::size_t scanThreshold
    ) noexcept
        : NumOfRecords_(numOfRecords)
        , NumOfRetires_(numOfRetires)
        , ScanThreshold_(scanThreshold)
        , List_(THazardThreadDataFactory{this}, THazardThreadDataDeleter{})
    {}

    inline THazardPointerDomain::~THazardPointerDomain() {
        DetachThread();
    }

    template <class TValue, class TDeleter>
    requires(!std::is_base_of_v<NPrivate::THazardObject, TValue>)
    void THazardPointerDomain::Retire(TValue* value, TDeleter deleter) {
        struct TNonIntrusiveHazardObj: public NPrivate::THazardObject {
            using TBase = NPrivate::THazardObject;

            TNonIntrusiveHazardObj(TValue* value, TDeleter deleter) noexcept
                : TBase(value, [](NPrivate::THazardObject* obj) {
                    delete static_cast<TNonIntrusiveHazardObj*>(obj);
                })
                , Obj_(value, std::move(deleter))
            {}

        private:
            std::unique_ptr<TValue, TDeleter> Obj_;
        };

        auto retiredObj = new TNonIntrusiveHazardObj(value, std::move(deleter));
        Retire(retiredObj);
    }

    inline void THazardPointerDomain::AttachThread() {
        List_.AttachThread();
    }

    inline void THazardPointerDomain::DetachThread() {
        List_.DetachThread();
    }

    inline std::size_t THazardPointerDomain::NumOfRetired() const noexcept {
        auto result = std::size_t{0};
        for (auto it = List_.begin(); it != List_.end(); ++it) {
            result += it->NumOfRetired.load(std::memory_order_relaxed);
        }
        return result;
    }

    inline std::size_t THazardPointerDomain::NumOfReclaimed() const noexcept {
        auto result = std::size_t{0};
        for (auto it = List_.begin(); it != List_.end(); ++it) {
            result += it->NumOfReclaimed.load(std::memory_order_relaxed);
        }
        return result;
    }

    inline void THazardPointerDomain::ReclaimHazardPointers() {
        auto& threadData = List_.GetThreadLocal();
        threadData.HelpScan();
    }

    inline void THazardPointerDomain::Retire(NPrivate::THazardObject* retired) {
        auto& threadData = List_.GetThreadLocal();
        threadData.Retire(retired);
    }

    inline NPrivate::THazardRecord* THazardPointerDomain::AcquireRecord() {
        auto& threadData = List_.GetThreadLocal();
        return threadData.AcquireRecord();
    }

    inline void THazardPointerDomain::ReleaseRecord(NPrivate::THazardRecord* record) {
        auto& threadData = List_.GetThreadLocal();
        threadData.ReleaseRecord(record);
    }

    inline THazardPointerDomain& GetDefaultDomain() noexcept {
        static THazardPointerDomain domain;
        return domain;
    }

    inline void AttachThread(THazardPointerDomain& domain) {
        domain.AttachThread();
    }

    inline void DetachThread(THazardPointerDomain& domain) {
        domain.DetachThread();
    }

    inline void ReclaimHazardPointers(THazardPointerDomain& domain) {
        domain.ReclaimHazardPointers();
    }

    inline THazardPointer MakeHazardPointer(THazardPointerDomain& domain) {
        return THazardPointer(&domain);
    }

    // ============================================================================================
    // THazardPointer
    // ============================================================================================

    inline THazardPointer::THazardPointer(THazardPointerDomain* domain) noexcept
        : Domain_(domain)
        , Record_(Domain_->AcquireRecord())
    {
        Y_ABORT_UNLESS(Record_, "unable to create hazard pointer, there are not enough per-thread records");
    }

    inline THazardPointer::THazardPointer(THazardPointer&& other) noexcept
        : Domain_(std::exchange(other.Domain_, nullptr))
        , Record_(std::exchange(other.Record_, nullptr))
    {}

    inline THazardPointer& THazardPointer::operator=(THazardPointer&& other) noexcept {
        THazardPointer temp(std::move(other));
        swap(temp);
        return *this;
    }

    inline THazardPointer::~THazardPointer() {
        if (Record_) [[likely]] {
            Record_->Reset();
            Domain_->ReleaseRecord(Record_);
        }
    }

    inline bool THazardPointer::empty() const noexcept {
        return !Record_;
    }

    inline THazardPointer::operator bool() const noexcept {
        return !empty();
    }

    template <class T>
    T* THazardPointer::Protect(const std::atomic<T*>& src) noexcept {
        return Protect(src, [](auto&& p) { return std::forward<decltype(p)>(p); });
    }

    template <class T, CTransformFunc<T> TFunc>
    T THazardPointer::Protect(const std::atomic<T>& src, TFunc&& func) noexcept {
        auto ptr = src.load(std::memory_order_relaxed);
        while (!TryProtect(ptr, src, std::forward<TFunc>(func))) {
        }
        return ptr;
    }

    template <class T>
    bool THazardPointer::TryProtect(T*& expected, const std::atomic<T*>& src) noexcept {
        return TryProtect(expected, src, [](auto&& p) { return std::forward<decltype(p)>(p); });
    }

    template <class T, CTransformFunc<T> TFunc>
    bool THazardPointer::TryProtect(T& expected, const std::atomic<T>& src, TFunc&& func) noexcept {
        Y_ABORT_IF(empty(), "hazard pointer must be initialized");
        auto old = expected;
        ResetProtection(func(old));
        expected = src.load(std::memory_order_acquire);
        if (old != expected) {
            ResetProtection();
            return false;
        }
        return true;
    }

    template <class T>
    void THazardPointer::ResetProtection(const T* ptr) noexcept {
        Y_ABORT_IF(empty(), "hazard pointer must be initialized");
        if constexpr (std::is_base_of_v<NPrivate::THazardObject, T>) {
            Record_->Reset(static_cast<const NPrivate::THazardObject*>(ptr));
        } else { // if non intrusive hazard obj
            Record_->Reset(ptr);
        }
        std::atomic_thread_fence(std::memory_order_seq_cst);
    }

    inline void THazardPointer::ResetProtection(std::nullptr_t) noexcept {
        Y_ABORT_IF(empty(), "hazard pointer must be initialized");
        Record_->Reset();
    }

    inline void THazardPointer::CopyProtection(const THazardPointer& other) noexcept {
        Y_ABORT_IF(other.empty(), "other hazard pointer must be initialized");
        ResetProtection(other.Record_->Get());
    }

    inline THazardPointerDomain* THazardPointer::Domain() const noexcept {
        return Domain_;
    }

    inline void THazardPointer::swap(THazardPointer& other) noexcept {
        std::swap(Domain_, other.Domain_);
        std::swap(Record_, other.Record_);
    }

    inline void swap(THazardPointer& left, THazardPointer& right) noexcept { // NOLINT(readability-identifier-naming)
        left.swap(right);
    }

    // ============================================================================================
    // THazardPointerObjBase
    // ============================================================================================

    template <class TValue, class TDeleter>
    THazardPointerObjBase<TValue, TDeleter>::THazardPointerObjBase() noexcept
        : TBase(static_cast<THazardObject*>(this), [](NPrivate::THazardObject* obj) {
            auto objBase = static_cast<THazardPointerObjBase*>(obj);
            auto value = static_cast<TValue*>(objBase);
            objBase->Deleter_(value);
        })
    {
        static_assert(std::derived_from<TValue, THazardPointerObjBase>, "TValue must be derived from THazardPointerObjBase<TValue, ...>");
    }

    template <class TValue, class TDeleter>
    THazardPointerObjBase<TValue, TDeleter>::THazardPointerObjBase(const THazardPointerObjBase&) noexcept
        : THazardPointerObjBase()
    {}

    template <class TValue, class TDeleter>
    THazardPointerObjBase<TValue, TDeleter>& THazardPointerObjBase<TValue, TDeleter>::operator=(const THazardPointerObjBase&) noexcept {
        return *this;
    }

    template <class TValue, class TDeleter>
    void THazardPointerObjBase<TValue, TDeleter>::Retire(TDeleter deleter, THazardPointerDomain& domain) noexcept {
#ifndef NDEBUG
        Y_ASSERT(!Retired_.exchange(true, std::memory_order_relaxed) && "Double retire is not allowed");
#endif
        Deleter_ = std::move(deleter);
        domain.Retire(this);
    }

    template <class TValue, class TDeleter>
    void THazardPointerObjBase<TValue, TDeleter>::Retire(THazardPointerDomain& domain) noexcept {
        Retire({}, domain);
    }

    namespace NPrivate::NCpo {

        template <class TValue>
        void Reclaim(TValue* value) = delete;

        template <class TValue>
        concept CStaticReclaim = requires(TValue* value) {
            TValue::Reclaim(value);
        };

        template <class TValue>
        concept CAdlReclaim = requires(TValue* value) {
            Reclaim(value);
        };

        struct TReclaimCpo {
            template <class TValue>
            requires CStaticReclaim<TValue>
            void operator()(TValue* value) const {
                TValue::Reclaim(value);
            }

            template <class TValue>
            requires(CAdlReclaim<TValue> && !CStaticReclaim<TValue>)
            void operator()(TValue* value) const {
                Reclaim(value);
            }

            template <class TValue>
            void operator()(TValue* value) const {
                return std::default_delete<TValue>{}(value);
            }
        };

    } // namespace NPrivate::NCpo

    namespace NPrivate {

        constexpr auto Reclaim = NCpo::TReclaimCpo{};

    } // namespace NPrivate

    template <class TValue>
    THazardPointerObjBase<TValue, void>::THazardPointerObjBase() noexcept
        : TBase(static_cast<THazardObject*>(this), [](NPrivate::THazardObject* obj) {
            auto objBase = static_cast<THazardPointerObjBase*>(obj);
            auto value = static_cast<TValue*>(objBase);
            NPrivate::Reclaim(value);
        })
    {
        static_assert(std::derived_from<TValue, THazardPointerObjBase>, "TValue must be derived from THazardPointerObjBase<TValue, ...>");
    }

    template <class TValue>
    THazardPointerObjBase<TValue, void>::THazardPointerObjBase(const THazardPointerObjBase&) noexcept
        : THazardPointerObjBase()
    {}

    template <class TValue>
    THazardPointerObjBase<TValue, void>& THazardPointerObjBase<TValue, void>::operator=(const THazardPointerObjBase&) noexcept {
        return *this;
    }

    template <class TValue>
    void THazardPointerObjBase<TValue, void>::Retire(THazardPointerDomain& domain) noexcept {
#ifndef NDEBUG
        Y_ASSERT(!Retired_.exchange(true, std::memory_order_relaxed) && "Double retire is not allowed");
#endif
        domain.Retire(this);
    }

} // namespace NHp
