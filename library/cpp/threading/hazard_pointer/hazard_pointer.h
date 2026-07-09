#pragma once

#include <library/cpp/threading/hazard_pointer/intrusive/unordered_set.h>
#include <library/cpp/threading/hazard_pointer/structs/thread_local_list.h>
#include <library/cpp/threading/hazard_pointer/utils/marked_ptr.h>

#include <util/generic/array_ref.h>
#include <util/generic/ptr.h>
#include <util/str_stl.h>
#include <util/system/compiler.h>
#include <util/system/yassert.h>

#include <cstddef>
#include <type_traits>
#include <utility>

namespace NHp {

    class THazardPointerDomain;

    template <class, class>
    class THazardPointerObjBase;

} // namespace NHp

namespace NHp::NPrivate {

    class THazardPointerTag;

    using THazardRetiresHook = NIntrusive::TUnorderedSetBaseHook<
        NOptions::TTag<THazardPointerTag>,
        NOptions::TStoreHash<false>,
        NOptions::TIsAutoUnlink<false>>;

    class THazardObject: public THazardRetiresHook {
        using TReclaimSig = void(THazardObject* value);

    protected:
        THazardObject(const void* key, TReclaimSig* reclaim) noexcept;

        THazardObject(const THazardObject&) = delete;
        THazardObject& operator=(const THazardObject&) = delete;

        ~THazardObject();

    private:
        template <class, class>
        friend class NHp::THazardPointerObjBase;
        friend class NHp::THazardPointerDomain;
        friend struct THazardKeyOfValue;

        void Reclaim();
        const void* GetKey() const noexcept;

        bool IsProtected() const noexcept;
        void SetProtection(bool value) noexcept;

    private:
        NUtil::TMarkedPtr<const void> Key_;
        TReclaimSig* ReclaimFunc_;
    };

    class THazardRecord;

} // namespace NHp::NPrivate

namespace NHp {

    class THazardPointerDomain {
        class THazardThreadData;

        class THazardThreadDataFactory;
        class THazardThreadDataDeleter;

    public:
        explicit THazardPointerDomain(
            std::size_t numOfRecords = DefaultNumOfRecords,
            std::size_t numOfRetires = DefaultNumOfRetires,
            std::size_t scanThreshold = DefaultScanThreshold
        ) noexcept;

        THazardPointerDomain(const THazardPointerDomain&) = delete;
        THazardPointerDomain& operator=(const THazardPointerDomain&) = delete;

        ~THazardPointerDomain();

        template <class TValue, class TDeleter = std::default_delete<TValue>>
        requires(!std::is_base_of_v<NPrivate::THazardObject, TValue>)
        void Retire(TValue* value, TDeleter deleter = {});

        void ReclaimHazardPointers();

        void AttachThread();
        void DetachThread();

        std::size_t NumOfRetired() const noexcept;
        std::size_t NumOfReclaimed() const noexcept;

    private:
        template <class, class>
        friend class THazardPointerObjBase;
        friend class THazardPointer;

        void Retire(NPrivate::THazardObject* retired);

        NPrivate::THazardRecord* AcquireRecord();
        void ReleaseRecord(NPrivate::THazardRecord* record);

    private:
        static constexpr std::size_t DefaultNumOfRecords = 8;
        static constexpr std::size_t DefaultNumOfRetires = 1024;
        static constexpr std::size_t DefaultScanThreshold = 512;

        const std::size_t NumOfRecords_;
        const std::size_t NumOfRetires_;
        const std::size_t ScanThreshold_;

        NStructs::TThreadLocalList<THazardThreadData> List_;
    };

    THazardPointerDomain& GetDefaultDomain() noexcept;

    void AttachThread(THazardPointerDomain& domain = GetDefaultDomain());
    void DetachThread(THazardPointerDomain& domain = GetDefaultDomain());
    void ReclaimHazardPointers(THazardPointerDomain& domain = GetDefaultDomain());

    class THazardPointer;
    [[nodiscard]] THazardPointer MakeHazardPointer(THazardPointerDomain& domain = GetDefaultDomain());

    template <class T, class TValue>
    concept CTransformFunc = requires(T&& func, const TValue& pointer) {
        { std::forward<T>(func)(pointer) } -> std::convertible_to<const void*>;
    };

    class THazardPointer {
        explicit THazardPointer(THazardPointerDomain* domain) noexcept;

    public:
        THazardPointer() = default;

        THazardPointer(const THazardPointer& other) = delete;
        THazardPointer& operator=(const THazardPointer& other) = delete;

        THazardPointer(THazardPointer&& other) noexcept;
        THazardPointer& operator=(THazardPointer&& other) noexcept;

        ~THazardPointer();

        bool empty() const noexcept;
        explicit operator bool() const noexcept;

        template <class T>
        T* Protect(const std::atomic<T*>& src) noexcept;

        template <class T, CTransformFunc<T> TFunc>
        T Protect(const std::atomic<T>& src, TFunc&& func) noexcept;

        template <class T>
        bool TryProtect(T*& expected, const std::atomic<T*>& src) noexcept;

        template <class T, CTransformFunc<T> TFunc>
        bool TryProtect(T& expected, const std::atomic<T>& src, TFunc&& func) noexcept;

        template <class T>
        void ResetProtection(const T* ptr) noexcept;
        void ResetProtection(std::nullptr_t = nullptr) noexcept;

        void CopyProtection(const THazardPointer& other) noexcept;

        THazardPointerDomain* Domain() const noexcept;

        void swap(THazardPointer& other) noexcept;

        friend void swap(THazardPointer& left, THazardPointer& right) noexcept; // NOLINT(readability-identifier-naming)
        friend THazardPointer MakeHazardPointer(THazardPointerDomain& domain);

    private:
        THazardPointerDomain* Domain_{};
        NPrivate::THazardRecord* Record_{};
    };

    template <class TValue, class TDeleter = void>
    class THazardPointerObjBase: public NPrivate::THazardObject {
        using TBase = NPrivate::THazardObject;

    protected:
        THazardPointerObjBase() noexcept;

        THazardPointerObjBase(const THazardPointerObjBase& other) noexcept;
        THazardPointerObjBase& operator=(const THazardPointerObjBase& other) noexcept;

        ~THazardPointerObjBase() = default;

    public:
        void Retire(TDeleter deleter, THazardPointerDomain& domain = GetDefaultDomain()) noexcept;
        void Retire(THazardPointerDomain& domain = GetDefaultDomain()) noexcept;

    private:
#ifndef NDEBUG
        std::atomic<bool> Retired_{false};
#endif
        Y_NO_UNIQUE_ADDRESS TDeleter Deleter_;
    };

    template <class TValue>
    class THazardPointerObjBase<TValue, void>: public NPrivate::THazardObject {
        using TBase = NPrivate::THazardObject;

    protected:
        THazardPointerObjBase() noexcept;

        THazardPointerObjBase(const THazardPointerObjBase& other) noexcept;
        THazardPointerObjBase& operator=(const THazardPointerObjBase& other) noexcept;

        ~THazardPointerObjBase() = default;

    public:
        void Retire(THazardPointerDomain& domain = GetDefaultDomain()) noexcept;

    private:
#ifndef NDEBUG
        std::atomic<bool> Retired_{false};
#endif
    };

} // namespace NHp

#define INCLUDE_HAZARD_POINTER_INL_H
#include "hazard_pointer-inl.h"
#undef INCLUDE_HAZARD_POINTER_INL_H
