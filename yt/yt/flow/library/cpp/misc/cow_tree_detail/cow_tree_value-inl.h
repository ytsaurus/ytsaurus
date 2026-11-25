#ifndef COW_TREE_VALUE_INL_H
    #error "Direct inclusion of this file is not allowed, include cow_tree_value.h"
    // For the sake of sane code completion.
    #include "cow_tree_value.h"
#endif

#include <library/cpp/yt/string/format.h>
#include <library/cpp/yt/string/raw_formatter.h>

#include <util/generic/yexception.h>

namespace NYT::NFlow::NCowTreeInternals {

////////////////////////////////////////////////////////////////////////////////

[[noreturn]] inline void AbortOnCowTreeException(TStringBuf operation)
{
    try {
        YT_ABORT(Format("%v (ExceptionType: %v, Exception: %v)",
            operation,
            CurrentExceptionTypeName(),
            CurrentExceptionMessage()));
    } catch (...) {
        // Formatting the original exception can itself run out of memory.
    }

    TRawFormatter<1024> fallback;
    fallback.AppendString(operation);
    try {
        throw;
    } catch (const std::exception& ex) {
        fallback.AppendString(": ");
        fallback.AppendString(ex.what());
    } catch (...) {
        // The operation is still available for a non-standard exception.
    }
    YT_ABORT(fallback.GetBuffer());
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TValueHolder<T>::TValueHolder()
{
    DebugSetEmpty();
}

template <class T>
TValueHolder<T>::~TValueHolder()
{
    DebugCheckEmpty();
}

template <class T>
template <class... U>
void TValueHolder<T>::Set(U&&... u) noexcept
{
    DebugCheckEmpty();
    try {
        std::construct_at(&Data, std::forward<U>(u)...);
    } catch (...) {
        AbortOnCowTreeException("Failed to construct a CoW tree slot");
    }
    DebugSetNotEmpty();
}

template <class T>
void TValueHolder<T>::Replace(T&& replacement) noexcept
{
    static_assert(std::is_nothrow_move_constructible_v<T>);
    Clear();
    Set(std::move(replacement));
}

template <class T>
void TValueHolder<T>::CopyFrom(const TValueHolder<T>& holder) noexcept
{
    holder.DebugCheckNotEmpty();
    Set(holder.Data);
}

template <class T>
void TValueHolder<T>::MoveFrom(TValueHolder<T>&& holder)
{
    holder.DebugCheckNotEmpty();
    Set(std::move(holder.Data));
    holder.Clear();
}

template <class T>
T& TValueHolder<T>::Get() &
{
    return Data;
}

template <class T>
T&& TValueHolder<T>::Get() &&
{
    DebugCheckNotEmpty();
    return std::move(Data);
}

template <class T>
const T& TValueHolder<T>::Get() const&
{
    DebugCheckNotEmpty();
    return Data;
}

template <class T>
const T&& TValueHolder<T>::Get() const&&
{
    DebugCheckNotEmpty();
    return std::move(Data);
}

template <class T>
void TValueHolder<T>::Clear()
{
    DebugCheckNotEmpty();
    std::destroy_at(&Data);
    DebugSetEmpty();
}

template <class T>
void TValueHolder<T>::DebugSetEmpty()
{
#ifndef NDEBUG
    std::memset(&Dummy, '#', sizeof(Dummy));
#endif
#if COW_TREE_VALUE_ENABLE_DEBUG
    DebugIsEmpty_ = true;
#endif
}

template <class T>
void TValueHolder<T>::DebugSetNotEmpty()
{
#if COW_TREE_VALUE_ENABLE_DEBUG
    DebugIsEmpty_ = false;
#endif
}

template <class T>
void TValueHolder<T>::DebugCheckEmpty() const
{
    YT_ASSERT(std::ranges::all_of(Dummy, [] (char c) {
        return c == '#';
    }));
#if COW_TREE_VALUE_ENABLE_DEBUG
    YT_VERIFY(DebugIsEmpty_ == true);
#endif
}

template <class T>
void TValueHolder<T>::DebugCheckNotEmpty() const
{
#if COW_TREE_VALUE_ENABLE_DEBUG
    YT_VERIFY(DebugIsEmpty_ == false);
#endif
}

////////////////////////////////////////////////////////////////////////////////

inline void TNullMutex::Acquire() noexcept
{ }

inline void TNullMutex::Release() noexcept
{ }

inline void TPlainRefCounter::Ref() noexcept
{
    ++Count_;
}

inline bool TPlainRefCounter::Unref() noexcept
{
    YT_ASSERT(Count_ > 0);
    return --Count_ == 0;
}

inline void TAtomicRefCounter::Ref() noexcept
{
    Count_.fetch_add(1, std::memory_order_relaxed);
}

inline bool TAtomicRefCounter::Unref() noexcept
{
    auto count = Count_.fetch_sub(1, std::memory_order_acq_rel);
    YT_ASSERT(count > 0);
    return count == 1;
}

////////////////////////////////////////////////////////////////////////////////

template <class T, class TOps>
TRef<T, TOps>::TRef(T* node) noexcept
    : Node_(node)
{ }

template <class T, class TOps>
TRef<T, TOps>::TRef(const TRef& other) noexcept
    : Node_(other.Node_)
{
    if (Node_) {
        TOps::Ref(Node_);
    }
}

template <class T, class TOps>
TRef<T, TOps>::TRef(TRef&& other) noexcept
    : Node_(std::exchange(other.Node_, nullptr))
{ }

template <class T, class TOps>
TRef<T, TOps>& TRef<T, TOps>::operator=(TRef other) noexcept
{
    std::swap(Node_, other.Node_);
    return *this;
}

template <class T, class TOps>
TRef<T, TOps>::~TRef()
{
    Reset();
}

template <class T, class TOps>
T* TRef<T, TOps>::Get() const noexcept
{
    return Node_;
}

template <class T, class TOps>
T* TRef<T, TOps>::operator->() const noexcept
{
    return Node_;
}

template <class T, class TOps>
TRef<T, TOps>::operator bool() const noexcept
{
    return Node_ != nullptr;
}

template <class T, class TOps>
T* TRef<T, TOps>::Release() noexcept
{
    return std::exchange(Node_, nullptr);
}

template <class T, class TOps>
void TRef<T, TOps>::Reset() noexcept
{
    if (Node_) {
        TOps::Unref(Node_);
        Node_ = nullptr;
    }
}

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

template <class T>
void HolderOrValueAssign(T& dst, const T& src)
{
    dst = src;
}

template <class T>
void HolderOrValueAssign(T& dst, T&& src)
{
    dst = std::forward<T>(src);
}

template <class T>
void HolderOrValueAssign(TValueHolder<T>& dst, const TValueHolder<T>& src)
{
    dst.CopyFrom(src);
}

template <class T>
void HolderOrValueAssign(TValueHolder<T>& dst, TValueHolder<T>&& src)
{
    dst.MoveFrom(std::move(src));
}

template <class T>
void HolderOrValueAssign(TValueHolder<T>& dst, const T& src)
{
    dst.Set(src);
}

template <class T>
void HolderOrValueAssign(TValueHolder<T>& dst, T&& src)
{
    dst.Set(std::forward<T>(src));
}

template <class T>
void HolderOrValueClear(T&)
{ }

template <class T>
void HolderOrValueClear(TValueHolder<T>& dst)
{
    dst.Clear();
}

template <class T>
void HolderOrValueMove(T* from, T* to, T* dst)
{
    if constexpr (HolderOrValueBulkCopyable<T>) {
        std::memmove(static_cast<void*>(dst), static_cast<const void*>(from), (to - from) * sizeof(T));
        return;
    }
    while (from != to) {
        HolderOrValueAssign(*(dst++), std::move(*(from++)));
    }
}

template <class T>
void HolderOrValueMoveBackward(T* from, T* to, T* dst)
{
    if constexpr (HolderOrValueBulkCopyable<T>) {
        auto count = to - from;
        std::memmove(static_cast<void*>(dst - count), static_cast<const void*>(from), count * sizeof(T));
        return;
    }
    while (from != to) {
        HolderOrValueAssign(*(--dst), std::move(*(--to)));
    }
}

template <class T>
void HolderCopyRange(const TValueHolder<T>* from, const TValueHolder<T>* to, TValueHolder<T>* dst)
{
    if constexpr (HolderOrValueBulkCopyable<TValueHolder<T>>) {
        std::memcpy(static_cast<void*>(dst), static_cast<const void*>(from), (to - from) * sizeof(*from));
        return;
    }
    while (from != to) {
        dst->CopyFrom(*from);
        ++dst;
        ++from;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCowTreeInternals
