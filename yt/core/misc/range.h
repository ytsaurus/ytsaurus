#pragma once

#include "small_vector.h"
#include "nullable.h"

#include <vector>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! TRange (inspired by TArrayRef from LLVM)
/*!
 * Represents a constant reference to an array (zero or more elements
 * consecutively in memory), i. e. a start pointer and a length. It allows
 * various APIs to take consecutive elements easily and conveniently.
 *
 * This class does not own the underlying data, it is expected to be used in
 * situations where the data resides in some other buffer, whose lifetime
 * extends past that of the TRange. For this reason, it is not in general
 * safe to store an TRange.
 *
 * This is intended to be trivially copyable, so it should be passed by
 * value.
 */
template <class T>
class TRange
{
public:
    typedef const T* iterator;
    typedef const T* const_iterator;
    typedef size_t size_type;

    //! Constructs a null TRange.
    TRange()
        : Data_(nullptr)
        , Length_(0)
    { }

    //! Constructs a TRange from a pointer and length.
    TRange(const T* data, size_t length)
        : Data_(data)
        , Length_(length)
    { }

    //! Constructs a TRange from a range.
    TRange(const T* begin, const T* end)
        : Data_(begin)
        , Length_(end - begin)
    { }

    //! Constructs a TRange from a SmallVector. This is templated in order to
    //! avoid instantiating SmallVectorTemplateBase<T> whenever we
    //! copy-construct a TRange.
    TRange(const SmallVectorImpl<T>& elements)
        : Data_(elements.data())
        , Length_(elements.size())
    { }

    //! Constructs a TRange from an std::vector.
    template <class A>
    TRange(const std::vector<T, A>& elements)
        : Data_(elements.empty() ? nullptr : elements.data())
        , Length_(elements.size())
    { }

    //! Constructs a TRange from a C array.
    template <size_t N>
    TRange(const T (&elements)[N])
        : Data_(elements)
        , Length_(N)
    { }

    //! Constructs a TRange from std::array.
    template <size_t N>
    TRange(const std::array<T, N>& elements)
        : Data_(elements.begin())
        , Length_(N)
    { }

    //! Constructs a TRange from TNullable.
    //! Range will contain 0-1 elements.
    TRange(const TNullable<T>& element)
        : Data_(element.GetPtr())
        , Length_(element ? 1 : 0)
    { }

    const_iterator Begin() const
    {
        return Data_;
    }

    // STL interop, for gcc.
    const_iterator begin() const
    {
        return Begin();
    }

    const_iterator End() const
    {
        return Data_ + Length_;
    }

    // STL interop, for gcc.
    const_iterator end() const
    {
        return End();
    }

    bool Empty() const
    {
        return Length_ == 0;
    }

    explicit operator bool() const
    {
        return Data_ != nullptr;
    }

    size_t Size() const
    {
        return Length_;
    }

    const T& operator[](size_t index) const
    {
        Y_ASSERT(index < Size());
        return Data_[index];
    }


    const T& Front() const
    {
        Y_ASSERT(Length_ > 0);
        return Data_[0];
    }

    const T& Back() const
    {
        Y_ASSERT(Length_ > 0);
        return Data_[Length_ - 1];
    }


    TRange<T> Slice(size_t startOffset, size_t endOffset) const
    {
        Y_ASSERT(startOffset <= Size());
        Y_ASSERT(endOffset >= startOffset && endOffset <= Size());
        return TRange<T>(Begin() + startOffset, endOffset - startOffset);
    }

    std::vector<T> ToVector() const
    {
        return std::vector<T>(Data_, Data_ + Length_);
    }

protected:
    //! The start of the array, in an external buffer.
    const T* Data_;

    //! The number of elements.
    size_t Length_;

};

// STL interop.
template <class T>
typename TRange<T>::const_iterator begin(TRange<T> ref)
{
    return ref.Begin();
}

template <class T>
typename TRange<T>::const_iterator end(TRange<T> ref)
{
    return ref.End();
}

////////////////////////////////////////////////////////////////////////////////

//! Constructs a TRange from a pointer and length.
template <class T>
TRange<T> MakeRange(const T* data, size_t length)
{
    return TRange<T>(data, length);
}

//! Constructs a TRange from a native range.
template <class T>
TRange<T> MakeRange(const T* begin, const T* end)
{
    return TRange<T>(begin, end);
}

//! Constructs a TRange from a SmallVector.
template <class T>
TRange<T> MakeRange(const SmallVectorImpl<T>& elements)
{
    return elements;
}

//! "Copy-constructor".
template <class T>
TRange<T> MakeRange(TRange<T> range)
{
    return range;
}

//! Constructs a TRange from an std::vector.
template <class T>
TRange<T> MakeRange(const std::vector<T>& elements)
{
    return elements;
}

//! Constructs a TRange from an std::array.
template <class T, size_t N>
TRange<T> MakeRange(const std::array<T, N>& elements)
{
    return elements;
}

//! Constructs a TRange from a C array.
template <class T, size_t N>
TRange<T> MakeRange(const T (& elements)[N])
{
    return TRange<T>(elements);
}

//! Constructs a TRange from RepeatedField.
template <class T>
TRange<T> MakeRange(const ::google::protobuf::RepeatedField<T>& elements)
{
    return TRange<T>(elements.data(), elements.size());
}

//! Constructs a TRange from RepeatedPtrField.
template <class T>
TRange<const T*> MakeRange(const ::google::protobuf::RepeatedPtrField<T>& elements)
{
    return TRange<const T*>(elements.data(), elements.size());
}

template <class U, class T>
TRange<U> ReinterpretCastRange(TRange<T> range)
{
    static_assert(sizeof(T) == sizeof(U), "T and U must have equal sizes.");
    return TRange<U>(reinterpret_cast<const U*>(range.Begin()), range.Size());
};

////////////////////////////////////////////////////////////////////////////////

// TMutableRange (inspired by TMutableArrayRef from LLVM)
/*
 * Represents a mutable reference to an array (zero or more elements
 * consecutively in memory), i. e. a start pointer and a length.
 * It allows various APIs to take and modify consecutive elements easily and
 * conveniently.
 *
 * This class does not own the underlying data, it is expected to be used in
 * situations where the data resides in some other buffer, whose lifetime
 * extends past that of the TMutableRange. For this reason, it is not in
 * general safe to store a TMutableRange.
 *
 * This is intended to be trivially copyable, so it should be passed by value.
 */
template <class T>
class TMutableRange
    : public TRange<T>
{
public:
    typedef T* iterator;

    //! Constructs a null TMutableRange.
    TMutableRange()
    { }

    //! Constructs a TMutableRange from a pointer and length.
    TMutableRange(T* data, size_t length)
        : TRange<T>(data, length)
    { }

    //! Constructs a TMutableRange from a range.
    TMutableRange(T* begin, T* end)
        : TRange<T>(begin, end)
    { }

    //! Constructs a TMutableRange from a SmallVector.
    TMutableRange(SmallVectorImpl<T>& elements)
        : TRange<T>(elements)
    { }

    //! Constructs a TMutableRange from an std::vector.
    TMutableRange(std::vector<T>& elements)
        : TRange<T>(elements)
    { }

    //! Construct a TMutableRange from an TNullable
    //! Range will contain 0-1 elements.
    TMutableRange(TNullable<T>& nullable)
        : TRange<T>(nullable)
    { }

    //! Constructs a TMutableRange from a C array.
    template <size_t N>
    TMutableRange(T (& elements)[N])
        : TRange<T>(elements)
    { }

    using TRange<T>::Begin;
    using TRange<T>::End;
    using TRange<T>::Front;
    using TRange<T>::Back;
    using TRange<T>::operator[];

    iterator Begin() const
    {
        return const_cast<T*>(this->Data_);
    }

    // STL interop, for gcc.
    iterator begin() const
    {
        return Begin();
    }

    iterator End() const
    {
        return this->Begin() + this->Size();
    }

    // STL interop, for gcc.
    iterator end() const
    {
        return End();
    }

    T& operator[](size_t index)
    {
        Y_ASSERT(index <= this->Size());
        return Begin()[index];
    }

    T& Front()
    {
        Y_ASSERT(this->Length_ > 0);
        return Begin()[0];
    }

    T& Back()
    {
        Y_ASSERT(this->Length_ > 0);
        return Begin()[this->Length_ - 1];
    }

    TMutableRange<T> Slice(T* begin, T* end) const
    {
        Y_ASSERT(begin >= Begin());
        Y_ASSERT(end <= End());
        return TMutableRange<T>(begin, end);
    }
};

// STL interop.
template <class T>
typename TMutableRange<T>::iterator begin(TMutableRange<T> ref)
{
    return ref.Begin();
}

template <class T>
typename TMutableRange<T>::iterator end(TMutableRange<T> ref)
{
    return ref.End();
}

////////////////////////////////////////////////////////////////////////////////

//! TRange with ownership semantics.
template <class T>
class TSharedRange
    : public TRange<T>
{
public:
    typedef TIntrusivePtr<TIntrinsicRefCounted> THolderPtr;

    //! Constructs a null TMutableRange.
    TSharedRange()
    { }

    //! Constructs a TSharedRange from TRange.
    TSharedRange(TRange<T> range, THolderPtr holder)
        : TRange<T>(range)
        , Holder_(std::move(holder))
    { }

    //! Constructs a TSharedRange from a pointer and length.
    TSharedRange(const T* data, size_t length, THolderPtr holder)
        : TRange<T>(data, length)
        , Holder_(std::move(holder))
    { }

    //! Constructs a TSharedRange from a range.
    TSharedRange(const T* begin, const T* end, THolderPtr holder)
        : TRange<T>(begin, end)
        , Holder_(std::move(holder))
    { }

    //! Constructs a TSharedRange from a SmallVector.
    TSharedRange(const SmallVectorImpl<T>& elements, THolderPtr holder)
        : TRange<T>(elements)
        , Holder_(std::move(holder))
    { }

    //! Constructs a TSharedRange from an std::vector.
    TSharedRange(const std::vector<T>& elements, THolderPtr holder)
        : TRange<T>(elements)
        , Holder_(std::move(holder))
    { }

    //! Constructs a TSharedRange from a C array.
    template <size_t N>
    TSharedRange(const T (& elements)[N], THolderPtr holder)
        : TRange<T>(elements)
        , Holder_(std::move(holder))
    { }


    void Reset()
    {
        TRange<T>::Data_ = nullptr;
        TRange<T>::Length_ = 0;
        Holder_.Reset();
    }

    TSharedRange<T> Slice(size_t startOffset, size_t endOffset) const
    {
        Y_ASSERT(startOffset <= this->Size());
        Y_ASSERT(endOffset >= startOffset && endOffset <= this->Size());
        return TSharedRange<T>(this->Begin() + startOffset, endOffset - startOffset, Holder_);
    }

    TSharedRange<T> Slice(const T* begin, const T* end) const
    {
        Y_ASSERT(begin >= this->Begin());
        Y_ASSERT(end <= this->End());
        return TSharedRange<T>(begin, end, Holder_);
    }

    THolderPtr GetHolder() const
    {
        return Holder_;
    }

protected:
    THolderPtr Holder_;

};

////////////////////////////////////////////////////////////////////////////////

//! Constructs a combined holder instance by taking ownership of a given list of holders.
template <class... THolders>
TIntrusivePtr<TIntrinsicRefCounted> MakeIntrinsicHolder(THolders&&... holders)
{
    struct THolder
        : public TIntrinsicRefCounted
    {
        std::tuple<typename std::decay<THolders>::type...> Holders;
    };

    auto holder = New<THolder>();
    holder->Holders = std::tuple<THolders...>(std::forward<THolders>(holders)...);
    return holder;
}

template <class T, class TContainer, class... THolders>
TSharedRange<T> DoMakeSharedRange(TContainer&& elements, THolders&&... holders)
{
    struct THolder
        : public TIntrinsicRefCounted
    {
        typename std::decay<TContainer>::type Elements;
        std::tuple<typename std::decay<THolders>::type...> Holders;
    };

    auto holder = New<THolder>();
    holder->Holders = std::tuple<THolders...>(std::forward<THolders>(holders)...);
    holder->Elements = std::forward<TContainer>(elements);

    auto range = MakeRange<T>(holder->Elements);

    return TSharedRange<T>(range, holder);
}

//! Constructs a TSharedRange by taking ownership of an std::vector.
template <class T, class... THolders>
TSharedRange<T> MakeSharedRange(std::vector<T>&& elements, THolders&&... holders)
{
    return DoMakeSharedRange<T>(std::move(elements), std::forward<THolders>(holders)...);
}

//! Constructs a TSharedRange by taking ownership of an SmallVector.
template <class T, unsigned N, class... THolders>
TSharedRange<T> MakeSharedRange(SmallVector<T, N>&& elements, THolders&&... holders)
{
    return DoMakeSharedRange<T>(std::move(elements), std::forward<THolders>(holders)...);
}

//! Constructs a TSharedRange by copying an std::vector.
template <class T, class... THolders>
TSharedRange<T> MakeSharedRange(const std::vector<T>& elements, THolders&&... holders)
{
    return DoMakeSharedRange<T>(elements, std::forward<THolders>(holders)...);
}

template <class T, class... THolders>
TSharedRange<T> MakeSharedRange(TRange<T> range, THolders&&... holders)
{
    return TSharedRange<T>(range, MakeIntrinsicHolder(std::forward<THolders>(holders)...));
}

template <class U, class T>
TSharedRange<U> ReinterpretCastRange(const TSharedRange<T>& range)
{
    static_assert(sizeof(T) == sizeof(U), "T and U must have equal sizes.");
    return TSharedRange<U>(reinterpret_cast<const U*>(range.Begin()), range.Size(), range.GetHolder());
};

////////////////////////////////////////////////////////////////////////////////

//! TMutableRange with ownership semantics.
//! Use with caution :)
template <class T>
class TSharedMutableRange
    : public TMutableRange<T>
{
public:
    typedef TIntrusivePtr<TIntrinsicRefCounted> THolderPtr;

    //! Constructs a null TSharedMutableRange.
    TSharedMutableRange()
    { }

    //! Constructs a TSharedMutableRange from TMutableRange.
    TSharedMutableRange(TMutableRange<T> range, THolderPtr holder)
        : TMutableRange<T>(range)
        , Holder_(std::move(holder))
    { }

    //! Constructs a TSharedMutableRange from a pointer and length.
    TSharedMutableRange(T* data, size_t length, THolderPtr holder)
        : TMutableRange<T>(data, length)
        , Holder_(std::move(holder))
    { }

    //! Constructs a TSharedMutableRange from a range.
    TSharedMutableRange(T* begin, T* end, THolderPtr holder)
        : TMutableRange<T>(begin, end)
        , Holder_(std::move(holder))
    { }

    //! Constructs a TSharedMutableRange from a SmallVector.
    TSharedMutableRange(SmallVectorImpl<T>& elements, THolderPtr holder)
        : TMutableRange<T>(elements)
        , Holder_(std::move(holder))
    { }

    //! Constructs a TSharedMutableRange from an std::vector.
    TSharedMutableRange(std::vector<T>& elements, THolderPtr holder)
        : TMutableRange<T>(elements)
        , Holder_(std::move(holder))
    { }

    //! Constructs a TSharedMutableRange from a C array.
    template <size_t N>
    TSharedMutableRange(T (& elements)[N], THolderPtr holder)
        : TMutableRange<T>(elements)
        , Holder_(std::move(holder))
    { }


    void Reset()
    {
        TRange<T>::Data_ = nullptr;
        TRange<T>::Length_ = 0;
        Holder_.Reset();
    }

    TSharedMutableRange<T> Slice(size_t startOffset, size_t endOffset) const
    {
        Y_ASSERT(startOffset <= this->Size());
        Y_ASSERT(endOffset >= startOffset && endOffset <= this->Size());
        return TSharedMutableRange<T>(this->Begin() + startOffset, endOffset - startOffset, Holder_);
    }

    TSharedMutableRange<T> Slice(T* begin, T* end) const
    {
        Y_ASSERT(begin >= this->Begin());
        Y_ASSERT(end <= this->End());
        return TSharedMutableRange<T>(begin, end, Holder_);
    }

    THolderPtr GetHolder() const
    {
        return Holder_;
    }

protected:
    THolderPtr Holder_;

};

template <class T, class TContainer, class... THolders>
TSharedMutableRange<T> DoMakeSharedMutableRange(TContainer&& elements, THolders&&... holders)
{
    struct THolder
        : public TIntrinsicRefCounted
    {
        typename std::decay<TContainer>::type Elements;
        std::tuple<typename std::decay<THolders>::type...> Holders;
    };

    auto holder = New<THolder>();
    holder->Holders = std::tuple<THolders...>(std::forward<THolders>(holders)...);
    holder->Elements = std::forward<TContainer>(elements);

    auto range = TMutableRange<T>(holder->Elements);

    return TSharedMutableRange<T>(range, holder);
}

//! Constructs a TSharedMutableRange by taking ownership of an std::vector.
template <class T, class... THolders>
TSharedMutableRange<T> MakeSharedMutableRange(std::vector<T>&& elements, THolders&&... holders)
{
    return DoMakeSharedMutableRange<T>(std::move(elements), std::forward<THolders>(holders)...);
}

////////////////////////////////////////////////////////////////////////////////

// Mark TRange and TMutableRange as PODs.
namespace NMpl {

template <class T>
struct TIsPod;

template <class T>
struct TIsPod<TRange<T>>
{
    static const bool Value = true;
};

template <class T>
struct TIsPod<TMutableRange<T>>
{
    static const bool Value = true;
};

} // namespace NMpl

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

