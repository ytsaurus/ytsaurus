#pragma once

#include "small_vector.h"

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

    //! Constructs a TRange from a single element.
    TRange(const T& element)
        : Data_(&element)
        , Length_(1)
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
    TRange(const T (&a)[N])
        : Data_(a)
        , Length_(N)
    { }


    const_iterator Begin() const
    {
        return Data_;
    }

    const_iterator End() const
    {
        return Data_ + Length_;
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
        YASSERT(index < Size());
        return Data_[index];
    }


    TRange<T> Slice(size_t startOffset, size_t endOffset) const
    {
        YASSERT(startOffset <= Size());
        YASSERT(endOffset >= startOffset && endOffset <= Size());
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
typename TRange<T>::const_iterator begin(const TRange<T>& ref)
{
    return ref.Begin();
}

template <class T>
typename TRange<T>::const_iterator end(const TRange<T>& ref)
{
    return ref.End();
}

////////////////////////////////////////////////////////////////////////////////

//! Constructs a TRange from a single element.
template <class T>
TRange<T> MakeRange(const T& element)
{
    return element;
}

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

//! Constructs a TRange from an std::vector.
template <class T>
TRange<T> MakeRange(const std::vector<T>& elements)
{
    return elements;
}

//! Constructs a TRange from a C array.
template <class T, size_t N>
TRange<T> MakeRange(const T (& elements)[N])
{
    return TRange<T>(elements);
}

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

    //! Constructs a TMutableRange from a single element.
    TMutableRange(T& element)
        : TRange<T>(element)
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

    //! Constructs a TMutableRange from a C array.
    template <size_t N>
    TMutableRange(T (& elements)[N])
        : TRange<T>(elements)
    { }

    
    iterator Begin() const
    {
        return const_cast<T*>(this->Data_);
    }

    iterator End() const
    {
        return this->Begin() + this->Size();
    }

    T& operator[](size_t index) const
    {
        YASSERT(index <= this->Size());
        return this->Begin()[index];
    }


    TMutableRange<T> Slice(T* begin, T* end) const
    {
        YASSERT(begin >= Begin());
        YASSERT(end <= End());
        return TMutableRange<T>(begin, end);
    }

};

// STL interop.
template <class T>
typename TMutableRange<T>::iterator begin(const TMutableRange<T>& ref)
{
    return ref.Begin();
}

template <class T>
typename TMutableRange<T>::iterator end(const TMutableRange<T>& ref)
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
    TSharedRange(const TRange<T>& range, THolderPtr holder)
        : TRange<T>(range)
        , Holder_(std::move(holder))
    { }

    //! Constructs a TSharedRange from a single element.
    TSharedRange(const T& element, THolderPtr holder)
        : TRange<T>(element)
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
        YASSERT(startOffset <= this->Size());
        YASSERT(endOffset >= startOffset && endOffset <= this->Size());
        return TSharedRange<T>(this->Begin() + startOffset, endOffset - startOffset, Holder_);
    }

    TSharedRange<T> Slice(const T* begin, const T* end) const
    {
        YASSERT(begin >= this->Begin());
        YASSERT(end <= this->End());
        return TSharedRange<T>(begin, end, Holder_);
    }

protected:
    THolderPtr Holder_;

};

////////////////////////////////////////////////////////////////////////////////

//! Constructs a combined holder instance by taking ownership of a given list of holders.
template <class... THolders>
TIntrusivePtr<TIntrinsicRefCounted> MakeHolder(THolders&&... holders)
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

//! Constructs a TSharedRange by taking ownership of an std::vector.
template <class T, class... THolders>
TSharedRange<T> MakeSharedRange(std::vector<T>&& elements, THolders&&... holders)
{
    auto range = MakeRange(elements);
    return TSharedRange<T>(range, MakeHolder(std::move(elements), std::forward<THolders>(holders)...));
}

//! Constructs a TSharedRange by copying an std::vector.
template <class T, class... THolders>
TSharedRange<T> MakeSharedRange(const std::vector<T>& elements, THolders&&... holders)
{
    auto elementsCopy = elements;
    return MakeSharedRange(std::move(elementsCopy), std::forward<THolders>(holders)...);
}

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
    TSharedMutableRange(const TMutableRange<T>& range, THolderPtr holder)
        : TMutableRange<T>(range)
        , Holder_(std::move(holder))
    { }

    //! Constructs a TSharedMutableRange from a single element.
    TSharedMutableRange(T& element, THolderPtr holder)
        : TMutableRange<T>(element)
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
        YASSERT(startOffset <= this->Size());
        YASSERT(endOffset >= startOffset && endOffset <= this->Size());
        return TSharedMutableRange<T>(this->Begin() + startOffset, endOffset - startOffset, Holder_);
    }

    TSharedMutableRange<T> Slice(T* begin, T* end) const
    {
        YASSERT(begin >= this->Begin());
        YASSERT(end <= this->End());
        return TSharedMutableRange<T>(begin, end, Holder_);
    }

protected:
    THolderPtr Holder_;

};

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

