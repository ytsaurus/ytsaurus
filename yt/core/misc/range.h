#pragma once

#include "assert.h"
#include "hash.h"

#include <vector>
#include <array>

// For size_t.
#include <stddef.h>

// Forward declarations
namespace google {
namespace protobuf {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class RepeatedField;

template <class T>
class RepeatedPtrField;

////////////////////////////////////////////////////////////////////////////////

} // namespace protobuf
} // namespace google

// Forward declarations
namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class SmallVectorImpl;

template <class T>
class TNullable;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

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

    size_t size() const
    {
        return Size();
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
TRange<T> MakeRange(const google::protobuf::RepeatedField<T>& elements)
{
    return TRange<T>(elements.data(), elements.size());
}

//! Constructs a TRange from RepeatedPtrField.
template <class T>
TRange<const T*> MakeRange(const google::protobuf::RepeatedPtrField<T>& elements)
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

template <class T>
struct hash<NYT::TRange<T>>
{
    size_t operator()(const NYT::TRange<T>& range) const
    {
        size_t result = 0;
        for (const auto& element : range) {
            NYT::HashCombine(result, element);
        }
        return result;
    }
};


