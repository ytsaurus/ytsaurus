#pragma once

#include "nullable.h"
#include "small_vector.h"

#include <vector>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// TArrayRef (courtesy of LLVM)
// Represents a constant reference to an array (zero or more elements
// consecutively in memory), i. e. a start pointer and a length. It allows
// various APIs to take consecutive elements easily and conveniently.
//
// This class does not own the underlying data, it is expected to be used in
// situations where the data resides in some other buffer, whose lifetime
// extends past that of the TArrayRef. For this reason, it is not in general
// safe to store an TArrayRef.
//
// This is intended to be trivially copyable, so it should be passed by
// value.
template <class T>
class TArrayRef
{
public:
    typedef const T* iterator;
    typedef const T* const_iterator;
    typedef size_t size_type;

    typedef std::reverse_iterator<iterator> reverse_iterator;

private:
    //! The start of the array, in an external buffer.
    const T* Data_;

    //! The number of elements.
    size_type Length_;

public:
    //! @name Constructors
    //! @{

    //! Construct an empty TArrayRef.
    TArrayRef()
        : Data_(nullptr)
        , Length_(0)
    { }

    //! Construct an empty TArrayRef from nullptr.
    TArrayRef(decltype(nullptr))
        : Data_(nullptr)
        , Length_(0)
    { }

    //! Construct an empty TArrayRef from Null.
    TArrayRef(TNull)
        : Data_(nullptr)
        , Length_(0)
    { }

    //! Construct an TArrayRef from a single element.
    TArrayRef(const T& element)
        : Data_(&element)
        , Length_(1)
    { }

    //! Construct an TArrayRef from a pointer and length.
    TArrayRef(const T* data, size_t length)
        : Data_(data)
        , Length_(length)
    { }

    //! Construct an TArrayRef from a range.
    TArrayRef(const T* begin, const T* end)
        : Data_(begin)
        , Length_(end - begin)
    { }

    //! Construct an TArrayRef from a SmallVector. This is templated in order to
    //! avoid instantiating SmallVectorTemplateBase<T> whenever we
    //! copy-construct an TArrayRef.
    TArrayRef(const SmallVectorImpl<T>& v)
        : Data_(v.data())
        , Length_(v.size())
    { }

    //! Construct an TArrayRef from a std::vector.
    template <class A>
    TArrayRef(const std::vector<T, A>& v)
        : Data_(v.empty() ? nullptr : v.data())
        , Length_(v.size())
    { }

    //! Construct an TArrayRef from a C array.
    template <size_t N>
    TArrayRef(const T (&a)[N])
        : Data_(a)
        , Length_(N)
    { }

    //! @}
    //! @name Container Operations
    //! @{

    const_iterator begin() const
    {
        return Data_;
    }

    const_iterator end() const
    {
        return Data_ + Length_;
    }

    reverse_iterator rbegin() const
    {
        return reverse_iterator(end());
    }

    reverse_iterator rend() const
    {
        return reverse_iterator(begin());
    }

    bool empty() const
    {
        return Length_== 0;
    }

    const T* data() const
    {
        return Data_;
    }

    size_t size() const
    {
        return Length_;
    }

    const T& front() const
    {
        YASSERT(!empty());
        return Data_[0];
    }

    const T& back() const
    {
        YASSERT(!empty());
        return Data_[Length_ - 1];
    }

    //! @}
    //! @name Helpers
    //! @{

    bool Equals(TArrayRef other) const
    {
        if (Length_ != other.Length_) {
            return false;
        }
        for (size_type i = 0; i != Length_; i++) {
            if (Data_[i] != other.Data_[i]) {
                return false;
            }
        }
        return true;
    }

    //! Slice(n) - Chop off the first N elements of the array.
    TArrayRef<T> Slice(unsigned int n) const
    {
        YASSERT(n <= size());
        return TArrayRef<T>(data() + n, size() - n);
    }

    //! Slice(n, m) - Chop off the first N elements of the array, and keep M
    //! elements in the array.
    TArrayRef<T> Slice(unsigned int n, unsigned int m) const
    {
        YASSERT(n + m <= size());
        return TArrayRef<T>(data() + n, m);
    }

    //! @}
    //! @name Operator Overloads
    //! @{

    const T& operator[](size_t i) const
    {
        YASSERT(i < size());
        return Data_[i];
    }

    //! @}
    //! @name Expensive Operations
    //! @{

    std::vector<T> ToVector() const
    {
        return std::vector<T>(Data_, Data_ + Length_);
    }

    //! @}
    //! @name Conversion operators
    //! @{

    operator std::vector<T>() const
    {
        return std::vector<T>(Data_, Data_ + Length_);
    }

    //! @}
};

// TMutableArrayRef (courtesy of LLVM)
// Represents a mutable reference to an array (zero or more elements
// consecutively in memory), i. e. a start pointer and a length.
// It allows various APIs to take and modify consecutive elements easily and
// conveniently.
//
// This class does not own the underlying data, it is expected to be used in
// situations where the data resides in some other buffer, whose lifetime
// extends past that of the TMutableArrayRef. For this reason, it is not in
// general safe to store a TMutableArrayRef.
//
// This is intended to be trivially copyable, so it should be passed by value.
template <class T>
class TMutableArrayRef
    : public TArrayRef<T>
{
public:
    typedef T* iterator;

    //! Construct an empty TMutableArrayRef.
    TMutableArrayRef()
        : TArrayRef<T>()
    { }

    //! Construct an empty TMutableArrayRef from nullptr.
    TMutableArrayRef(decltype(nullptr))
        : TArrayRef<T>()
    { }

    //! Construct an empty TMutableArrayRef from Null.
    TMutableArrayRef(TNull)
        : TArrayRef<T>()
    { }

    //! Construct an TMutableArrayRef from a single element.
    TMutableArrayRef(T& element)
        : TArrayRef<T>(element)
    { }

    //! Construct an TMutableArrayRef from a pointer and length.
    TMutableArrayRef(T* data, size_t length)
        : TArrayRef<T>(data, length)
    { }

    //! Construct an TMutableArrayRef from a range.
    TMutableArrayRef(T* begin, T* end)
        : TArrayRef<T>(begin, end)
    { }

    //! Construct an TMutableArrayRef from a SmallVector.
    TMutableArrayRef(SmallVectorImpl<T>& v)
        : TArrayRef<T>(v)
    { }

    //! Construct a TMutableArrayRef from a std::vector.
    TMutableArrayRef(std::vector<T>& v)
        : TArrayRef<T>(v)
    { }

    //! Construct an TMutableArrayRef from a C array.
    template <size_t N>
    TMutableArrayRef(T (&a)[N])
        : TArrayRef<T>(a)
    { }

    T* data() const
    {
        return const_cast<T*>(TArrayRef<T>::data());
    }

    iterator begin() const
    {
        return this->data();
    }

    iterator end() const
    {
        return this->data() + this->size();
    }

    T& front() const
    {
        YASSERT(!this->empty());
        return this->data()[0];
    }

    T& back() const
    {
        YASSERT(!this->empty());
        return this->data()[this->size() - 1];
    }

    //! Slice(n) - Chop off the first N elements of the array.
    TMutableArrayRef<T> Slice(unsigned int n) const
    {
        YASSERT(n <= this->size());
        return TMutableArrayRef<T>(this->data() + n, this->size() - n);
    }

    //! Slice(n, m) - Chop off the first N elements of the array, and keep M
    //! elements in the array.
    TMutableArrayRef<T> Slice(unsigned int n, unsigned int m) const
    {
        YASSERT(n + m <= this->size());
        return TMutableArrayRef<T>(this->data() + n, m);
    }

    //! @}
    //! @name Operator Overloads
    //! @{

    T& operator[](size_t i) const
    {
        YASSERT(i <= this->size());
        return this->data()[i];
    }

};

//! @name TArrayRef Convenience constructors
//! @{

//! Construct an TArrayRef from a single element.
template <class T>
TArrayRef<T> MakeArrayRef(const T& element)
{
    return element;
}

//! Construct an TArrayRef from a pointer and length.
template <class T>
TArrayRef<T> MakeArrayRef(const T* data, size_t length)
{
    return TArrayRef<T>(data, length);
}

//! Construct an TArrayRef from a range.
template <class T>
TArrayRef<T> MakeArrayRef(const T* begin, const T* end)
{
    return TArrayRef<T>(begin, end);
}

//! Construct an TArrayRef from a SmallVector.
template <class T>
TArrayRef<T> MakeArrayRef(const SmallVectorImpl<T>& v)
{
    return v;
}

//! Construct an TArrayRef from a std::vector.
template <class T>
TArrayRef<T> MakeArrayRef(const std::vector<T>& v)
{
    return v;
}

//! Construct an TArrayRef from a C array.
template <class T, size_t N>
TArrayRef<T> MakeArrayRef(const T (&a)[N])
{
    return TArrayRef<T>(a);
}

//! @}
//! @name TArrayRef Comparison Operators
//! @{

template <class T>
inline bool operator==(TArrayRef<T> lhs, TArrayRef<T> rhs)
{
    return lhs.Equals(rhs);
}

template <class T>
inline bool operator!=(TArrayRef<T> lhs, TArrayRef<T> rhs)
{
    return !(lhs == rhs);
}

//! @}

// TArrayRefs can be treated like a POD type.
namespace NMpl {
template <class T> struct TIsPod;
template <class T> struct TIsPod<TArrayRef<T>>
{
    static const bool Value = true;
};
} // namespace NMpl

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

