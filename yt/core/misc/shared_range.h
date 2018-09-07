#pragma once

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T, unsigned N>
class SmallVector;

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

} // namespace NYT
