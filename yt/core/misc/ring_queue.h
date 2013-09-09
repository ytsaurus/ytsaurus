#pragma once

#include <vector>
#include <algorithm>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A simple and high-performant drop-in replacement for |std::queue|.
/*!
 *  Things to keep in mind:
 *  - Capacity is doubled each time it is exhausted and is never shrinked back.
 *  - Removed items are invalidated by assigning a default-constructed instance.
 *  - Iteration is supported but iterator movement involve calling |move_forward| and |move_backward|.
 */
template <class T>
class TRingQueue
{
public:
    typedef T value_type;
    typedef T& reference;
    typedef const T& const_reference;
    typedef T* pointer;
    typedef const T* const_pointer;
    typedef size_t size_type;

    class TIterator
    {
    public:
        TIterator()
        { }

        const T& operator* () const
        {
            return *It;
        }

        T& operator* ()
        {
            return *It;
        }

        const T* operator-> () const
        {
            return &*It;
        }

        T* operator-> ()
        {
            return &*It;
        }

        bool operator == (TIterator other) const
        {
            return It == other.It;
        }

        bool operator != (TIterator other) const
        {
            return It != other.It;
        }

        TIterator& operator = (TIterator other)
        {
            It = other.It;
            return *this;
        }

    private:
        friend class TRingQueue<T>;
        typedef typename std::vector<T>::iterator TUnderlying;
        
        explicit TIterator(TUnderlying it)
            : It(it)
        { }

        TUnderlying It;
    
    };


    TRingQueue()
    {
        Items_.resize(16);
        clear();
    }


    T& front()
    {
        return *Head_;
    }

    const T& front() const
    {
        return *Head_;
    }

    T& back()
    {
        return *(Tail_ - 1);
    }

    const T& back() const
    {
        return *(Tail_ - 1);
    }


    // For performance reasons iterators do not provide their own operator++ and operator--.
    // move_forward and move_backward are provided instead.
    TIterator begin()
    {
        return TIterator(Head_);
    }

    TIterator end()
    {
        return TIterator(Tail_);
    }

    void move_forward(TIterator& it) const
    {
        ++it.It;
        if (it.It == Items_.end()) {
            it.It = const_cast<TItems&>(Items_).begin();
        }
    }

    void move_backward(TIterator& it) const
    {
        if (it.It == Items_.begin()) {
            it.It = const_cast<TItems&>(Items_).end() - 1;
        } else {
            --it.It;
        }
    }


    size_t size() const
    {
        return Size_;
    }

    bool empty() const
    {
        return Size_ == 0;
    }


    void push(const T& value)
    {
        // NB: Avoid filling Items_ completely and collapsing Head_ with Tail_.
        if (Size_ == Items_.size() - 1) {
            std::vector<T> newItems(Size_ * 2);
            if (Head_ <= Tail_) {
                std::copy(Head_, Tail_, newItems.begin());
            } else {
                auto it = std::copy(Head_, Items_.end(), newItems.begin());
                std::copy(Items_.begin(), Tail_, it);
            }
            Items_.swap(newItems);
            Head_ = Items_.begin();
            Tail_ = Head_ + Size_;
        }
        *Tail_++ = value;
        if (Tail_ == Items_.end()) {
            Tail_ = Items_.begin();
        }            
        ++Size_;
    }

    void pop()
    {
        YASSERT(Size_ > 0);
        // TODO(babenko): consider calling placement ctors and dtors instead
        *Head_ = T();
        ++Head_;
        if (Head_ == Items_.end()) {
            Head_ = Items_.begin();
        }
        --Size_;
    }

    void clear()
    {
        Head_ = Tail_ = Items_.begin();
        Size_ = 0;
    }

private:
    typedef std::vector<T> TItems;
    TItems Items_;
    typename TItems::iterator Head_;
    typename TItems::iterator Tail_;
    size_t Size_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
