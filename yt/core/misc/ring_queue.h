#pragma once

#include "common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A simple and high-performant drop-in replacement for |std::queue|.
/*!
 *  Things to keep in mind:
 *  - Capacity is doubled each time it is exhausted and is never shrinked back.
 *  - Iteration is supported but iterator movements involve calling |move_forward| and |move_backward|.
 *  - |T| must be nothrow move constructable.
 */
template <class T, class TAllocator = std::allocator<T>>
class TRingQueue
{
public:
    typedef T value_type;
    typedef T& reference;
    typedef const T& const_reference;
    typedef T* pointer;
    typedef const T* const_pointer;
    typedef size_t size_type;

    static_assert(std::is_nothrow_move_constructible<T>::value, "T must be nothrow move constructable.");

    class TIterator
    {
    public:
        TIterator()
        { }

        const T& operator* () const
        {
            return *Ptr_;
        }

        T& operator* ()
        {
            return *Ptr_;
        }

        const T* operator-> () const
        {
            return Ptr_;
        }

        T* operator-> ()
        {
            return Ptr_;
        }

        bool operator == (TIterator other) const
        {
            return Ptr_ == other.Ptr_;
        }

        bool operator != (TIterator other) const
        {
            return Ptr_ != other.Ptr_;
        }

        TIterator& operator = (TIterator other)
        {
            Ptr_ = other.Ptr_;
            return *this;
        }

    private:
        friend class TRingQueue<T>;

        explicit TIterator(T* ptr)
            : Ptr_(ptr)
        { }

        T* Ptr_;
    
    };

    explicit TRingQueue(const TAllocator& allocator = TAllocator())
        : Allocator_(allocator)
    {
        Capacity_ = InitialCapacity;
        Begin_ = Allocator_.allocate(Capacity_);
        End_ = Begin_ + Capacity_;

        Size_ = 0;
        Head_ = Tail_ = Begin_;
    }

    TRingQueue(TRingQueue&& other)
        : Allocator_(std::move(other.Allocator_))
    {
        Capacity_ = other.Capacity_;
        Begin_ = other.Begin_;
        End_ = other.End_;

        Size_ = other.Size_;
        Head_ = other.Head_;
        Tail_ = other.Tail_;

        other.Capacity_ = other.Size_ = 0;
        other.Begin_ = other.End_ = other.Head_ = other.Tail_ = nullptr;
    }

    TRingQueue(const TRingQueue& other) = delete;

    ~TRingQueue()
    {
        DestroyElements();
        if (Begin_) {
            Allocator_.deallocate(Begin_, Capacity_);
        }
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
        if (Tail_ == Begin_) {
            return *(End_ - 1);
        } else {
            return *(Tail_ - 1);
        }
    }

    const T& back() const
    {
        if (Tail_ == Begin_) {
            return *(End_ - 1);
        } else {
            return *(Tail_ - 1);
        }
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
        ++it.Ptr_;
        if (it.Ptr_ == End_) {
            it.Ptr_ = Begin_;
        }
    }

    void move_backward(TIterator& it) const
    {
        if (it.Ptr_ == Begin_) {
            it.Ptr_ = End_ - 1;
        } else {
            --it.Ptr_;
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
        BeforePush();
        new(Tail_) T(value);
        AfterPush();
    }

    void push(T&& value)
    {
        BeforePush();
        new(Tail_) T(std::move(value));
        AfterPush();
    }

    template <class... TArgs>
    T* emplace(TArgs&&... args)
    {
        BeforePush();
        auto* ptr = Tail_;
        new (ptr) T(std::forward<TArgs>(args)...);
        AfterPush();
        return ptr;
    }

    void pop()
    {
        Y_ASSERT(Size_ > 0);
        Head_->T::~T();
        ++Head_;
        if (Head_ == End_) {
            Head_ = Begin_;
        }
        --Size_;
    }

    void clear()
    {
        DestroyElements();
        Size_ = 0;
        Head_ = Tail_ = Begin_;
    }

private:
    TAllocator Allocator_;

    size_t Capacity_;
    T* Begin_;
    T* End_;

    size_t Size_;
    T* Head_;
    T* Tail_;

    static const size_t InitialCapacity = 16;


    void DestroyElements()
    {
        if (Head_ <= Tail_) {
            DestroyRange(Head_, Tail_);
        } else {
            DestroyRange(Head_, End_);
            DestroyRange(Begin_, Tail_);
        }
    }

    static void DestroyRange(T* begin, T* end)
    {
        if (!std::is_trivially_destructible<T>::value) {
            for (auto* current = begin; current != end; ++current) {
                current->T::~T();
            }
        }
    }

    static void MoveRange(T* begin, T* end, T* result)
    {
        if (std::is_trivially_move_constructible<T>::value) {
            ::memcpy(result, begin, sizeof (T) * (end - begin));
        } else {
            for (auto* current = begin; current != end; ++current) {
                new(result++) T(std::move(*current));
                current->T::~T();
            }
        }
    }

    void BeforePush() noexcept
    {
        // NB: Avoid filling Items_ completely and collapsing Head_ with Tail_.
        if (Size_ == Capacity_ - 1) {
            auto newCapacity = Capacity_ * 2;
            auto* newBegin = Allocator_.allocate(newCapacity);

            if (Head_ <= Tail_) {
                MoveRange(Head_, Tail_, newBegin);
            } else {
                MoveRange(Head_, End_, newBegin);
                MoveRange(Begin_, Tail_, newBegin + (End_ - Head_));
            }

            Allocator_.deallocate(Begin_, Capacity_);

            Capacity_ = newCapacity;
            Begin_ = newBegin;
            End_ = Begin_ + newCapacity;

            Head_ = Begin_;
            Tail_ = Head_ + Size_;
        }
    }

    void AfterPush()
    {
        if (++Tail_ == End_) {
            Tail_ = Begin_;
        }
        ++Size_;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
