#ifndef MPSC_QUEUE_INL_H_
#error "Direct inclusion of this file is not allowed, include mpsc_queue.h"
// For the sake of sane code completion.
#include "mpsc_queue.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TMpscQueue<T>::TNode
{
    T Value;
    TNode* Next = nullptr;

    explicit TNode(const T& value)
        : Value(value)
    { }

    explicit TNode(T&& value)
        : Value(std::move(value))
    { }
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
TMpscQueue<T>::~TMpscQueue()
{
    Destroy(Head_);
    Destroy(Tail_);
}

template <class T>
void TMpscQueue<T>::Enqueue(const T& value)
{
    DoEnqueue(new TNode(value));
}

template <class T>
void TMpscQueue<T>::Enqueue(T&& value)
{
    DoEnqueue(new TNode(std::move(value)));
}

template <class T>
void TMpscQueue<T>::DoEnqueue(TNode* node)
{
    auto* expected = Head_.load(std::memory_order_relaxed);
    do {
        node->Next = expected;
    } while (!Head_.compare_exchange_weak(expected, node));
}

template <class T>
bool TMpscQueue<T>::TryDequeue(T* value)
{
    if (!Tail_) {
        Tail_ = Head_.exchange(nullptr);
        if (auto* current = Tail_) {
            auto* next = current->Next;
            current->Next = nullptr;
            while (next) {
                auto* second = next->Next;
                next->Next = current;
                current = next;
                next = second;
            }
            Tail_ = current;
        }
    }

    if (!Tail_) {
        return false;
    }

    *value = std::move(Tail_->Value);
    auto* next = Tail_->Next;
    delete Tail_;
    Tail_ = next;

    return true;
}

template <class T>
bool TMpscQueue<T>::IsEmpty() const
{
    return !Tail_ && !Head_.load();
}

template <class T>
void TMpscQueue<T>::Destroy(TNode* node)
{
    while (node) {
        auto* next = node->Next;
        delete node;
        node = next;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
