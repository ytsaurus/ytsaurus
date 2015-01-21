#ifndef LOCK_FREE_INL_H_
#error "Direct inclusion of this file is not allowed, include lock_free.h"
#endif
#undef LOCK_FREE_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TMultipleProducerSingleConsumerLockFreeStack<T>::TNode
{
    T Value;
    TNode* Next;

    TNode(const T& value)
        : Value(value)
        , Next(nullptr)
    { }
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
TMultipleProducerSingleConsumerLockFreeStack<T>::TMultipleProducerSingleConsumerLockFreeStack()
    : Head(nullptr)
{ }

template <class T>
TMultipleProducerSingleConsumerLockFreeStack<T>::~TMultipleProducerSingleConsumerLockFreeStack()
{
    auto* current = Head.load();
    while (current) {
        auto* next = current->Next;
        delete current;
        current = next;
    }
}

template <class T>
void TMultipleProducerSingleConsumerLockFreeStack<T>::Enqueue(const T& value)
{
    auto* node = new TNode(value);
    TNode* expected;
    do {
        expected = Head.load(std::memory_order_relaxed);
        node->Next = expected;
    } while (!Head.compare_exchange_weak(expected, node));
}

template <class T>
bool TMultipleProducerSingleConsumerLockFreeStack<T>::Dequeue(T* value)
{
    TNode* expected;
    do {
        expected = Head.load(std::memory_order_relaxed);
        if (!expected) {
            return false;
        }
    } while (!Head.compare_exchange_weak(expected, expected->Next));

    *value = std::move(expected->Value);
    delete expected;
    return true;
}

template <class T>
std::vector<T> TMultipleProducerSingleConsumerLockFreeStack<T>::DequeueAll()
{
    std::vector<T> results;

    TNode* expected;
    do {
        expected = Head.load(std::memory_order_relaxed);
        if (!expected) {
            return results;
        }
    } while (!Head.compare_exchange_weak(expected, nullptr));

    auto* current = expected;
    while (current) {
        results.push_back(std::move(current->Value));
        auto* next = current->Next;
        delete current;
        current = next;
    }
    return results;
}

template <class T>
bool TMultipleProducerSingleConsumerLockFreeStack<T>::DequeueAll(bool reverse, std::function<void(T&)> functor)
{
    TNode* expected;
    do {
        expected = Head.load(std::memory_order_relaxed);
        if (!expected) {
            return false;
        }
    } while (!Head.compare_exchange_weak(expected, nullptr));

    auto* current = expected;
    if (reverse) {
        auto* next = current->Next;
        current->Next = nullptr;
        while (next) {
            auto* second = next->Next;
            next->Next = current;
            current = next;
            next = second;
        }
    }
    while (current) {
        functor(current->Value);
        auto* next = current->Next;
        delete current;
        current = next;
    }
    return true;
}

template <class T>
bool TMultipleProducerSingleConsumerLockFreeStack<T>::IsEmpty() const
{
    return !Head.load();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
