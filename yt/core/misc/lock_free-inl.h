#pragma once
#ifndef LOCK_FREE_INL_H_
#error "Direct inclusion of this file is not allowed, include lock_free.h"
// For the sake of sane code completion
#include "lock_free.h"
#endif

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

    TNode(T&& value)
        : Value(std::move(value))
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
void TMultipleProducerSingleConsumerLockFreeStack<T>::Enqueue(T&& value)
{
    auto* node = new TNode(std::move(value));
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
std::vector<T> TMultipleProducerSingleConsumerLockFreeStack<T>::DequeueAll(bool reverse)
{
    std::vector<T> results;
    DequeueAll(reverse, [&results] (T& value) {
        results.push_back(std::move(value));
    });
    return results;
}

template <class T>
template <class F>
bool TMultipleProducerSingleConsumerLockFreeStack<T>::DequeueAll(bool reverse, F functor)
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
