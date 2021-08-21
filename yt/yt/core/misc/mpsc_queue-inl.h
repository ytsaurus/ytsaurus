#pragma once
#ifndef MPSC_QUEUE_INL_H_
#error "Direct inclusion of this file is not allowed, include mpsc_queue.h"
// For the sake of sane code completion.
#include "mpsc_queue.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T, TMpscQueueHook T::*Hook>
TIntrusiveMpscQueue<T, Hook>::~TIntrusiveMpscQueue()
{
    while (auto item = TryDequeue()) {
    }
}

template <class T, TMpscQueueHook T::*Hook>
void TIntrusiveMpscQueue<T, Hook>::Enqueue(std::unique_ptr<T> node)
{
    EnqueueImpl(HookFromNode(node.release()));
}

template <class T, TMpscQueueHook T::*Hook>
std::unique_ptr<T> TIntrusiveMpscQueue<T, Hook>::TryDequeue()
{
    return std::unique_ptr<T>(NodeFromHook(TryDequeueImpl()));
}

template <class T, TMpscQueueHook T::*Hook>
TMpscQueueHook* TIntrusiveMpscQueue<T, Hook>::HookFromNode(T* node) noexcept
{
    if (!node) {
        return nullptr;
    }
    return &(node->*Hook);
}

template <class T, TMpscQueueHook T::*Hook>
T* TIntrusiveMpscQueue<T, Hook>::NodeFromHook(TMpscQueueHook* hook) noexcept
{
    if (!hook) {
        return nullptr;
    }
    const T* const fakeNode = nullptr;
    const char* const fakeHook = static_cast<const char*>(static_cast<const void*>(&(fakeNode->*Hook)));
    const size_t offset = fakeHook - static_cast<const char*>(static_cast<const void*>(fakeNode));
    return static_cast<T*>(static_cast<void*>(static_cast<char*>(static_cast<void*>(hook)) - offset));
}

/////////////////////////////////////////////////////////////////////////////

template <class T>
TMpscQueue<T>::TNode::TNode(T&& value)
    : Value(std::move(value))
{ }

template <class T>
void TMpscQueue<T>::Enqueue(T&& value)
{
    Impl_.Enqueue(std::make_unique<TNode>(std::move(value)));
}

template <class T>
bool TMpscQueue<T>::TryDequeue(T* value)
{
    auto node = Impl_.TryDequeue();
    if (!node) {
        return false;
    }
    *value = std::move(node->Value);
    return true;
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
