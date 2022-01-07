#pragma once

#include "public.h"

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Multiple producer single consumer lock-free queue.
/*!
 *  Internally implemented by a pair of lock-free stack (head) and
 *  linked-list of popped-but-not-yet-dequeued items (tail).
 */
template <class T>
class TMpscQueue
{
public:
    TMpscQueue(const TMpscQueue&) = delete;
    void operator=(const TMpscQueue&) = delete;

    TMpscQueue() = default;
    ~TMpscQueue();

    void Enqueue(const T& value);
    void Enqueue(T&& value);

    bool TryDequeue(T* value);

    bool IsEmpty() const;

private:
    struct TNode;

    std::atomic<TNode*> Head_ = nullptr;
    TNode* Tail_ = nullptr;

    void DoEnqueue(TNode* node);
    void Destroy(TNode* node);
};

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define MPSC_QUEUE_INL_H_
#include "mpsc_queue-inl.h"
#undef MPSC_QUEUE_INL_H_
