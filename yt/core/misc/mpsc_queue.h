#pragma once

#include <yt/core/misc/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// MPSC queue.
/*
 * Based on http://www.1024cores.net/home/lock-free-algorithms/queues/non-intrusive-mpsc-node-based-queue
 */
class TMPSCQueueBase
{
protected:
    struct TNodeBase
    {
        std::atomic<TNodeBase*> Next_ = {nullptr};
    };

    TMPSCQueueBase();
    ~TMPSCQueueBase();

    // Pushes the (detached) node to the queue. Node ownership is transferred to the queue.
    void PushImpl(TNodeBase* node);

    // Pops and detachs a node from the queue. Node ownership is transferred to the caller.
    // When nullptr is returned that means that either queue is empty or queue is blocked.
    // This method does not distinguishes between these two cases.
    TNodeBase* PopImpl();

private:
    TNodeBase Stub_;

    // Producer-side.
    std::atomic<TNodeBase*> Head_;
    char HeadPadding_[64 - sizeof(TNodeBase*)];

    // Consumer-side.
    TNodeBase* Tail_;
    char TailPadding_[64 - sizeof(TNodeBase*)];
};

template <class T>
class TMPSCQueue
    : public TMPSCQueueBase
{
private:
    struct TNode
        : TNodeBase
    {
        T Value_;

        template <class... TArgs>
        TNode(TArgs&&... args)
            : TNodeBase()
            , Value_(std::forward<TArgs>(args)...)
        { }
    };

public:
    class TNodeProxy
    {
    private:
        std::unique_ptr<TNode> Node_;

    public:
        explicit TNodeProxy(TNode* node)
            : Node_(node)
        { }

        TNodeProxy(const TNodeProxy&) = delete;
        TNodeProxy(TNodeProxy&&) = default;

        explicit operator bool() const
        {
            return static_cast<bool>(Node_);
        }

        T* operator->() const
        {
            return Node_ ? &Node_->Value_ : nullptr;
        }

        const T& operator*() const &
        {
            return Node_->Value_;
        }

        T& operator*() &
        {
            return Node_->Value_;
        }

        T&& operator*() &&
        {
            return std::move(Node_->Value_);
        }

        static TNodeProxy FromOpaque(void* opaque)
        {
            return TNodeProxy{reinterpret_cast<TNode*>(opaque)};
        }

        static void* ToOpaque(TNodeProxy proxy)
        {
            return proxy.Node_.release();
        }
    };

    template <class... TArgs>
    void Push(TArgs&&... args)
    {
        auto node = std::make_unique<TNode>(std::forward<TArgs>(args)...);
        PushImpl(node.release());
    }

    TNodeProxy Pop()
    {
        auto node = static_cast<TNode*>(PopImpl());
        return TNodeProxy{std::move(node)};
    }

    TMPSCQueue() = default;
    TMPSCQueue(const TMPSCQueue&) = delete;
    TMPSCQueue(TMPSCQueue&&) = delete;

    ~TMPSCQueue()
    {
        while (auto item = Pop());
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

