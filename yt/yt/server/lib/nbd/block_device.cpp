#include "block_device.h"

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

//! Get the latest error set for device.
const TError& TBaseBlockDevice::GetError() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(Lock_);
    return Error_;
}

//! Set an error for device.
void TBaseBlockDevice::SetError(TError error)
{
    // Do not allow ok.
    YT_VERIFY(!error.IsOK());

    YT_ASSERT_THREAD_AFFINITY_ANY();

    {
        auto guard = WriterGuard(Lock_);
        Error_ = std::move(error);
    }

    // Notify subscribers about error.
    CallSubscribers();
}

bool TBaseBlockDevice::SubscribeForErrors(TGuid id, const TCallback<void()>& callback)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    // NB. Callbacks can be called multiple times (e.g. if multiple errors occur).
    // If you need `exactly once` semantics make sure to support it in the callback itself.

    bool callSubscriber = false;

    {
        auto guard = WriterGuard(Lock_);

        auto [it, inserted] = SubscriberCallbacks_.emplace(id, callback);
        if (!inserted) {
            return false;
        }

        // Error is already set, call subscriber.
        callSubscriber = !Error_.IsOK();
    }

    if (callSubscriber) {
        callback.Run();
    }

    return true;
}

bool TBaseBlockDevice::UnsubscribeFromErrors(TGuid id)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto guard = WriterGuard(Lock_);

    auto it = SubscriberCallbacks_.find(id);
    if (it == SubscriberCallbacks_.end()) {
        return false;
    }

    SubscriberCallbacks_.erase(it);
    return true;
}

void TBaseBlockDevice::CallSubscribers() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    std::vector<TCallback<void()>> callbacks;

    {
        auto guard = ReaderGuard(Lock_);

        callbacks.reserve(SubscriberCallbacks_.size());
        for (const auto& [_, callback] : SubscriberCallbacks_) {
            callbacks.push_back(callback);
        }
    }

    // If any of the callbacks throws the remaining ones are not called.
    for (const auto& callback : callbacks) {
        callback.Run();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
