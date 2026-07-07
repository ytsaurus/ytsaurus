#include "block_device_detail.h"

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

TError TBlockDeviceBase::GetError() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return Error_.Load();
}

void TBlockDeviceBase::SetError(TError error)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();
    YT_VERIFY(!error.IsOK());

    if (ErrorList_.Fire(error)) {
        Error_.Store(error);
    }
}

void TBlockDeviceBase::SubscribeError(const TCallback<void(const TError&)>& callback)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    ErrorList_.Subscribe(callback);
}

void TBlockDeviceBase::UnsubscribeError(const TCallback<void(const TError&)>& callback)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    ErrorList_.Unsubscribe(callback);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
