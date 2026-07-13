#include "block_device_detail.h"

#include <yt/yt/core/ytree/ypath_service.h>

namespace NYT::NNbd {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

IYPathServicePtr TBlockDeviceBase::GetOrchidService()
{
    return IYPathService::FromProducer(BIND(&TBlockDeviceBase::BuildOrchid, MakeWeak(this)));
}

void TBlockDeviceBase::BuildOrchid(IYsonConsumer* consumer) const
{
    auto error = GetError();
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("total_size").Value(GetTotalSize())
            .Item("block_size").Value(GetBlockSize())
            .Item("read_only").Value(IsReadOnly())
            .Item("description").Value(GetDescription())
            .DoIf(!error.IsOK(), [&] (TFluentMap fluent) {
                fluent.Item("error").Value(error);
            })
            .Do([this] (TFluentMap fluent) {
                DoBuildOrchid(fluent.GetConsumer());
            })
        .EndMap();
}

void TBlockDeviceBase::DoBuildOrchid(IYsonConsumer* /*consumer*/) const
{ }

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
