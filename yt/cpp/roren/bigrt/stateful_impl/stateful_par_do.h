#pragma once

#include "../fwd.h"

#include <yt/cpp/roren/interface/private/fwd.h>
#include <yt/cpp/roren/interface/private/save_loadable_pointer_wrapper.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

struct TBigRtStateManagerVtable
{
    using TCreateStateStoreFunction = IRawStateStorePtr(*)();
    using TKeyToRequestRawFunction = NBigRT::TBaseStateRequestPtr(*)(NBigRT::TBaseStateManager*, const void*);
    using TStateStorePutFunction = void(*)(IRawStateStore*, NBigRT::TBaseStateRequest*);

    TCreateStateStoreFunction CreateStateStore = nullptr;
    TKeyToRequestRawFunction KeyToRequest = nullptr;
    TStateStorePutFunction StateStorePut = nullptr;

    Y_SAVELOAD_DEFINE(
        SaveLoadablePointer(CreateStateStore),
        SaveLoadablePointer(KeyToRequest),
        SaveLoadablePointer(StateStorePut)
    );
};

////////////////////////////////////////////////////////////////////////////////

NPrivate::IRawParDoPtr CreateStatefulParDo(
    IRawStatefulParDoPtr rawStatefulParDo,
    TString stateManagerId,
    TBigRtStateManagerVtable bigRtStateVtable,
    TBigRtStateConfig config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
