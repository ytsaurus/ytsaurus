#pragma once

#include "../fwd.h"

#include <yt/cpp/roren/interface/private/fwd.h>

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
};

////////////////////////////////////////////////////////////////////////////////

NPrivate::IRawParDoPtr CreateStatefulParDo(
    IRawStatefulParDoPtr rawStatefulParDo,
    TString stateManagerId,
    TBigRtStateManagerVtable bigRtStateVtable,
    TBigRtStateConfig config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate

template <typename>
class TSerializer;

template <>
class TSerializer<NRoren::NPrivate::TBigRtStateManagerVtable>
{
public:
    static void Save(IOutputStream* output, const NRoren::NPrivate::TBigRtStateManagerVtable& rowVtable);
    static void Load(IInputStream* input, NRoren::NPrivate::TBigRtStateManagerVtable& rowVtable);
};
