#include "construction_serializable.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/library/syncmap/map.h>

#include <util/generic/singleton.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

std::any GetOrCreateConstructionSerializable(const TString& guid, std::function<std::any()> createFunc)
{
    using TRegistry = NYT::NConcurrency::TSyncMap<TString, NYT::TFuture<std::any>>; // guid -> TConstructionSerializablePtr<T>
    NYT::TPromise<std::any> createPromise;
    auto future = *::Singleton<TRegistry>()->FindOrInsert(guid, [&] {
        createPromise = NYT::NewPromise<std::any>();
        return createPromise.ToFuture().ToUncancelable();
    }).first;

    // Do it outside of any locks due to possible FindOrInsert recursion.
    if (createPromise) {
        createPromise.Set(createFunc());
    }

    return NYT::NConcurrency::WaitForFast(future).ValueOrThrow();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
