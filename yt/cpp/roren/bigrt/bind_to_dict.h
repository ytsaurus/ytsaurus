#pragma once

#include <yt/cpp/roren/bigrt/graph/parser.h>
#include <yt/cpp/roren/transforms/dict_join.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

template <class TDictResolver, class TKey = typename TDictResolver::TKey, class TValue = typename TDictResolver::TValue, class... TArgs>
TTransform<void, TKV<TKey, TValue>> BindToDict(TArgs... args)
{
    return [=] (const TPipeline& pipeline) {
        auto readTransform = DummyRead<TKV<TKey, TValue>>();
        SetAttribute(readTransform, NRoren::NPrivate::BindToDictTag, true);

        auto result = pipeline | readTransform;
        IDictResolverPtr<TKey, TValue> dictResolver = ::MakeIntrusive<TDictResolver>(args...);

        NRoren::NPrivate::SetAttribute(*NRoren::NPrivate::GetRawDataNode(result), NRoren::NPrivate::DictResolverTag<TKey, TValue>(), dictResolver);

        TResolveKeysOptions resolveKeysOptions = {
            .BatchSize = Max<ssize_t>(),
        };
        NRoren::NPrivate::SetAttribute(*NRoren::NPrivate::GetRawDataNode(result), NRoren::NPrivate::ResolveKeysOptionsTag, resolveKeysOptions);

        return result;
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
