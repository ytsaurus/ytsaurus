#pragma once

#ifndef OUTPUT_STORE_H_
#error "Direct inclusion of this file is not allowed; include output_store.h instead."
#endif

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class TRange>
void IOutputStore::TryUnregisterBatch(const TRange& messages)
{
    std::vector<const TMessageMeta*> metas;
    metas.reserve(std::size(messages));
    for (const auto& msg : messages) {
        metas.push_back(&*msg);
    }
    TryUnregisterBatch(std::span<const TMessageMeta* const>(metas));
}

template <class TRange>
void IOutputStore::AsyncUnregisterBatch(const TRange& messages)
{
    std::vector<const TMessageMeta*> metas;
    metas.reserve(std::size(messages));
    for (const auto& msg : messages) {
        metas.push_back(&*msg);
    }
    AsyncUnregisterBatch(std::span<const TMessageMeta* const>(metas));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
