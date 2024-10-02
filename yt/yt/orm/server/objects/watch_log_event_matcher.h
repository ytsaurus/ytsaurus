#pragma once

#include "public.h"

#include <yt/yt/orm/server/objects/proto/watch_record.pb.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct IWatchLogEventMatcher
{
    virtual TErrorOr<bool> Match(
        const NProto::TWatchRecord::TEvent& protoEvent,
        const TWatchLogChangedAttributesConfigPtr& changedAttributesConfig,
        NTableClient::TRowBufferPtr rowBuffer = nullptr) const = 0;

    virtual ~IWatchLogEventMatcher() = default;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IWatchLogEventMatcher> CreateWatchLogEventMatcher(
    const TObjectFilter& filter,
    const THashSet<TString>& selector,
    TTagSet requiredTags,
    TTagSet excludedTags,
    bool verifyEventTags,
    bool allowFilteringByMeta = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
