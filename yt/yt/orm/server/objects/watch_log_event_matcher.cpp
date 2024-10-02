#include "watch_log_event_matcher.h"

#include "private.h"
#include "tags.h"
#include "watch_log.h"
#include "watch_manager.h"

#include <yt/yt/orm/client/objects/registry.h>
#include <yt/yt/orm/client/objects/tags.h>

#include <yt/yt/orm/library/attributes/attribute_path.h>

#include <yt/yt/orm/library/query/filter_matcher.h>

#include <yt/yt/core/yson/string.h>

namespace NYT::NOrm::NServer::NObjects {

using namespace NYT::NOrm::NQuery;

using NYT::NYson::TYsonStringBuf;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TWatchLogEventMatcher
    : public IWatchLogEventMatcher
{
public:
    TWatchLogEventMatcher(
        IFilterMatcherPtr filterMatcher,
        THashSet<TString> selectors,
        TTagSet requiredTags,
        TTagSet excludedTags,
        bool verifyEventTags,
        bool allowFilteringByMeta)
        : FilterMatcher_(std::move(filterMatcher))
        , Selectors_(std::move(selectors))
        , RequiredTags_(std::move(requiredTags))
        , ExcludedTags_(std::move(excludedTags))
        , ValidateEventTags_(verifyEventTags)
        , AllowFilteringByMeta_(allowFilteringByMeta)
    { }

    TErrorOr<bool> Match(
        const NProto::TWatchRecord::TEvent& protoEvent,
        const TWatchLogChangedAttributesConfigPtr& changedAttributesConfig,
        NTableClient::TRowBufferPtr rowBuffer) const override
    {
        {
            std::vector<TNonOwningAttributePayload> attributePayloads{TYsonStringBuf(protoEvent.labels_yson())};
            if (AllowFilteringByMeta_) {
                attributePayloads.push_back(TYsonStringBuf(protoEvent.meta_yson()));
            }

            // Constant filter matcher must not fail on an invalid #labels_yson,
            // so we delegate labels parsing to the filter matcher.
            TErrorOr<bool> match = FilterMatcher_->Match(attributePayloads, std::move(rowBuffer));

            if (!match.IsOK() || !match.Value()) {
                return match;
            }
        }
        auto selectorMatch = MatchSelector(protoEvent, changedAttributesConfig);
        if (!selectorMatch.IsOK() || !selectorMatch.Value()) {
            return selectorMatch;
        }

        return MatchTags(protoEvent);
    }

private:
    const IFilterMatcherPtr FilterMatcher_;
    const THashSet<TString> Selectors_;
    const TTagSet RequiredTags_;
    const TTagSet ExcludedTags_;
    const bool ValidateEventTags_;
    const bool AllowFilteringByMeta_;

    TErrorOr<bool> MatchSelector(
        const NProto::TWatchRecord::TEvent& protoEvent,
        const TWatchLogChangedAttributesConfigPtr& changedAttributesConfig) const
    {
        if (Selectors_.empty()) {
            return true;
        }

        if (protoEvent.type() != ObjectUpdatedEventTypeName) {
            return true;
        }

        if (!protoEvent.has_changed_attributes()) {
            return TError(NClient::EErrorCode::WatchesConfigurationMismatch,
                "Watch log event does not contain enough data "
                "for filtering by a query selector")
                << TErrorAttribute("selectors", Selectors_);
        }

        if (!changedAttributesConfig) {
            return TError(NClient::EErrorCode::WatchesConfigurationMismatch,
                "Watch log event is not compatible with the current master configuration: "
                "no compatible changed attributes config found");
        }

        const auto& changedAttributeToIndex = changedAttributesConfig->PathToIndex;
        if (protoEvent.changed_attributes().changed_summary().size() != changedAttributeToIndex.size()) {
            return TError(NClient::EErrorCode::WatchesConfigurationMismatch,
                "Watch log event is not compatible with the current master configuration: "
                "expected changed attributes summary of size %v in event, but got %v",
                changedAttributeToIndex.size(),
                protoEvent.changed_attributes().changed_summary().size());
        }

        if (protoEvent.changed_attributes().paths_md5() != changedAttributesConfig->MD5) {
            return TError(NClient::EErrorCode::WatchesConfigurationMismatch,
                "Watch log event is not compatible with the current master configuration: "
                "expected changed attributes summary MD5 hash %v in event, but got %v",
                changedAttributesConfig->MD5,
                protoEvent.changed_attributes().paths_md5());
        }

        bool changed = false;
        for (const auto& selectorPath : Selectors_) {
            auto it = changedAttributeToIndex.find(selectorPath);
            if (it == changedAttributeToIndex.end()) {
                return TError(NClient::EErrorCode::WatchesConfigurationMismatch,
                    "Watch log event does not contain enough data "
                    "for filtering by the query selector with attribute %v",
                    selectorPath);
            }

            switch (protoEvent.changed_attributes().changed_summary().at(it->second)) {
                case NClient::EOptionalBoolean::OB_UNKNOWN:
                case NClient::EOptionalBoolean::OB_TRUE:
                    changed = true;
                    break;
                case NClient::EOptionalBoolean::OB_FALSE:
                    break;
                default:
                    YT_ABORT();
            }
        }

        return changed;
    }

    bool MatchTagSet(const TTagSet& tagsMask) const
    {
        return
            (!RequiredTags_ || (tagsMask & RequiredTags_)) &&
            (!ExcludedTags_ || !(tagsMask & ExcludedTags_));
    }

    TErrorOr<bool> MatchTags(const NProto::TWatchRecord::TEvent& protoEvent) const
    {
        if (!RequiredTags_ && !ExcludedTags_) {
            return true;
        }

        if (protoEvent.type() != ObjectUpdatedEventTypeName) {
            return true;
        }

        auto tagsRegistry = NClient::NObjects::GetGlobalTagsRegistry();
        bool matched = false;
        for (const auto& attributeTags : protoEvent.changed_attribute_tags()) {
            auto tagSet = FromProto(attributeTags);

            if (ValidateEventTags_) {
                auto unknownTags = tagsRegistry->FilterUnknownTags(tagSet);
                if (unknownTags.size() > 0) {
                    return TError(NClient::EErrorCode::WatchesConfigurationMismatch,
                        "Watch log event is not compatible with the current master configuration: "
                        "unknown tags found %v",
                        unknownTags);
                }
            }

            if (MatchTagSet(tagSet)) {
                matched = true;
                if (!ValidateEventTags_) {
                    break;
                }
            }
        }

        return matched;
    }
};

////////////////////////////////////////////////////////////////////////////////

void ValidateQuerySelector(const THashSet<TString>& selector)
{
    try {
        for (const auto& path : selector) {
            NAttributes::ValidateAttributePath(path);
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error validating query selector %v",
            selector)
            << ex;
    }
}

NQuery::IFilterMatcherPtr CreateFilterMatcher(TObjectFilter filter, bool allowFilteringByMeta)
{
    if (filter.Query.empty()) {
        return NQuery::CreateConstantFilterMatcher(true);
    }

    std::vector<TString> attributePaths{"/labels"};
    if (allowFilteringByMeta) {
        attributePaths.push_back("/meta");
    }
    return NOrm::NQuery::CreateFilterMatcher(std::move(filter.Query), attributePaths);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IWatchLogEventMatcher> CreateWatchLogEventMatcher(
    const TObjectFilter& filter,
    const THashSet<TString>& selector,
    TTagSet requiredTagsMask,
    TTagSet excludedTagsMask,
    bool verifyEventTags,
    bool allowFilteringByMeta)
{
    auto filterMatcher = CreateFilterMatcher(filter, allowFilteringByMeta);
    ValidateQuerySelector(selector);
    return std::make_unique<TWatchLogEventMatcher>(
        std::move(filterMatcher),
        selector,
        std::move(requiredTagsMask),
        std::move(excludedTagsMask),
        verifyEventTags,
        allowFilteringByMeta);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
