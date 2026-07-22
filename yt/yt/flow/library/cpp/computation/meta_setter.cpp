#include "meta_setter.h"

#include "event_timestamp_assigner.h"

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>
#include <yt/yt/flow/library/cpp/common/timer.h>
#include <yt/yt/flow/library/cpp/misc/lexicographically_serialize.h>

#include <library/cpp/containers/absl/flat_hash_map.h>

#include <util/digest/city.h>
#include <util/string/hex.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TMessageParents::TMessageParents(
    std::vector<TInputMessageConstPtr> parentMessages,
    std::vector<TInputTimerConstPtr> parentTimers,
    std::vector<TInputVisitConstPtr> parentVisits)
    : ParentMessages(std::move(parentMessages))
    , ParentTimers(std::move(parentTimers))
    , ParentVisits(std::move(parentVisits))
{ }

////////////////////////////////////////////////////////////////////////////////

class TMetaSetterBase
    : public IMetaSetter
{
public:
    TMetaSetterBase(
        TComputationSpecPtr spec,
        IEventTimestampAssignerPtr eventTimestampAssigner)
        : Spec_(std::move(spec))
        , EventTimestampAssigner_(std::move(eventTimestampAssigner))
    { }

    TFillResult Fill(TMessage& message, const TMessageParentsConstPtr& parents) override
    {
        if (message.StreamId.Underlying().empty()) {
            if (Spec_->OutputStreamIds.size() == 1) {
                message.StreamId = *Spec_->OutputStreamIds.begin();
            } else {
                THROW_ERROR_EXCEPTION("Impossible to guess output stream id");
            }
        }
        auto actualParents = CheckParents(message, parents);
        FillMetaImpl(message, actualParents);
        EventTimestampAssigner_->Assign(message);
        return {.ActualParentMessageIds = std::move(actualParents)};
    }

    TFillResult Fill(TTimer& timer, const TMessageParentsConstPtr& parents) override
    {
        if (timer.StreamId.Underlying().empty()) {
            if (Spec_->TimerStreams.size() == 1) {
                timer.StreamId = Spec_->TimerStreams.begin()->first;
            } else {
                THROW_ERROR_EXCEPTION("Impossible to guess timer stream id");
            }
        }

        if (timer.Key.Underlying().GetCount() == 0) {
            auto& info = ParentInfo_[parents];
            if (!info.Key.has_value()) {
                std::optional<TKey> key;
                auto onKey = [&] (const TKey& newKey) {
                    if (!key) {
                        key = newKey;
                    } else if (*key != newKey) {
                        THROW_ERROR_EXCEPTION("Impossible to guess timer key: different keys")
                            << TErrorAttribute("key_1", *key)
                            << TErrorAttribute("key_2", newKey);
                    }
                };
                for (const auto& parent : parents->ParentMessages) {
                    onKey(parent->Key);
                }
                for (const auto& parent : parents->ParentTimers) {
                    onKey(parent->Key);
                }
                for (const auto& parent : parents->ParentVisits) {
                    onKey(parent->Key);
                }

                if (!key) {
                    THROW_ERROR_EXCEPTION("Impossible to guess timer key: unknown parent key");
                }
                info.Key = key;
            }

            timer.Key = *info.Key;
        }
        timer.KeySchema = Spec_->GroupBySchema;

        auto actualParents = CheckParents(timer, parents);
        FillMetaImpl(timer, actualParents);
        return {.ActualParentMessageIds = std::move(actualParents)};
    }

    virtual void FillMetaImpl(TMessageMeta& meta, const TMessageParentsConstPtr& parents) = 0;

protected:
    struct TParentsInfo
    {
        // Cached value of common parent key (nullopt if it is not computed).
        std::optional<TKey> Key;

        // Cached actual parents (filtered by stream dependency).
        absl::flat_hash_map<TStreamId, TMessageParentsConstPtr, ::THash<TStreamId>> ActualParentMessageIds;
    };

    const TComputationSpecPtr Spec_;

    TMessageParentsConstPtr CheckParents(const TMessageMeta& meta, const TMessageParentsConstPtr& parents)
    {
        // Fast path for pure map.
        if (parents->ParentMessages.size() == 1 && parents->ParentTimers.empty() && parents->ParentVisits.empty()) {
            const auto& parent = parents->ParentMessages[0];
            if (!Spec_->StreamsDependency.at(meta.StreamId).contains(parent->StreamId)) {
                THROW_ERROR_EXCEPTION("Message of stream %Qv violated streams dependency by depending on %Qv",
                    meta.StreamId,
                    parent->StreamId);
            }
            return parents;
        }

        auto& actualParents = ParentInfo_[parents].ActualParentMessageIds.emplace(meta.StreamId, nullptr).first->second;
        if (!actualParents) {
            std::vector<const TInputMessageConstPtr*> newParentMessages;
            newParentMessages.reserve(parents->ParentMessages.size());
            for (const auto& parent : parents->ParentMessages) {
                if (Spec_->StreamsDependency.at(meta.StreamId).contains(parent->StreamId)) {
                    newParentMessages.push_back(&parent);
                }
            }
            std::vector<const TInputTimerConstPtr*> newParentTimers;
            newParentTimers.reserve(parents->ParentTimers.size());
            for (const auto& parent : parents->ParentTimers) {
                if (Spec_->StreamsDependency.at(meta.StreamId).contains(parent->StreamId)) {
                    newParentTimers.push_back(&parent);
                }
            }
            std::vector<const TInputVisitConstPtr*> newParentVisits;
            newParentVisits.reserve(parents->ParentVisits.size());
            for (const auto& parent : parents->ParentVisits) {
                if (Spec_->StreamsDependency.at(meta.StreamId).contains(parent->StreamId)) {
                    newParentVisits.push_back(&parent);
                }
            }
            if (newParentMessages.empty() && newParentTimers.empty() && newParentVisits.empty()) {
                THROW_ERROR_EXCEPTION("Message %v of stream %v has no valid parents",
                    meta.MessageId,
                    meta.StreamId);
            }

            if (newParentMessages.size() == parents->ParentMessages.size() &&
                newParentTimers.size() == parents->ParentTimers.size() &&
                newParentVisits.size() == parents->ParentVisits.size())
            {
                // Fast path.
                actualParents = parents;
            } else {
                std::vector<TInputMessageConstPtr> newRealParentMessages;
                newRealParentMessages.reserve(newParentMessages.size());
                for (const auto* parent : newParentMessages) {
                    newRealParentMessages.push_back(*parent);
                }
                std::vector<TInputTimerConstPtr> newRealParentTimers;
                newRealParentTimers.reserve(newParentTimers.size());
                for (const auto* parent : newParentTimers) {
                    newRealParentTimers.push_back(*parent);
                }
                std::vector<TInputVisitConstPtr> newRealParentVisits;
                newRealParentVisits.reserve(newParentVisits.size());
                for (const auto* parent : newParentVisits) {
                    newRealParentVisits.push_back(*parent);
                }
                actualParents = New<TMessageParents>(
                    std::move(newRealParentMessages),
                    std::move(newRealParentTimers),
                    std::move(newRealParentVisits));
            }
        }
        return actualParents;
    }

private:
    const IEventTimestampAssignerPtr EventTimestampAssigner_;
    absl::flat_hash_map<TMessageParentsConstPtr, TParentsInfo, ::THash<TMessageParentsConstPtr>> ParentInfo_;
};

////////////////////////////////////////////////////////////////////////////////

class TUniqueMetaSetter
    : public TMetaSetterBase
{
public:
    TUniqueMetaSetter(
        TComputationSpecPtr spec,
        const TUniqueSeqNo& uniqueSeqNo,
        TSystemTimestamp now,
        IEventTimestampAssignerPtr eventTimestampAssigner)
        : TMetaSetterBase(std::move(spec), std::move(eventTimestampAssigner))
        , UniqueSeqNo_(uniqueSeqNo)
        , CurrentTimestamp_(now)
    { }

    void FillMetaImpl(TMessageMeta& meta, const TMessageParentsConstPtr& parents) override
    {
        meta.MessageId = GenerateOrderedMessageId(UniqueSeqNo_, meta.StreamId, LexicographicallySerialize(Index_));
        TSystemTimestamp sourceEventTimestamp = GetEventTimestamp(parents);
        if (meta.EventTimestamp == ZeroSystemTimestamp) {
            meta.EventTimestamp = sourceEventTimestamp;
        }
        meta.SystemTimestamp = CurrentTimestamp_;
        meta.AlignmentTimestamp = CurrentTimestamp_;
        ++Index_;
    }

private:
    const TUniqueSeqNo UniqueSeqNo_;
    const TSystemTimestamp CurrentTimestamp_;
    i64 Index_ = 0;
    absl::flat_hash_map<TMessageParentsConstPtr, TSystemTimestamp, ::THash<TMessageParentsConstPtr>> EventTimestamps_;

    TSystemTimestamp GetEventTimestamp(const TMessageParentsConstPtr& parents)
    {
        auto computeTimestamp = [&] {
            auto eventTimestamp = InfinitySystemTimestamp;
            for (const auto& parent : parents->ParentMessages) {
                eventTimestamp = std::min(eventTimestamp, parent->EventTimestamp);
            }
            for (const auto& parent : parents->ParentTimers) {
                eventTimestamp = std::min(eventTimestamp, parent->EventTimestamp);
            }
            for (const auto& parent : parents->ParentVisits) {
                eventTimestamp = std::min(eventTimestamp, parent->EventTimestamp);
            }
            YT_VERIFY(eventTimestamp != InfinitySystemTimestamp);
            YT_VERIFY(eventTimestamp != ZeroSystemTimestamp);
            return eventTimestamp;
        };

        if (parents->ParentMessages.size() + parents->ParentTimers.size() + parents->ParentVisits.size() <= 1) {
            return computeTimestamp();
        }

        auto& eventTimestamp = EventTimestamps_.emplace(parents, InfinitySystemTimestamp).first->second;
        if (eventTimestamp == InfinitySystemTimestamp) {
            eventTimestamp = computeTimestamp();
        }
        return eventTimestamp;
    }
};

////////////////////////////////////////////////////////////////////////////////

IMetaSetterPtr CreateUniqueMetaSetter(
    TComputationSpecPtr spec,
    const TUniqueSeqNo& uniqueSeqNo,
    TSystemTimestamp now,
    IEventTimestampAssignerPtr eventTimestampAssigner)
{
    return New<TUniqueMetaSetter>(std::move(spec), uniqueSeqNo, now, std::move(eventTimestampAssigner));
}

////////////////////////////////////////////////////////////////////////////////

class TDeterministicMetaSetter
    : public TMetaSetterBase
{
public:
    TDeterministicMetaSetter(
        TComputationSpecPtr spec,
        IEventTimestampAssignerPtr eventTimestampAssigner)
        : TMetaSetterBase(std::move(spec), eventTimestampAssigner)
    { }

    void FillMetaImpl(TMessageMeta& meta, const TMessageParentsConstPtr& parents) override
    {
        if (parents->ParentMessages.size() != 1 || parents->ParentTimers.size() != 0) {
            THROW_ERROR_EXCEPTION("Message should have exactly one parent message (not timer)")
                << TErrorAttribute("stream_id", meta.StreamId);
        }

        const auto& parent = parents->ParentMessages[0];
        auto& index = Indices_[std::pair(parent, meta.StreamId)];
        meta.MessageId = GenerateInheritedMessageId(parent->MessageId, meta.StreamId, LexicographicallySerialize(index));
        if (meta.EventTimestamp == ZeroSystemTimestamp) {
            meta.EventTimestamp = parent->EventTimestamp;
        }
        meta.SystemTimestamp = parent->SystemTimestamp;
        meta.AlignmentTimestamp = parent->AlignmentTimestamp;
        index += 1;
    }

private:
    absl::flat_hash_map<std::pair<TInputMessageConstPtr, TStreamId>, i64, ::THash<std::pair<TInputMessageConstPtr, TStreamId>>> Indices_;
};

////////////////////////////////////////////////////////////////////////////////

IMetaSetterPtr CreateDeterministicMetaSetter(
    TComputationSpecPtr spec,
    IEventTimestampAssignerPtr eventTimestampAssigner)
{
    return New<TDeterministicMetaSetter>(std::move(spec), std::move(eventTimestampAssigner));
}

////////////////////////////////////////////////////////////////////////////////

class TSwiftMergeMetaSetter
    : public TMetaSetterBase
{
public:
    TSwiftMergeMetaSetter(
        TComputationSpecPtr spec,
        // Deliberately unused: the merged MessageId is derived deterministically from the parents, not
        // from this per-epoch (non-deterministic) seq no. Kept for signature compatibility.
        [[maybe_unused]] const TUniqueSeqNo& uniqueSeqNo,
        IEventTimestampAssignerPtr eventTimestampAssigner)
        : TMetaSetterBase(std::move(spec), std::move(eventTimestampAssigner))
    { }

    void FillMetaImpl(TMessageMeta& meta, const TMessageParentsConstPtr& parents) override
    {
        if (!parents->ParentTimers.empty()) {
            THROW_ERROR_EXCEPTION("Swift map does not support timers as parents")
                << TErrorAttribute("stream_id", meta.StreamId);
        }
        if (parents->ParentMessages.empty()) {
            THROW_ERROR_EXCEPTION("Message must have at least one parent")
                << TErrorAttribute("stream_id", meta.StreamId);
        }

        if (parents->ParentMessages.size() == 1) {
            // Single-parent fast path: inherit MessageId and timestamps from the parent — same as the deterministic setter.
            const auto& parent = parents->ParentMessages[0];
            auto& index = InheritedIndices_[std::pair(parent, meta.StreamId)];
            meta.MessageId = GenerateInheritedMessageId(parent->MessageId, meta.StreamId, LexicographicallySerialize(index));
            if (meta.EventTimestamp == ZeroSystemTimestamp) {
                meta.EventTimestamp = parent->EventTimestamp;
            }
            meta.SystemTimestamp = parent->SystemTimestamp;
            meta.AlignmentTimestamp = parent->AlignmentTimestamp;
            index += 1;
            return;
        }

        // Merged-parents path. The MessageId is derived DETERMINISTICALLY from the parent MessageIds
        // (the lexicographically minimal parent id followed by a 128-bit digest of all parent ids), NOT
        // from the per-epoch UniqueSeqNo. A merged output that is replayed after a job restart must get
        // the SAME MessageId: otherwise the message distributor cannot match the replay to the in-flight
        // task, drops the dead predecessor's task on re-route, and loses the OnDistributed
        // (merge-tracker) callback — so the merged message's parents would never be marked persisted (a
        // deadlock, amplified up the graph by fan-in). As before, the per-key MessageId order across the
        // merge is not preserved; downstream must tolerate this (see the
        // AllowBatchingWithRelaxedGuarantees parameter of TSwiftMapComputation).
        const auto& merged = GetMergedInfo(parents);
        auto& index = MergedIndices_[std::pair(parents.Get(), meta.StreamId)];
        meta.MessageId = GenerateInheritedMessageId(merged.Digest, meta.StreamId, LexicographicallySerialize(index));
        if (meta.EventTimestamp == ZeroSystemTimestamp) {
            meta.EventTimestamp = merged.EventTimestamp;
        }
        meta.SystemTimestamp = merged.SystemTimestamp;
        meta.AlignmentTimestamp = merged.AlignmentTimestamp;
        index += 1;
    }

private:
    struct TMergedInfo
    {
        TSystemTimestamp SystemTimestamp{};
        TSystemTimestamp EventTimestamp{};
        TSystemTimestamp AlignmentTimestamp{};
        // Merged MessageId base: "<min parent id>-<hex of a deterministic 128-bit digest of the parent
        // MessageIds>". The min-parent prefix keeps merged ids starting with a UniqueSeqNo like all
        // other ids.
        TMessageId Digest;
    };

    absl::flat_hash_map<std::pair<TInputMessageConstPtr, TStreamId>, i64, ::THash<std::pair<TInputMessageConstPtr, TStreamId>>> InheritedIndices_;
    absl::flat_hash_map<std::pair<const TMessageParents*, TStreamId>, i64, ::THash<std::pair<const TMessageParents*, TStreamId>>> MergedIndices_;
    absl::flat_hash_map<TMessageParentsConstPtr, TMergedInfo, ::THash<TMessageParentsConstPtr>> MergedInfos_;

    const TMergedInfo& GetMergedInfo(const TMessageParentsConstPtr& parents)
    {
        auto [it, inserted] = MergedInfos_.emplace(parents, TMergedInfo{});
        if (inserted) {
            auto& merged = it->second;
            merged.SystemTimestamp = ZeroSystemTimestamp;
            merged.AlignmentTimestamp = InfinitySystemTimestamp;
            merged.EventTimestamp = InfinitySystemTimestamp;
            uint128 digestHash{0, 0};
            std::string_view minParentId;
            for (const auto& parent : parents->ParentMessages) {
                // SystemTimestamp: max — if any parent has dropped below the downstream system watermark,
                // picking the min would let the watermark filter drop the whole merged aggregate.
                merged.SystemTimestamp = std::max(merged.SystemTimestamp, parent->SystemTimestamp);
                // AlignmentTimestamp / EventTimestamp: min — these drive watermarks, which must advance
                // no faster than the slowest parent that has been absorbed into the merged message.
                merged.AlignmentTimestamp = std::min(merged.AlignmentTimestamp, parent->AlignmentTimestamp);
                merged.EventTimestamp = std::min(merged.EventTimestamp, parent->EventTimestamp);
                // Order-sensitive 128-bit digest over the parent MessageIds. Deterministic across epoch
                // re-runs because the parents arrive in a deterministic order with stable MessageIds.
                const auto idView = parent->MessageId.Underlying();
                if (minParentId.empty() || idView < minParentId) {
                    minParentId = idView;
                }
                digestHash = CityHash128WithSeed(idView.data(), idView.size(), digestHash);
            }
            YT_VERIFY(merged.EventTimestamp != InfinitySystemTimestamp);
            YT_VERIFY(merged.EventTimestamp != ZeroSystemTimestamp);
            const ui64 digestRaw[2] = {Uint128Low64(digestHash), Uint128High64(digestHash)};
            auto digestHex = HexEncode(digestRaw, sizeof(digestRaw));
            // Prefix the digest with the lexicographically minimal parent id: every other id starts with
            // a UniqueSeqNo prefix (generated ids by construction, swift ids by inheriting the parent
            // prefix), so keep merged ids in the same family and lexicographically >= that parent.
            std::string base;
            base.reserve(minParentId.size() + 1 + digestHex.size());
            base.append(minParentId);
            base.push_back('-');
            base.append(digestHex.data(), digestHex.size());
            merged.Digest = TMessageId(std::move(base));
        }
        return it->second;
    }
};

////////////////////////////////////////////////////////////////////////////////

IMetaSetterPtr CreateSwiftMergeMetaSetter(
    TComputationSpecPtr spec,
    const TUniqueSeqNo& uniqueSeqNo,
    IEventTimestampAssignerPtr eventTimestampAssigner)
{
    return New<TSwiftMergeMetaSetter>(std::move(spec), uniqueSeqNo, std::move(eventTimestampAssigner));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
