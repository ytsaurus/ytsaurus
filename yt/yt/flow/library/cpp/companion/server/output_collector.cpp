#include "output_collector.h"

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

TGroupingOutputCollector::TGroupingOutputCollector(
    std::shared_ptr<TRecorder> recorder,
    std::vector<TMessageId> parentIds,
    std::vector<TKey> parentKeys)
    : Recorder_(std::move(recorder))
    , ParentIds_(std::move(parentIds))
    , ParentKeys_(std::move(parentKeys))
{ }

TGroupingOutputCollectorPtr TGroupingOutputCollector::CreateRoot(
    std::vector<TMessageId> batchParentIds,
    std::vector<TKey> batchParentKeys)
{
    return New<TGroupingOutputCollector>(
        std::make_shared<TRecorder>(),
        std::move(batchParentIds),
        std::move(batchParentKeys));
}

TGroupingOutputCollectorPtr TGroupingOutputCollector::CreateRoot(
    const std::vector<TInputMessageConstPtr>& messages,
    const std::vector<TInputTimerConstPtr>& timers,
    const std::vector<TInputVisitConstPtr>& visits)
{
    auto parents = ExtractParents(messages, timers, visits);
    return CreateRoot(std::move(parents.Ids), std::move(parents.Keys));
}

TGroupingOutputCollector::TParents TGroupingOutputCollector::ExtractParents(
    const std::vector<TInputMessageConstPtr>& messages,
    const std::vector<TInputTimerConstPtr>& timers,
    const std::vector<TInputVisitConstPtr>& visits)
{
    TParents parents;
    parents.Ids.reserve(messages.size() + timers.size() + visits.size());
    parents.Keys.reserve(parents.Ids.capacity());
    for (const auto& message : messages) {
        parents.Ids.push_back(message->GetMeta().MessageId);
        parents.Keys.push_back(message->Key);
    }
    for (const auto& timer : timers) {
        parents.Ids.push_back(timer->GetMeta().MessageId);
        parents.Keys.push_back(timer->Key);
    }
    for (const auto& visit : visits) {
        parents.Ids.push_back(visit->GetMeta().MessageId);
        parents.Keys.push_back(visit->Key);
    }
    return parents;
}

IOutputCollectorPtr TGroupingOutputCollector::SetParents(
    const std::vector<TInputMessageConstPtr>& messages,
    const std::vector<TInputTimerConstPtr>& timers,
    const std::vector<TInputVisitConstPtr>& visits)
{
    auto parents = ExtractParents(messages, timers, visits);
    return New<TGroupingOutputCollector>(
        Recorder_,
        std::move(parents.Ids),
        std::move(parents.Keys));
}

void TGroupingOutputCollector::AddMessage(TMessage&& message, bool distribute)
{
    auto& group = CurrentGroup();
    group.Messages.push_back(std::move(message));
    group.Distribute.push_back(distribute);
}

void TGroupingOutputCollector::AddTimer(
    TSystemTimestamp triggerTimestamp,
    std::optional<TSystemTimestamp> eventTimestamp)
{
    ValidateImplicitTimerKey();
    CurrentGroup().Timers.push_back(NCompanion::TNewTimer{
        .TriggerTimestamp = triggerTimestamp,
        .EventTimestamp = eventTimestamp,
        .StreamId = std::nullopt,
    });
}

void TGroupingOutputCollector::AddTimer(
    const TStreamId& streamId,
    TSystemTimestamp triggerTimestamp,
    std::optional<TSystemTimestamp> eventTimestamp)
{
    ValidateImplicitTimerKey();
    CurrentGroup().Timers.push_back(NCompanion::TNewTimer{
        .TriggerTimestamp = triggerTimestamp,
        .EventTimestamp = eventTimestamp,
        .StreamId = streamId,
    });
}

void TGroupingOutputCollector::ValidateImplicitTimerKey() const
{
    THROW_ERROR_EXCEPTION_IF(ParentKeys_.empty(),
        "Companion output timers require parent entities to provide their key");
    for (const auto& parentKey : ParentKeys_) {
        THROW_ERROR_EXCEPTION_UNLESS(parentKey == ParentKeys_.front(),
            "Companion output timers require all group parents to have the same key");
    }
}

void TGroupingOutputCollector::AddTimer(TTimer&& timer)
{
    auto newTimer = NCompanion::TNewTimer{
        .TriggerTimestamp = timer.TriggerTimestamp,
        .EventTimestamp = timer.EventTimestamp != TSystemTimestamp(0)
            ? std::optional(timer.EventTimestamp)
            : std::nullopt,
        .StreamId = !timer.StreamId.Underlying().empty()
            ? std::optional(timer.StreamId)
            : std::nullopt,
    };
    if (!timer.Key.Underlying()) {
        // A keyless timer is keyed by the worker from the group parents,
        // which therefore must agree on the key.
        ValidateImplicitTimerKey();
        CurrentGroup().Timers.push_back(std::move(newTimer));
        return;
    }
    KeyedTimerGroup(timer.Key).Timers.push_back(std::move(newTimer));
}

TOutputGroup& TGroupingOutputCollector::KeyedTimerGroup(const TKey& key)
{
    // The wire timer carries no key: the worker keys new timers by the group
    // parents. Route a keyed timer into a group holding only the parents with
    // the same key; a key foreign to all parents is not representable.
    if (auto it = KeyedGroupIndex_.find(key); it != KeyedGroupIndex_.end()) {
        return Recorder_->Groups[it->second];
    }
    std::vector<TMessageId> parentIds;
    for (int index = 0; index < std::ssize(ParentIds_); ++index) {
        if (ParentKeys_[index] == key) {
            parentIds.push_back(ParentIds_[index]);
        }
    }
    THROW_ERROR_EXCEPTION_IF(parentIds.empty(),
        "Companion output timers must target the key of a group parent; "
        "the timer key is not representable on the wire");
    auto groupIndex = Recorder_->Groups.size();
    auto& group = Recorder_->Groups.emplace_back();
    group.ParentIds = std::move(parentIds);
    KeyedGroupIndex_[key] = groupIndex;
    return group;
}

std::vector<TOutputGroup> TGroupingOutputCollector::TakeGroups()
{
    std::vector<TOutputGroup> groups;
    for (auto& group : Recorder_->Groups) {
        if (!group.Messages.empty() || !group.Timers.empty()) {
            groups.push_back(std::move(group));
        }
    }
    Recorder_->Groups.clear();
    return groups;
}

TOutputGroup& TGroupingOutputCollector::CurrentGroup()
{
    if (!GroupIndex_) {
        GroupIndex_ = Recorder_->Groups.size();
        auto& group = Recorder_->Groups.emplace_back();
        group.ParentIds = ParentIds_;
    }
    return Recorder_->Groups[*GroupIndex_];
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
