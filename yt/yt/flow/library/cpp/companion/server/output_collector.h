#pragma once

#include "public.h"

#include "codec.h"

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/output_collector.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TGroupingOutputCollector);

//! Companion-side IOutputCollector: records output into per-parent groups matching
//! the wire's TResponseData.TGroup. #SetParents opens a group attributed to the
//! given entities; output added on the root collector is attributed to the whole
//! batch input.
class TGroupingOutputCollector
    : public IOutputCollector
{
public:
    //! Creates the root collector; |batchParentIds| / |batchParentKeys| are all input
    //! entity ids and keys of the batch, used for output emitted without SetParents.
    static TGroupingOutputCollectorPtr CreateRoot(
        std::vector<TMessageId> batchParentIds,
        std::vector<TKey> batchParentKeys);

    //! Creates the root collector attributed to all input entities of the batch.
    static TGroupingOutputCollectorPtr CreateRoot(
        const std::vector<TInputMessageConstPtr>& messages,
        const std::vector<TInputTimerConstPtr>& timers,
        const std::vector<TInputVisitConstPtr>& visits);

    [[nodiscard]] IOutputCollectorPtr SetParents(
        const std::vector<TInputMessageConstPtr>& messages,
        const std::vector<TInputTimerConstPtr>& timers,
        const std::vector<TInputVisitConstPtr>& visits) override;

    void AddMessage(TMessage&& message, bool distribute = true) override;

    void AddTimer(
        TSystemTimestamp triggerTimestamp,
        std::optional<TSystemTimestamp> eventTimestamp = {}) override;
    void AddTimer(
        const TStreamId& streamId,
        TSystemTimestamp triggerTimestamp,
        std::optional<TSystemTimestamp> eventTimestamp = {}) override;
    void AddTimer(TTimer&& timer) override;

    //! Returns the non-empty groups recorded so far (root collector only).
    std::vector<TOutputGroup> TakeGroups();

private:
    struct TRecorder final
    {
        std::vector<TOutputGroup> Groups;
    };

    struct TParents
    {
        std::vector<TMessageId> Ids;
        std::vector<TKey> Keys;
    };

    TGroupingOutputCollector(
        std::shared_ptr<TRecorder> recorder,
        std::vector<TMessageId> parentIds,
        std::vector<TKey> parentKeys);

    static TParents ExtractParents(
        const std::vector<TInputMessageConstPtr>& messages,
        const std::vector<TInputTimerConstPtr>& timers,
        const std::vector<TInputVisitConstPtr>& visits);

    TOutputGroup& CurrentGroup();
    TOutputGroup& KeyedTimerGroup(const TKey& key);
    void ValidateImplicitTimerKey() const;

    const std::shared_ptr<TRecorder> Recorder_;
    const std::vector<TMessageId> ParentIds_;
    //! Aligned with |ParentIds_|; used to route and validate keyed timers,
    //! whose key is not representable on the wire.
    const std::vector<TKey> ParentKeys_;
    std::optional<size_t> GroupIndex_;
    //! Per-key indexes of the timer groups opened by #KeyedTimerGroup.
    THashMap<TKey, size_t> KeyedGroupIndex_;

    DECLARE_NEW_FRIEND()
};

DEFINE_REFCOUNTED_TYPE(TGroupingOutputCollector);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
