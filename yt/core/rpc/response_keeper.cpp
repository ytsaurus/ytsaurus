#include "stdafx.h"
#include "response_keeper.h"
#include "config.h"

#include <core/misc/serialize.h>

#include <core/concurrency/thread_affinity.h>

#include <queue>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TResponseKeeper::TImpl
    : public TRefCounted
{
public:
    explicit TImpl(
        TResponseKeeperConfigPtr config,
        const NProfiling::TProfiler& profiler)
        : Config_(config)
        , ResponseSpace_(0)
        , Profiler(profiler)
        , CountCounter_("/kept_response_count")
        , SpaceCounter_("/kept_response_space")
    {
        YCHECK(Config_);
    }

    TSharedRefArray FindResponse(const TMutationId& id)
    {
        VERIFY_THREAD_AFFINITY(ClientThread);
        YASSERT(id != NullMutationId);

        auto it = ResponseMap_.find(id);
        return it == ResponseMap_.end() ? TSharedRefArray() : it->second;
    }

    void RegisterResponse(
        const TMutationId& id,
        const TSharedRefArray& data,
        TInstant now)
    {
        VERIFY_THREAD_AFFINITY(ClientThread);
        YASSERT(data);
        YASSERT(id != NullMutationId);

        auto pair = ResponseMap_.insert(std::make_pair(id, data));
        YASSERT(pair.second);

        TItem item;
        item.Iterator = pair.first;
        item.When = now;
        ResponseQueue_.push_back(item);

        UpdateCounters(data, +1);
        OnProfiling();
    }

    void RemoveExpiredResponses(TInstant now)
    {
        VERIFY_THREAD_AFFINITY(ClientThread);

        bool changed = false;
        auto deadline = now - Config_->ExpirationTime;
        while (!ResponseQueue_.empty()) {
            const auto& item = ResponseQueue_.front();
            if (item.When > deadline) {
                break;
            }

            UpdateCounters(item.Iterator->second, -1);
            ResponseMap_.erase(item.Iterator);
            ResponseQueue_.pop_front();
            changed = true;
        }

        if (changed) {
            OnProfiling();
        }
    }

    void Clear()
    {
        VERIFY_THREAD_AFFINITY(ClientThread);

        ResponseMap_.clear();
        ResponseQueue_.clear();
        ResponseSpace_ = 0;

        OnProfiling();
    }

    void Save(TStreamSaveContext& context) const
    {
        using NYT::Save;
        
        Save(context, ResponseMap_);
        
        for (const auto& item : ResponseQueue_) {
            Save(context, item.Iterator->first);
            Save(context, item.When);
        }
    }

    void Load(TStreamLoadContext& context)
    {
        using NYT::Load;
        
        Load(context, ResponseMap_);

        ResponseQueue_.clear();
        for (int index = 0; index < static_cast<int>(ResponseMap_.size()); ++index) {
            TItem item;
            auto id = Load<TMutationId>(context);
            item.Iterator = ResponseMap_.find(id);
            YCHECK(item.Iterator != ResponseMap_.end());
            item.When = Load<TInstant>(context);
        }

        ResponseSpace_ = 0;
        for (const auto& pair : ResponseMap_) {
            UpdateCounters(pair.second, +1);
        }
    }

private:
    TResponseKeeperConfigPtr Config_;

    i64 ResponseSpace_;

    typedef yhash_map<TMutationId, TSharedRefArray> TResponseMap;
    TResponseMap ResponseMap_;

    struct TItem
    {
        TResponseMap::iterator Iterator;
        TInstant When;
    };

    typedef std::deque<TItem> TResponseQueue;
    TResponseQueue ResponseQueue_;

    NProfiling::TProfiler Profiler;
    NProfiling::TAggregateCounter CountCounter_;
    NProfiling::TAggregateCounter SpaceCounter_;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);


    void UpdateCounters(const TSharedRefArray& data, int delta)
    {
        for (const auto& part : data) {
            ResponseSpace_ += delta * part.Size();
        }
    }

    void OnProfiling()
    {
        Profiler.Aggregate(CountCounter_, ResponseMap_.size());
        Profiler.Aggregate(SpaceCounter_, ResponseSpace_);
    }


};

////////////////////////////////////////////////////////////////////////////////

TResponseKeeper::TResponseKeeper(
    TResponseKeeperConfigPtr config,
    const NProfiling::TProfiler& profiler)
    : Impl_(New<TImpl>(
        config,
        profiler))
{ }

TResponseKeeper::~TResponseKeeper()
{ }

TSharedRefArray TResponseKeeper::FindResponse(const TMutationId& id)
{
    return Impl_->FindResponse(id);
}

void TResponseKeeper::RegisterResponse(
    const TMutationId& id,
    const TSharedRefArray& data,
    TInstant now)
{
    Impl_->RegisterResponse(id, data, now);
}

void TResponseKeeper::RemoveExpiredResponses(TInstant now)
{
    Impl_->RemoveExpiredResponses(now);
}

void TResponseKeeper::Clear()
{
    Impl_->Clear();
}

void TResponseKeeper::Save(TStreamSaveContext& context) const
{
    Impl_->Save(context);
}

void TResponseKeeper::Load(TStreamLoadContext& context)
{
    Impl_->Load(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
