#include "stdafx.h"
#include "composite_meta_state.h"
#include "common.h"
#include "meta_state_manager.h"
#include "composite_meta_state_detail.h"

#include <ytlib/misc/foreach.h>
#include <ytlib/actions/bind.h>

namespace NYT {
namespace NMetaState {

using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

////////////////////////////////////////////////////////////////////////////////

TMetaStatePart::TMetaStatePart(
    IMetaStateManager* metaStateManager,
    TCompositeMetaState* metaState)
    : MetaStateManager(metaStateManager)
    , MetaState(metaState)
{
    YASSERT(metaStateManager);
    YASSERT(metaState);

    metaStateManager->SubscribeStartLeading(BIND(
        &TThis::OnStartLeading,
        MakeWeak(this)));
    metaStateManager->SubscribeLeaderRecoveryComplete(BIND(
        &TThis::OnLeaderRecoveryComplete,
        MakeWeak(this)));
    metaStateManager->SubscribeStopLeading(BIND(
        &TThis::OnStopLeading,
        MakeWeak(this)));
}

void TMetaStatePart::Clear()
{ }

bool TMetaStatePart::IsLeader() const
{
    auto status = MetaStateManager->GetStateStatus();
    return status == EPeerStatus::Leading;
}

bool TMetaStatePart::IsFolllower() const
{
    auto status = MetaStateManager->GetStateStatus();
    return status == EPeerStatus::Following;
}

bool TMetaStatePart::IsRecovery() const
{
    auto status = MetaStateManager->GetStateStatus();
    return status == EPeerStatus::LeaderRecovery || status == EPeerStatus::FollowerRecovery;
}

void TMetaStatePart::OnStartLeading()
{ }

void TMetaStatePart::OnLeaderRecoveryComplete()
{ }

void TMetaStatePart::OnStopLeading()
{ }

////////////////////////////////////////////////////////////////////////////////

void TCompositeMetaState::RegisterPart(TMetaStatePartPtr part)
{
    YASSERT(part);

    Parts.push_back(part);
}

void TCompositeMetaState::Save(TOutputStream* output)
{
    i32 size = Savers.size();
    ::Save(output, size);
    
    // XXX(sandello): :7
    typedef TPair<TPair<ESavePhase, Stroka>, TSaver> TElement;
    yvector<TElement> savers;
    FOREACH (const auto& pair, Savers) {
        savers.push_back(
            MakePair(MakePair(pair.second.second, pair.first), pair.second.first));
    }
    std::sort(
        savers.begin(), savers.end(),
        [] (const TElement& lhs, const TElement& rhs)
            {
                return lhs.first.first < rhs.first.first;
            });

    FOREACH (auto pair, savers) {
        Stroka name = pair.first.second;
        ::Save(output, name);
        auto saver = pair.second;
        saver.Run(output);
    }
}

void TCompositeMetaState::Load(TInputStream* input)
{
    i32 size;
    ::Load(input, size);
    
    for (i32 i = 0; i < size; ++i) {
        Stroka name;
        ::Load(input, name);
        auto it = Loaders.find(name);
        if (it == Loaders.end()) {
            LOG_FATAL("No appropriate loader is registered (PartName: %s)",
                ~name);
        }
        auto loader = it->second;
        loader.Run(input);
    }
}

void TCompositeMetaState::ApplyChange(const TRef& changeData)
{
    TMsgChangeHeader header;
    TRef messageData;
    DeserializeChange(
        changeData,
        &header,
        &messageData);

    Stroka changeType = header.change_type();

    auto it = Methods.find(changeType);
    YASSERT(it != Methods.end());

    it->second.Run(messageData);
}

void TCompositeMetaState::Clear()
{
    FOREACH (auto& part, Parts) {
        part->Clear();
    }
}

void TCompositeMetaState::RegisterLoader(const Stroka& name, TLoader loader)
{
    YASSERT(!loader.IsNull());

    YVERIFY(Loaders.insert(MakePair(name, MoveRV(loader))).second);
}

void TCompositeMetaState::RegisterSaver(
    const Stroka& name,
    TSaver saver, 
    ESavePhase phase)
{
    YASSERT(!saver.IsNull());

    YVERIFY(Savers.insert(MakePair(name, MakePair(MoveRV(saver), phase))).second);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
