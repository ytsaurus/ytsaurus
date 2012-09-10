#include "stdafx.h"
#include "composite_meta_state.h"
#include "private.h"
#include "meta_state_manager.h"

#include <ytlib/misc/foreach.h>
#include <ytlib/actions/bind.h>

namespace NYT {
namespace NMetaState {

using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

////////////////////////////////////////////////////////////////////////////////

TMetaStatePart::TMetaStatePart(
    IMetaStateManagerPtr metaStateManager,
    TCompositeMetaStatePtr metaState)
    : MetaStateManager(metaStateManager)
    , MetaState(metaState)
{
    YASSERT(metaStateManager);
    YASSERT(metaState);

    metaStateManager->SubscribeStartLeading(BIND(
        &TThis::OnStartLeading,
        MakeWeak(this)));
    metaStateManager->SubscribeStartLeading(BIND(
        &TThis::OnStartRecovery,
        MakeWeak(this)));
    metaStateManager->SubscribeLeaderRecoveryComplete(BIND(
        &TThis::OnStopRecovery,
        MakeWeak(this)));
    metaStateManager->SubscribeLeaderRecoveryComplete(BIND(
        &TThis::OnLeaderRecoveryComplete,
        MakeWeak(this)));
    metaStateManager->SubscribeActiveQuorumEstablished(BIND(
        &TThis::OnActiveQuorumEstablished,
        MakeWeak(this)));
    metaStateManager->SubscribeStopLeading(BIND(
        &TThis::OnStopLeading,
        MakeWeak(this)));

    metaStateManager->SubscribeStartFollowing(BIND(
        &TThis::OnStartFollowing,
        MakeWeak(this)));
    metaStateManager->SubscribeStartFollowing(BIND(
        &TThis::OnStartRecovery,
        MakeWeak(this)));
    metaStateManager->SubscribeFollowerRecoveryComplete(BIND(
        &TThis::OnStopRecovery,
        MakeWeak(this)));
    metaStateManager->SubscribeFollowerRecoveryComplete(BIND(
        &TThis::OnFollowerRecoveryComplete,
        MakeWeak(this)));
    metaStateManager->SubscribeStopFollowing(BIND(
        &TThis::OnStopFollowing,
        MakeWeak(this)));
}

void TMetaStatePart::Clear()
{ }

bool TMetaStatePart::IsLeader() const
{
    return MetaStateManager->IsLeader();
}

bool TMetaStatePart::IsFolllower() const
{
    return MetaStateManager->IsFolllower();
}

bool TMetaStatePart::IsRecovery() const
{
    return MetaStateManager->IsRecovery();
}

void TMetaStatePart::OnStartLeading()
{ }

void TMetaStatePart::OnLeaderRecoveryComplete()
{ }

void TMetaStatePart::OnActiveQuorumEstablished()
{ }

void TMetaStatePart::OnStopLeading()
{ }

void TMetaStatePart::OnStartFollowing()
{ }

void TMetaStatePart::OnFollowerRecoveryComplete()
{ }

void TMetaStatePart::OnStopFollowing()
{ }

void TMetaStatePart::OnStartRecovery()
{ }

void TMetaStatePart::OnStopRecovery()
{ }

////////////////////////////////////////////////////////////////////////////////

TCompositeMetaState::TSaverInfo::TSaverInfo(
    const Stroka& name,
    TSaver saver,
    ESavePhase phase)
    : Name(name)
    , Saver(saver)
    , Phase(phase)
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
 
    std::vector<TSaverInfo> saverInfos;
    FOREACH (const auto& pair, Savers) {
        saverInfos.push_back(pair.second);
    }
    std::sort(
        saverInfos.begin(),
        saverInfos.end(),
        [] (const TSaverInfo& lhs, const TSaverInfo& rhs) {
            return lhs.Phase < rhs.Phase ||
                (lhs.Phase == rhs.Phase && lhs.Name < rhs.Name);
        });

    FOREACH (const auto& info, saverInfos) {
        ::Save(output, info.Name);
        info.Saver.Run(output);
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
            LOG_FATAL("No appropriate loader is registered for part %s", ~name.Quote());
        }
        auto loader = it->second;
        loader.Run(input);
    }
}

void TCompositeMetaState::ApplyMutation(TMutationContext* context) throw()
{
    if (context->GetType() == "")  {
        // Empty mutation. It is used for debug purpose.
        return;
    }
    auto it = Methods.find(context->GetType());
    YCHECK(it != Methods.end());
    it->second.Run(context);
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

    YCHECK(Loaders.insert(MakePair(name, MoveRV(loader))).second);
}

void TCompositeMetaState::RegisterSaver(
    const Stroka& name,
    TSaver saver, 
    ESavePhase phase)
{
    YASSERT(!saver.IsNull());

    TSaverInfo info(name, MoveRV(saver), phase);
    YCHECK(Savers.insert(MakePair(name, info)).second);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
