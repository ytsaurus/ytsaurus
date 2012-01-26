#include "stdafx.h"
#include "composite_meta_state.h"
#include "composite_meta_state_detail.h"

#include <ytlib/misc/foreach.h>
#include <ytlib/actions/action_util.h>

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

    metaStateManager->OnStartLeading().Subscribe(FromMethod(
        &TThis::OnStartLeading,
        TPtr(this)));
    metaStateManager->OnLeaderRecoveryComplete().Subscribe(FromMethod(
        &TThis::OnLeaderRecoveryComplete,
        TPtr(this)));
    metaStateManager->OnStopLeading().Subscribe(FromMethod(
        &TThis::OnStopLeading,
        TPtr(this)));
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

TCompositeMetaState::TSaveContext::TSaveContext(
    TOutputStream* output,
    IInvoker::TPtr invoker)
    : Output(output)
    , Invoker(invoker)
{ }

////////////////////////////////////////////////////////////////////////////////

void TCompositeMetaState::RegisterPart(TMetaStatePart::TPtr part)
{
    YASSERT(part);

    Parts.push_back(part);
}

TFuture<TVoid>::TPtr TCompositeMetaState::Save(TOutputStream* output, IInvoker::TPtr invoker)
{
    i32 size = Savers.size();
    invoker->Invoke(FromFunctor([=] ()
        {
            ::Save(output, size);
        }));

    yvector< TPair<Stroka, TSaver::TPtr> > savers(Savers.begin(), Savers.end());
    std::sort(savers.begin(), savers.end());

    TFuture<TVoid>::TPtr result;
    FOREACH(auto pair, savers) {
        Stroka name = pair.first;
        invoker->Invoke(FromFunctor([=] ()
            {
                ::Save(output, name);
            }));
        auto saver = pair.second;
        TSaveContext context(output, invoker);
        result = saver->Do(context);
    }
    return result;
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
        loader->Do(input);
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

    it->second->Do(messageData);
}

void TCompositeMetaState::Clear()
{
    FOREACH(auto& part, Parts) {
        part->Clear();
    }
}

void TCompositeMetaState::RegisterLoader(const Stroka& name, TLoader::TPtr loader)
{
    YASSERT(loader);

    YVERIFY(Loaders.insert(MakePair(name, loader)).second);
}

void TCompositeMetaState::RegisterSaver(const Stroka& name, TSaver::TPtr saver)
{
    YASSERT(saver);

    YVERIFY(Savers.insert(MakePair(name, saver)).second);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
