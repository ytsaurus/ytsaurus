#pragma once

#include "public.h"
#include "meta_state.h"

#include <ytlib/rpc/service.h>
#include <ytlib/meta_state/meta_state_manager.pb.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////
    
struct TSaveContext
{
    DEFINE_BYVAL_RW_PROPERTY(TOutputStream*, Output);
};

struct TLoadContext
{
    DEFINE_BYVAL_RW_PROPERTY(i32, Version);
    DEFINE_BYVAL_RW_PROPERTY(TInputStream*, Input);
};

typedef TCallback<void(const TSaveContext&)> TSaver;
typedef TCallback<void(const TLoadContext&)> TLoader;
typedef TCallback<void(int)> TVersionValidator;

class TMetaStatePart
    : public virtual TRefCounted
{
public:
    TMetaStatePart(
        IMetaStateManagerPtr metaStateManager,
        TCompositeMetaStatePtr metaState);

protected:
    IMetaStateManagerPtr MetaStateManager;
    TCompositeMetaStatePtr MetaState;

    void RegisterSaver(int priority, const Stroka& name, i32 version, TSaver saver);
    
    template <class TContext>
    void RegisterSaver(
        int priority,
        const Stroka& name,
        i32 version,
        TCallback<void(const TContext&)> saver,
        const TContext& context);

    void RegisterLoader(
        const Stroka& name,
        TVersionValidator versionValidator,
        TLoader loader);

    template <class TContext>
    void RegisterLoader(
        const Stroka& name,
        TVersionValidator versionValidator,
        TCallback<void(const TContext&)> loader,
        const TContext& context);

    template <class TRequest, class TResponse>
    void RegisterMethod(TCallback<TResponse(const TRequest&)> handler);
    
    bool IsLeader() const;
    bool IsFolllower() const;
    bool IsRecovery() const;

    virtual void Clear();

    virtual void OnStartLeading();
    virtual void OnLeaderRecoveryComplete();
    virtual void OnActiveQuorumEstablished();
    virtual void OnStopLeading();

    virtual void OnStartFollowing();
    virtual void OnFollowerRecoveryComplete();
    virtual void OnStopFollowing();

    virtual void OnStartRecovery();
    virtual void OnStopRecovery();

private:
    typedef TMetaStatePart TThis;
    friend class TCompositeMetaState;

    template <class TRequest, class TResponse>
    struct TThunkTraits;
};

////////////////////////////////////////////////////////////////////////////////
    
class TCompositeMetaState
    : public IMetaState 
{
public:
    void RegisterPart(TMetaStatePartPtr part);

private:
    friend class TMetaStatePart;

    struct TSaverInfo
    {
        int Priority;
        Stroka Name;
        i32 Version;
        TSaver Saver;

        TSaverInfo(int priority, const Stroka& name, i32 version, TSaver saver);
    };

    struct TLoaderInfo
    {
        Stroka Name;
        TVersionValidator VersionValidator;
        TLoader Loader;

        TLoaderInfo(const Stroka& name, TVersionValidator versionValidator, TLoader loader);
    };

    typedef yhash_map< Stroka, TCallback<void(TMutationContext* context)> > TMethodMap;
    TMethodMap Methods;

    std::vector<TMetaStatePartPtr> Parts;

    yhash_map<Stroka, TLoaderInfo> Loaders;
    yhash_map<Stroka, TSaverInfo>  Savers;

    virtual void Save(TOutputStream* output) override;
    virtual void Load(TInputStream* input) override;

    virtual void ApplyMutation(TMutationContext* context) throw() override;

    virtual void Clear() override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT

#define COMPOSITE_META_STATE_INL_H_
#include "composite_meta_state-inl.h"
#undef COMPOSITE_META_STATE_INL_H_
