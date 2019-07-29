#pragma once

#include "config.h"

#include <yt/client/api/public.h>
#include <yt/client/api/client.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A data structure for keeping and updating a list of participants that are alive
//! in the certain group (defined by its Cypress directory path) with their attributes.
class TDiscovery
    : public virtual TRefCounted
{
public:
    using TAttributeDictionary = THashMap<TString, NYTree::INodePtr>; 

    TDiscovery(
        TDiscoveryConfigPtr config,
        NApi::IClientPtr client,
        IInvokerPtr invoker,
        std::vector<TString> extraAttributes,
        const NLogging::TLogger& logger);

    //! Make this participant exposed to the group.
    TFuture<void> Enter(TString name, TAttributeDictionary attributes);
    //! Make this participant unexposed to the group.
    TFuture<void> Leave();

    THashMap<TString, TAttributeDictionary> List() const;
    //! Temporary exclude |name| from the list of available participants.
    void Ban(TString name); 

    //! Start updating the list of available participants.
    //! Returns a future that becomes set after first update.
    TFuture<void> StartPolling();
    //! Stop updating the list of available participants.
    //! Returns a future that becomes set after stopping PeriodicExecutor.
    TFuture<void> StopPolling();

private:
    TDiscoveryConfigPtr Config_;
    NApi::IClientPtr Client_;
    IInvokerPtr Invoker_;
    THashMap<TString, TAttributeDictionary> List_;
    THashMap<TString, TInstant> BannedSince_;
    NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;
    NApi::TListNodeOptions ListOptions_;
    mutable NConcurrency::TReaderWriterSpinLock Lock_;
    NApi::ITransactionPtr Transaction_;
    NLogging::TLogger Logger;
    std::optional<std::pair<TString, TAttributeDictionary>> SelfAttributes_;
    
    void DoEnter(TString name, TAttributeDictionary attributes);
    void DoLeave();
    
    void UpdateList();
};

DEFINE_REFCOUNTED_TYPE(TDiscovery)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
