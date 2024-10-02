#pragma once

#include "public.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct IObjectFilter
{
    virtual bool SkipByType(IObjectTypeHandler* typeHandler, TObjectTypeValue type, int count) = 0;

    virtual ~IObjectFilter() = default;
};

////////////////////////////////////////////////////////////////////////////////

struct ISessionChangeTracker
{
    virtual std::vector<TObject*> GetCreatedObjects(IObjectFilter* filter) const = 0;
    virtual std::vector<TObject*> GetRemovedObjects(IObjectFilter* filter) const = 0;
    virtual std::vector<TObject*> GetUpdatedObjects() const = 0;

    virtual THashMap<TObjectTypeValue, bool> BuildTypeToSkipEventPermissionMap(bool skipDefault) = 0;

    virtual ~ISessionChangeTracker() = default;
};

////////////////////////////////////////////////////////////////////////////////

struct IObjectLogContext
{
    virtual void Preload() = 0;
    virtual void Write() = 0;

    virtual ~IObjectLogContext() = default;
};

using IObjectLogContextPtr = std::unique_ptr<IObjectLogContext>;

////////////////////////////////////////////////////////////////////////////////

IObjectLogContextPtr MakeHistoryWriteContext(
    const TMutatingTransactionOptions& options,
    ISessionChangeTracker* tracker,
    TTransaction* transaction,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

IObjectLogContextPtr MakeWatchLogWriteContext(
    const TMutatingTransactionOptions& options,
    ISessionChangeTracker* tracker,
    TTransaction* transaction,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
