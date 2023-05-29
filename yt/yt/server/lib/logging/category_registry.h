#pragma once

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/logging/log.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NLogging {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TStructuredCategoryRegistry
{
public:
    static TStructuredCategoryRegistry* Get();

    void RegisterStructuredCategory(TString category, TTableSchemaPtr schema);

    void DumpCategories(NYson::IYsonConsumer* consumer);

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    THashMap<TString, TTableSchemaPtr> Categories_;
};

////////////////////////////////////////////////////////////////////////////////

TLogger CreateSchemafulLogger(TString category, TTableSchemaPtr schema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
