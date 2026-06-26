#pragma once

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/logging/log.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

class TStructuredCategoryRegistry
{
public:
    static TStructuredCategoryRegistry* Get();

    void RegisterStructuredCategory(std::string category, NTableClient::TTableSchemaPtr schema);

    void DumpCategories(NYson::IYsonConsumer* consumer);

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    THashMap<std::string, NTableClient::TTableSchemaPtr> Categories_;
};

////////////////////////////////////////////////////////////////////////////////

TLogger CreateSchemafulLogger(std::string category, NTableClient::TTableSchemaPtr schema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
