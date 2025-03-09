#pragma once

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/kafka/requests.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/queue_client/public.h>

namespace NYT::NKafkaProxy {

////////////////////////////////////////////////////////////////////////////////

std::vector<NKafka::TRecord> ConvertQueueRowsToRecords(
    const NApi::IUnversionedRowsetPtr& rowset);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafkaProxy
