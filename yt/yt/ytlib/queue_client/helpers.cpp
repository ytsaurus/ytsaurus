#include "helpers.h"

#include <yt/yt/ytlib/orchid/orchid_ypath_service.h>

#include <yt/yt/client/queue_client/public.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/core/ypath/token.h>

namespace NYT::NQueueClient {

using namespace NOrchid;
using namespace NYPath;
using namespace NYTree;
using namespace NRpc;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TYPath GetQueueAgentObjectRemotePath(const TString& cluster, const TString& objectKind, const TYPath& objectPath)
{
    auto objectRef = Format("%v:%v", cluster, objectPath);
    // NB: Mind the plural!
    return Format("//queue_agent/%vs/%v", objectKind, ToYPathLiteral(objectRef));
}

IYPathServicePtr CreateQueueAgentYPathService(
    IChannelPtr queueAgentChannel,
    const TString& cluster,
    const TString& objectKind,
    const TYPath& objectPath)
{
    return CreateOrchidYPathService(TOrchidOptions{
        .Channel = std::move(queueAgentChannel),
        .RemoteRoot = GetQueueAgentObjectRemotePath(cluster, objectKind, objectPath),
    });
}

////////////////////////////////////////////////////////////////////////////////

std::optional<i64> GetSequenceNumberFromRow(
    TUnversionedRow row,
    int sequenceNumberColumnId)
{
    i64 sequenceNumber = -1;

    for (const auto& value : row) {
        if (value.Id == sequenceNumberColumnId && value.Type != EValueType::Null) {
            try {
                FromUnversionedValue(&sequenceNumber, value);
                return sequenceNumber;
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error parsing sequence number from row, Int64 was expected")
                    << ex;
            }
        }
    }

    return {};
}

TValidatePushQueueProducerRowsResult ValidatePushQueueProducerRows(
    const NTableClient::TNameTablePtr& nameTable,
    const TSharedRange<NTableClient::TUnversionedRow>& rows,
    i64 lastProducerSequenceNumber,
    std::optional<i64> initialSequenceNumber)
{
    auto sequenceNumberColumnId = nameTable->GetIdOrRegisterName(SequenceNumberColumnName);

    std::optional<i64> nextSequenceNumber = initialSequenceNumber;

    i64 lastSequenceNumber = -1;
    i64 skipRowCount = 0;

    auto updateLastSequenceNumber = [&lastSequenceNumber](i64 sequenceNumber) {
        if (sequenceNumber <= lastSequenceNumber) {
            THROW_ERROR_EXCEPTION(
                NQueueClient::EErrorCode::InvalidRowSequenceNumbers,
                "Sequence numbers are not strongly monotonic");
        }
        lastSequenceNumber = sequenceNumber;
    };

    for (const auto& row : rows) {
        auto rowSequenceNumber = GetSequenceNumberFromRow(row, sequenceNumberColumnId);

        THROW_ERROR_EXCEPTION_IF(rowSequenceNumber && initialSequenceNumber,
            NQueueClient::EErrorCode::InvalidRowSequenceNumbers,
            "Only one of explicit $sequence_number or initial sequence number can be set");

        if (rowSequenceNumber) {
            THROW_ERROR_EXCEPTION_IF(
                *rowSequenceNumber < 0,
                NQueueClient::EErrorCode::InvalidRowSequenceNumbers,
                "Sequence number %v cannot be negative",
                *rowSequenceNumber);

            updateLastSequenceNumber(*rowSequenceNumber);
        } else {
            THROW_ERROR_EXCEPTION_IF(
                !nextSequenceNumber,
                NQueueClient::EErrorCode::InvalidRowSequenceNumbers,
                "There is no $sequence_number in the row and initial sequence number was not received");

            updateLastSequenceNumber(*nextSequenceNumber);
            ++(*nextSequenceNumber);
        }

        // Such rows should be ignored, they were written before.
        if (lastSequenceNumber <= lastProducerSequenceNumber) {
            ++skipRowCount;
        }
    }

    return TValidatePushQueueProducerRowsResult{
        .LastSequenceNumber = lastSequenceNumber,
        .SkipRowCount = skipRowCount,
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
