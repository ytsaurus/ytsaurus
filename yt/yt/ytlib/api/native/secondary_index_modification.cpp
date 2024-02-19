#include "secondary_index_modification.h"

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/yson/pull_parser.h>

namespace NYT::NApi::NNative {

using namespace NTableClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

struct TSecondaryIndexModificationsBufferTag { };

THashMap<ui16, int> BuildRowMapping(TUnversionedRow row)
{
    THashMap<ui16, int> idToPosition;
    idToPosition.reserve(row.GetCount());
    for (int position = 0; position < static_cast<int>(row.GetCount()); ++position) {
        idToPosition[row[position].Id] = position;
    }

    return idToPosition;
};

TSharedRange<TRowModification> GetFullSyncIndexModifications(
    const TSharedRange<TRowModification>& modifications,
    const TSharedRange<TUnversionedRow>& lookedUpRows,
    const TNameTableToSchemaIdMapping& writeIdMapping,
    const TNameTableToSchemaIdMapping& deleteIdMapping,
    const TTableSchema& indexSchema,
    const std::optional<TUnversionedValue>& empty)
{
    std::vector<TRowModification> secondaryModifications;

    auto rowBuffer = New<TRowBuffer>(TSecondaryIndexModificationsBufferTag());

    auto writeRow = [&] (TUnversionedRow row) {
        auto rowToWrite = rowBuffer->CaptureAndPermuteRow(
            row,
            indexSchema,
            indexSchema.GetKeyColumnCount(),
            writeIdMapping,
            /*columnPresenceBuffer*/ nullptr,
            empty);
        secondaryModifications.push_back(TRowModification{
            ERowModificationType::Write,
            rowToWrite.ToTypeErasedRow(),
            TLockMask(),
        });
    };
    auto deleteRow = [&] (TUnversionedRow row) {
        auto rowToDelete = rowBuffer->CaptureAndPermuteRow(
            row,
            indexSchema,
            indexSchema.GetKeyColumnCount(),
            deleteIdMapping,
            /*columnPresenceBuffer*/ nullptr);
        secondaryModifications.push_back(TRowModification{
            ERowModificationType::Delete,
            rowToDelete.ToTypeErasedRow(),
            TLockMask(),
        });
    };

    for (int modificationIndex = 0; modificationIndex < std::ssize(modifications); ++modificationIndex) {
        const auto& modification = modifications[modificationIndex];
        const auto& modificationRow = TUnversionedRow(modification.Row);

        auto oldRow = rowBuffer->CaptureRow(lookedUpRows[modificationIndex]);

        switch (modification.Type) {
            case ERowModificationType::Write: {
                if (!oldRow) {
                    writeRow(modificationRow);
                } else {
                    deleteRow(oldRow);

                    auto oldRowMapping = BuildRowMapping(oldRow);
                    for (const auto& value : modificationRow) {
                        if (oldRowMapping.contains(value.Id)) {
                            oldRow[oldRowMapping[value.Id]] = value;
                        }
                    }
                    writeRow(oldRow);
                }
                break;
            }
            case ERowModificationType::Delete: {
                if (oldRow) {
                    deleteRow(oldRow);
                }
                break;
            }
            default:
                YT_ABORT();
        }
    }

    return MakeSharedRange(std::move(secondaryModifications), std::move(rowBuffer));
}

TSharedRange<TRowModification> GetUnfoldedIndexModifications(
    const TSharedRange<TRowModification>& modifications,
    const TSharedRange<TUnversionedRow>& lookedUpRows,
    const TNameTableToSchemaIdMapping& idMapping,
    const TNameTableToSchemaIdMapping& keyIdMapping,
    const TTableSchema& indexSchema,
    const std::optional<TUnversionedValue>& empty,
    int unfoldedKeyPosition)
{
    std::vector<TRowModification> secondaryModifications;

    auto rowBuffer = New<TRowBuffer>(TSecondaryIndexModificationsBufferTag());


    auto unfoldValue = [&] (TUnversionedRow row, std::function<void(TUnversionedRow)> consumeRow) {
        if (row[unfoldedKeyPosition].Type == EValueType::Null) {
            return;
        }
        TMemoryInput memoryInput(FromUnversionedValue<NYson::TYsonStringBuf>(row[unfoldedKeyPosition]).AsStringBuf());

        TYsonPullParser parser(&memoryInput, EYsonType::Node);
        TYsonPullParserCursor cursor(&parser);

        cursor.ParseList([&] (TYsonPullParserCursor* cursor) {
            auto producedRow = rowBuffer->CaptureRow(row);
            switch (cursor->GetCurrent().GetType()) {
                case EYsonItemType::EntityValue: {
                    producedRow[unfoldedKeyPosition].Type = EValueType::Null;
                    break;
                }
                case EYsonItemType::Int64Value: {
                    auto value = cursor->GetCurrent().UncheckedAsInt64();
                    producedRow[unfoldedKeyPosition].Type = EValueType::Int64;
                    producedRow[unfoldedKeyPosition].Data.Int64 = value;
                    break;
                }
                case EYsonItemType::Uint64Value: {
                    auto value = cursor->GetCurrent().UncheckedAsUint64();
                    producedRow[unfoldedKeyPosition].Type = EValueType::Uint64;
                    producedRow[unfoldedKeyPosition].Data.Uint64 = value;
                    break;
                }
                case EYsonItemType::DoubleValue: {
                    auto value = cursor->GetCurrent().UncheckedAsDouble();
                    producedRow[unfoldedKeyPosition].Type = EValueType::Double;
                    producedRow[unfoldedKeyPosition].Data.Double = value;
                    break;
                }
                case EYsonItemType::StringValue: {
                    auto value = cursor->GetCurrent().UncheckedAsString();
                    producedRow[unfoldedKeyPosition].Type = EValueType::String;
                    producedRow[unfoldedKeyPosition].Data.String = value.data();
                    producedRow[unfoldedKeyPosition].Length = value.size();
                    break;
                }
                default:
                    YT_ABORT();
            }

            consumeRow(producedRow);
            cursor->Next();
        });

    };

    auto writeRow = [&] (TUnversionedRow row) {
        auto permuttedRow = rowBuffer->CaptureAndPermuteRow(
            row,
            indexSchema,
            indexSchema.GetKeyColumnCount(),
            idMapping,
            /*columnPresenceBuffer*/ nullptr,
            empty);

        unfoldValue(permuttedRow, [&] (TUnversionedRow rowToWrite) {
            secondaryModifications.push_back(TRowModification{
                ERowModificationType::Write,
                rowToWrite.ToTypeErasedRow(),
                TLockMask(),
            });
        });
    };
    auto deleteRow = [&] (TUnversionedRow row) {
        auto permuttedRow = rowBuffer->CaptureAndPermuteRow(
            row,
            indexSchema,
            indexSchema.GetKeyColumnCount(),
            keyIdMapping,
            /*columnPresenceBuffer*/ nullptr);

        unfoldValue(permuttedRow, [&] (TUnversionedRow rowToDelete) {
            secondaryModifications.push_back(TRowModification{
                ERowModificationType::Delete,
                rowToDelete.ToTypeErasedRow(),
                TLockMask(),
            });
        });
    };

    for (int modificationIndex = 0; modificationIndex < std::ssize(modifications); ++modificationIndex) {
        const auto& modification = modifications[modificationIndex];
        const auto& modificationRow = TUnversionedRow(modification.Row);

        auto oldRow = rowBuffer->CaptureRow(lookedUpRows[modificationIndex]);

        switch (modification.Type) {
            case ERowModificationType::Write: {
                if (!oldRow) {
                    writeRow(modificationRow);
                } else {
                    deleteRow(oldRow);

                    auto oldRowMapping = BuildRowMapping(oldRow);
                    for (const auto& value : modificationRow) {
                        if (oldRowMapping.contains(value.Id)) {
                            oldRow[oldRowMapping[value.Id]] = value;
                        }
                    }
                    writeRow(oldRow);
                }
                break;
            }
            case ERowModificationType::Delete: {
                if (oldRow) {
                    deleteRow(oldRow);
                }
                break;
            }
            default:
                YT_ABORT();
        }
    }

    return MakeSharedRange(std::move(secondaryModifications), std::move(rowBuffer));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
