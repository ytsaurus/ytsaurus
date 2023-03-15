#pragma once

#include <yt/cpp/roren/library/kv_state_manager/proto/kv_state_manager_config.pb.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <bigrt/lib/processing/state_manager/generic/manager.h>
#include <bigrt/lib/processing/state_manager/generic/factory.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
concept IsPrimitiveYtValue =
    std::is_same_v<T, bool> ||
    std::is_same_v<T, i64> ||
    std::is_same_v<T, ui64> ||
    std::is_same_v<T, double> ||
    std::is_same_v<T, TString>;


template <typename TStateId_, typename TState_>
class TKvStateManager
    : public NBigRT::TGenericStateManager<TStateId_, TState_>
{
    public:
        using TStateId = TStateId_;
        using TState = TState_;

        static_assert(IsPrimitiveYtValue<TStateId>);
        static_assert(IsPrimitiveYtValue<TState>);

    private:
        using TBase = NBigRT::TGenericStateManager<TStateId, TState>;
        using TBaseRequestPtr = NBigRT::TGenericStateRequestPtr<TStateId, TState>;

    public:
        TKvStateManager(const TKvStateManagerConfig& config, NSFStats::TSolomonContext sensorsContext)
            : TBase(config.GetStateManagerConfig(), sensorsContext)
            , Config_(config)
        {
            YT_VERIFY(!Config_.GetKeyColumn().empty());
            YT_VERIFY(!Config_.GetValueColumn().empty());
        }

    private:
        NYT::TFuture<TVector<NBigRT::TBaseStateRequestPtr>>
        LoadStates(
            TVector<TBaseRequestPtr> requestList,
            NYT::NApi::IClientPtr client,
            NYT::NTransactionClient::TTimestamp timestamp
        ) override {
            auto rowBuffer = NYT::New<NYT::NTableClient::TRowBuffer>();

            const auto keyId = NameTable_->GetIdOrRegisterName(Config_.GetKeyColumn());

            auto rowRange = AllocateRowRange(rowBuffer, requestList.size());
            for (ssize_t i = 0; i < std::ssize(requestList); ++i) {
                const auto& key = requestList[i]->GetStateId();

                auto row = rowBuffer->AllocateUnversioned(1);
                rowRange[i] = row;
                row[0] = rowBuffer->CaptureValue(MakeValue(keyId, key));
            }

            NYT::NApi::TLookupRowsOptions lookupRowsOptions;
            lookupRowsOptions.KeepMissingRows = true;
            lookupRowsOptions.Timestamp = timestamp;

            return client->LookupRows(
                Config_.GetStateTable(),
                NameTable_,
                Freeze(std::move(rowRange)),
                lookupRowsOptions
            ).Apply(BIND([
                requestList = std::move(requestList),
                valueColumn = Config_.GetValueColumn()
            ] (const NYT::NApi::IUnversionedRowsetPtr& rowset) {
                const auto valueId = rowset->GetNameTable()->GetIdOrThrow(valueColumn);

                const auto& rowList = rowset->GetRows();
                Y_VERIFY(std::ssize(requestList) == std::ssize(rowList));

                TVector<NBigRT::TBaseStateRequestPtr> baseRequestList;
                baseRequestList.reserve(requestList.size());

                for (ssize_t i = 0; i < std::ssize(requestList); ++i) {
                    const auto& request = requestList[i];
                    const auto row = rowList[i];
                    if (row) {
                        YT_VERIFY(valueId < static_cast<int>(row.GetCount()));
                        const auto value = row[valueId];

                        request->GetState() = UnpackValue<TState>(value);
                    } else {
                        request->GetState() = TState{};
                    }

                    baseRequestList.push_back(request);
                }

                return baseRequestList;
            }));
        }

        NBigRT::TBaseStateManager::TStateWriter WriteStates(TVector<TBaseRequestPtr> requestList) override
        {
            auto rowBuffer = NYT::New<NYT::NTableClient::TRowBuffer>();
            auto mutableWriteRowRange = AllocateRowRange(rowBuffer, requestList.size());
            auto mutableDeleteRowRange = AllocateRowRange(rowBuffer, requestList.size());
            int writeIndex = 0;
            int deleteIndex = 0;

            const auto keyId = NameTable_->GetIdOrRegisterName(Config_.GetKeyColumn());
            const auto valueId = NameTable_->GetIdOrRegisterName(Config_.GetValueColumn());

            for (const auto& request : requestList) {
                const auto& state = request->GetState();
                const auto& keyValue = request->GetStateId();

                if (state == TState{}) {
                    auto row = rowBuffer->AllocateUnversioned(1);
                    row[0] = rowBuffer->CaptureValue(MakeValue(keyId, keyValue));
                    mutableDeleteRowRange[deleteIndex++] = row;
                } else {
                    auto row = rowBuffer->AllocateUnversioned(2);
                    row[0] = rowBuffer->CaptureValue(MakeValue(keyId, keyValue));
                    row[1] = rowBuffer->CaptureValue(MakeValue(valueId, state));
                    mutableWriteRowRange[writeIndex++] = row;
                }
            }

            auto writeRowRange = Freeze(mutableWriteRowRange.Slice(0, writeIndex));
            auto deleteRowRange = Freeze(mutableDeleteRowRange.Slice(0, deleteIndex));

            return [
                table = Config_.GetStateTable(),
                nameTable = NameTable_,
                writeRowRange = writeRowRange,
                deleteRowRange = deleteRowRange
            ] (NYT::NApi::ITransactionPtr tx) {
                if (!writeRowRange.Empty()) {
                    tx->WriteRows(table, nameTable, writeRowRange);
                }

                if (!deleteRowRange.Empty()) {
                    tx->DeleteRows(table, nameTable, deleteRowRange);
                }
            };
        }

        static NYT::TSharedMutableRange<NYT::NTableClient::TUnversionedRow> AllocateRowRange(NYT::NTableClient::TRowBufferPtr rowBuffer, int rowCount)
        {
            std::vector<NYT::NTableClient::TUnversionedRow> range;
            range.resize(rowCount);
            return NYT::MakeSharedMutableRange(std::move(range), std::move(rowBuffer));
        }

        template <typename T>
        static NYT::TSharedRange<T> Freeze(NYT::TSharedMutableRange<T>&& range)
        {
            return {
                range.Begin(),
                range.End(),
                range.ReleaseHolder()
            };
        }

        template <typename T>
        static NYT::NTableClient::TUnversionedValue MakeValue(int id, const T& value)
        {
            if constexpr (std::is_same_v<T, bool>) {
                return NYT::NTableClient::MakeUnversionedBooleanValue(value, id);
            } else if constexpr (std::is_same_v<T, i64>) {
                return NYT::NTableClient::MakeUnversionedInt64Value(value, id);
            } else if constexpr (std::is_same_v<T, ui64>) {
                return NYT::NTableClient::MakeUnversionedUint64Value(value, id);
            } else if constexpr (std::is_same_v<T, double>) {
                return NYT::NTableClient::MakeUnversionedUint64Value(value, id);
            } else if constexpr (std::is_same_v<T, TString>) {
                return NYT::NTableClient::MakeUnversionedStringValue(value, id);
            } else {
                static_assert(TDependentFalse<T>);
            }
        }

        template <typename T>
        static T UnpackValue(NYT::NTableClient::TUnversionedValue value)
        {
            if constexpr (std::is_same_v<T, bool>) {
                return value.Data.Boolean;
            } else if constexpr (std::is_same_v<T, i64>) {
                return value.Data.Int64;
            } else if constexpr (std::is_same_v<T, ui64>) {
                return value.Data.Uint64;
            } else if constexpr (std::is_same_v<T, double>) {
                return value.Data.Double;
            } else if constexpr (std::is_same_v<T, TString>) {
                return value.AsString();
            } else {
                static_assert(TDependentFalse<T>);
            }
        }

    private:
        const TKvStateManagerConfig Config_;
        NYT::NTableClient::TNameTablePtr NameTable_ = NYT::New<NYT::NTableClient::TNameTable>();
};

////////////////////////////////////////////////////////////////////////////////

template <typename TStateId_, typename TState_>
class TKvStateManagerFactory
    : public NBigRT::TGenericStateManagerFactory<TStateId_, TState_>
{
public:
    using TStateId = TStateId_;
    using TState = TState_;

    using TManager = TKvStateManager<TStateId, TState>;

    using TBase = NBigRT::TGenericStateManagerFactory<TStateId, TState>;

public:
    explicit TKvStateManagerFactory(const TKvStateManagerConfig& config, NSFStats::TSolomonContext sensorsContext)
        : TBase(Config_.GetStateManagerConfig(), std::move(sensorsContext))
        , Config_(config)
    { }

public:
    NBigRT::TBaseStateManagerPtr Make(ui64 /*shard*/) override
    {
        return NYT::New<TManager>(Config_, this->SensorsContext);
    }

private:
    TKvStateManagerConfig Config_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
