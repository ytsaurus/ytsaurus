#include "connection_validators.h"

#include "db_schema.h"
#include "private.h"
#include "type_handler.h"

#include <yt/yt/orm/server/master/yt_connector.h>
#include <yt/yt/orm/server/master/config.h>

#include <yt/yt/client/api/transaction.h>

namespace NYT::NOrm::NServer::NObjects {

using namespace NYT::NConcurrency;
using namespace NYT::NYTree;
using namespace NYT::NYson;
using namespace NYT::NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TTypeHandlerValidator
    : public TConnectionValidatorBase<IObjectTypeHandler*>
{
public:
    void Validate() override
    {
        for (auto* handler : ItemsToValidate_) {
            ValidateTypeHandler(handler);
        }
    }

private:
    static void ValidateTypeHandler(IObjectTypeHandler* handler)
    {
        YT_VERIFY(handler);
        YT_VERIFY(handler->GetTable());

        auto keys = handler->GetTable()->GetKeyFields(/*filterEvaluatedFields*/ true);

        std::vector<const TDBField*> expectedKeys;
        if (handler->HasParent()) {
            const auto& keyFields = handler->GetParentKeyFields();
            expectedKeys.insert(expectedKeys.end(), keyFields.begin(), keyFields.end());
        }
        const auto& keyFields = handler->GetKeyFields();
        expectedKeys.insert(expectedKeys.end(), keyFields.begin(), keyFields.end());

        if (keys.size() != expectedKeys.size()) {
            THROW_ERROR_EXCEPTION("Table %v key columns mismatch: expected %v, found %v",
                handler->GetTable()->GetName(),
                expectedKeys,
                keys);
        }

        for (ui32 keyIndex = 0; keyIndex < expectedKeys.size(); ++keyIndex) {
            auto expectedKey = expectedKeys[keyIndex];
            auto key = keys[keyIndex];

            if (*expectedKey != *key) {
                THROW_ERROR_EXCEPTION("Table %v key column mismatch: expected %v, found %v",
                    handler->GetTable()->GetName(),
                    *expectedKey,
                    *key);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTableValidator
    : public TConnectionValidatorBase<TItemToValidateTable>
{
public:
    TTableValidator(NMaster::TYTConnectorPtr connector, bool enableSchemaValidation, EDBVersionCompatibility dbVersionCompatibility)
        : YTConnector_(std::move(connector))
        , EnableSchemaValidation_(enableSchemaValidation)
        , DBVersionCompatibility_(dbVersionCompatibility)
    { }

    void Validate() override
    {
        auto transaction = YTConnector_->GetInstanceTransaction();
        std::vector<TCallback<TFuture<void>()>> asyncResultCallbacks;
        asyncResultCallbacks.reserve(ItemsToValidate_.size() * (EnableSchemaValidation_ ? 2 : 1));

        for (const auto& itemToValidate : ItemsToValidate_) {
            const auto path = YTConnector_->GetTablePath(itemToValidate.Table);
            const auto expectedDBVersion = YTConnector_->GetExpectedDBVersion();

            if (EnableSchemaValidation_) {
                auto onSchema = BIND([itemToValidate, path] (TErrorOr<TYsonString>&& ysonSchemaOrError) {
                    if (ysonSchemaOrError.IsOK()) {
                        auto schema = ConvertTo<TTableSchema>(ysonSchemaOrError.Value());
                        ValidateSchema(schema, itemToValidate.Table->GetKeyFields(/*filterEvaluatedFields*/ false), path);
                        for (const auto& fields : itemToValidate.CommonLockGroupFieldsList) {
                            ValidateCommonLockGroupFields(schema, fields);
                        }
                    } else {
                        THROW_ERROR_EXCEPTION("Error getting table %v schema",
                            path)
                            << ysonSchemaOrError;
                    }
                });
                asyncResultCallbacks.push_back(BIND([=] () {
                    return transaction->GetNode(path + "/@schema").ApplyUnique(std::move(onSchema));
                }));
            }

            auto onVersion = BIND([
                expectedDBVersion,
                path,
                dbVersionCompatibility = DBVersionCompatibility_
            ] (TErrorOr<TYsonString>&& ysonVersionOrError) {
                if (ysonVersionOrError.IsOK()) {
                    auto version = ConvertTo<int>(ysonVersionOrError.Value());
                    ValidateDBVersionCompatibility(version, expectedDBVersion, path, dbVersionCompatibility);
                } else {
                    THROW_ERROR_EXCEPTION("Error getting version of table %v",
                        path)
                        << ysonVersionOrError;
                }
            });
            asyncResultCallbacks.push_back(BIND([=] () {
                return transaction->GetNode(path + "/@version").ApplyUnique(std::move(onVersion));
            }));
        }

        auto concurrencyLimit = YTConnector_->GetConfig()->ValidationsConcurrencyLimit;
        WaitFor(RunWithAllSucceededBoundedConcurrency(std::move(asyncResultCallbacks), concurrencyLimit))
            .ThrowOnError();

        ItemsToValidate_.clear();
    }

private:
    static void ValidateSchema(
        const TTableSchema& schema,
        const std::vector<const TDBField*>& keys,
        const TYPath& path)
    {
        auto columns = schema.GetKeyColumns();

        if (columns.size() != keys.size()) {
            THROW_ERROR_EXCEPTION("Table %v key columns mismatch: expected %v, found %v",
                path,
                columns,
                keys);
        }

        for (ui32 keyIndex = 0; keyIndex < keys.size(); ++keyIndex) {
            if (keys[keyIndex]->Name != columns[keyIndex]) {
                THROW_ERROR_EXCEPTION(
                    "Table %v key column name mismatch: expected %v, found %v",
                    path,
                    *keys[keyIndex],
                    columns[keyIndex]);
            }

            const auto& column = schema.GetColumn(columns[keyIndex]);
            if (keys[keyIndex]->Type != column.GetWireType()) {
                THROW_ERROR_EXCEPTION(
                    "Table %v key column type mismatch: expected %v, found %v",
                    path,
                    *keys[keyIndex],
                    column);
            }
        }
    }

    static void ValidateCommonLockGroupFields(
        const TTableSchema& schema,
        const TItemToValidateTable::TDBFields& fields)
    {
        if (fields.empty()) {
            return;
        }

        const auto& firstColumnName = (*fields.begin())->Name;
        const auto& lock = schema.GetColumnOrThrow(firstColumnName).Lock();
        for (const auto* field : fields) {
            const auto& column = schema.GetColumnOrThrow(field->Name);
            if (column.Lock() != lock) {
                THROW_ERROR_EXCEPTION(
                    "Field lock group mismatch: field %v, expected %v, found %v",
                    field->Name,
                    lock,
                    column.Lock());
            }
        }
    }

    static void ValidateDBVersionCompatibility(
        int tableVersion,
        int masterVersion,
        const TYPath& path,
        EDBVersionCompatibility dbVersionCompatibility)
    {
        switch (dbVersionCompatibility) {
            case EDBVersionCompatibility::SameAsDBVersion:
                if (tableVersion != masterVersion) {
                    THROW_ERROR_EXCEPTION("Table %v version mismatch: expected %v, found %v",
                        path,
                        masterVersion,
                        tableVersion);
                }
                return;
            case EDBVersionCompatibility::LowerOrEqualThanDBVersion:
                if (tableVersion < masterVersion) {
                    THROW_ERROR_EXCEPTION("Table %v version mismatch: expected master version <= table version, but got %v > %v",
                        path,
                        masterVersion,
                        tableVersion);
                }
                return;
            case EDBVersionCompatibility::DoNotValidate:
                YT_LOG_DEBUG("Skipping table version validation (TableVersion: %v, MasterVersion: %v)",
                    tableVersion,
                    masterVersion);
                return;
            default:
                THROW_ERROR_EXCEPTION("Unknown DB version compatibility mode %v", dbVersionCompatibility);
        }
    }

private:
    const NMaster::TYTConnectorPtr YTConnector_;
    const bool EnableSchemaValidation_;
    const EDBVersionCompatibility DBVersionCompatibility_;
};

////////////////////////////////////////////////////////////////////////////////

ITypeHandlerConnectionValidator CreateTypeHandlerValidator()
{
    return std::make_unique<TTypeHandlerValidator>();
}

ITableConnectionValidator CreateTableValidator(
    NMaster::TYTConnectorPtr connector,
    bool enableSchemaValidation,
    EDBVersionCompatibility dbVersionCompatibility)
{
    return std::make_unique<TTableValidator>(std::move(connector), enableSchemaValidation, dbVersionCompatibility);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
