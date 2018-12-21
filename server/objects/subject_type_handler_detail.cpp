#include "subject_type_handler_detail.h"
#include "db_schema.h"
#include "transaction.h"

#include <yp/server/access_control/public.h>

namespace NYP::NServer::NObjects {

using namespace NTableClient;
using namespace NAccessControl;

////////////////////////////////////////////////////////////////////////////////

void TSubjectTypeHandlerBase::BeforeObjectCreated(
    TTransaction* transaction,
    TObject* object)
{
    const auto& id = object->GetId();
    if (id == EveryoneSubjectId) {
        THROW_ERROR_EXCEPTION(
            NClient::NApi::EErrorCode::InvalidObjectId,
            "Subject %Qv cannot be created",
            id);
    }

    TObjectTypeHandlerBase::BeforeObjectCreated(transaction, object);

    auto* session = object->GetSession();
    session->ScheduleLoad(
        [&] (ILoadContext* context) {
            context->ScheduleLookup(
                &SubjectToTypeTable,
                ToUnversionedValues(
                    context->GetRowBuffer(),
                    id),
                MakeArray(&SubjectToTypeTable.Fields.Type),
                [=] (const std::optional<TRange<TVersionedValue>>& optionalValues) {
                    if (optionalValues) {
                        THROW_ERROR_EXCEPTION(
                            NClient::NApi::EErrorCode::DuplicateObjectId,
                            "Subject %Qv already exists",
                            id);
                    }
                });
        });
    session->FlushLoads();

    session->ScheduleStore(
        [=] (IStoreContext* context) {
            context->WriteRow(
                &SubjectToTypeTable,
                ToUnversionedValues(
                    context->GetRowBuffer(),
                    id),
                MakeArray(
                    &SubjectToTypeTable.Fields.Type),
                ToUnversionedValues(
                    context->GetRowBuffer(),
                    object->GetType()));
        });
}

void TSubjectTypeHandlerBase::AfterObjectRemoved(
    TTransaction* transaction,
    TObject* object)
{
    TObjectTypeHandlerBase::AfterObjectRemoved(transaction, object);

    auto* session = object->GetSession();
    session->ScheduleStore(
        [=] (IStoreContext* context) {
            context->DeleteRow(
                &SubjectToTypeTable,
                ToUnversionedValues(
                    context->GetRowBuffer(),
                    object->GetId()));
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

