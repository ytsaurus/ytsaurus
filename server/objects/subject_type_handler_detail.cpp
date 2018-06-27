#include "subject_type_handler_detail.h"
#include "db_schema.h"
#include "transaction.h"

#include <yp/server/access_control/public.h>

namespace NYP {
namespace NServer {
namespace NObjects {

using namespace NTableClient;
using namespace NAccessControl;

////////////////////////////////////////////////////////////////////////////////

void TSubjectTypeHandlerBase::BeforeObjectCreated(
    const TTransactionPtr& transaction,
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
                ToDBValues(
                    context->GetRowBuffer(),
                    id),
                MakeArray(&SubjectToTypeTable.Fields.Type),
                [=] (const TNullable<TRange<TVersionedValue>>& maybeValues) {
                    if (maybeValues) {
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
                ToDBValues(
                    context->GetRowBuffer(),
                    id),
                MakeArray(
                    &SubjectToTypeTable.Fields.Type),
                ToDBValues(
                    context->GetRowBuffer(),
                    object->GetType()));
        });
}

void TSubjectTypeHandlerBase::AfterObjectRemoved(
    const TTransactionPtr& transaction,
    TObject* object)
{
    TObjectTypeHandlerBase::AfterObjectRemoved(transaction, object);

    auto* session = object->GetSession();
    session->ScheduleStore(
        [=] (IStoreContext* context) {
            context->DeleteRow(
                &SubjectToTypeTable,
                ToDBValues(
                    context->GetRowBuffer(),
                    object->GetId()));
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP

