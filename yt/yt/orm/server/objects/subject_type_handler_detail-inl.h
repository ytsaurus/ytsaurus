#ifndef SUBJECT_TYPE_HANDLER_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include subject_type_handler_detail.h"
// For the sake of sane code completion.
#include "subject_type_handler_detail.h"
#endif

#include "db_schema.h"
#include "transaction.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <class TObjectTypeHandlerBase>
void TSubjectTypeHandlerBase<TObjectTypeHandlerBase>::InitializeCreatedObject(
    TTransaction* transaction,
    TObject* object)
{
    const auto key = object->GetKey();
    if (key == EveryoneSubjectKey) {
        THROW_ERROR_EXCEPTION(
            NYT::NOrm::NClient::EErrorCode::InvalidObjectId,
            "Subject %v cannot be created",
            key);
    }

    TObjectTypeHandlerBase::InitializeCreatedObject(transaction, object);

    auto* session = object->GetSession();

    session->ScheduleLoad(
        [key] (ILoadContext* context) {
            auto handler = [key] (const auto& optionalValues) {
                if (optionalValues) {
                    THROW_ERROR_EXCEPTION(
                        NYT::NOrm::NClient::EErrorCode::DuplicateObjectId,
                        "Subject %v already exists",
                        key);
                }
            };
            context->ScheduleLookup(
                &SubjectToTypeTable,
                key,
                std::array{&SubjectToTypeTable.Fields.Type},
                std::move(handler));
        });

    session->ScheduleStore(
        [key, object] (IStoreContext* context) {
            context->WriteRow(
                &SubjectToTypeTable,
                key,
                std::array{&SubjectToTypeTable.Fields.Type},
                ToUnversionedValues(
                    context->GetRowBuffer(),
                    object->GetType()));
        });
}

template <class TObjectTypeHandlerBase>
void TSubjectTypeHandlerBase<TObjectTypeHandlerBase>::FinishObjectRemoval(
    TTransaction* transaction,
    TObject* object)
{
    TObjectTypeHandlerBase::FinishObjectRemoval(transaction, object);

    auto* session = object->GetSession();
    session->ScheduleStore(
        [object] (IStoreContext* context) {
            context->DeleteRow(
                &SubjectToTypeTable,
                object->GetKey());
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
