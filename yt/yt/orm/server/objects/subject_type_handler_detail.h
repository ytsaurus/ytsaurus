#pragma once

#include "type_handler_detail.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <class TObjectTypeHandlerBase>
class TSubjectTypeHandlerBase
    : public TObjectTypeHandlerBase
{
public:
    using TObjectTypeHandlerBase::TObjectTypeHandlerBase;

protected:
    virtual void InitializeCreatedObject(
        TTransaction* transaction,
        TObject* object) override;
    virtual void FinishObjectRemoval(
        TTransaction* transaction,
        TObject* object) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

#define SUBJECT_TYPE_HANDLER_DETAIL_INL_H_
#include "subject_type_handler_detail-inl.h"
#undef SUBJECT_TYPE_HANDLER_DETAIL_INL_H_
