#pragma once

#include "type_handler_detail.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TSubjectTypeHandlerBase
    : public TObjectTypeHandlerBase
{
public:
    using TObjectTypeHandlerBase::TObjectTypeHandlerBase;

protected:
    virtual void BeforeObjectCreated(
        TTransaction* transaction,
        TObject* object) override;
    virtual void AfterObjectRemoved(
        TTransaction* transaction,
        TObject* object) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
