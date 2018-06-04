#pragma once

#include "type_handler_detail.h"

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

class TSubjectTypeHandlerBase
    : public TObjectTypeHandlerBase
{
public:
    using TObjectTypeHandlerBase::TObjectTypeHandlerBase;

protected:
    virtual void BeforeObjectCreated(
        const TTransactionPtr& transaction,
        TObject* object) override;
    virtual void AfterObjectRemoved(
        const TTransactionPtr& transaction,
        TObject* object) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP
