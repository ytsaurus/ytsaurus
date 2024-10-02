#pragma once

#include "public.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <class TItem>
struct IConnectionValidator
{
    virtual ~IConnectionValidator() = default;

    virtual void Schedule(TItem handler) = 0;

    virtual void Validate() = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <class TItem>
class TConnectionValidatorBase
    : public IConnectionValidator<TItem>
{
public:
    void Schedule(TItem handler) override
    {
        ItemsToValidate_.push_back(std::move(handler));
    }

protected:
    std::vector<TItem> ItemsToValidate_;
};

struct TItemToValidateTable
{
    using TDBFields = std::vector<const TDBField*>;

    const TDBTable* Table;
    std::vector<TDBFields> CommonLockGroupFieldsList;
};

////////////////////////////////////////////////////////////////////////////////

using ITypeHandlerConnectionValidator = std::unique_ptr<IConnectionValidator<IObjectTypeHandler*>>;
using ITableConnectionValidator = std::unique_ptr<IConnectionValidator<TItemToValidateTable>>;

ITypeHandlerConnectionValidator CreateTypeHandlerValidator();
ITableConnectionValidator CreateTableValidator(
    NMaster::TYTConnectorPtr connector,
    bool enableSchemaValidation = true,
    EDBVersionCompatibility dbVersionCompatibility = EDBVersionCompatibility::SameAsDBVersion);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
