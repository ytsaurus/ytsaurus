#pragma once

#include "object.h"

#include <yt/core/misc/ref_tracked.h>

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

class TSchema
    : public TObject
    , public NYT::TRefTracked<TSchema>
{
public:
    static constexpr EObjectType Type = EObjectType::Schema;

    TSchema(
        const TObjectId& id,
        IObjectTypeHandler* typeHandler,
        ISession* session);

    virtual EObjectType GetType() const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP
