#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

class TCellConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Cell id.
    TCellId CellId;

    //! Peer addresses.
    std::vector<Stroka> Addresses;

    TCellConfig()
    {
        RegisterParameter("cell_id", CellId)
            .Default();
        RegisterParameter("addresses", Addresses)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TCellConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
