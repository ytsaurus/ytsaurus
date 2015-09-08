#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

class TCellConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Cell id; an arbitrary random object id of |Cell| type.
    TCellId CellId;

    //! Peer addresses. Some could be |Null| to indicate that the peer is temporarily missing.
    std::vector<TNullable<Stroka>> Addresses;

    TCellConfig();

    void ValidateAllPeersPresent();

};

DEFINE_REFCOUNTED_TYPE(TCellConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
