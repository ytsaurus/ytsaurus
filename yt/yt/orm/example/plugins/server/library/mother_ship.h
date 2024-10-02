#pragma once

#include <yt/yt/orm/example/server/library/autogen/objects.h>

namespace NYT::NOrm::NExample::NServer::NPlugins {

////////////////////////////////////////////////////////////////////////////////

class TMotherShip
    : public NLibrary::TMotherShip
{
public:
    using NLibrary::TMotherShip::TMotherShip;

    void PreloadCheck() const;
    void CheckEnoughInterceptors() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NPlugins
