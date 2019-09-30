#pragma once

#include <yp/server/master/yt_connector.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TTableVersionChecker
{
public:
    TTableVersionChecker(
        NMaster::TYTConnectorPtr connector)
        : YTConnector_(std::move(connector))
    { }

    void ScheduleCheck(
        const TDBTable* table);

    void Check();

private:
    const NMaster::TYTConnectorPtr YTConnector_;
    std::vector<const TDBTable*> TablesToCheck_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
