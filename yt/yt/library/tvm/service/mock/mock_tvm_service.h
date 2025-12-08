#pragma once

#include <yt/yt/library/tvm/service/config.h>
#include <yt/yt/library/tvm/service/tvm_service.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

class TMockTvmService
    : public ITvmService
{
public:
    MOCK_METHOD(const TTvmServiceConfigPtr&, GetConfig, (), (override));
    MOCK_METHOD(TTvmId, GetSelfTvmId, (), (override));
    MOCK_METHOD(std::string, GetServiceTicket, (const std::string&), (override));
    MOCK_METHOD(std::string, GetServiceTicket, (TTvmId), (override));
    MOCK_METHOD(TParsedTicket, ParseUserTicket, (const std::string&), (override));
    MOCK_METHOD(TParsedServiceTicket, ParseServiceTicket, (const std::string&), (override));
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
