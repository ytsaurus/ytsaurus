#pragma once

#include "command.h"

#include <yt/yt/client/ypath/rich.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

class TRegisterQueueConsumerCommand
    : public TTypedCommand<NApi::TRegisterQueueConsumerOptions>
{
public:
    TRegisterQueueConsumerCommand();

private:
    NYPath::TRichYPath QueuePath;
    NYPath::TRichYPath ConsumerPath;
    bool Vital;
    std::optional<std::vector<int>> Partitions;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TUnregisterQueueConsumerCommand
    : public TTypedCommand<NApi::TUnregisterQueueConsumerOptions>
{
public:
    TUnregisterQueueConsumerCommand();

private:
    NYPath::TRichYPath QueuePath;
    NYPath::TRichYPath ConsumerPath;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TListQueueConsumerRegistrationsCommand
    : public TTypedCommand<NApi::TListQueueConsumerRegistrationsOptions>
{
public:
    TListQueueConsumerRegistrationsCommand();

private:
    std::optional<NYPath::TRichYPath> QueuePath;
    std::optional<NYPath::TRichYPath> ConsumerPath;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
