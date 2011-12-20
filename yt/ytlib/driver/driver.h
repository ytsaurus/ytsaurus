#pragma once

#include "common.h"

#include "../misc/configurable.h"
#include "../misc/error.h"
#include "../election/leader_lookup.h"
#include "../ytree/ytree.h"
#include "../ytree/yson_events.h"
#include "../ytree/yson_writer.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TDriver
    : private TNonCopyable
{
public:
    struct TConfig
        : TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        NElection::TLeaderLookup::TConfig::TPtr Masters;
        bool BoxSuccess;

        TConfig()
        {
            Register("masters", Masters);
            Register("box_success", BoxSuccess).Default(false);
        }
    };

    TDriver(TConfig* config);
    ~TDriver();

    TError Execute(
        const NYTree::TYson& request,
        NYTree::IYsonConsumer* responseConsumer);

private:
    class TImpl;

    THolder<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

