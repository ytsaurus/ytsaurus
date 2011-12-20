#pragma once

#include "common.h"

#include "../misc/config.h"
#include "../misc/error.h"
#include "../election/leader_lookup.h"
#include "../ytree/ytree.h"
#include "../ytree/yson_events.h"
#include "../ytree/yson_writer.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct IDriverStreamProvider
{
    virtual ~IDriverStreamProvider()
    { }

    virtual TAutoPtr<TInputStream>  CreateInputStream(const Stroka& spec) = 0;
    virtual TAutoPtr<TOutputStream> CreateOutputStream(const Stroka& spec) = 0;
    virtual TAutoPtr<TOutputStream> CreateErrorStream() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TDriver
    : private TNonCopyable
{
public:
    struct TConfig
        : TConfigBase
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        NElection::TLeaderLookup::TConfig::TPtr Masters;
        NYTree::TYsonWriter::EFormat Format;

        TConfig()
        {
            Register("masters", Masters);
            Register("format", Format).Default(NYTree::TYsonWriter::EFormat::Text);
        }
    };

    TDriver(
        TConfig* config,
        IDriverStreamProvider* streamProvider);
    ~TDriver();

    TError Execute(const NYTree::TYson& request);

private:
    class TImpl;

    THolder<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

