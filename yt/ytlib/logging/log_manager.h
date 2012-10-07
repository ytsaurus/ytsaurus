#pragma once

#include "common.h"

#include <ytlib/ytree/public.h>

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

class TLogManager
{
public:
    TLogManager();
    
    static TLogManager* Get();

    void Configure(NYTree::INodePtr node);
    void Configure(const Stroka& fileName, const NYPath::TYPath& path);

    void Shutdown();

    int GetConfigVersion();
    int GetConfigRevision();

    void GetLoggerConfig(
        const Stroka& category,
        ELogLevel* minLevel,
        int* configVersion);

    void Enqueue(const TLogEvent& event);

    void Reopen();

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT

template <>
struct TSingletonTraits<NYT::NLog::TLogManager>
{
    enum
    {
        Priority = 2048
    };
};
