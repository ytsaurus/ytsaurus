#pragma once

#include "common.h"

#include <core/misc/public.h>

#include <core/ytree/public.h>

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

class TLogManager
{
public:
    static TLogManager* Get();

    static void Shutdown();

    void Configure(NYTree::INodePtr node);
    void Configure(const Stroka& fileName, const NYPath::TYPath& path);

    int GetVersion() const;
    ELogLevel GetMinLevel(const Stroka& category) const;

    void Enqueue(TLogEvent&& event);

    void Reopen();

    DECLARE_SINGLETON_MIXIN(TLogManager, TStaticInstanceMixin);
    DECLARE_SINGLETON_PRIORITY(TLogManager, 20);

private:
    TLogManager();
    ~TLogManager();

    class TImpl;
    TIntrusivePtr<TImpl> Impl;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT

