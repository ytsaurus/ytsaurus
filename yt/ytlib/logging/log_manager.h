#pragma once

#include "common.h"
#include "writer.h"
#include "log.h"

#include "../actions/action_queue.h"
#include "../actions/action_util.h"
#include "../actions/future.h"
#include "../misc/fs.h"
#include "../ytree/ytree.h"

#include <dict/json/json.h>

#include <util/generic/pair.h>
#include <util/datetime/base.h>
#include <util/system/thread.h>

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

class TLogManager
{
public:
    TLogManager();

    static TLogManager* Get();

    void Configure(NYTree::INode::TPtr node);
    void Configure(TJsonObject* root);
    void Configure(const Stroka& fileName, const Stroka& rootPath);

    void Flush();
    void Shutdown();

    int GetConfigVersion();
    void GetLoggerConfig(
        Stroka category,
        ELogLevel* minLevel,
        int* configVersion);

    void Write(const TLogEvent& event);

private:
    typedef yvector<ILogWriter::TPtr> TLogWriters;
    typedef ymap<TPair<Stroka, ELogLevel>, TLogWriters> TCachedWriters;

    TLogWriters GetWriters(const TLogEvent& event);
    TLogWriters GetConfiguredWriters(const TLogEvent& event);

    ELogLevel GetMinLevel(Stroka category);

    void DoWrite(const TLogEvent& event);
    TVoid DoFlush();

    // Configuration.
    TSpinLock SpinLock;
    TAtomic ConfigVersion;

    class TConfig;
    TIntrusivePtr<TConfig> Configuration;

    TActionQueue::TPtr Queue;

    TLogWriters SystemWriters;
    TCachedWriters CachedWriters;

    void ConfigureDefault();
    void ConfigureSystem();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
