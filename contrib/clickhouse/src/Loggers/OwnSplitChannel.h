#pragma once

#include <atomic>
#include <memory>
#include <map>
#include <DBPoco/AutoPtr.h>
#include <DBPoco/Channel.h>
#include "ExtendedLogChannel.h"


#ifndef WITHOUT_TEXT_LOG
namespace DB
{
    template <typename> class SystemLogQueue;
    struct TextLogElement;
    using TextLogQueue = SystemLogQueue<TextLogElement>;
}
#endif

namespace DB
{
/// Works as DBPoco::SplitterChannel, but performs additional work:
///  passes logs to Client via TCP interface
///  tries to use extended logging interface of child for more comprehensive logging
class OwnSplitChannel : public DBPoco::Channel
{
public:
    /// Makes an extended message from msg and passes it to the client logs queue and child (if possible)
    void log(const DBPoco::Message & msg) override;

    void setChannelProperty(const std::string& channel_name, const std::string& name, const std::string& value);

    /// Adds a child channel
    void addChannel(DBPoco::AutoPtr<DBPoco::Channel> channel, const std::string & name);

#ifndef WITHOUT_TEXT_LOG
    void addTextLog(std::shared_ptr<DB::TextLogQueue> log_queue, int max_priority);
#endif

    void setLevel(const std::string & name, int level);

private:
    void logSplit(const DBPoco::Message & msg);
    void tryLogSplit(const DBPoco::Message & msg);

    using ChannelPtr = DBPoco::AutoPtr<DBPoco::Channel>;
    /// Handler and its pointer casted to extended interface
    using ExtendedChannelPtrPair = std::pair<ChannelPtr, ExtendedLogChannel *>;
    std::map<std::string, ExtendedChannelPtrPair> channels;

#ifndef WITHOUT_TEXT_LOG
    std::weak_ptr<DB::TextLogQueue> text_log;
    std::atomic<int> text_log_max_priority = -1;
#endif
};

}
