#include "table_stream_registry.h"

#include "yt_io_private.h"

#include <util/system/mutex.h>
#include <util/stream/file.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TTableStreamRegistry
{
public:
    IZeroCopyOutput* GetTableStream(int tableIndex)
    {
        auto g = Guard(Lock_);

        if (std::ssize(Streams_) <= tableIndex) {
            Streams_.resize(tableIndex + 1);
        }
        if (!Streams_[tableIndex]) {
            int fd = GetOutputFD(tableIndex);
            Streams_[tableIndex] = std::make_unique<TFileOutput>(Duplicate(fd));
        }
        return Streams_[tableIndex].get();
    }

private:
    TMutex Lock_;
    std::vector<std::unique_ptr<TFileOutput>> Streams_;
};


IZeroCopyOutput* GetTableStream(int tableIndex)
{
    static TTableStreamRegistry registry;
    return registry.GetTableStream(tableIndex);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
