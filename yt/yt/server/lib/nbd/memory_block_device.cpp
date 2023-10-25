#include "memory_block_device.h"
#include "config.h"

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

class TMemoryBlockDevice
    : public IBlockDevice
{
public:
    explicit TMemoryBlockDevice(TMemoryBlockDeviceConfigPtr config)
        : Config_(std::move(config))
        , Data_(TSharedMutableRef::Allocate(Config_->Size))
    { }

    i64 GetTotalSize() const override
    {
        return Config_->Size;
    }

    bool IsReadOnly() const override
    {
        return false;
    }

    TString DebugString() const override
    {
        return "Mememory Size " + ToString(GetTotalSize());
    }

    TFuture<TSharedRef> Read(
        i64 offset,
        i64 length) override
    {
        return MakeFuture<TSharedRef>(Data_.Slice(offset, offset + length));
    }

    TFuture<void> Write(
        i64 offset,
        const TSharedRef& data,
        const TWriteOptions& /*options*/) override
    {
        // XXX(babenko): racy
        std::copy(data.Begin(), data.End(), Data_.Begin() + offset);
        return VoidFuture;
    }

    TFuture<void> Flush() override
    {
        return VoidFuture;
    }

    TFuture<void> Initialize() override
    {
        return VoidFuture;
    }

private:
    const TMemoryBlockDeviceConfigPtr Config_;

    TSharedMutableRef Data_;
};

////////////////////////////////////////////////////////////////////////////////

IBlockDevicePtr CreateMemoryBlockDevice(TMemoryBlockDeviceConfigPtr config)
{
    return New<TMemoryBlockDevice>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
