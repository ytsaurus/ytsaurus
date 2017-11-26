#pragma once

#include <yt/core/misc/ref.h>

#include <util/system/file.h>
#include <util/system/types.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(IIOEngine)

class IIOEngine
    : public TRefCounted
{
public:
    virtual ~IIOEngine() = default;

    virtual size_t Pread(const TFile& handle, TSharedMutableRef & result, size_t len, i64 offset) = 0;
    virtual void Pwrite(const TFile& handle, const void* buf, size_t len, i64 offset) = 0;
    virtual std::unique_ptr<TFile> Open(const TString& fName, EOpenMode oMode) = 0;
};

IIOEnginePtr CreateIOEngine(const TString & name);
IIOEnginePtr CreateDefaultIOEngine();

DEFINE_REFCOUNTED_TYPE(IIOEngine)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
