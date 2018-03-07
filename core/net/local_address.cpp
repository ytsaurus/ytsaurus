#include "local_address.h"

#include <yt/core/concurrency/atomic_flag_spinlock.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <errno.h>

#include <array>

namespace NYT {
namespace NNet {

////////////////////////////////////////////////////////////////////////////////

namespace {

static constexpr size_t MaxLocalHostNameLength = 256;
static constexpr size_t MaxLocalHostNameDataSize = 1024;

// All static variables below must be const-initialized.
// - char[] is a POD, so it must be const-initialized.
// - std::atomic has constexpr value constructors.
// - std::atomic_flag is specified to be const-initialized.
// However, there is no way to enforce in compile-time that these variables
// are really const-initialized, so please double-check it with `objdump -s`.
static char LocalHostNameData[MaxLocalHostNameDataSize] = "(unknown)";
static std::atomic<char*> LocalHostNamePtr{nullptr};
static std::atomic_flag LocalHostNameLock = ATOMIC_FLAG_INIT;

} // namespace

////////////////////////////////////////////////////////////////////////////////

// Read* & Write* functions are carefully engineered to be as safe and robust as possible.
// These functions may be called at any time (for example, at atexit() hooks)
// and they must not throw / crash / corrupt program.
// That is why we statically allocate working memory for these functions
// and never allocate in getter and setter.

const char* ReadLocalHostName() noexcept
{
    // Writer-side imposes AcqRel ordering, so all preceding writes must be visible.
    char* ptr = LocalHostNamePtr.load(std::memory_order_relaxed);
    return ptr ? ptr : LocalHostNameData;
}

void WriteLocalHostName(const char* data, size_t length) noexcept
{
    auto guard = Guard(LocalHostNameLock);

    char* ptr = LocalHostNamePtr.load(std::memory_order_relaxed);
    ptr = ptr ? ptr : LocalHostNameData;

    if (strncmp(ptr, data, length) == 0) {
        return; // No changes; just return.
    }

    ptr = ptr + strlen(ptr) + 1;

    if (ptr + length + 1 >= LocalHostNameData + MaxLocalHostNameDataSize) {
        abort(); // Once we crash here, we can start reusing space.
    }

    memcpy(ptr, data, length);
    *(ptr + length) = 0;

    LocalHostNamePtr.store(ptr, std::memory_order_seq_cst);
}

TString GetLocalHostName()
{
    return TString(ReadLocalHostName());
}

void SetLocalHostName(const TString& hostname)
{
    WriteLocalHostName(hostname.c_str(), hostname.length());
}

bool UpdateLocalHostName(std::function<void(const char*, const char*)> errorCb)
{
    std::array<char, MaxLocalHostNameLength> hostname;
    hostname.fill(0);

    int result;

    result = gethostname(hostname.data(), hostname.size() - 1);
    if (result != 0) {
        errorCb("gethostname failed", strerror(errno));
        return false;
    }

    addrinfo request;
    memset(&request, 0, sizeof(request));
    request.ai_family = AF_UNSPEC;
    request.ai_socktype = SOCK_STREAM;
    request.ai_flags |= AI_CANONNAME;

    addrinfo* response = nullptr;
    result = getaddrinfo(hostname.data(), nullptr, &request, &response);
    if (result != 0) {
        errorCb("getaddrinfo failed", gai_strerror(result));
        return false;
    }

    std::unique_ptr<addrinfo, void(*)(addrinfo*)> holder(response, &freeaddrinfo);

    if (!response->ai_canonname) {
        errorCb("getaddrinfo failed", "no canonical hostname");
        return false;
    }

    WriteLocalHostName(response->ai_canonname, strlen(response->ai_canonname));

    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNet
} // namespace NYT

