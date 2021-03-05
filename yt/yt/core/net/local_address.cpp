#include "local_address.h"

#include <yt/yt/core/concurrency/fork_aware_spinlock.h>

#include <yt/yt/core/misc/proc.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <errno.h>

#include <array>

namespace NYT::NNet {

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr size_t MaxLocalHostNameLength = 256;
constexpr size_t MaxLocalHostNameDataSize = 1024;

// All static variables below must be const-initialized.
// - char[] is a POD, so it must be const-initialized.
// - std::atomic has constexpr value constructors.
// However, there is no way to enforce in compile-time that these variables
// are really const-initialized, so please double-check it with `objdump -s`.
char LocalHostNameData[MaxLocalHostNameDataSize] = "(unknown)";
std::atomic<char*> LocalHostNamePtr;

} // namespace

////////////////////////////////////////////////////////////////////////////////

const char* ReadLocalHostName() noexcept
{
    // Writer-side imposes AcqRel ordering, so all preceding writes must be visible.
    char* ptr = LocalHostNamePtr.load(std::memory_order_relaxed);
    return ptr ? ptr : LocalHostNameData;
}

void WriteLocalHostName(TStringBuf hostName) noexcept
{
    static NConcurrency::TForkAwareSpinLock Lock;
    auto guard = Guard(Lock);

    char* ptr = LocalHostNamePtr.load(std::memory_order_relaxed);
    ptr = ptr ? ptr : LocalHostNameData;

    if (::strncmp(ptr, hostName.data(), hostName.length()) == 0) {
        return; // No changes; just return.
    }

    ptr = ptr + strlen(ptr) + 1;

    if (ptr + hostName.length() + 1 >= LocalHostNameData + MaxLocalHostNameDataSize) {
        ::abort(); // Once we crash here, we can start reusing space.
    }

    ::memcpy(ptr, hostName.data(), hostName.length());
    *(ptr + hostName.length()) = 0;

    LocalHostNamePtr.store(ptr, std::memory_order_seq_cst);
}

TString GetLocalHostName()
{
    return TString(ReadLocalHostName());
}

bool UpdateLocalHostName(std::function<void(const char* /* failedCall */, const char* /* details */)> errorCallback, bool resolveIntoFqdn)
{
    std::array<char, MaxLocalHostNameLength> hostName;
    hostName.fill(0);

    int result = HandleEintr(::gethostname, hostName.data(), hostName.size() - 1);
    if (result != 0) {
        errorCallback("gethostname", ::strerror(errno));
        return false;
    }

    if (!resolveIntoFqdn) {
        WriteLocalHostName(TStringBuf(hostName.data()));
        return true;
    }

    addrinfo request;
    ::memset(&request, 0, sizeof(request));
    request.ai_family = AF_UNSPEC;
    request.ai_socktype = SOCK_STREAM;
    request.ai_flags |= AI_CANONNAME;

    addrinfo* response = nullptr;
    result = ::getaddrinfo(hostName.data(), nullptr, &request, &response);
    if (result != 0) {
        errorCallback("getaddrinfo", gai_strerror(result));
        return false;
    }

    std::unique_ptr<addrinfo, void(*)(addrinfo*)> holder(response, &::freeaddrinfo);

    if (!response->ai_canonname) {
        errorCallback("getaddrinfo", "no canonical hostname");
        return false;
    }

    WriteLocalHostName(TStringBuf(response->ai_canonname));

    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet

