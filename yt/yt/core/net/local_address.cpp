#include "local_address.h"

#include "config.h"

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

void UpdateLocalHostName(const TAddressResolverConfigPtr& config)
{
    std::array<char, MaxLocalHostNameLength> hostName;
    hostName.fill(0);

    auto onFail = [&] (const std::vector<TError>& errors) {
        THROW_ERROR_EXCEPTION("Failed to update localhost name") << errors;
    };

    auto runWithRetries = [&] (std::function<int()> func, std::function<TError(int /*result*/)> onError) {
        std::vector<TError> errors;

        for (int retryIndex = 0; retryIndex < config->Retries; ++retryIndex) {
            auto result = func();
            if (result == 0) {
                return;
            }

            errors.push_back(onError(result));

            if (retryIndex + 1 == config->Retries) {
                onFail(errors);
            } else {
                Sleep(config->RetryDelay);
            }
        }
    };

    runWithRetries(
        [&] { return HandleEintr(::gethostname, hostName.data(), hostName.size() - 1); },
        [&] (int /*result*/) { return TError("gethostname failed: %v", strerror(errno)); });

    if (!config->ResolveHostNameIntoFqdn) {
        WriteLocalHostName(TStringBuf(hostName.data()));
        return;
    }

    addrinfo request;
    ::memset(&request, 0, sizeof(request));
    request.ai_family = AF_UNSPEC;
    request.ai_socktype = SOCK_STREAM;
    request.ai_flags |= AI_CANONNAME;

    addrinfo* response = nullptr;

    runWithRetries(
        [&] { return getaddrinfo(hostName.data(), nullptr, &request, &response); },
        [&] (int result) { return TError("getaddrinfo failed: %v", gai_strerror(result)); });

    std::unique_ptr<addrinfo, void(*)(addrinfo*)> holder(response, &::freeaddrinfo);

    if (!response->ai_canonname) {
        auto error = TError("getaddrinfo failed: no canonical hostname");
        onFail({error});
    }

    WriteLocalHostName(TStringBuf(response->ai_canonname));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet

