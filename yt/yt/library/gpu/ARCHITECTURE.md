## `yt/yt/library/gpu` — GPU Info Provider

Uniform interface for querying per-GPU runtime metrics and RDMA device info from the host, abstracting over three different backends. Also exposes a narrow mutation API for adjusting per-RDMA-device network priority.

**Include:** `<yt/yt/library/gpu/gpu_info_provider.h>`

**Key types:**

- `IGpuInfoProvider` — the main interface; obtain with `CreateGpuInfoProvider(config)`. Three synchronous methods:
  - `GetGpuInfos(timeout)` — one `TGpuInfo` per GPU.
  - `GetRdmaDeviceInfos(timeout)` — one `TRdmaDeviceInfo` per RDMA device.
  - `ApplyNetworkServiceLevel(deviceIds, level, timeout)` — mutator; support is backend-specific.
- `TGpuInfo` — snapshot of one GPU: `UpdateTime`, `Index`, `Name`, utilization/memory/power, `ClocksSM`/`ClocksMaxSM`, `SMUtilizationRate`/`SMOccupancyRate`, NVLink/PCIe RX/TX byte rates, `TensorActivityRate`, `DramActivityRate`, a `TEnumIndexedArray<ESlowdownType, bool> Slowdowns` mask, and a `Stuck { Status, LastTransitionTime }` flag. Individual fields may be left at their defaults depending on which backend produced the snapshot.
- `TRdmaDeviceInfo` — `Name`, `DeviceId`, `RxByteRate`, `TxByteRate`.
- `ESlowdownType` — `HW`, `HWPowerBrake`, `HWThermal`, `SWThermal`.
- `TNetworkPriority` — `i8` alias used by `ApplyNetworkServiceLevel` (declared in `public.h`).
- `TGpuInfoProviderConfig` — polymorphic YSON struct keyed by `EGpuInfoProviderType`. Default concrete type is `NvidiaSmi`.

**Backends (`EGpuInfoProviderType`):**

| Value | Backend | Notes |
|-------|---------|-------|
| `NvidiaSmi` | Shells out to `nvidia-smi` (opensource default) | Only overrides `GetGpuInfos`; populates a basic subset of `TGpuInfo` fields (index, name, utilization rates, memory, power, SM clocks). `GetRdmaDeviceInfos` and `ApplyNetworkServiceLevel` return empty / no-op from the base class. |
| `GpuAgent` | gRPC to `gpu-agent` | Only overrides `GetGpuInfos`. `GetRdmaDeviceInfos` / `ApplyNetworkServiceLevel` inherit empty / no-op defaults. |
| `NvGpuManager` | gRPC to `nv-gpu-manager`, Yandex-internal | Stub in opensource builds — `CreateNvManagerGpuInfoProvider` throws "not supported in this build"; the real implementation is linked in only in internal builds via a `Y_WEAK` override. |
| `Base` | Sentinel / abstract | Not valid at runtime — `CreateGpuInfoProvider` aborts for this type. |

gRPC backends share `TGrpcGpuInfoProviderBase` (see `gpu_info_provider_detail.h`), which wraps the channel with retry via `TRetryingChannelConfig`. The default implementations in `TGpuInfoProviderBase` (`gpu_info_provider_detail.cpp`) return empty vectors / no-op, so consumers can safely call every method regardless of backend.

**Typical usage:**

```cpp
TGpuInfoProviderConfig config; // defaults to NvidiaSmi
auto provider = CreateGpuInfoProvider(std::move(config));

auto gpuInfos = provider->GetGpuInfos(TDuration::Seconds(5));
for (const auto& info : gpuInfos) {
    // info.Index, info.UtilizationGpuRate, info.MemoryUsed, etc.
}

auto rdmaInfos = provider->GetRdmaDeviceInfos(TDuration::Seconds(5));
```

`TGpuInfo` and `TRdmaDeviceInfo` have `Serialize(...)` overloads for YSON; `TGpuInfo` also has `FormatValue` (for `%v` logging).

**Notes:**
- All methods are synchronous and block for up to the given `timeout`.
- `NvidiaSmi` issues two subprocess calls per `GetGpuInfos` — first `nvidia-smi -q` (via `helpers.h::GetGpuMinorNumbers`, UUID→minor map), then `nvidia-smi --query-gpu=...`. Both must complete within the overall `timeout`.
- `helpers.h::GetGpuMinorNumbers(timeout)` is also exported as a standalone utility, independent of the provider.
- The interface name is a misnomer — it has a mutator (`ApplyNetworkServiceLevel`) despite the `Info` in the name (see the `TODO` in the header).
- The reconstruction pattern used by `gpu_manager.cpp` (primary consumer) is to store the provider in an atomic pointer and replace it wholesale when configuration changes — do not expect the provider to hot-reload its own config.

**See also:** `yt/yt/server/node/exec_node/gpu_manager.cpp` (primary consumer), `yt/gpuagent` (the GPU agent service).
