## `yt/yt/library/vector_hdrf` — Hierarchical DRF Fair Share Computation

Computes the target fair resource distribution across a pool tree using Hierarchical Dominant Resource Fairness (HDRF), as used by the YTsaurus scheduler.

---

### Algorithm Overview

Resources are vectors of fractions of total cluster capacity (`TResourceVector`). For every element `v` of the pool tree, the algorithm reasons about three monotone piecewise-linear functions:

- **`FairShareByFitFactor(v, f)`** — the amount of resources `v` would consume as a function of an abstract scalar `f` ("fit factor"; the name has no deeper semantics, it is just a monotone parameter we control). The library represents this explicitly as a `TVectorPiecewiseLinearFunction`.
  - For an operation, `f` scales usage/demand directly.
  - For a pool, each `f` corresponds to some vector of child suggestions; the function is the sum of children's `FairShareBySuggestion` at those offers.
- **`MaxFitFactorBySuggestion(v, s)`** — the largest `f` such that the dominant-resource share of `FairShareByFitFactor(v, f)` does not exceed the scalar suggestion `s`. Computed by taking each per-resource component of `FairShareByFitFactor`, transposing it (inversion of a monotone piecewise-linear function = swap `(x, y)` pairs in its representation), and reducing by pointwise min across resources.
- **`FairShareBySuggestion(v, s)`** — `FairShareByFitFactor(v, MaxFitFactorBySuggestion(v, s))`. The share `v` ends up with when its parent offers suggestion `s`. Composition of two monotone piecewise-linear functions is again piecewise-linear.

**Two-pass structure.**

1. **Bottom-up (function building).** Leaves get trivial explicit forms. A pool's `FairShareByFitFactor` is built from its children's `FairShareBySuggestion` by argument-scaling each child by its weight, summing, and then computing `MaxFitFactorBySuggestion` + the composition. After each build a compaction step (`CompressFunction` in the internal helpers) collapses near-colinear critical points — function sizes can grow geometrically with tree depth otherwise.
2. **Top-down (distribution).** Starting from the root with suggestion `s = 1`, each pool samples `MaxFitFactorBySuggestion` to get its own `f`, then propagates a per-child suggestion `min(f * child.weight, 1)` recursively. Operations write their `FairShareByFitFactor(f)` into `Attributes().FairShare`. `FairShareByFitFactor(v, 0)` always equals the element's guarantee share (base allocation before any weight-proportional distribution).

**Guarantees and limits** are folded into `MaxFitFactorBySuggestion`: the scalar suggestion `s` is treated as share on top of the element's strong guarantee, and the whole thing is capped at `LimitsShare`. Concretely, vector suggestion = `clip(guarantee + s * Ones, 0, limits)`.

**Discontinuities.** The algorithm fundamentally has to cope with non-strictly-monotone `DS(FairShareByFitFactor)`; these invert into jumps in `MaxFitFactorBySuggestion`, which propagate upward. All functions are stored as **left-continuous** (so the composition identity `FairShareBySuggestion = FairShareByFitFactor ∘ MaxFitFactorBySuggestion` holds); in the representation a discontinuity at `x` appears as two adjacent pairs `(x, P(x-0))` and `(x, P(x+0))`. The top-down pass has a vector-suggestion overload (`ComputeAndSetFairShare(TResourceVector, ...)`) that handles the gap best-effort: mandatory share = `FairShareByFitFactor(v, f-0)`, then any slack is distributed greedily among children.

**Promised guarantee fair share** (`EFairShareType::PromisedGuarantee`). Pools may opt in via `ShouldComputePromisedGuaranteeFairShare()`. An additional top-down pass computes the share the pool would get if only the guaranteed part (no weight-proportional distribution) were honored. Results land in `Attributes().PromisedGuaranteeFairShare`. Nesting opted-in pools is illegal and produces `NestedPromisedGuaranteeFairSharePools`.

**Improved fair share computation** (`EnableImprovedFairShareByFitFactorComputation`). Addresses an unfairness in the standard algorithm: when multiple children of a pool share a discontinuity at the same fit factor, greedy slack distribution gives all the extra resources to one child. The improved mode rebuilds the composite `FairShareByFitFactor` so that each child's critical points expand into a stepwise segment — one step per child — which preserves the sum but lets the top-down pass reconstruct per-child vector suggestions precisely. `ChildFairSharesByFitFactor_` is populated in this mode. `EnableImprovedFairShareByFitFactorComputationDistributionGap` adjusts how the top-down pass handles the remaining gap near a joint discontinuity.

**Complexity.** The bottom-up scheme is exponential in the number of resources `K` (function sizes grow by a factor of `K` per tree level), but not in tree depth `H`. Rough bound: `O(K^(H+1) * Sort(N + M))` for building all three function trees; the top-down pass is then logarithmic in function sizes per node. In practice even the largest production pool trees complete within a second. The naive top-down binary-search approach (no explicit function representation) is `O(K * (N + M) * L^(H+1))` where `L ≈ 64` is the IEEE-754 bisection bound — exponential in tree height and untenable in practice.

**Floating-point discipline.** The implementation:
- Uses `std::lerp`-style interpolation (`t < 0.5 ? P(x) + t*(P(x')-P(x)) : P(x') - (1-t)*(P(x')-P(x))`) to preserve monotonicity under rounding.
- Compares with an `epsilon` tolerance in invariant checks and when deciding whether to collapse near-colinear critical points.
- Uses `FloatingPointLowerBound` (from `yt/yt/library/numeric`) for exact, IEEE-754-bit-width binary search — the implementation's sampling/inversion rarely depends on bisection, but the primitive is used where it is needed.

---

### Guarantee Handling

**Strong guarantees** are configured per-element via `GetStrongGuaranteeResourcesConfig()`. Only pools receive strong guarantees; operations always report an empty config. Non-main resources without an explicit config are inferred proportionally based on the main resource ratio. If the sum of children's guarantees exceeds the parent's, they are scaled down proportionally with priority-tier awareness.

**Strong guarantee tiers** (`EStrongGuaranteeTier`, lower index = higher priority): `PriorityPools` (0), `RegularPools` (1). Since only pools receive strong guarantees, operations contribute to no tier. A pool can be marked as a *priority pool* (its guarantee is fully in the `PriorityPools` tier) and/or a *donor* pool (stops propagation of priority tier upward). During adjustment, higher-priority tiers are preserved before lower tiers are scaled. Per-tier totals land in `StrongGuaranteeShareByTier`. A priority-adjusted pool without a donor ancestor produces `PriorityStrongGuaranteeAdjustmentPoolsWithoutDonor`.

**Sharing-incentive note.** When guarantees are present, "fairness of the remainder" has two defensible definitions: either distribute the free share (`Ones - sum(guarantees)`) equally, or keep distributing total cluster share equally regardless of how much has been reserved as guarantees. This library uses the second interpretation — it does **not** subtract reserved guarantees from the pool of weight-proportional resources. This is a deliberate choice: it works well in practice and keeps the code simpler.

**Integral guarantees** (`EIntegralGuaranteeType`): pools can accumulate resource × time volume between updates. During each update, `TFairShareUpdateExecutor` refills accumulated volumes and then distributes integral shares to burst/relaxed pools. `Burst` pools spend from their accumulated volume first; `Relaxed` pools are filled from leftover free volume up to `GetIntegralShareLimitForRelaxedPool()`. `AdjustProposedIntegralShare(limitsShare, strongGuaranteeShare, proposedIntegralShare)` is a free function exposed for callers that need to reproduce the "don't exceed limits" clamping outside the executor.

---

### Special Cases

- **FIFO pools** (`ESchedulingMode::Fifo`): children are sorted by `HasHigherPriorityInFifoMode`, then `FairShareByFitFactor` is defined on `[0, len(children)]`: child `i` is offered `1` if `i <= f`, `0` if `i > f+1`, and the linear interpolation `f + 1 - i` in between. `Attributes().FifoIndex` is set on each child. FIFO pools require all children to be operations (enforced by `YT_VERIFY` in `PrepareFifoPool`).
- **Gang operations** (`IsGangLike() == true`) when `EnableStepFunctionForGangOperations` is on: `FairShareByFitFactor` on `[0, 2]`: `0` for `f < 1`, `demand` for `f >= 1`. Makes the "all or nothing" nature of a gang a discontinuity the algorithm handles naturally. Without this flag a gang operation is modeled as a regular operation, which can lead to wasted reservations.
- **Multistage operations** (usage-feedback): `FairShareByFitFactor` on `[0, 2]`: `f * usage` for `f ∈ [0, 1]`, and `usage + (f-1) * (demand - usage)` for `f ∈ [1, 2]`. Lets the scheduler adapt to actual consumption before targeting full demand across subsequent updates.

---

### Components

**`fair_share_update.h`** — Core algorithm: element interfaces, update context, executor.
- `TElement` / `TCompositeElement` / `TPool` / `TRootElement` / `TOperationElement` — abstract base classes with virtual `TRefCounted` inheritance (diamond hierarchies are expected). Callers subclass these and implement resource demand, usage, limits, weight, pool-type queries, and the many feature-toggle virtuals (FIFO priority, gang semantics, free-volume acceptance, priority-adjustment flags, promised-guarantee opt-in, etc.). Results are written to `Attributes()` after the update. Each `TElement` caches its three functions as `std::optional<...>`; drop them between reuses via `ResetFairShareFunctions()`.
- `TSchedulableAttributes` — per-element outputs. `FairShare` and `PromisedGuaranteeFairShare` (each a `TDetailedFairShare` with `StrongGuarantee`, `IntegralGuarantee`, `WeightProportional`, `Total`); `DemandShare`, `LimitsShare`, `UsageShare`; `StrongGuaranteeShare` and `StrongGuaranteeShareByTier`; `ProposedIntegralShare`, `EstimatedGuaranteeShare`; integral volume state (`AcceptableVolume`, `AcceptedFreeVolume`, `VolumeOverflow`, `ChildrenVolumeOverflow`); `InferredStrongGuaranteeResources`; burst/flow ratios; `FifoIndex`; `DominantResource`. Use `GetFairShare(type)` / `SetDetailedFairShare(...)` to read/write by `EFairShareType`.
- `TFairShareFunctionsStatistics` — sizes of the three internal functions per element (for diagnostics). Fetch via `TElement::GetFairShareFunctionsStatistics()`.
- `TFairShareUpdateOptions` — `MainResource`, integral-guarantee timings (`IntegralPoolCapacitySaturationPeriod`, `IntegralSmoothPeriod`), and feature toggles (`EnableStepFunctionForGangOperations`, `EnableImprovedFairShareByFitFactorComputation`, `EnableImprovedFairShareByFitFactorComputationDistributionGap`).
- `TFairShareUpdateContext` — constructed with options, total cluster limits, current time, and previous update time. Collects `Errors`, the `BurstPools`/`RelaxedPools` working lists, pool-config validation state (`NestedPromisedGuaranteeFairSharePools`, `PriorityStrongGuaranteeAdjustmentPoolsWithoutDonor`, …), and per-phase CPU timings.
- `TFairShareUpdateExecutor` — entry point. `Run()` is synchronous and single-threaded; the caller keeps the pool tree alive and must not mutate it during `Run()`. The executor only holds an intrusive ref to the root.

**`base_element.h`** — Convenience stubs. `TBaseElement`, `TBaseCompositeElement`, `TBasePool`, `TBaseRootElement`, `TBaseOperationElement` provide default no-op / false / empty implementations for the optional feature virtuals (integral guarantee state, logging, gang semantics, free-volume routing, priority-adjustment flags, promised-guarantee opt-in). Inherit from these to avoid boilerplate when those features are not needed. `TBasePool` also owns an `IntegralResourcesState_` member.

**`resource_vector.h`** — `TResourceVector`: per-resource fraction of cluster capacity, stored as a `TDoubleArrayBase<ResourceCount, TResourceVector>`, indexed by `EJobResourceType`. All fair share outputs are in this form. Convert from absolute resources via `TResourceVector::FromJobResources(res, totalLimits)`. The header also defines the type aliases the algorithm uses everywhere: `TVectorPiecewiseLinearFunction`, `TScalarPiecewiseLinearFunction`, and their segment analogues.

**`job_resources.h`** — `TJobResources`: absolute resource amounts (user slots, CPU, GPU, memory, network; CPU is a `TFixedPointNumber<i64, 2>`). Input to the algorithm. `TJobResourcesConfig`: optional-valued resource config for strong guarantees, with `ForEachResource` for generic traversal. `EJobResourceType`: resource dimension enum. Free functions include `GetDominantResource`, `GetMinResourceRatio`/`GetMaxResourceRatio`, `Dominates` / `StrictlyDominates`, `Min` / `Max`, and serialization hooks.

**`resource_volume.h`** — `TResourceVolume`: accumulated `TJobResources × TDuration` for integral-guarantee bookkeeping, with the usual arithmetic, `Min`/`Max`, YSON serialization, and `ForEachResource`.

**`serialize.h`** — YSON serialization helpers for the output types: `Serialize(TDetailedFairShare, ...)`, `SerializeDominant(TDetailedFairShare, TFluentAny)`, `Serialize(TFairShareFunctionsStatistics, ...)`. Include only where you need to emit results to YSON — keeps the core headers free of the YSON dependency tree.

---

### Typical Usage

```cpp
// 1. Subclass TBaseRootElement, TBasePool, TBaseOperationElement,
//    implementing GetResourceDemand(), GetResourceLimits(), GetWeight(), etc.

// 2. Build the pool tree (link parent/child) and attach operations.

// 3. Create the context.
TFairShareUpdateContext context(
    {.MainResource = EJobResourceType::Cpu, ...},
    totalResourceLimits,
    now,
    prevUpdateTime);

// 4. Run.
TFairShareUpdateExecutor executor(rootElement, &context);
executor.Run();

// 5. Check errors and read results.
auto errors = std::move(context.Errors);
auto fairShare = op->Attributes().FairShare.Total;  // TResourceVector in [0, 1]
auto promised = op->Attributes().PromisedGuaranteeFairShare.Total;  // for opted-in pools
```

---

### Notes

- All `TResourceVector` values are fractions in `[0, 1]`. Multiply by total `TJobResources` limits to recover absolute values.
- `context.Errors` may be non-empty after `Run()` (guarantee overcommit, nested promised-guarantee pools, priority-adjustment misconfiguration). Always check them.
- `piecewise_linear_function_helpers.h` lives in `NVectorHdrf::NDetail` — do not depend on it from outside the library. It contains `CompressFunction` (the compaction loop that prevents geometric size growth under repeated summation), the transposed-segment / component-extraction helpers that `MaxFitFactorBySuggestion` needs, and a `VerifyNondecreasing` debug aid.
- The implementation of function sum / pointwise-min is slightly less efficient than the asymptotically optimal version (it recomputes values at each breakpoint rather than maintaining a running derivative, and accumulates pairwise for the max). This is a known simplification that does not dominate runtime in practice.
- `TFairShareUpdateExecutor::Run()` must not overlap with tree mutation. The executor accepts a `TRootElementPtr` by value (intrusive refcount), but does not take ownership of anything else.

**See also:** `yt/yt/library/numeric` — `TPiecewiseLinearFunction`, `TDoubleArray`, IEEE-754-bit-width binary search (all three are foundational here).
