# Vector HDRF Architecture

This document explains the architecture and algorithm implemented in `yt/yt/library/vector_hdrf`.
It is intended as a practical guide for AI agents and developers who need to modify or debug the fair-share computation used by the scheduler.

The authoritative theoretical description is the Russian document at repository root: `vector_hdrf.txt`.
This file connects that theory to the actual C++ implementation.

## Purpose

The library computes a target hierarchical fair-share allocation for a tree of pools and operations under multiple resource dimensions:

- `user_slots`
- `cpu`
- `gpu`
- `memory`
- `network`

The algorithm is a hierarchical extension of Dominant Resource Fairness with several production extensions:

- hierarchical pool trees
- per-element limits
- strong guarantees
- integral guarantees for burst/relaxed pools
- FIFO pools
- usage-aware operation behavior
- gang-like operation step functions
- conservative handling of discontinuities and floating-point precision

The library only computes the **target fair-share state**.
It does not launch jobs itself.
Scheduler code uses the result later when deciding which allocations/jobs to start.

## Main entry point

The main entry point is `TFairShareUpdateExecutor::Run()` in `fair_share_update.cpp`.

Inputs:

- `TRootElementPtr rootElement`
- `TFairShareUpdateContext`

Outputs are written back into each element's `TSchedulableAttributes`:

- fair share
- promised guarantee fair share
- usage/demand/limit shares
- strong/integral guarantee shares
- burst/resource-flow metadata
- diagnostics for function sizes and profiling

## File map

### Core algorithm

- `fair_share_update.h`
- `fair_share_update.cpp`

Defines the tree interfaces, scheduler-facing attributes, update context/options, and the whole fair-share update pipeline.

### Default stub implementations

- `base_element.h`
- `base_element.cpp`

Provide default no-op behavior for optional features so external users can implement only the methods they need.

### Resource primitives

- `job_resources.h/.cpp`
- `resource_vector.h/.cpp`
- `resource_volume.h/.cpp`

These files define the discrete resource container, normalized resource shares, and time-integrated resource volume.

### Function helpers

- `piecewise_linear_function_helpers.h/.cpp`

Helpers for manipulating monotone piecewise-linear scalar/vector functions, especially compression and component extraction.
The actual generic piecewise-linear abstractions come from `yt/yt/library/numeric`.

### Serialization

- `serialize.h/.cpp`

YSON serialization for fair-share structures and diagnostics.

### Build/test metadata

- `ya.make`
- `CMakeLists.*`
- `unittests/`

## Core abstractions

## Resource types

### `TJobResources`

Represents absolute resource amounts.
Used for demand, usage, limits, and guarantees in scheduler units.

Key helpers:

- `GetDominantResource`
- `GetDominantResourceUsage`
- `Dominates`
- `Min` / `Max`
- arithmetic operators

### `TResourceVector`

Represents normalized resource shares, usually as fractions of total cluster capacity.
For example, `0.25` in CPU means 25% of total cluster CPU.

Important properties:

- fixed dimensionality equal to the number of resource types
- supports pointwise arithmetic
- used by the fair-share functions
- includes `Epsilon()` and `Infinity()` helpers

Conversion boundary:

- absolute values live in `TJobResources`
- normalized values live in `TResourceVector`
- conversion uses `TResourceVector::FromJobResources(resources, totalLimits)`

### `TResourceVolume`

Represents time-integrated resource consumption/credit.
This is used by integral guarantees (`Burst` and `Relaxed`) to accumulate or spend volume over time.

## Tree node hierarchy

The algorithm operates on a tree of polymorphic elements.
The library does not own scheduler business objects directly; instead, scheduler-side wrappers implement these interfaces.

### `TElement`

Abstract base for any schedulable node.

Required scheduler-supplied methods include:

- `GetResourceDemand()`
- `GetResourceUsageAtUpdate()`
- `GetResourceLimits()`
- `GetStrongGuaranteeResourcesConfig()`
- `GetWeight()`
- `Attributes()`
- `GetParentElement()`
- `GetId()`
- logging hooks

Internally, `TElement` stores three lazily prepared functions:

- `FairShareByFitFactor_`
- `MaxFitFactorBySuggestion_`
- `FairShareBySuggestion_`

These are prepared once per update and cached until `ResetFairShareFunctions()`.

### `TCompositeElement`

Base for nodes with children.
Adds:

- child enumeration
- scheduling mode selection: `FairShare` or `Fifo`
- integral/burst/free-volume policy hooks
- promised-guarantee and priority-adjustment hooks

This class contains most of the pool recursion logic:

- building pool fair-share functions from children
- propagating suggestions to children
- computing cumulative attributes
- free-volume distribution

### `TPool`

A composite element with integral-guarantee state.
Adds:

- `IntegralResourcesState()`
- `GetIntegralGuaranteeType()`
- `GetIntegralShareLimitForRelaxedPool()`

The two supported integral guarantee modes are:

- `Burst`
- `Relaxed`

### `TRootElement`

Special composite root.
It owns total cluster resources and validates/adjusts tree-wide guarantee consistency.

### `TOperationElement`

Leaf representing an operation.
Adds:

- `GetBestAllocationShare()`
- `IsGangLike()`

Operations provide the leaf `FairShareByFitFactor` behavior.
This is where usage-aware and gang-specific semantics are implemented.

### `TBase*` classes

`base_element.*` provides minimal defaults for optional behavior.
These are useful for tests or for consumers that do not use all YT scheduler features.

## Persistent per-node state

`TSchedulableAttributes` is the central mutable state structure attached to every element.

Important fields:

- `FairShare`
- `PromisedGuaranteeFairShare`
- `UsageShare`
- `DemandShare`
- `LimitsShare`
- `StrongGuaranteeShare`
- `StrongGuaranteeShareByTier`
- `ProposedIntegralShare`
- `EstimatedGuaranteeShare`
- `PromisedFairShare`
- `VolumeOverflow`
- `AcceptableVolume`
- `AcceptedFreeVolume`
- `ChildrenVolumeOverflow`
- `InferredStrongGuaranteeResources`
- `BurstRatio`
- `TotalBurstRatio`
- `ResourceFlowRatio`
- `TotalResourceFlowRatio`

`TDetailedFairShare` splits total fair share into three components:

- `StrongGuarantee`
- `IntegralGuarantee`
- `WeightProportional`
- `Total`

This split is computed by `SetDetailedFairShare` after total fair share is known.

## Algorithm model

The Russian document uses the following theoretical functions:

- `G(v, s)` / `F(v, t)` / `H(v, s)`

The implementation renames them to three production-oriented functions:

- `FairShareByFitFactor(v, f)`
- `MaxFitFactorBySuggestion(v, s)`
- `FairShareBySuggestion(v, s)`

with the key identity:

`FairShareBySuggestion = FairShareByFitFactor ∘ MaxFitFactorBySuggestion`

### Meaning of the parameters

#### Suggestion

A scalar `s` in `[0, 1]` representing how much additional dominant-share budget is offered to a node on top of its guarantee, subject to limits.

Because guarantees and limits are vector-valued, `s` is converted to a vector bound by:

- start from `StrongGuaranteeShare + ProposedIntegralShare`
- add scalar suggestion equally across resources
- clip by `LimitsShare`

See `TElement::GetVectorSuggestion()` and `PrepareMaxFitFactorBySuggestion()`.

#### Fit factor

An abstract monotonically increasing parameter used to build pool-level piecewise-linear functions.
It has no single universal physical meaning.
Its semantics depend on node type:

- for normal pools: weighted progression among children
- for FIFO pools: number of fully advanced children plus fractional progress of the next child
- for operations: progression from zero to usage, then from usage to demand
- for gang operations with step mode: zero until threshold, then all demand

### Dominant share

Like DRF, comparison is based on the maximum normalized resource component.
In code this is effectively `MaxComponent(TResourceVector)` and helpers such as `GetDominantResource` / `GetDominantResourceUsage`.

## Leaf behavior: operations

`TOperationElement::PrepareFairShareByFitFactor()` builds the operation's piecewise-linear function.

### Default two-phase operation model

The operation fair-share curve is defined on fit factor range `[0, 2]`:

- `0..1`: ramp toward current usage
- `1..2`: ramp from current usage toward full demand

This matches the practical extension described in `vector_hdrf.txt`:

- it uses actual observed usage as feedback
- it adapts to multi-stage operations whose current resource mix differs from total demand

Implementation detail:

- the first phase is not simply `f * usage`
- it equalizes across resources relative to the operation's maximum usage share, producing breakpoints whenever another resource saturates
- the second phase is a single segment from `UsageShare` to `DemandShare`

### Gang-like operations

If both of the following are true:

- the update option enables step functions for gang operations
- the parent pool allows it
- `IsGangLike()` is true

then the curve becomes a step:

- zero up to fit factor `1`
- full demand at and above `1`

This models all-or-nothing usefulness.

### Discretized fair share (staircase approximation)

When all of the following conditions are met the operation uses a staircase curve instead of the default two-phase ramp:

- `TFairShareUpdateOptions::EnableDiscretizedFairShare` is `true` (set from `TStrategyTreeConfig::EnableDiscretizedFairShare`)
- the parent pool's `IsDiscretizedFairShareEnabled()` returns `true`
- `GetPendingJobCount() > 0`
- `MaxComponent(GetPerJobResourceVector()) > 0`
- the operation is not gang-like (gang step function takes priority)

#### Purpose

A standard operation can receive a fractional fair share such as 1.5 jobs worth of CPU.
The scheduler cannot schedule half a job, so it rounds down and wastes resources, or round-robins jobs non-deterministically.
Discretized fair share restricts the allocation to an exact integer multiple of the per-job resource vector, ensuring the fair share always corresponds to a schedulable number of whole jobs.

#### Construction: small operation (`K ≤ MaxDiscretizedSteps`)

A staircase of `K` steps is built, where each step adds exactly one job worth of resources:

```
FairShareByFitFactor:
  [0, ff1)        → 0
  [ff1, ff2)      → 1 * perJobVec
  [ff2, ff3)      → 2 * perJobVec
  ...
  [ff_K, ∞)       → K * perJobVec  (= DemandShare)
```

Each step boundary `ff_k` is placed at `k * 2 / (K + 1)` so the steps are evenly spread across the normal `[0, 2]` operation domain and the gaps provide space for competing operations to receive resources.

#### Construction: large operation (`K > MaxDiscretizedSteps`)

When `K > MaxDiscretizedSteps` (default 100) a full staircase would be too large.
A windowed staircase of at most `MaxDiscretizedSteps` steps is built around the hint:

1. **Hint derivation**: use the previous-cycle fair share if available; otherwise fall back to current usage share.
2. **Hint-to-job-count conversion**: divide the hint's dominant-resource component by `perJobVec[dominantResource]` to get `K_hint`.
3. **Window**: `[K_lo, K_hi]` of width at most `MaxDiscretizedSteps` centered on `K_hint`, clamped to `[0, K]`.
4. Below `K_lo` the staircase is flat at `K_lo * perJobVec`; above `K_hi` it is flat at `K_hi * perJobVec`.

This preserves the discrete-boundary property within the expected allocation region and degrades gracefully as `K → ∞`.

#### Configuration surface

| Config field | Location | Default | Description |
|---|---|---|---|
| `EnableDiscretizedFairShare` | `TStrategyTreeConfig` | `false` | Master switch for the tree |
| `MaxDiscretizedSteps` | `TStrategyTreeConfig` | `100` | Maximum staircase steps per operation |
| `IsDiscretizedFairShareEnabled()` | `TPoolTreePoolElement` (pool-level) | reads tree config | Returns `TreeConfig_->EnableDiscretizedFairShare` |

#### Diagnostics

- `TSchedulableAttributes::DiscretizedFairShareActive` is set to `true` for each operation that used the discretized path during the last fair-share update.
- This flag is exported to the scheduler orchid under the key `discretized_fair_share_active` on the operation's orchid entry.
- When detailed logging is enabled (`AreDetailedLogsEnabled()`), `PrepareFairShareByFitFactor` emits a `DEBUG`-level log entry showing `PendingJobCount`, `PerJobVector`, `DemandShare`, and (for large operations) the window bounds.

#### Interaction with other features

- Gang step functions take priority: if `IsGangLike()` is true and gang step functions are enabled, the standard step-function path is used, not the staircase.
- The staircase path is mutually exclusive with the default two-phase ramp; only one path is taken per `PrepareFairShareByFitFactor` call.
- Integral guarantees are not affected: they interact with the pool-level functions, not directly with the operation curves.

### Operation limits

Operation limit share is additionally clipped by `GetBestAllocationShare()`.
So even if theoretical demand is higher, fair share cannot exceed the best allocatable vector.

## Internal behavior: pools

Pools build their functions bottom-up from child `FairShareBySuggestion` functions.

### Normal fair-share pools

`TCompositeElement::PrepareFairShareByFitFactorNormal()`:

- scales each child function's argument by `child_weight / min_child_weight`
- extends each function to the common right boundary
- sums all children functions

This is the production encoding of weighted hierarchical DRF.

### FIFO pools

`TCompositeElement::PrepareFairShareByFitFactorFifo()` changes the meaning of fit factor.
A FIFO fit factor is:

- number of fully advanced children
- plus a fractional suggestion for the first not-yet-fully-advanced child

The resulting function is a shifted sum of child `FairShareBySuggestion` functions.
Children are first reordered in `PrepareFifoPool()` according to pool-defined priority.

Practical effect:

- older or higher-priority children get resources first
- many small one-job operations become almost fully satisfied or almost completely unsatisfied, which reduces granularity pathologies

### Improved discontinuity-aware pool computation

If `EnableImprovedFairShareByFitFactorComputation` is enabled, pool construction uses `ComputeImprovedFairShareByFitFactor()` instead of a plain sum.

This implements the advanced idea from `vector_hdrf.txt` of preserving more information around child discontinuities by stretching critical points into a stepwise sequence.

Important consequence:

- the pool stores per-child reconstructed functions in `ChildFairSharesByFitFactor_`
- later top-down distribution can suggest child vector shares more accurately than with a simple scalar-only reconstruction

Without this option, the algorithm still works, but discontinuity handling is more conservative.

## How `MaxFitFactorBySuggestion` is built

After `FairShareByFitFactor` is prepared, `TElement::PrepareMaxFitFactorBySuggestion()` derives the inverse-like mapping.

For each resource component separately, it:

- extracts the scalar component of `FairShareByFitFactor`
- transposes it
- narrows it between guarantee and limit for that resource
- shifts it so the guarantee becomes argument `0`
- extends to the full suggestion domain `[0, 1]`

Then it takes the pointwise minimum across resource components.

Interpretation:

- for a given scalar suggestion `s`, each resource gives the maximum fit factor that still keeps that resource within the suggested vector bound
- the actual admissible fit factor is the minimum over resources

This is exactly the vector generalization of “find the largest parameter that still fits inside the offered box”.

## How `FairShareBySuggestion` is built

`FairShareBySuggestion` is composed as:

`FairShareByFitFactor.Compose(MaxFitFactorBySuggestion)`

Then it is compressed with `NDetail::CompressFunction`.

Compression is an optimization only.
It tries to reduce segment count while remaining within a small error bound and preserving monotonicity.

## Update pipeline in `TFairShareUpdateExecutor::Run()`

The implementation comments in `fair_share_update.cpp` are authoritative.
The effective pipeline is:

### 1. Validate pool configuration

`RootElement_->ValidatePoolConfigs(Context_)`

Finds invalid feature combinations such as:

- nested promised-guarantee pools
- priority-strong-guarantee adjustment pools without donors

Errors are accumulated in `Context_->Errors`.

### 2. Infer strong guarantees through the tree

`DetermineInferredStrongGuaranteeResources`

Behavior:

- root starts with total cluster resources
- explicit child guarantees are read from config
- missing child guarantees for non-main resources can be inferred proportionally from main-resource guarantees

This creates the effective strong-guarantee resource vectors used by the algorithm.

### 3. Initialize integral-pool lists

`InitIntegralPoolLists`

Collects pools with:

- `Burst` integral guarantees
- `Relaxed` integral guarantees

### 4. Compute cumulative attributes

`UpdateCumulativeAttributes`

This fills normalized per-node values such as:

- `LimitsShare`
- `UsageShare`
- `DemandShare`
- `StrongGuaranteeShare`
- cumulative burst/resource-flow ratios

### 5. Consume/refill integral-pool resource volume

`ConsumeAndRefillIntegralPools()`:

- updates `AccumulatedVolume` for burst/relaxed pools
- computes overflow and acceptable volume recursively
- distributes free volume toward pools that can absorb it

This is the time-coupled part of the algorithm.
It makes integral guarantees depend on recent history rather than only the current snapshot.

### 6. Validate and adjust guarantees

`ValidateAndAdjustSpecifiedGuarantees`

Checks tree-wide overcommit:

- strong guarantees + resource flow
- strong guarantees + burst ratios

If needed, it rescales strong guarantee shares and burst ratios to restore vector invariants.
Then it computes:

- guarantee tiers
- adjusted guarantees
- estimated guarantee share

### 7. Assign integral shares to burst pools

`UpdateBurstPoolIntegralShares`

For each burst pool, the executor:

- estimates integral-share ratio from accumulated resource volume
- temporarily proposes an integral share
- builds the pool fair-share functions under that temporary guarantee
- reads how much fair share is actually consumable inside guarantees
- converts that into effective integral share
- writes the result back hierarchically

### 8. Assign integral shares to relaxed pools

`UpdateRelaxedPoolIntegralShares`

For relaxed pools, the executor:

- computes cluster share available after guaranteed usage
- temporarily tightens relaxed-pool limits according to available integral budget
- prepares fair-share functions for those pools
- binary-searches a common fit factor across relaxed pools weighted by accumulated volume
- converts resulting within-guarantee fair share into integral share
- clips by hierarchical residual limits

### 9. Build all fair-share functions

`RootElement_->PrepareFairShareFunctions(Context_)`

This is bottom-up:

- children first
- parent after children
- for each node: `FairShareByFitFactor`, then `MaxFitFactorBySuggestion`, then `FairShareBySuggestion`

### 10. Compute regular fair share top-down

`RootElement_->ComputeAndSetFairShare(...)`

Two variants exist:

- scalar-suggestion recursion
- vector-suggested-fair-share recursion used by improved mode

The algorithm:

- computes this node’s suggested fair share
- finds a fit factor that does not over-suggest children
- derives child suggestions or child vector shares
- recurses into children
- validates that child totals match the predicted parent share closely enough
- stores detailed fair-share components

### 11. Compute promised-guarantee fair share

`ComputePromisedGuaranteeFairShare`

Some pools request a second fair-share calculation at suggestion `0`, representing what is guaranteed independent of extra weight-proportional share.

### 12. Fix root accounting

`UpdateRootFairShare()`

The root’s detailed fair-share split is recomputed from the sum of children, using the actually used strong-guarantee share rather than the configured tree total.

## Guarantees and limits

The code implements the “guarantee + residual equal suggestion” model described in `vector_hdrf.txt`.

Practical meaning:

- strong guarantees create a vector floor
- scalar suggestion adds equal dominant-share budget on top of that floor
- limits cap the result

`AdjustProposedIntegralShare()` handles precision-induced violations where guarantee plus integral share slightly exceeds limits.

## Discontinuities and left continuity

The theory document spends significant effort on discontinuities caused by zero-demand resources or step-like behavior.
The implementation follows a practical conservative rule:

- functions are treated as effectively left-continuous for safe allocation
- when a discontinuity creates ambiguity, the code prefers a fit factor / child allocation that does not exceed the offered vector
- improved mode preserves more structure around child discontinuities, reducing lost share and unfairness

Relevant code paths:

- `PrepareMaxFitFactorBySuggestion`
- `ComputeAndSetFairShare`
- `ComputeImprovedFairShareByFitFactor`

## Floating-point strategy

The library is deliberately defensive about floating-point issues.

Notable tactics:

- pervasive epsilon-based dominance checks
- monotonicity verification of constructed functions
- compression with small error tolerance
- precise adjustment of integral share via `std::nextafter`
- recomputation / fallback binary search when the nominal fit factor would overshoot child totals

When changing logic, preserve these invariants. Small-looking arithmetic changes can break monotonicity or feasibility.

## Important invariants

These invariants are assumed widely in the code:

- fair-share functions are monotone nondecreasing
- prepared functions are trimmed and have expected domains
- `DemandShare` dominates `UsageShare`
- `LimitsShare` stays within `[0, 1]` componentwise
- final fair share should not significantly exceed demand
- parent predicted fair share should dominate recursively used child fair share
- guarantee share must not exceed limits share

Violating these typically causes subtle scheduler bugs rather than immediate crashes.

## Extension points for future development

The intended customization points are mostly virtual methods on tree elements.

### Operation-side extension points

- `GetResourceDemand`
- `GetResourceUsageAtUpdate`
- `GetResourceLimits`
- `GetBestAllocationShare`
- `IsGangLike`

These control leaf-level behavior.

### Pool-side extension points

- `GetMode`
- `HasHigherPriorityInFifoMode`
- `GetSpecifiedBurstRatio`
- `GetSpecifiedResourceFlowRatio`
- `CanAcceptFreeVolume`
- `ShouldDistributeFreeVolumeAmongChildren`
- `ShouldComputePromisedGuaranteeFairShare`
- priority strong-guarantee adjustment hooks
- relaxed-pool integral limit hook

These control how child resources are ordered and how guarantees/history interact.

## What to read first when modifying behavior

### If you change leaf semantics

Read:

- `TOperationElement::PrepareFairShareByFitFactor`
- `TOperationElement::ComputeLimitsShare`

### If you change pool fairness semantics

Read:

- `TCompositeElement::PrepareFairShareByFitFactorNormal`
- `TCompositeElement::PrepareFairShareByFitFactorFifo`
- `TCompositeElement::ComputeAndSetFairShare`
- `TCompositeElement::ComputeImprovedFairShareByFitFactor`

### If you change guarantees

Read:

- `DetermineInferredStrongGuaranteeResources`
- `ComputeStrongGuaranteeShareByTier`
- `AdjustStrongGuarantees`
- `ValidateAndAdjustSpecifiedGuarantees`
- `AdjustProposedIntegralShare`

### If you change integral guarantees

Read:

- `TPool::UpdateAccumulatedResourceVolume`
- `UpdateOverflowAndAcceptableVolumesRecursively`
- `DistributeFreeVolume`
- `UpdateBurstPoolIntegralShares`
- `UpdateRelaxedPoolIntegralShares`

### If you change numeric behavior

Read:

- `PrepareMaxFitFactorBySuggestion`
- `piecewise_linear_function_helpers.*`
- `yt/yt/library/numeric/*piecewise*`
- `yt/yt/library/numeric/binary_search.h`

## Common pitfalls for AI agents

- Do not assume fit factor always lives in `[0, 1]`; operations use `[0, 2]`, FIFO pools use child-count scale, improved mode can stretch critical points further.
- Do not confuse absolute resources (`TJobResources`) with normalized shares (`TResourceVector`).
- Do not bypass `SetDetailedFairShare`; regular/promised share decomposition depends on it.
- Do not remove epsilon checks casually; many exist because exact equality is unstable here.
- Do not treat the root as an ordinary pool; it has special guarantee validation and accounting logic.
- Do not change pool child ordering semantics without checking FIFO-specific code and scheduler-side priorities.
- If you alter function construction, re-check monotonicity, trimming, and composition assumptions.

## Relationship to `vector_hdrf.txt`

`vector_hdrf.txt` provides the theory and motivation.
The code corresponds to it as follows:

- theory `Dominant Resource Fairness` -> dominant-share comparison in normalized vectors
- theory `hierarchical distribution` -> recursive pool/operation tree with child suggestions
- theory `bottom-up piecewise-linear functions` -> `PrepareFairShareFunctions`
- theory `guarantees and limits` -> guarantee floor plus limited suggestion box
- theory `FIFO pools` -> `PrepareFairShareByFitFactorFifo`
- theory `usage-aware operations` -> operation two-phase curve
- theory `gang operations` -> optional step function
- theory `advanced discontinuity handling` -> `EnableImprovedFairShareByFitFactorComputation`

## Minimal mental model

If you need a short working model, use this:

1. Normalize every demand/usage/limit into cluster shares.
2. Build a monotone piecewise-linear function for each node that says how much share it can consume as fairness pressure increases.
3. Invert that function componentwise to learn how much pressure fits inside a suggested vector box.
4. Compose the two to get “if I offer this node suggestion `s`, what share will it actually use?”.
5. Build parent functions from child functions bottom-up.
6. Starting from root suggestion `1`, propagate fair-share decisions top-down.
7. Split the final share into strong-guarantee, integral-guarantee, and weight-proportional parts.

That is the essence of the library.
