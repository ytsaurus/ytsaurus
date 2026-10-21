# Input Slice Architecture

## Scope

This document defines the in-memory model used to describe input chunks and the portions of
those chunks processed by jobs. It is the source of truth for the contracts of `TInputChunk`,
`TInputChunkSlice`, `TInputSliceLimit`, and `TDataSlice`.

The physical layout of ordinary and striped erasure chunks is documented in
[`ERASURE_CHUNK_FORMAT.md`](ERASURE_CHUNK_FORMAT.md).

Controller Agent orchestration of these types is documented in
[`server/controller_agent/ARCHITECTURE.md`](../../server/controller_agent/ARCHITECTURE.md#input-slices-and-read-ranges).

## Type Topology

```text
TInputChunk
    shared chunk metadata and original input read limits
        │
        ├── TInputChunkSlice
        │       one physical portion of one chunk
        │
        └── TInputChunkSlice ...
                    │
                    └── TDataSlice
                            one logical scheduling unit containing one or more chunk slices
```

| Type | Responsibility | Key ownership |
|------|----------------|---------------|
| `TInputChunk` | Compact, shared representation of `NProto::TChunkSpec`: identity, replicas, table/range indexes, sizes, read limits, and boundary keys | Read-limit and boundary-key rows are owning |
| `TInputSliceLimit` | Lower or upper restriction composed of an optional row index and a `TKeyBound` | `TKeyBound::Prefix` is non-owning |
| `TInputChunkSlice` | Physical restriction and size estimate for exactly one `TInputChunk` | Limit rows normally live in a controller-owned `TRowBuffer` |
| `TDataSlice` | Logical unit passed through input streams and chunk pools; aggregates one or more `TInputChunkSlice` objects | Logical bounds may use a different comparator prefix than child bounds |

## Limits and Comparators

A lower or upper `TInputSliceLimit` may restrict rows by row index, key bound, or both. These
selectors are independent: changing a key bound must not silently discard a row index, and
splitting by row index must preserve key bounds.

`TKeyBound` is contextually schemaful. Its interpretation depends on a `TComparator`, and its
prefix length must not exceed the comparator length used for comparison. Code that converts a
proto read limit, a chunk boundary key, or an owning row into `TKeyBound` must know:

- whether the limit is lower or upper;
- the comparator length;
- the `TRowBuffer` that owns the resulting prefix.

`TKeyBound::Prefix` is a non-owning row. Capture prefixes in the supplied `TRowBuffer` whenever
the source row does not already have a lifetime covering the slice. This includes rows decoded
from protobuf and rows created while inferring or shortening bounds.

Shortening a key bound is a projection to a shorter key space, not a formatting operation.
`ShortenKeyBound` makes a truncated bound inclusive so that the projected range remains a safe
over-approximation. For example, a lower bound `>=[1, a]` projected to one key column is
`>=[1]`.

## `TInputChunk`

`TInputChunk` is shared by all slices of the same chunk. It keeps source metadata and the
original table-path read-limit semantics. Its `BoundaryKeys` describe physical chunk contents;
they are metadata, not user read limits.

Size getters apply row and data selectivity factors to chunk totals. A slice constructor uses
these getters as its initial estimates. `OverrideSize` marks estimates supplied by a fetcher,
an interruption descriptor, or erasure-part slicing as authoritative for that slice.

`IsCompleteChunk` means that the input read limits do not restrict the chunk. Code that creates
erasure-part slices relies on this invariant and verifies it before assigning part indexes and
row ranges.

## `TInputChunkSlice`

Every `TInputChunkSlice` references exactly one `TInputChunk`. Its lower and upper limits are the
physical restrictions used when constructing the chunk spec for a reader. `SliceIndex` identifies
a slice among slices of the same chunk; `PartIndex` identifies an erasure part.

Use the factory that expresses the available schema information:

- `CreateKeylessInputChunkSlice` copies row-index selectors and verifies that key selectors are
  absent.
- `CreateInputChunkSlice(inputChunk, rowBuffer, comparator)` converts the input chunk read
  limits to key bounds.
- `CreateInputChunkSlice(inputChunk, rowBuffer, protoChunkSpec, comparator)` additionally merges
  limits from an inline chunk spec, as used by interruption reconstruction.
- `CreateInputChunkSlice(inputSlice, comparator, lower, upper)` intersects an existing slice with
  stronger key bounds.
- `CreateInputChunkSliceFromCompleteErasureChunkPart` constructs one explicitly described
  erasure-part slice.

Merging limits always keeps the stronger lower and upper restrictions according to the supplied
comparator. `InferLimitsFromBoundaryKeys(TInputChunkSlice, ...)` intersects a slice with the
physical min/max keys of its chunk. The optional key-column count controls conversion of boundary
keys before they become key bounds; it is not a replacement for the comparator length.

`SliceEvenly` and `SplitByRowIndex` produce physical subdivisions and preserve the enclosing key
bounds. They may tighten row indexes and update size estimates.

## `TDataSlice`

`TDataSlice` is the logical unit consumed and produced by chunk pools. Its own limits describe the
range in the key space understood by the consumer. `ChunkSlices` retain the physical description
needed to build reader chunk specs.

An unversioned data slice contains one chunk slice. A versioned data slice may contain several
chunk slices from the same table. `CreateVersionedInputDataSlice` requires every child to have the
same key bounds because the data slice exposes a single logical range for the group.

Logical and physical bounds are deliberately separate. A pool comparator may cover only a prefix
of the physical table key. `CreateInputDataSlice(dataSlice, comparator, lower, upper)` therefore
intersects only `TDataSlice::LowerLimit` and `TDataSlice::UpperLimit`; it copies child chunk slices
without intersecting their key bounds with the shorter comparator.

`InferLimitsFromBoundaryKeys(TDataSlice, ...)` derives a common logical range from all child chunk
metadata. For a versioned group, infer the data-slice range after the group is assembled. Inferring
each child independently may give different bounds and violates the versioned-slice invariant.

Payload fields have the following contracts:

- `InputStreamIndex` identifies the input stream and selects stream-specific schema information.
- `ReadRangeIndex` is opaque integration state used to recover the original input range.
- `Tag` correlates interrupted output with the originating data slice.
- `IsTeleportable` records that an unversioned slice satisfies teleport preconditions.
- `VirtualRowIndex` preserves ordered-input positioning where physical row indexes are insufficient.

`CopyPayloadFrom` carries `InputStreamIndex`, `ReadRangeIndex`, `Tag`, and `VirtualRowIndex` to
derived slices. `IsTeleportable` is computed by the controller for a particular scheduling path
and is not part of the generic payload copy.

## Serialization

`TInputChunk`, `TInputSliceLimit`, `TInputChunkSlice`, and `TDataSlice` participate in Phoenix
snapshot persistence. Field tags are part of the snapshot format. Renumber or change their
serialized types only together with the snapshot-version policy that governs Controller Agent
revival.

`ToProto(TInputChunkSlice, ...)` creates the reader-facing `NProto::TChunkSpec`. It may omit key
bounds already implied by chunk boundary keys, but the in-memory slice must remain correct before
that pruning step.

## Invariants for Changes

When modifying slice code, preserve all of the following:

1. A chunk slice references one chunk; a data slice owns the logical range for its whole child set.
2. All children of a versioned data slice have equal key bounds and belong to the same table.
3. Physical child bounds are not projected through a shorter pool comparator.
4. Every non-owning key-bound prefix outlives the slice through an appropriate `TRowBuffer`.
5. Lower limits remain lower, upper limits remain upper, and intersections select the stronger
   bound according to the correct comparator.
6. Row-index and key-bound selectors survive independent splitting and merging operations.
7. Keyless factories reject keyed input instead of silently dropping key selectors.
8. Persistent payload fields survive copies and pool-produced subdivisions through
   `CopyPayloadFrom`; scheduling-derived flags are recomputed by their owner.
9. Phoenix field tags remain stable unless the snapshot compatibility boundary changes explicitly.

Relevant focused tests live in `yt/yt/ytlib/unittests/chunk_slice_ut.cpp`; chunk-pool invariants are
covered by `yt/yt/server/lib/chunk_pools/unittests`.
