package tech.ytsaurus.flow.service;

/**
 * Canonical binary YSON of the specs the current instance was initialized from.
 *
 * <p>Both sides of every comparison are produced by the same serialization of the parsed
 * argument, and within one incarnation the worker serializes the same spec objects, so byte
 * equality is exact.
 */
record AppliedSpecs(byte[] spec, byte[] dynamicSpec, byte[] resourceRevision) {
}
