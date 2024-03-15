# Possible problems

## LVC (Lost Vital Chunks) { #lvc }

If some chunks become unavailable, the alert `lost_vital_chunks` is triggered. That can happen, for example, if some `Data Nodes` are unavailable.

To see the list of unavailable chunks, use `yt list //sys/lost_vital_chunks`.

To figure out where the replicas of a chunk were, use `yt get #chunk-id/@last_seen_replicas`.

For erasure chunks, you could look at `yt get #chunk_id/@stored_replicas` to find out which parts of the chunk are still available.

## Tablet Cell Bundle's health is not good { #tabletcellbundles }

The UI on the Bundles page shows bundle states. If a bundle isn't in the `Good` state, you need to find out why.

{% note warning "Remark" %}

If there are bundles that aren't in the `Good` state, you can't start updating the cluster.

{% endnote %}

