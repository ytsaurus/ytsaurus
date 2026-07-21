# Installing a custom CHYT version

By default, the latest stable CHYT version is automatically installed when you start a clique. However, you can manually pin a different required version — this lets you control updates and avoid compatibility and stability issues.

In this section, you’ll find information on when it’s important to specify a version manually, where to find the list of available versions, and how to specify a custom version.

## When to specify a version manually { #manual }

Sometimes, automatic installation of the latest stable version isn’t suitable, so you need to select a CHYT version yourself. Below are possible reasons for such situations:

1. Compatibility. Some integrations or services work only with a specific CHYT version. If you don’t pin the version, they might not work correctly after an update (for example, if the JSON processing format changes).
1. Update policies. In organizations, software is often updated only after review (audit) and during pre‑agreed periods (outside peak loads).
1. Testing. You can create a separate clique with a new CHYT version to test functionality without risking production workflows, or compare results with the old version without affecting service operations.

## CHYT version format { #schema }

A CHYT version has the following format:
`ytserver-clickhouse-2.17.16028654~22a615876d+sb`,
where:

- `2.17` — the base version;
- `16028654~22a615876d+sb` — the update identifier.

## Where to find available CHYT versions { #versions }

All available CHYT versions are stored in binary files in [Cypress](../../../../../overview/about.md#cypress) at the path `//sys/bin/ytserver-clickhouse`.

In addition to binary files with CHYT builds, the directory contains symlinks (files that store references to binary files) for the *latest stable* and *latest base* versions.

## Ways to specify CHYT versions { #types }

The update behaviour depends on the option you choose. There are three approaches:

1. Strict version specification — for example, `ytserver-clickhouse-2.17.16028654~22a615876d+sb` — completely disables updates. This is useful when you need absolute stability and predictable system behaviour.
1. Loose version specification — for example, just the base part `ytserver-clickhouse-2.17` — means the system will update the version within the `2.17` range. This is suitable if minor fixes are acceptable but major changes should be avoided.
1. If no version is specified, the system installs the latest stable CHYT version when you start the clique. This approach is convenient for test environments or systems where stability isn’t critical.

{% note warning %}

   Updating to a higher stable version only happens when you restart the clique. This applies to both cliques without a specified version and those with a base version specified.

   Example:
   A new clique is started without specifying an exact version. The system automatically installs the latest stable CHYT version `2.17.some_id`. The clique runs continuously: when a new update `2.17.some_new_id` or a new base CHYT version `2.18.yet_another_id` is released, no automatic update occurs.
   Only after restarting the clique will the version automatically update to the new stable version `2.17.some_new_id` or `2.18.yet_another_id`.

{% endnote %}

## How to install a custom version { #instruction }

1. Go to Cypress at the path `//sys/bin/ytserver-clickhouse`.
1. In the list of available versions, select the required CHYT version or symlink.
1. Copy the version name (for example, `ytserver-clickhouse-2.17`).
1. Open the clique interface as described in the section [How to open the clique interface](../../../../../user-guide/data-processing/chyt/cliques/ui.md#where).
1. Click ![edit speclet](../../../../../../images/edit-btn.png){width=24 height=24} **Edit speclet** in the upper-right corner of the [Action buttons](../../../../../user-guide/data-processing/chyt/cliques/ui.md#action-menu) block, or the button **Edit speclet** on the **Speclet** tab in the [Tabs panel](../../../../../user-guide/data-processing/chyt/cliques/ui.md#tabs).
1. On the left, select the **Advanced** section.
1. In the *Chyt version* field, paste the copied version name (for example, `ytserver-clickhouse-2.17`).

Useful links:

[Clique web interface in {{product-name}}](../../../../../user-guide/data-processing/chyt/cliques/ui.md)  
[Getting CHYT and ClickHouse versions](../../../../../user-guide/data-processing/chyt/how-to-guides/versions.md)
