### SPYT Update Guide

This document outlines how to update SPYT using the Kubernetes operator.

#### Important Information Before Updating

During a SPYT update:

* The {{product-name}} cluster continues operating normally.
* {{product-name}} server components aren’t restarted.
* Data access via the {{product-name}} API remains available.
* The update installs the new version alongside the current one without replacing it ([Multiple SPYT Versions](#multiple-versions)).

**Before updating:**

1. **Verify compatibility** between SPYT, Spark, Java, Scala, and Python versions.  
   {% if audience == "public" %}  
   Refer to the [compatibility table](../../../user-guide/data-processing/spyt/overview.md#spyt-compatibility).  
   {% endif %}

2. **Review available SPYT versions and their changes.**  
   {% if audience == "public" %}  
   See the [SPYT releases](../../../admin-guide/releases.md#spyt).  
   {% endif %}

---

### Preparation for the Update

#### 1. Check the Current SPYT Version

Retrieve the current SPYT version from the specification:

```bash
kubectl get spyt -n <namespace> -o yaml | grep image
```

**Example output:**
```yaml
image: ghcr.io/ytsaurus/spyt:2.8.0
```

#### 2. Check SPYT Status

Verify that SPYT is in the `Finished` state:

```bash
kubectl get spyt -n <namespace>
```

**Example output:**
```
NAME            STATUS
<spyt-name>     Finished
```

If SPYT isn’t in the `Finished` state, wait for ongoing operations to complete.

#### 3. Save a Backup of the SPYT Specification

Save a backup of the current SPYT specification:

```bash
kubectl get spyt -n <namespace> -o yaml > spyt-backup.yaml
```

---

### Updating SPYT

The operator doesn’t modify existing SPYT resources. Instead, it creates a new version as a separate resource. Both versions coexist on the cluster — their artifacts are stored side by side in Cypress. You can delete the old version later.

#### 1. Prepare the Specification for the New Version

Use the current SPYT specification as a basis:

```bash
kubectl get spyt <spyt-name> -n <namespace> -o yaml
```

Create a `spyt-<new-version>.yaml` file with:

* A new resource name in the `metadata.name` field.
* A new image version in the `image` field.

**Example specification for version 2.9.0:**

```yaml
apiVersion: cluster.ytsaurus.tech/v1
kind: Spyt
metadata:
  name: spyt-2-9-0
  namespace: <namespace>
spec:
  image: ghcr.io/ytsaurus/spyt:2.9.0
  ytsaurus:
    name: <ytsaurus-name>
```

#### 2. Apply the New Version

Apply the new version specification:

```bash
kubectl apply -f spyt-<new-version>.yaml -n <namespace>
```

The operator creates a new SPYT resource and uploads its artifacts to Cypress. The existing resource remains unchanged.

#### 3. Track the Update Progress

Monitor the SPYT status during the update:

```bash
kubectl get spyt -n <namespace> -w
```

**Update States:**

| Status | Description |
|--------|-------------|
| `CreatingUser` | The operator creates a user in Cypress. |
| `UploadingIntoCypress` | The operator uploads files to Cypress. |
| `Finished` | The update is complete. |

#### 4. Verify the Update

1. Confirm that both resources are in the `Finished` state:

   ```bash
   kubectl get spyt -n <namespace>
   ```

   **Example output:**
   ```
   NAME             STATUS
   <spyt-name>      Finished
   <new-spyt-name>  Finished
   ```

2. Check the new image version:

   ```bash
   kubectl get spyt -n <namespace> -o yaml | grep image
   ```

   **Example output:**
   ```yaml
   image: ghcr.io/ytsaurus/spyt:2.9.0
   ```

---

### Multiple SPYT Versions on a Single Cluster {#multiple-versions}

The update process adds a new SPYT version as a separate resource, allowing multiple versions to coexist on the same {{product-name}} cluster. Different teams can use different versions simultaneously.

SPYT and Spark artifacts are stored in Cypress at the path `//home/spark/`. The operator doesn’t delete old artifacts, so versions don’t interfere with each other.

---

### Removing an Old SPYT Version {#remove-old-version}

When the old SPYT version is no longer needed:

1. **Delete the SPYT resource:**

   ```bash
   kubectl delete spyt <old-spyt-name> -n <namespace>
   ```

2. **Remove old artifacts from Cypress (if necessary):**

   * Open the `//home/spark/spyt/releases` and `//home/spark/conf/releases` directories.
   * Locate nodes with the unwanted version number.
   * Delete them from the context menu
   