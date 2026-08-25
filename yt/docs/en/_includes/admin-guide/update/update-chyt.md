### CHYT Update Guide

This document outlines how to update CHYT using the Kubernetes operator.

#### Important Information Before Updating

During a CHYT update:

* The {{product-name}} cluster continues operating normally.
* {{product-name}} server components aren’t restarted.
* Data access via the {{product-name}} API remains available.
* CHYT clusters are temporarily unavailable.
* The update installs the new version alongside the current one without replacing it ([Multiple CHYT Versions](#multiple-versions)).
* Cliques automatically use the latest created resource with `makeDefault: true` as the default version.

**Before updating:**

1. **Verify compatibility** between CHYT, YTsaurus, and Strawberry versions.

   {% if audience == "public" %}  

   Refer to the [compatibility table](../../../admin-guide/compatibility.md). 
    
   {% endif %}

2. **Ensure the `strawberry controller` is installed and configured** — CHYT depends on it.  

   {% note warning "Important" %}  

   Confirm that the `strawberry controller` is properly set up.  

   {% endnote %}

3. **Review available CHYT versions and their changes.**  

   {% if audience == "public" %}  

   See the [CHYT releases](../../../admin-guide/releases.md#chyt).  

   {% endif %}

---

### Preparation for the Update

#### 1. Check the Current CHYT Version

Use the current CHYT specification to retrieve the version:

```bash
kubectl get chyt -n <namespace> -o yaml | grep image
```

**Example output:**
```yaml
image: ghcr.io/ytsaurus/chyt:2.17.3
```

#### 2. Check CHYT Status

Verify that CHYT is in the `Finished` state:

```bash
kubectl get chyt -n <namespace>
```

**Example output:**
```
NAME         STATUS
<chyt-name>  Finished
```

If CHYT isn’t in the `Finished` state, wait for ongoing operations to complete.

#### 3. Save a Backup of the CHYT Specification

Save a backup of the current CHYT specification:

```bash
kubectl get chyt -n <namespace> -o yaml > chyt-backup.yaml
```

---

### Updating CHYT

The operator doesn’t modify existing CHYT resources. Instead, it creates a new version as a separate resource.

#### 1. Prepare the Specification for the New Version

Use the current CHYT specification as a basis:

```bash
kubectl get chyt <chyt-name> -n <namespace> -o yaml
```

Create a `chyt-<new-version>.yaml` file with:

* A new resource name in the `metadata.name` field.
* A new image version in the `image` field.
* `makeDefault: true` to set the new version as default for cliques.

**Example specification for version 2.18.0:**

```yaml
apiVersion: cluster.ytsaurus.tech/v1
kind: Chyt
metadata:
  name: chyt-2-18-0
  namespace: <namespace>
spec:
  image: ghcr.io/ytsaurus/chyt:2.18.0
  makeDefault: true
  ytsaurus:
    name: <ytsaurus-name>
```

#### 2. Apply the New Version

Apply the new version specification:

```bash
kubectl apply -f chyt-<new-version>.yaml -n <namespace>
```

The operator creates a new CHYT resource and uploads its artifacts to Cypress. The existing resource remains unchanged.

#### 3. Track the Update Progress

Monitor the CHYT status during the update:

```bash
kubectl get chyt -n <namespace> -w
```

**Update States:**

| Status | Description |
|--------|-------------|
| `CreatingUser` | The operator creates a user in Cypress. |
| `UploadingIntoCypress` | The operator uploads files to Cypress. |
| `CreatingChPublicClique` | The operator creates a public clique. |
| `Finished` | The update is complete. |

#### 4. Verify the Update

1. Confirm that both resources are in the `Finished` state:

   ```bash
   kubectl get chyt -n <namespace>
   ```

   **Example output:**
   ```
   NAME             STATUS
   <chyt-name>      Finished
   <new-chyt-name>  Finished
   ```

2. Check the new image version:

   ```bash
   kubectl get chyt -n <namespace> -o yaml | grep image
   ```

   **Example output:**
   ```yaml
   image: ghcr.io/ytsaurus/chyt:2.18.0
   ```

---

### Multiple CHYT Versions on a Single Cluster {#multiple-versions}

The update process adds a new CHYT version as a separate resource, allowing multiple versions to coexist on the same {{product-name}} cluster.

* Each version has its own image.
* Cliques use the latest resource with `makeDefault: true` as the default.
* You don’t need to modify the previous resource — it automatically loses default status.

---

### Removing an Old CHYT Version {#remove-old-version}

When the old CHYT version is no longer needed:

1. **Delete the CHYT resource:**

   ```bash
   kubectl delete chyt <old-chyt-name> -n <namespace>
   ```

2. **Remove old artifacts from Cypress (if necessary):**

   * Open the `//sys/bin/ytserver-clickhouse` and `//sys/bin/clickhouse-trampoline` directories.
   * Locate old version files by creation date.
   * Delete them from the context menu.
   