# CHYT

Push a `Chyt` resource to K8s to install CHYT on the {{product-name}} cluster.

{% note info "Important" %}

The `strawberry controller` must be created on the cluster. You can configure it in the `Ytsaurus` resource's `strawberry` field.

{% endnote %}

You can find an example of the specification [here](https://github.com/ytsaurus/yt-k8s-operator/blob/main/config/samples/cluster_v1_chyt.yaml):
```yaml
apiVersion: cluster.ytsaurus.tech/v1
kind: Chyt
metadata:
  name: mychyt
spec:
  ytsaurus:
    name:
      minisaurus
  image: ytsaurus/chyt:2.10.0-relwithdebinfo
  makeDefault: true
```

You can start the specification using `kubectl`:

```bash
$ kubectl apply -f cluster_v1_chyt.yaml -n <namespace>
chyt.cluster.ytsaurus.tech/mychyt created
```

After that, the K8s operator will launch several init jobs that will write the files to Cypress.

```bash
$ kubectl get chyt
NAME     RELEASESTATUS
mychyt   CreatingUser

$ kubectl get chyt
NAME     RELEASESTATUS
mychyt   UploadingIntoCypress

$ kubectl get chyt
NAME     RELEASESTATUS
mychyt   CreatingChPublicClique

$ kubectl get chyt
NAME     RELEASESTATUS
mychyt   Finished
```

Once all jobs are successfully completed, you can run the `CHYT` clique. For more information about cliques, see [this section](../../user-guide/data-processing/chyt/cliques/start).

If the `makeDefault` flag has been set in the `Chyt` specification, and the `strawberry controller` is running on the cluster, the default clique `ch_public` will also be created.

