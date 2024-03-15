# SPYT

To install SPYT on the {{product-name}} cluster, push a `SPYT` resource to K8s.

You can find an example of the specification [here](https://github.com/ytsaurus/yt-k8s-operator/blob/main/config/samples/cluster_v1_spyt.yaml):
```yaml
apiVersion: cluster.ytsaurus.tech/v1
kind: Spyt
metadata:
  name: myspyt
spec:
  ytsaurus:
    name:
      minisaurus
  image: ytsaurus/spyt:1.76.1
```

You can start the specification using `kubectl`:

```bash
$ kubectl apply -f cluster_v1_spyt.yaml -n <namespace>
spyt.cluster.ytsaurus.tech/myspyt created
```

After that, the K8s operator will launch several init jobs that will write the files to Cypress. To monitor the status, use `kubectl`:

```bash
$ kubectl get spyt
NAME     RELEASESTATUS
myspyt   CreatingUser

$ kubectl get spyt
NAME     RELEASESTATUS
myspyt   UploadingIntoCypress

$ kubectl get spyt
NAME     RELEASESTATUS
myspyt   Finished
```

Once all jobs are successfully completed (when `RELEASESTATUS` changes to `Finished`), you can run `SPYT`. For more information, see [this section](../../user-guide/data-processing/spyt/launch).
