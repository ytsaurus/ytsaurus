# FAQ for {{product-name}} Flow

#### **Q: Can you update the pipeline binary files without stopping or pausing?** {#deploy-without-drain}

**A:** The short answer is no. There are many compatibility nuances between the old and new binaries at the user code level, and the Flow team isn’t ready to take responsibility for saying that this is safe.

When you roll out a new binary with a stop but without [drain](../../flow/concepts/glossary.md#start-stop-pause-pipeline):
- Intermediate messages from the old binary may remain on {{product-name}}, and the new binary might not be ready to handle them (for example, if some invariant has changed).
- You might get duplicate events or lose events if the logic for generating output messages in [Swift](../../flow/concepts/glossary.md#swift) computations changes.
- The diff between the two binaries might include commits from Flow itself. These changes might be unsafe to roll out without draining.

You can still update binary files without draining in these cases:
- In a dev environment that you don’t mind completely breaking. If it breaks (or you notice anything odd), you can just redeploy it from scratch.

The Flow team reserves the right **not** to help resolve any pipeline issues that stem from such unsafe operations in the pipeline’s history.

#### **Q: My pipeline is in the Completed state—how did this happen?** {#unexpectedly-completed-pipeline}

**A:** The most common situation is launching a new pipeline before setting the spec. A less common situation is when the pipeline is launched with all finite sources (`finite=true` in the spec), as in tests, and the sources have been processed.

In both cases, you can take the pipeline out of the Completed state only by fully recreating it. That means you need to stop the controllers and workers, delete the pipeline object on {{product-name}}, and then create and launch everything again.

#### **Q: How do you run Flow in an IPv4-only environment?** {#ipv4-support}

**A:** By default, Flow uses IPv6 for inter-node communication (Controller ↔ Worker, Worker ↔ Worker). To run in an IPv4-only environment, you need to configure the `address_resolver` section:

```yson
{
    cluster_url = "your-cluster";
    path = "//home/your-pipeline";

    address_resolver = {
        enable_ipv4 = %true;
        enable_ipv6 = %false;
    };

    # ... other config parameters
}
```

{% note warning %}

Exactly one protocol must be enabled: either `enable_ipv4 = %true` or `enable_ipv6 = %true`. Enabling both or disabling both will cause an error when the node starts.

{% endnote %}

##### Important details {#ipv4-details}

The `address_resolver` setting is a global singleton and affects **all** DNS resolutions in the process, including:

- Resolving addresses of other Flow nodes (Controller, Worker).
- Resolving addresses of the {{product-name}} RPC proxy.
- Any other DNS queries within the process.

This means that with `enable_ipv6 = %false`, the DNS resolver will request only A records (IPv4). Make sure that all hosts the pipeline interacts with (including the {{product-name}} RPC proxy) have A records in DNS.