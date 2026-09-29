# Phase 0 spike: Databricks Free Edition reading our S3 bucket

**Date:** 2026-09-28. **Workspace:** Databricks Free Edition, AWS us-east-2
(`metastore_aws_us_east_2`). **Bucket:** `wikistream-landing-060647991548` (us-east-2),
Terraform in `infra/`. **Notebook:** [`databricks/spike_autoloader.py`](../../databricks/spike_autoloader.py).

**Verdict: Free Edition works for Option A.** No paid trial needed.

## Results

| Check | Result |
|---|---|
| Create a UC storage credential on our own IAM role | PASS. Free Edition allows it (docs were silent) |
| Validate credential on `landing/` (read-only) | PASS on all 6 checks, once the prefix held an object (an empty prefix fails LIST / PATH_EXISTS) |
| Create read-only external location `wikistream-landing` | PASS |
| IAM least privilege (policy simulator on the role) | GetObject allowed only on `landing/*`; Put/Delete denied everywhere; ListBucket allowed only for prefix `landing/` |
| Run 1: Auto Loader, `Trigger.AvailableNow`, serverless job | 500 lines in, 500 rows, 500 distinct `meta.id`, 0 rescued rows, 29 inferred top-level columns; 87 s wall time (cold) |
| Run 2: same checkpoint, no new files | 0 rows added; 25 s |
| Run 3: same key rewritten with identical bytes (new S3 version, same ETag `3a33a670...`) | 0 rows added; 26 s |

Test input: 500 real `recentchange` events captured from the live feed (all four types:
categorize 250, edit 148, log 86, new 16).

## What run 3 means for the consumer design

Auto Loader (default `cloudFiles.allowOverwrites = false`) skips a **path** it has already
processed, whatever the new content. So:

- A byte-identical rewrite after a crash is harmless: skipped, no duplicates. This is the
  behavior the consumer relies on.
- A rewrite with **different** content under the same key would be **silently ignored**,
  which is data loss. Deterministic file contents per offset range are therefore a
  correctness requirement, not a nicety.
- Turning on `allowOverwrites` would turn that loss into duplicates instead. Neither is
  acceptable; the consumer must guarantee determinism, and the kill drill checks it by
  comparing ETags across S3 versions.

## Not managed by Terraform (yet)

Created with the Databricks CLI: storage credential `wikistream-landing`, external location
`wikistream-landing`, schema `workspace.wikistream_spike` (table `bronze_spike`, volume
`state`). Whether to manage these with the Databricks Terraform provider is a Phase 5 decision.
