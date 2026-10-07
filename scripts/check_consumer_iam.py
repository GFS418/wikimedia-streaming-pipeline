"""Prove the wikistream-consumer role is upload + list on its own prefix only (D12).

Two kinds of evidence:
- IAM policy simulator (admin profile) for every action, including the destructive ones.
- Real calls made AS the role, limited to harmless ones (list, read), which must be
  allowed or denied exactly as the simulator says. Nothing is written or deleted.

    uv run python scripts/check_consumer_iam.py
"""

import sys

import boto3
from botocore.exceptions import ClientError

ACCOUNT = boto3.client("sts").get_caller_identity()["Account"]
BUCKET = f"wikistream-landing-{ACCOUNT}"
ROLE = f"arn:aws:iam::{ACCOUNT}:role/wikistream-consumer"
B = f"arn:aws:s3:::{BUCKET}"

# (action, resource, s3:prefix context or None, expected decision)
SIMULATED = [
    ("s3:PutObject", f"{B}/landing/recentchange/p00/x.jsonl.gz", None, "allowed"),
    ("s3:PutObject", f"{B}/landing/_spike/x", None, "implicitDeny"),
    ("s3:PutObject", f"{B}/other/x", None, "implicitDeny"),
    ("s3:GetObject", f"{B}/landing/recentchange/p00/x.jsonl.gz", None, "implicitDeny"),
    ("s3:DeleteObject", f"{B}/landing/recentchange/p00/x.jsonl.gz", None, "implicitDeny"),
    ("s3:DeleteObjectVersion", f"{B}/landing/recentchange/p00/x.jsonl.gz", None, "implicitDeny"),
    ("s3:PutBucketPolicy", B, None, "implicitDeny"),
    ("s3:ListBucket", B, "landing/recentchange/p00/", "allowed"),
    ("s3:ListBucket", B, "landing/", "implicitDeny"),
    ("s3:ListBucket", B, "landing/_spike/", "implicitDeny"),
]


def simulate() -> bool:
    iam = boto3.client("iam")
    ok = True
    for action, resource, prefix, expected in SIMULATED:
        kwargs = {}
        if prefix is not None:
            kwargs["ContextEntries"] = [
                {
                    "ContextKeyName": "s3:prefix",
                    "ContextKeyValues": [prefix],
                    "ContextKeyType": "string",
                }
            ]
        got = iam.simulate_principal_policy(
            PolicySourceArn=ROLE, ActionNames=[action], ResourceArns=[resource], **kwargs
        )["EvaluationResults"][0]["EvalDecision"]
        ok &= got == expected
        where = resource.removeprefix(B) or "(bucket)"
        extra = f" prefix={prefix}" if prefix else ""
        print(f"  {'PASS' if got == expected else 'FAIL'}  {action:24s} {where}{extra}: {got}")
    return ok


def real_calls() -> bool:
    s3 = boto3.Session(profile_name="wikistream-consumer").client("s3")
    checks = [
        (
            "list landing/recentchange/p00/",
            True,
            lambda: s3.list_objects_v2(Bucket=BUCKET, Prefix="landing/recentchange/p00/"),
        ),
        (
            "list landing/_spike/",
            False,
            lambda: s3.list_objects_v2(Bucket=BUCKET, Prefix="landing/_spike/"),
        ),
        (
            "read landing/_spike/recentchange/spike-000.jsonl",
            False,
            lambda: s3.get_object(Bucket=BUCKET, Key="landing/_spike/recentchange/spike-000.jsonl"),
        ),
    ]
    ok = True
    for name, should_work, call in checks:
        try:
            call()
            worked, detail = True, "allowed"
        except ClientError as exc:
            worked, detail = False, exc.response["Error"]["Code"]
        ok &= worked == should_work
        print(f"  {'PASS' if worked == should_work else 'FAIL'}  as role: {name}: {detail}")
    return ok


if __name__ == "__main__":
    print("IAM policy simulator:")
    sim_ok = simulate()
    print("Real calls as wikistream-consumer:")
    real_ok = real_calls()
    sys.exit(0 if sim_ok and real_ok else 1)
