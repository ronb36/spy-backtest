"""
prune_dm.py — Remove deep OTM contracts from spy_data.json

Deletes any contract where otm_pct > OTM_MAX from the DM, then
commits the pruned file back to GitHub using the Git Data API.

Run once before deploying data_collector.py v1.2.5 to bring the
DM file size back under GitHub's 50MB blob limit.

Usage: python prune_dm.py
Requires: GITHUB_TOKEN, GITHUB_REPO env vars (same as collector)
"""

import os
import json
import base64
import requests
from datetime import datetime

GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
GITHUB_REPO  = os.environ.get("GITHUB_REPO", "ronb36/spy-backtest")
DATA_PATH    = "data/spy_data.json"
OTM_MAX      = 0.15   # Remove anything above 15% OTM

def log(msg):
    print(f"{datetime.now().strftime('%H:%M:%S')} — {msg}", flush=True)

def github_get_blobs(path):
    """Fetch large file via Blobs API."""
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }
    base = f"https://api.github.com/repos/{GITHUB_REPO}"
    r = requests.get(f"{base}/contents/{path}", headers=headers, timeout=30)
    data = r.json()
    raw_b64 = data.get("content", "").replace("\n", "")
    if not raw_b64:
        sha = data.get("sha", "")
        r2 = requests.get(f"{base}/git/blobs/{sha}", headers=headers, timeout=120)
        raw_b64 = r2.json().get("content", "").replace("\n", "")
    return json.loads(base64.b64decode(raw_b64).decode("utf-8"))

def github_commit_file(path, content_dict, message):
    """Commit using Git Data API — no file size limit."""
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
        "Content-Type": "application/json",
    }
    base = f"https://api.github.com/repos/{GITHUB_REPO}"
    content_bytes = json.dumps(content_dict, indent=2).encode("utf-8")
    content_b64   = base64.b64encode(content_bytes).decode("utf-8")

    # 1. Create blob
    r = requests.post(f"{base}/git/blobs",
        headers=headers,
        json={"content": content_b64, "encoding": "base64"},
        timeout=120)
    if r.status_code not in (200, 201):
        log(f"✗ Blob creation failed: {r.status_code} {r.text[:200]}")
        return False
    blob_sha = r.json()["sha"]
    size_mb = len(content_bytes) / 1024 / 1024
    log(f"✓ Blob created ({size_mb:.1f} MB)")

    # 2. Get HEAD
    r = requests.get(f"{base}/git/ref/heads/main", headers=headers, timeout=30)
    head_commit_sha = r.json()["object"]["sha"]
    r = requests.get(f"{base}/git/commits/{head_commit_sha}", headers=headers, timeout=30)
    base_tree_sha = r.json()["tree"]["sha"]

    # 3. Create tree
    r = requests.post(f"{base}/git/trees",
        headers=headers,
        json={"base_tree": base_tree_sha,
              "tree": [{"path": path, "mode": "100644",
                        "type": "blob", "sha": blob_sha}]},
        timeout=60)
    new_tree_sha = r.json()["sha"]

    # 4. Create commit
    r = requests.post(f"{base}/git/commits",
        headers=headers,
        json={"message": message, "tree": new_tree_sha,
              "parents": [head_commit_sha]},
        timeout=30)
    new_commit_sha = r.json()["sha"]

    # 5. Update ref
    r = requests.patch(f"{base}/git/refs/heads/main",
        headers=headers,
        json={"sha": new_commit_sha},
        timeout=30)
    if r.status_code not in (200, 201):
        log(f"✗ Ref update failed: {r.status_code} {r.text[:200]}")
        return False

    log(f"✓ Committed {path} ({new_commit_sha[:7]})")
    return True


def main():
    log(f"Loading DM from GitHub...")
    dm = github_get_blobs(DATA_PATH)
    before = len(dm["options"])
    log(f"Loaded — {before} contracts")

    # Prune contracts above OTM_MAX
    pruned = {
        ticker: contract
        for ticker, contract in dm["options"].items()
        if abs(contract.get("otm_pct", 0)) <= OTM_MAX
    }
    after = len(pruned)
    removed = before - after
    log(f"Pruned {removed} contracts > {OTM_MAX*100:.0f}% OTM — {after} remaining")

    dm["options"] = pruned
    dm["metadata"]["options_count"] = after
    dm["metadata"]["last_updated"] = datetime.now().strftime("%Y-%m-%d")

    # Estimate file size
    size_mb = len(json.dumps(dm, indent=2).encode("utf-8")) / 1024 / 1024
    log(f"Estimated file size: {size_mb:.1f} MB")

    if size_mb > 48:
        log(f"⚠ Still above 48MB threshold — consider lowering OTM_MAX further")
    
    msg = f"Prune DM: remove >{OTM_MAX*100:.0f}% OTM contracts ({removed} removed, {after} remaining)"
    log(f"Committing pruned DM...")
    github_commit_file(DATA_PATH, dm, msg)
    log("Done.")


if __name__ == "__main__":
    main()
