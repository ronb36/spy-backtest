"""
proxy.py — Railway web server for spy_data.json (Git LFS version)
Serves the LFS-tracked data mart to the backtester at ronb36.github.io.

The backtester can't call the LFS Batch API directly from the browser due to
CORS restrictions. This proxy calls it server-side and returns the JSON.

Railway env vars required:
  GH_PAT   — GitHub personal access token (read:repo scope is enough)

Optional (defaults shown):
  GH_OWNER     — ronb36
  GH_REPO      — spy-backtest
  GH_FILE_PATH — data/spy_data.json
  PORT         — set automatically by Railway, do not set manually
"""

import os
import logging
import requests
from flask import Flask, Response, jsonify
from flask_cors import CORS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)  # Allow all origins — backtester is on GitHub Pages

GH_PAT       = os.environ["GH_PAT"]
GH_OWNER     = os.environ.get("GH_OWNER", "ronb36")
GH_REPO      = os.environ.get("GH_REPO", "spy-backtest")
GH_FILE_PATH = os.environ.get("GH_FILE_PATH", "data/spy_data.json")

PROXY_VERSION = "1.0.0"


def fetch_via_lfs() -> bytes:
    """
    Retrieve spy_data.json from GitHub LFS using the two-step Batch API flow:
      1. POST to LFS Batch API to get a signed download URL
      2. GET that URL to download the actual file bytes
    """
    # Step 1 — ask LFS where the file lives
    batch_url = f"https://github.com/{GH_OWNER}/{GH_REPO}.git/info/lfs/objects/batch"
    headers = {
        "Authorization": f"token {GH_PAT}",
        "Accept": "application/vnd.git-lfs+json",
        "Content-Type": "application/vnd.git-lfs+json",
    }
    # The OID and size aren't strictly required for a download batch request —
    # GitHub will look them up from the pointer file in the repo.
    # We pass the path via the ref so GitHub knows which commit to look at.
    payload = {
        "operation": "download",
        "transfers": ["basic"],
        "ref": {"name": "refs/heads/main"},
        "objects": [{"oid": GH_FILE_PATH, "size": 0}],  # oid=path triggers pointer lookup
    }

    log.info(f"proxy v{PROXY_VERSION} — LFS batch request for {GH_FILE_PATH}")
    r = requests.post(batch_url, json=payload, headers=headers, timeout=30)

    if r.status_code != 200:
        # Fallback: look up the real OID from the pointer file in the repo
        log.info("Batch with path OID failed, fetching pointer file for real OID...")
        oid, size = get_lfs_oid_from_pointer()
        payload["objects"] = [{"oid": oid, "size": size}]
        r = requests.post(batch_url, json=payload, headers=headers, timeout=30)
        r.raise_for_status()

    batch = r.json()
    objects = batch.get("objects", [])
    if not objects:
        raise ValueError("LFS batch response contained no objects")

    obj = objects[0]
    error = obj.get("error")
    if error:
        raise ValueError(f"LFS object error: {error.get('message', error)}")

    download_url = obj["actions"]["download"]["href"]
    download_headers = obj["actions"]["download"].get("header", {})

    # Step 2 — download the actual file from LFS storage
    log.info(f"Downloading from LFS storage ({obj.get('size', '?')} bytes)...")
    dl = requests.get(download_url, headers=download_headers, timeout=120)
    dl.raise_for_status()

    log.info(f"Downloaded {len(dl.content):,} bytes")
    return dl.content


def get_lfs_oid_from_pointer() -> tuple:
    """
    Read the LFS pointer file from the Git tree to extract the real OID and size.
    The pointer is a small text file stored in the normal Git tree; it looks like:
      version https://git-lfs.github.com/spec/v1
      oid sha256:<hex>
      size <bytes>
    """
    url = f"https://api.github.com/repos/{GH_OWNER}/{GH_REPO}/contents/{GH_FILE_PATH}"
    headers = {
        "Authorization": f"token {GH_PAT}",
        "Accept": "application/vnd.github.v3.raw",  # raw returns pointer text for LFS files
    }
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()

    pointer_text = r.text
    oid = None
    size = 0
    for line in pointer_text.splitlines():
        if line.startswith("oid sha256:"):
            oid = line.split("oid sha256:")[1].strip()
        elif line.startswith("size "):
            size = int(line.split("size ")[1].strip())

    if not oid:
        raise ValueError(f"Could not parse LFS pointer — got: {pointer_text[:200]}")

    log.info(f"LFS pointer: oid={oid[:16]}... size={size:,}")
    return oid, size


@app.route("/dm", methods=["GET"])
def serve_dm():
    """Fetch spy_data.json from LFS and return it to the backtester."""
    try:
        data = fetch_via_lfs()
        return Response(data, status=200, mimetype="application/json")
    except Exception as e:
        log.error(f"Error serving DM: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "version": PROXY_VERSION, "file": GH_FILE_PATH}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    log.info(f"proxy v{PROXY_VERSION} starting on port {port}")
    app.run(host="0.0.0.0", port=port)
