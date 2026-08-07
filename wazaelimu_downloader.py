#!/usr/bin/env python3
"""
wazaelimu_downloader.py

A Python tool to download all Wazaelimu exams and books and push them to GitHub.

Site: https://wazaelimu.com

The site is a WordPress installation with content served via the REST API
at https://wazaelimu.com/wp-json/wp/v2/.  Exam posts and book posts contain
download links that are either:

  * Google Drive  – ``drive.google.com/file/d/<ID>/view`` (primary source)
  * Google Docs   – ``docs.google.com/document/d/<ID>/view``
  * Direct PDF    – any URL ending in ``.pdf``
  * belacash.net  – an intermediary that uses anti-bot / anonymous-proxy
                    detection; these are recorded but not downloadable.

Usage
-----
    python wazaelimu_downloader.py --download          # download everything
    python wazaelimu_downloader.py --download --limit 5  # dry-run with limits
    python wazaelimu_downloader.py --push              # push existing files to GitHub
    python wazaelimu_downloader.py --download --push   # do both
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, parse_qs, unquote

import requests
from bs4 import BeautifulSoup

# ──────────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────────

BASE_URL = "https://wazaelimu.com"
WP_API = f"{BASE_URL}/wp-json/wp/v2"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

# Categories we care about (discovered via the WordPress API).
# Each tuple: (category_id, directory_name)
TARGET_CATEGORIES = [
    # Past papers – secondary
    (171, "past-papers/all"),          # Past Papers (parent, 1869 posts)
    (922, "past-papers/form-1"),       # Past Papers F1
    (375, "past-papers/form-2"),       # Past Papers F2
    (923, "past-papers/form-3"),       # Past Papers F3
    (374, "past-papers/form-4"),       # Past Papers F4
    (924, "past-papers/form-5"),       # Past Papers F5
    (376, "past-papers/form-6"),       # Past Papers F6
    # Past papers – primary
    (357, "past-papers/primary"),      # Past Papers Primary
    (367, "past-papers/std-1-4"),      # Past Papers STD 1-4
    (387, "past-papers/std-5-7"),      # Past Papers STD 5-7
    # Other exam-related
    (478, "past-papers/ecz"),          # ECZ Past Papers
    (513, "past-papers/kcse"),         # KCSE Past Papers
    (14, "results/matokeo-necta"),      # MATOKEO NECTA (results)
    (382, "past-papers/necta"),        # Past Paper NECTA
    # Notes
    (25, "notes/form-i"),              # FORM I NOTES
    (26, "notes/form-ii"),             # FORM II NOTES
    (27, "notes/form-iii"),            # FORM III NOTES
    (28, "notes/form-iv"),             # FORM IV NOTES
    (29, "notes/form-v"),              # FORM V NOTES
    (30, "notes/form-vi"),             # FORM VI NOTES
    (175, "notes/pre-primary"),        # PRE AND PRIMARY LEVEL NOTES
    (151, "notes/secondary"),          # SECONDARY SCHOOL NOTES
    (115, "notes/basic-math"),         # BASIC MATHEMATICS
    # TIE Books
    (369, "books/tie-books"),          # Tie Books (parent)
    (660, "books/tie-books-std-1"),    # Tie Books STD 1
    (661, "books/tie-books-std-2"),    # Tie Books STD 2
    (662, "books/tie-books-std-3"),    # Tie Books STD 3
    (659, "books/tie-books-std-4"),    # Tie Books STD 4
    (663, "books/tie-books-std-5"),    # Tie Books STD 5
    (664, "books/tie-books-std-6"),    # Tie Books STD 6
    (665, "books/tie-books-std-7"),    # Tie Books STD 7
    (671, "books/tie-books-pre-primary"),# Tie Books Pre-Primary
    (666, "books/tie-books-f2"),       # Tie Books F2
    (667, "books/tie-books-f3"),       # Tie Books F3
    (668, "books/tie-books-f4"),       # Tie Books F4
    (669, "books/tie-books-f5"),       # Tie Books F5
    (670, "books/tie-books-f6"),       # Tie Books F6
    # Book analyses
    (8, "books/analysis"),             # BOOKS ANALYSIS
    (1091, "books/novels-analysis"),   # NOVELS ANALYSIS
    (1113, "books/plays-analysis"),    # PLAYS ANALYSIS
    # ZIMSEC / other
    (1280, "books/zimsec"),            # ZIMSEC Green Books
]


# ──────────────────────────────────────────────────────────────────────────────
# Data structures
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class DownloadLink:
    """A single download link extracted from a post."""
    url: str
    link_text: str
    file_id: Optional[str] = None   # Google Drive file ID
    file_type: str = "unknown"       # 'gdrive', 'gdocs', 'pdf', 'belacash'
    filename: Optional[str] = None  # suggested filename


@dataclass
class PostInfo:
    """Metadata about a WordPress post we're processing."""
    id: int
    title: str
    url: str
    category: str
    download_links: list[DownloadLink] = field(default_factory=list)


# ──────────────────────────────────────────────────────────────────────────────
# WordPress API helpers
# ──────────────────────────────────────────────────────────────────────────────


def wp_get(endpoint: str, params: dict | None = None, max_retries: int = 3) -> Optional[requests.Response]:
    """GET a WordPress REST API endpoint with retries."""
    url = f"{WP_API}{endpoint}"
    merged = {"per_page": 100, "orderby": "date", "order": "desc"}
    if params:
        merged.update(params)
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=merged, headers=HEADERS, timeout=30)
            if resp.status_code == 200:
                return resp
            if resp.status_code == 400 and attempt < max_retries - 1:
                # Bad request (e.g., invalid category) – stop retrying
                return None
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                continue
            print(f"  [WARN] {url} returned {resp.status_code}")
            return None
        except requests.RequestException as exc:
            if attempt < max_retries - 1:
                print(f"  [RETRY] network error: {exc}")
                time.sleep(2 ** attempt)
                continue
            print(f"  [ERROR] {url}: {exc}")
            return None
    return None


def fetch_posts_by_category(cat_id: int, limit: int | None = None) -> list[dict]:
    """Fetch all posts in a given WordPress category, handling pagination."""
    posts: list[dict] = []
    page = 1
    while True:
        resp = wp_get("/posts", {"categories": cat_id, "page": page})
        if resp is None:
            break
        try:
            data = resp.json()
        except json.JSONDecodeError:
            print(f"  [WARN] Invalid JSON on page {page}")
            break
        if not data:
            break
        posts.extend(data)
        if limit and len(posts) >= limit:
            return posts[:limit]
        page += 1
        time.sleep(0.2)  # be kind to the server
    return posts


# ──────────────────────────────────────────────────────────────────────────────
# Link extraction
# ──────────────────────────────────────────────────────────────────────────────

GDRIVE_FILE_RE = re.compile(
    r"https?://drive\.google\.com/file/d/([a-zA-Z0-9_-]+)", re.IGNORECASE
)
GDOCS_FILE_RE = re.compile(
    r"https?://docs\.google\.com/document/d/([a-zA-Z0-9_-]+)", re.IGNORECASE
)
PDF_URL_RE = re.compile(r'href="([^"]*\.pdf)"', re.IGNORECASE)


def extract_links(post: dict) -> list[DownloadLink]:
    """Parse the rendered HTML content of a WordPress post for download links."""
    content = post.get("content", {}).get("rendered", "")
    if not content:
        return []
    soup = BeautifulSoup(content, "html.parser")
    # Find all anchor tags with href
    anchors = soup.find_all("a", href=True)
    links: list[DownloadLink] = []
    seen_urls: set[str] = set()
    for a in anchors:
        href = a["href"].strip()
        if not href.startswith("http"):
            continue
        if href in seen_urls:
            continue
        text = a.get_text(strip=True) or "download"
        seen_urls.add(href)

        # Google Drive file
        m = GDRIVE_FILE_RE.search(href)
        if m:
            file_id = m.group(1)
            links.append(DownloadLink(
                url=href,
                link_text=text,
                file_id=file_id,
                file_type="gdrive",
                filename=None,
            ))
            continue

        # Google Docs document
        m = GDOCS_FILE_RE.search(href)
        if m:
            file_id = m.group(1)
            links.append(DownloadLink(
                url=href,
                link_text=text,
                file_id=file_id,
                file_type="gdocs",
                filename=None,
            ))
            continue

        # belacash.net (anti-bot, record only)
        if "belacash.net" in href:
            links.append(DownloadLink(
                url=href,
                link_text=text,
                file_id=None,
                file_type="belacash",
                filename=None,
            ))
            continue

        # Direct PDF link
        if re.search(r"\.pdf(\?|$)", href, re.IGNORECASE):
            links.append(DownloadLink(
                url=href,
                link_text=text,
                file_id=None,
                file_type="pdf",
                filename=None,
            ))
            continue

        # TIE open links (ol.tie.go.tz) – these are "OPEN" not "DOWNLOAD"
        # but some posts only have these, so we keep them
        if "tie.go.tz" in href:
            links.append(DownloadLink(
                url=href,
                link_text=text,
                file_id=None,
                file_type="tie-open",
                filename=None,
            ))
            continue

    return links


# ──────────────────────────────────────────────────────────────────────────────
# Downloaders
# ──────────────────────────────────────────────────────────────────────────────


def get_gdrive_filename(resp: requests.Response, fallback: str) -> str:
    """Try to extract the real filename from a Google Drive download response."""
    # Google Drive sets Content-Disposition with filename
    cd = resp.headers.get("Content-Disposition", "")
    m = re.search(r'filename="([^"]*)"', cd)
    if m:
        return m.group(1)
    m = re.search(r"filename\*=UTF-8''([^;]+)", cd)
    if m:
        return unquote(m.group(1))
    return fallback


def download_gdrive_file(file_id: str, dest: Path, session: requests.Session,
                         link_text: str = "") -> bool:
    """Download a Google Drive file by its file ID.

    Handles the confirmation token that Google Drive requires for large files.
    Uses streaming to avoid loading the entire file into memory.
    """
    url = f"https://drive.google.com/uc?id={file_id}"
    resp = session.get(url, stream=True, allow_redirects=True, timeout=(30, 600))

    if resp.status_code != 200:
        print(f"    FAIL: HTTP {resp.status_code}")
        return False

    # Check Content-Type – Google Drive may return an HTML confirm page
    ct = resp.headers.get("Content-Type", "")
    if "text/html" in ct:
        # Read the HTML page to find the confirm token
        page = resp.text
        resp.close()
        confirm_match = re.search(r"&confirm=([a-zA-Z0-9_]+)", page)
        if confirm_match:
            confirm_token = confirm_match.group(1)
            url = f"https://drive.google.com/uc?id={file_id}&confirm={confirm_token}"
            resp = session.get(url, stream=True, allow_redirects=True, timeout=(30, 600))
            if resp.status_code != 200:
                print(f"    FAIL: HTTP {resp.status_code} on retry")
                return False
        else:
            print(f"    SKIP: HTML confirm page but no token found")
            return False

    # Determine filename
    safe_name = sanitize_filename(link_text) if link_text else file_id
    filename = get_gdrive_filename(resp, f"{safe_name}_{file_id}.pdf")
    if not filename:
        filename = f"{safe_name}_{file_id}.pdf"
    filename = sanitize_filename(filename) if filename else f"{safe_name}_{file_id}.pdf"

    dest_file = dest / filename
    dest_file.parent.mkdir(parents=True, exist_ok=True)

    # Write the file in streaming mode
    total = int(resp.headers.get("Content-Length", 0))
    downloaded = 0
    with open(dest_file, "wb") as f:
        for chunk in resp.iter_content(chunk_size=65536):
            if chunk:
                f.write(chunk)
                downloaded += len(chunk)
    resp.close()

    # Sanity-check: Google Drive sometimes returns an HTML page instead of the
    # file when access is denied or the file is too large.
    if dest_file.stat().st_size < 1024 and b"<!DOCTYPE" in dest_file.read_bytes()[:500]:
        dest_file.unlink(missing_ok=True)
        print(f"    SKIP: got HTML page instead of file")
        return False

    size_mb = dest_file.stat().st_size / (1024 * 1024)
    print(f"    OK: {filename} ({size_mb:.1f} MB)")
    return True


def download_gdocs_file(file_id: str, dest: Path, session: requests.Session,
                        link_text: str = "") -> bool:
    """Download a Google Docs file (exported as PDF)."""
    export_url = f"https://docs.google.com/document/d/{file_id}/export?format=pdf"
    resp = session.get(export_url, allow_redirects=True, timeout=120)

    if resp.status_code != 200:
        # Try viewing the link instead
        print(f"    WARN: GDocs export failed ({resp.status_code}), trying uc URL")
        url = f"https://drive.google.com/uc?id={file_id}"
        resp = session.get(url, allow_redirects=True, timeout=120)
        if resp.status_code != 200:
            print(f"    FAIL: HTTP {resp.status_code}")
            return False

    safe_name = sanitize_filename(link_text) if link_text else file_id
    filename = get_gdrive_filename(resp, f"{safe_name}_{file_id}.pdf")
    if not filename:
        filename = f"{safe_name}_{file_id}.pdf"
    dest_file = dest / filename
    dest_file.parent.mkdir(parents=True, exist_ok=True)

    with open(dest_file, "wb") as f:
        f.write(resp.content)
    resp.close()

    if dest_file.stat().st_size < 1024 and b"<!DOCTYPE" in dest_file.read_bytes()[:500]:
        dest_file.unlink(missing_ok=True)
        print(f"    SKIP: got HTML page instead of file")
        return False

    size_mb = dest_file.stat().st_size / (1024 * 1024)
    print(f"    OK: {filename} ({size_mb:.1f} MB)")
    return True


def download_pdf_file(url: str, dest: Path, session: requests.Session,
                      link_text: str = "") -> bool:
    """Download a direct PDF from any URL."""
    resp = session.get(url, stream=True, allow_redirects=True, timeout=120)
    if resp.status_code != 200:
        print(f"    FAIL: HTTP {resp.status_code}")
        return False

    safe_name = sanitize_filename(link_text) if link_text else "download"
    # Try to get filename from URL or response
    parsed = urlparse(url)
    url_filename = os.path.basename(parsed.path)
    if url_filename.endswith(".pdf"):
        filename = url_filename
    else:
        filename = get_gdrive_filename(resp, f"{safe_name}.pdf")
        if not filename:
            filename = f"{safe_name}.pdf"

    dest_file = dest / filename
    dest_file.parent.mkdir(parents=True, exist_ok=True)
    total = int(resp.headers.get("Content-Length", 0))
    with open(dest_file, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    resp.close()

    size_mb = dest_file.stat().st_size / (1024 * 1024)
    print(f"    OK: {filename} ({size_mb:.1f} MB)")
    return True


def sanitize_filename(name: str) -> str:
    """Make a string safe for use as a filename."""
    # Strip HTML entities
    name = name.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    # Replace problematic characters
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)
    name = name.strip().strip("._")[:100]
    return name or "download"


# ──────────────────────────────────────────────────────────────────────────────
# Main download logic
# ──────────────────────────────────────────────────────────────────────────────

DOWNLOADS_DIR = Path(".") / "downloads"
MANIFEST_PATH = DOWNLOADS_DIR / "manifest.json"
UNRESOLVED_PATH = DOWNLOADS_DIR / "unresolved_links.json"
METADATA_PATH = DOWNLOADS_DIR / "download_metadata.json"


def load_manifest() -> dict:
    if MANIFEST_PATH.exists():
        with open(MANIFEST_PATH) as f:
            return json.load(f)
    return {"downloaded_files": [], "skipped_links": [], "errors": []}


def save_manifest(manifest: dict):
    with open(MANIFEST_PATH, "w") as f:
        json.dump(manifest, f, indent=2)


def main_download(args):
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)

    manifest = load_manifest()
    # Build set of already-downloaded file IDs to skip
    downloaded_ids = {entry.get("file_id") for entry in manifest["downloaded_files"] if entry.get("file_id")}

    session = requests.Session()
    session.headers.update(HEADERS)

    stats = {"posts_processed": 0, "files_downloaded": 0, "files_skipped": 0,
             "links_found": 0, "belacash_unresolved": 0}

    unresolved: list[dict] = []

    for cat_id, dir_name in TARGET_CATEGORIES:
        cat_dir = DOWNLOADS_DIR / dir_name
        cat_dir.mkdir(parents=True, exist_ok=True)
        print(f"\n{'='*60}")
        print(f"Category ID {cat_id} → {dir_name}")
        print(f"{'='*60}")

        posts = fetch_posts_by_category(cat_id, limit=args.limit)
        if not posts:
            print("  No posts found or skipped.")
            continue
        print(f"  Found {len(posts)} posts")

        for post in posts:
            stats["posts_processed"] += 1
            title = post.get("title", {}).get("rendered", "Untitled")
            # Strip HTML tags from title
            clean_title = BeautifulSoup(title, "html.parser").get_text(strip=True)
            post_url = post.get("link", "")
            print(f"\n  [{post.get('id')}] {clean_title[:70]}")

            links = extract_links(post)
            if not links:
                print("    No download links found")
                continue
            stats["links_found"] += len(links)

            # Create a per-post subdirectory
            safe_post_title = sanitize_filename(clean_title)
            post_dir = cat_dir / safe_post_title
            post_dir.mkdir(parents=True, exist_ok=True)

            # Write metadata for this post
            meta = {
                "post_id": post.get("id"),
                "title": clean_title,
                "url": post_url,
                "category": dir_name,
                "date": post.get("date", ""),
                "links": [],
            }

            # Separate links into: already-downloaded, downloadable, unresolved
            to_download: list[DownloadLink] = []
            for dl in links:
                link_info = {
                    "url": dl.url,
                    "text": dl.link_text,
                    "type": dl.file_type,
                    "file_id": dl.file_id,
                    "resolved": False,
                    "filename": None,
                }

                if dl.file_type in ("gdrive", "gdocs") and dl.file_id:
                    if dl.file_id in downloaded_ids:
                        print(f"    SKIP (already downloaded): {dl.link_text}")
                        stats["files_skipped"] += 1
                        link_info["resolved"] = True
                        link_info["filename"] = "already_downloaded"
                        meta["links"].append(link_info)
                    else:
                        to_download.append(dl)

                elif dl.file_type == "pdf":
                    to_download.append(dl)

                elif dl.file_type == "belacash":
                    stats["belacash_unresolved"] += 1
                    meta["links"].append(link_info)
                    unresolved.append({
                        "post_id": post.get("id"),
                        "post_title": clean_title,
                        "post_url": post_url,
                        "category": dir_name,
                        "link_url": dl.url,
                        "link_text": dl.link_text,
                        "reason": "belacash.net has anti-bot protection; manual download required",
                    })
                    print(f"    RECORDED (belacash): {dl.url}")

                elif dl.file_type == "tie-open":
                    meta["links"].append(link_info)
                    unresolved.append({
                        "post_id": post.get("id"),
                        "post_title": clean_title,
                        "post_url": post_url,
                        "category": dir_name,
                        "link_url": dl.url,
                        "link_text": dl.link_text,
                        "reason": "TIE open link (view-only browser)",
                    })
                    print(f"    RECORDED (tie-open): {dl.url}")

            # Download links (sequentially or in parallel)
            if to_download:
                if args.parallel:
                    print(f"    Downloading {len(to_download)} files in parallel "
                          f"(workers={args.workers})...")
                    results = download_links_parallel(
                        to_download, post_dir, session, max_workers=args.workers
                    )
                else:
                    results = []
                    for dl in to_download:
                        print(f"    Downloading: {dl.link_text}")
                        link_info = {
                            "url": dl.url, "text": dl.link_text, "type": dl.file_type,
                            "file_id": dl.file_id, "resolved": False, "filename": None,
                        }
                        if dl.file_type == "gdrive" and dl.file_id:
                            success = download_gdrive_file(dl.file_id, post_dir, session, dl.link_text)
                            if success:
                                downloaded_file = list(post_dir.glob(f"*{dl.file_id}*")) or list(post_dir.glob(f"{sanitize_filename(dl.link_text)}*"))
                                filename = downloaded_file[0].name if downloaded_file else ""
                                manifest["downloaded_files"].append({
                                    "post_id": post.get("id"),
                                    "post_title": clean_title,
                                    "category": dir_name,
                                    "file_id": dl.file_id,
                                    "filename": filename,
                                    "url": dl.url,
                                    "link_text": dl.link_text,
                                    "downloaded_at": datetime.now(timezone.utc).isoformat(),
                                })
                                downloaded_ids.add(dl.file_id)
                                stats["files_downloaded"] += 1
                                link_info["resolved"] = True
                                link_info["filename"] = filename
                            else:
                                stats["files_skipped"] += 1
                            meta["links"].append(link_info)
                        elif dl.file_type == "gdocs" and dl.file_id:
                            success = download_gdocs_file(dl.file_id, post_dir, session, dl.link_text)
                            if success:
                                downloaded_file = list(post_dir.glob(f"*{dl.file_id}*")) or list(post_dir.glob(f"{sanitize_filename(dl.link_text)}*"))
                                filename = downloaded_file[0].name if downloaded_file else ""
                                manifest["downloaded_files"].append({
                                    "post_id": post.get("id"),
                                    "post_title": clean_title,
                                    "category": dir_name,
                                    "file_id": dl.file_id,
                                    "filename": filename,
                                    "url": dl.url,
                                    "link_text": dl.link_text,
                                    "downloaded_at": datetime.now(timezone.utc).isoformat(),
                                })
                                downloaded_ids.add(dl.file_id)
                                stats["files_downloaded"] += 1
                                link_info["resolved"] = True
                                link_info["filename"] = filename
                            else:
                                stats["files_skipped"] += 1
                            meta["links"].append(link_info)
                        elif dl.file_type == "pdf":
                            success = download_pdf_file(dl.url, post_dir, session, dl.link_text)
                            if success:
                                manifest["downloaded_files"].append({
                                    "post_id": post.get("id"),
                                    "post_title": clean_title,
                                    "category": dir_name,
                                    "file_id": None,
                                    "url": dl.url,
                                    "link_text": dl.link_text,
                                    "downloaded_at": datetime.now(timezone.utc).isoformat(),
                                })
                                stats["files_downloaded"] += 1
                                link_info["resolved"] = True
                                link_info["filename"] = "downloaded"
                            else:
                                stats["files_skipped"] += 1
                            meta["links"].append(link_info)
                        results.append((dl, False, ""))

                # Process results
                for dl, success, filename in results:
                    link_info = {
                        "url": dl.url, "text": dl.link_text, "type": dl.file_type,
                        "file_id": dl.file_id, "resolved": False, "filename": None,
                    }
                    if success:
                        if dl.file_id:
                            if dl.file_type == "gdocs":
                                manifest["downloaded_files"].append({
                                    "post_id": post.get("id"),
                                    "post_title": clean_title,
                                    "category": dir_name,
                                    "file_id": dl.file_id,
                                    "filename": filename,
                                    "url": dl.url,
                                    "link_text": dl.link_text,
                                    "downloaded_at": datetime.now(timezone.utc).isoformat(),
                                })
                                downloaded_ids.add(dl.file_id)
                                stats["files_downloaded"] += 1
                                link_info["resolved"] = True
                                link_info["filename"] = filename
                            elif dl.file_type == "gdrive":
                                manifest["downloaded_files"].append({
                                    "post_id": post.get("id"),
                                    "post_title": clean_title,
                                    "category": dir_name,
                                    "file_id": dl.file_id,
                                    "filename": filename,
                                    "url": dl.url,
                                    "link_text": dl.link_text,
                                    "downloaded_at": datetime.now(timezone.utc).isoformat(),
                                })
                                downloaded_ids.add(dl.file_id)
                                stats["files_downloaded"] += 1
                                link_info["resolved"] = True
                                link_info["filename"] = filename
                        elif dl.file_type == "pdf":
                            manifest["downloaded_files"].append({
                                "post_id": post.get("id"),
                                "post_title": clean_title,
                                "category": dir_name,
                                "file_id": None,
                                "url": dl.url,
                                "link_text": dl.link_text,
                                "downloaded_at": datetime.now(timezone.utc).isoformat(),
                            })
                            stats["files_downloaded"] += 1
                            link_info["resolved"] = True
                            link_info["filename"] = "downloaded"
                    else:
                        stats["files_skipped"] += 1
                    meta["links"].append(link_info)

            # Save per-post metadata
            meta_path = post_dir / "_meta.json"
            with open(meta_path, "w") as f:
                json.dump(meta, f, indent=2, ensure_ascii=False)

            # Save manifest periodically
            save_manifest(manifest)

            # Progress summary
            print(f"  Progress: {stats['posts_processed']} posts, "
                  f"{stats['files_downloaded']} downloaded, "
                  f"{stats['links_found']} links found")

    # Save final manifest and unresolved links
    save_manifest(manifest)
    with open(UNRESOLVED_PATH, "w") as f:
        json.dump(unresolved, f, indent=2, ensure_ascii=False)
    with open(METADATA_PATH, "w") as f:
        json.dump(stats, f, indent=2)

    print(f"\n{'='*60}")
    print("Download Summary")
    print(f"{'='*60}")
    print(f"  Posts processed:      {stats['posts_processed']}")
    print(f"  Download links found: {stats['links_found']}")
    print(f"  Files downloaded:     {stats['files_downloaded']}")
    print(f"  Files skipped:        {stats['files_skipped']}")
    print(f"  Belacash unresolved:  {stats['belacash_unresolved']}")
    print(f"  Files saved to:       {DOWNLOADS_DIR}")


# ──────────────────────────────────────────────────────────────────────────────
# GitHub push
# ──────────────────────────────────────────────────────────────────────────────


def git_run(args_list: list[str], cwd: Path) -> str:
    """Run a git command and return stdout."""
    result = subprocess.run(["git", *args_list], cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0 and result.stderr:
        print(f"  [GIT WARN] {result.stderr.strip()}")
    return result.stdout.strip()


def download_links_parallel(links: list[DownloadLink], post_dir: Path,
                           session: requests.Session, max_workers: int = 5) -> list[tuple[DownloadLink, bool, str]]:
    """Download multiple links in parallel using a thread pool.

    Returns a list of (link, success, filename) tuples.
    """
    results: list[tuple[DownloadLink, bool, str]] = []

    def _download(dl: DownloadLink) -> tuple[DownloadLink, bool, str]:
        if dl.file_type == "gdrive" and dl.file_id:
            ok = download_gdrive_file(dl.file_id, post_dir, session, dl.link_text)
            fname = ""
            if ok:
                files = list(post_dir.glob(f"*{dl.file_id}*")) or list(post_dir.glob(f"{sanitize_filename(dl.link_text)}*"))
                fname = files[0].name if files else ""
            return (dl, ok, fname)
        elif dl.file_type == "gdocs" and dl.file_id:
            ok = download_gdocs_file(dl.file_id, post_dir, session, dl.link_text)
            fname = ""
            if ok:
                files = list(post_dir.glob(f"*{dl.file_id}*")) or list(post_dir.glob(f"{sanitize_filename(dl.link_text)}*"))
                fname = files[0].name if files else ""
            return (dl, ok, fname)
        elif dl.file_type == "pdf":
            ok = download_pdf_file(dl.url, post_dir, session, dl.link_text)
            return (dl, ok, "")
        return (dl, False, "")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_link = {executor.submit(_download, dl): dl for dl in links}
        for future in as_completed(future_to_link):
            dl = future_to_link[future]
            try:
                result = future.result(timeout=600)
                results.append(result)
            except Exception as exc:
                print(f"    ERROR: {dl.link_text}: {exc}")
                results.append((dl, False, ""))

    return results


def get_github_token() -> str | None:
    """Try to get a GitHub token from gh CLI config."""
    import yaml
    config_path = Path.home() / ".config" / "gh" / "hosts.yml"
    if config_path.exists():
        try:
            with open(config_path) as f:
                data = yaml.safe_load(f)
            host = data.get("github.com", {})
            token = host.get("oauth_token")
            if token:
                return token
        except Exception:
            pass
    # Fallback: try environment variable
    return os.environ.get("GITHUB_TOKEN")


def main_push(args):
    repo_dir = Path(args.repo_dir)

    # If repo_dir is the default and downloads dir is itself a git repo, use it directly
    if repo_dir == Path("./wazaelimu-archive") and DOWNLOADS_DIR.exists() and (DOWNLOADS_DIR / ".git").exists():
        repo_dir = DOWNLOADS_DIR

    repo_dir.mkdir(parents=True, exist_ok=True)
    git_dir = repo_dir / ".git"
    is_repo = git_dir.exists()

    if not is_repo:
        print("Initializing git repository...")
        git_run(["init"], repo_dir)
        git_run(["checkout", "-b", "main"], repo_dir) if False else git_run(["branch", "-M", "main"], repo_dir)
        git_run(["config", "user.email", args.github_email], repo_dir)
        git_run(["config", "user.name", args.github_name], repo_dir)
        git_run(["config", "commit.gpgsign", "false"], repo_dir)

    downloads_src = DOWNLOADS_DIR
    if not downloads_src.exists():
        print(f"ERROR: {downloads_src} does not exist. Run --download first.")
        return

    # If repo is in a different directory than downloads, sync files
    if repo_dir != downloads_src:
        repo_downloads = repo_dir / "downloads"
        repo_downloads.mkdir(parents=True, exist_ok=True)
        # Incremental copy – only copy new files
        import filecmp
        for src_item in downloads_src.rglob("*"):
            if src_item.is_file():
                rel = src_item.relative_to(downloads_src)
                dst = repo_downloads / rel
                if not dst.exists() or not filecmp.cmp(src_item, dst, shallow=False):
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(src_item, dst)
        # Copy script
        script_src = Path(__file__).resolve()
        script_dest = repo_dir / "wazaelimu_downloader.py"
        if script_src.exists():
            shutil.copy2(script_src, script_dest)
    else:
        # Repo is in the downloads directory itself
        repo_downloads = repo_dir
        script_dest = repo_dir / "wazaelimu_downloader.py"
        script_src = Path(__file__).resolve()
        if script_src.exists():
            shutil.copy2(script_src, script_dest)

    # Ensure README exists
    readme = repo_dir / "README.md"
    if not readme.exists() or args.force_readme:
        readme.write_text(README_TEXT)

    # Ensure .gitignore
    gitignore = repo_dir / ".gitignore"
    if not gitignore.exists():
        gitignore.write_text(".gitignore\n*.pyc\n__pycache__/\n.DS_Store\n")

    # Stage all new/modified files
    git_run(["add", "-A"], repo_dir)

    status = git_run(["status", "--porcelain"], repo_dir)
    # Count new + modified files
    n_changes = len([s for s in status.split("\n") if s.strip()])
    if n_changes == 0:
        print("No new files to commit. Working tree clean.")
        return

    print(f"  {n_changes} new/modified files to commit")
    commit_msg = f"Add Wazaelimu content ({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')})"
    git_run(["commit", "-m", commit_msg], repo_dir)
    print(f"  Committed: {commit_msg}")

    # Determine remote URL
    token = get_github_token()
    if args.github_remote:
        remote = args.github_remote
        if token and "github.com" in remote and remote.startswith("git@"):
            # Convert SSH URL to HTTPS with token
            remote = remote.replace("git@github.com:", f"https://{token}@github.com/")
    elif token:
        remote = f"https://{token}@github.com/hancykanda/wazaelimu-archive.git"
    else:
        remote = None

    if remote:
        remotes = git_run(["remote", "-v"], repo_dir)
        if "origin" not in remotes:
            git_run(["remote", "add", "origin", remote], repo_dir)
        else:
            git_run(["remote", "set-url", "origin", remote], repo_dir)
        print("Pushing to GitHub...")
        push_result = subprocess.run(
            ["git", "push", "-u", "origin", "main"],
            cwd=repo_dir, capture_output=True, text=True, timeout=300
        )
        if push_result.returncode == 0:
            print("  Successfully pushed to GitHub!")
        else:
            print(f"  Push warning: {push_result.stderr[:200]}")
    else:
        print("Skipping push (no GitHub remote or token configured).")
        print("Files committed to local repo.")


README_TEXT = """# Wazaelimu Exams and Books Archive

Auto-downloaded archive of educational materials from [wazaelimu.com](https://wazaelimu.com).

## Contents

- `past-papers/` – Past exam papers (Secondary Forms 1-6, Primary STD 1-7, ECZ, KCSE, NECTA, Results)
- `books/` – TIE Books for all standards and forms, plus book analyses
- `notes/` – Study notes organized by form level
- `manifest.json` – Full manifest of downloaded files
- `unresolved_links.json` – Links that could not be downloaded automatically

## Past Papers

### Secondary (Forms)
- **Form 1** (`past-papers/form-1/`)
- **Form 2** (`past-papers/form-2/`)
- **Form 3** (`past-papers/form-3/`)
- **Form 4** (`past-papers/form-4/`)
- **Form 5** (`past-papers/form-5/`)
- **Form 6** (`past-papers/form-6/`)

### Primary (Standards)
- **STD 1-4** (`past-papers/std-1-4/`)
- **STD 5-7** (`past-papers/std-5-7/`)

### Other Exams
- **ECZ Past Papers** (`past-papers/ecz/`)
- **KCSE Past Papers** (`past-papers/kcse/`)
- **NECTA Past Papers** (`past-papers/necta/`)
- **Results / MATOKEO NECTA** (`results/matokeo-necta/`)

## TIE Books

- **Primary** – Standards I-VII (English & Swahili Medium)
- **Secondary** – Forms I-VI
- **Pre-Primary**

## Notes

- **Form I** through **Form VI** study notes
- **Secondary school notes** (all subjects)
- **Basic Mathematics**

## How to Use

```bash
# Download everything
python wazaelimu_downloader.py --download

# Push to GitHub
python wazaelimu_downloader.py --push
```

## Notes

Some download links on wazaelimu.com point to `belacash.net`, which uses
anti-bot protection. Those links are recorded in `unresolved_links.json`
but cannot be downloaded programmatically.

## Generated

This archive was generated by an automated Python script. See `wazaelimu_downloader.py`.
"""


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Download Wazaelimu exams and books and push to GitHub."
    )
    parser.add_argument(
        "--download", action="store_true",
        help="Download exams and books from wazaelimu.com"
    )
    parser.add_argument(
        "--push", action="store_true",
        help="Push downloaded content to GitHub"
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Limit posts per category (for testing)"
    )
    parser.add_argument(
        "--parallel", action="store_true",
        help="Download files in parallel using a thread pool (faster)"
    )
    parser.add_argument(
        "--workers", type=int, default=5,
        help="Number of parallel download workers (default: 5)"
    )
    parser.add_argument(
        "--repo-dir", default="./wazaelimu-archive",
        help="Local git repository directory for push"
    )
    parser.add_argument(
        "--github-remote", default=None,
        help="GitHub remote URL to push to (e.g. git@github.com:user/repo.git)"
    )
    parser.add_argument(
        "--github-email", default="downloader@wazaelimu.local",
        help="Git commit email"
    )
    parser.add_argument(
        "--github-name", default="Wazaelimu Downloader",
        help="Git commit name"
    )
    parser.add_argument(
        "--create-github-repo", action="store_true",
        help="Create the GitHub repo if it doesn't exist (requires gh CLI auth)"
    )
    parser.add_argument(
        "--github-repo-name", default=None,
        help="Name for the GitHub repo (used with --create-github-repo)"
    )
    parser.add_argument(
        "--force-readme", action="store_true",
        help="Overwrite README.md even if it exists"
    )

    args = parser.parse_args()

    if not args.download and not args.push:
        parser.print_help()
        return

    if args.download:
        main_download(args)

    if args.push:
        # If we need to create the GitHub repo
        if args.create_github_repo and args.github_repo_name:
            repo_name = args.github_repo_name
            print(f"Creating GitHub repository: {repo_name}")
            result = subprocess.run(
                ["gh", "repo", "create", repo_name, "--public", "--confirm"],
                capture_output=True, text=True
            )
            if result.returncode == 0:
                # Get the remote URL
                remote_result = subprocess.run(
                    ["gh", "repo", "view", repo_name, "--json", "sshUrl", "-q", ".sshUrl"],
                    capture_output=True, text=True
                )
                if remote_result.returncode == 0:
                    args.github_remote = remote_result.stdout.strip()
                    print(f"Created repo and got remote: {args.github_remote}")
            else:
                print(f"Failed to create GitHub repo: {result.stderr}")
                print("Falling back to local git only.")

        main_push(args)


if __name__ == "__main__":
    main()
