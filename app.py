#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SEO Diagnostic Pro v3 — Comprehensive SEO Diagnostic Tool
실시간 크롤링 + 데이터 테이블 + 진단 리포트
Schema, E-E-A-T, Technical SEO, Security, Performance, Content Quality
"""

import re
import json
import time
import math
import pandas as pd
import streamlit as st
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urlparse, urljoin, urldefrag, parse_qs
from collections import defaultdict

# ── 설정 ─────────────────────────────────────────────────────────────────────
USER_AGENT = "SEODiagnosticPro/3.0 (+https://seodiagnosticpro.dev/bot)"
REQUEST_TIMEOUT = 15
TITLE_MIN, TITLE_MAX = 30, 60
DESC_MIN, DESC_MAX = 120, 160
THIN_CONTENT_THRESHOLD = 300
MIN_INCOMING_LINKS = 3
PAGESPEED_THRESHOLD = 90
URL_MAX_LENGTH = 100
PAGE_SIZE_WARN = 3 * 1024 * 1024  # 3MB
MAX_EXTERNAL_SCRIPTS = 15

# ── 페이지 설정 ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SEO Diagnostic Pro",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS 스타일 (Screaming Frog 느낌) ─────────────────────────────────────────
st.markdown("""
<style>
    /* 상단 헤더바 */
    .sf-header {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
        padding: 12px 20px;
        border-radius: 8px;
        margin-bottom: 16px;
        display: flex;
        align-items: center;
        justify-content: space-between;
    }
    .sf-header h1 {
        color: #e94560;
        font-size: 1.4rem;
        margin: 0;
        font-weight: 700;
    }
    .sf-header span {
        color: #a3a3a3;
        font-size: 0.85rem;
    }
    /* 실시간 수집 상태바 */
    .crawl-status {
        background: #0d1117;
        border: 1px solid #30363d;
        border-radius: 6px;
        padding: 10px 16px;
        font-family: 'SF Mono', 'Consolas', monospace;
        font-size: 0.82rem;
        color: #58a6ff;
        margin-bottom: 8px;
    }
    .crawl-status .url { color: #8b949e; }
    .crawl-status .count { color: #3fb950; font-weight: bold; }
    .crawl-status .eta { color: #f0883e; }
    /* 요약 카드 */
    .summary-card {
        background: #161b22;
        border: 1px solid #30363d;
        border-radius: 8px;
        padding: 14px;
        text-align: center;
    }
    .summary-card .num {
        font-size: 1.8rem;
        font-weight: 800;
        color: #58a6ff;
    }
    .summary-card .label {
        font-size: 0.78rem;
        color: #8b949e;
        margin-top: 2px;
    }
    .summary-card.red .num { color: #f85149; }
    .summary-card.yellow .num { color: #d29922; }
    .summary-card.green .num { color: #3fb950; }
    /* 이슈 뱃지 */
    .badge-high { background: #f8514922; color: #f85149; padding: 2px 8px; border-radius: 4px; font-size: 0.75rem; font-weight: 600; }
    .badge-med { background: #d2992222; color: #d29922; padding: 2px 8px; border-radius: 4px; font-size: 0.75rem; font-weight: 600; }
    .badge-low { background: #3fb95022; color: #3fb950; padding: 2px 8px; border-radius: 4px; font-size: 0.75rem; font-weight: 600; }
    /* 데이터 테이블 스타일 */
    .stDataFrame { font-size: 0.85rem; }
</style>
""", unsafe_allow_html=True)


# ── 유틸리티 ──────────────────────────────────────────────────────────────────
def normalize_url(url):
    url, _ = urldefrag(url)
    parsed = urlparse(url)
    path = parsed.path.rstrip("/") or "/"
    return f"{parsed.scheme}://{parsed.netloc}{path}" + (
        f"?{parsed.query}" if parsed.query else ""
    )


def is_same_domain(url, base_domain):
    try:
        return urlparse(url).netloc == base_domain
    except Exception:
        return False


def build_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
    })
    return s


def fmt_time(seconds):
    """초를 mm:ss 포맷으로"""
    if seconds < 0:
        return "--:--"
    m, s = divmod(int(seconds), 60)
    return f"{m:02d}:{s:02d}"


# ── robots.txt / sitemap ─────────────────────────────────────────────────────
def discover_sitemaps(base_url, session):
    sitemaps = []
    robots_url = urljoin(base_url, "/robots.txt")
    try:
        r = session.get(robots_url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            for line in r.text.splitlines():
                if line.strip().lower().startswith("sitemap:"):
                    sitemaps.append(line.split(":", 1)[1].strip())
    except Exception:
        pass
    if not sitemaps:
        for path in ["/sitemap.xml", "/sitemap_index.xml"]:
            try:
                url = urljoin(base_url, path)
                r = session.head(url, timeout=REQUEST_TIMEOUT)
                if r.status_code == 200:
                    sitemaps.append(url)
                    break
            except Exception:
                pass
    return sitemaps


def parse_sitemap(sitemap_url, session, max_pages, base_domain):
    urls = set()
    def _parse(url, depth=0):
        if depth > 3 or len(urls) >= max_pages:
            return
        try:
            r = session.get(url, timeout=REQUEST_TIMEOUT)
            soup = BeautifulSoup(r.content, "html.parser")
            for sm in soup.find_all("sitemap"):
                loc = sm.find("loc")
                if loc:
                    _parse(loc.text.strip(), depth + 1)
            for u in soup.find_all("url"):
                if len(urls) >= max_pages:
                    return
                loc = u.find("loc")
                if loc:
                    normalized = normalize_url(loc.text.strip())
                    if is_same_domain(normalized, base_domain):
                        urls.add(normalized)
        except Exception:
            pass
    _parse(sitemap_url)
    return urls


# ── Schema / Structured Data Detection ──────────────────────────────────────
COMMON_SCHEMA_REQUIRED_PROPS = {
    "Article": ["headline", "author", "datePublished"],
    "Product": ["name", "image"],
    "LocalBusiness": ["name", "address"],
    "Organization": ["name", "url"],
    "BreadcrumbList": ["itemListElement"],
    "FAQPage": ["mainEntity"],
    "WebSite": ["name", "url"],
    "Person": ["name"],
}

def detect_schema(soup, page_url):
    """Detect all structured data on a page."""
    schema_info = {
        "json_ld": [],
        "json_ld_types": [],
        "microdata_types": [],
        "rdfa_types": [],
        "all_types": [],
        "has_schema": False,
        "validation_issues": [],
    }

    # JSON-LD
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            text = script.string or ""
            data = json.loads(text)
            items = data if isinstance(data, list) else [data]
            # Handle @graph
            expanded = []
            for item in items:
                if isinstance(item, dict) and "@graph" in item:
                    expanded.extend(item["@graph"])
                else:
                    expanded.append(item)
            for item in expanded:
                if isinstance(item, dict):
                    schema_type = item.get("@type", "Unknown")
                    if isinstance(schema_type, list):
                        for t in schema_type:
                            schema_info["json_ld_types"].append(t)
                    else:
                        schema_info["json_ld_types"].append(schema_type)
                    schema_info["json_ld"].append(item)
                    # Validate required properties
                    st_name = schema_type if isinstance(schema_type, str) else (schema_type[0] if schema_type else "")
                    if st_name in COMMON_SCHEMA_REQUIRED_PROPS:
                        for prop in COMMON_SCHEMA_REQUIRED_PROPS[st_name]:
                            if prop not in item:
                                schema_info["validation_issues"].append(
                                    f"{st_name}: '{prop}' 속성 누락"
                                )
        except (json.JSONDecodeError, TypeError):
            pass

    # Microdata
    for el in soup.find_all(attrs={"itemtype": True}):
        itype = el.get("itemtype", "")
        # Extract type name from URL like http://schema.org/Article
        type_name = itype.rstrip("/").split("/")[-1] if "/" in itype else itype
        if type_name:
            schema_info["microdata_types"].append(type_name)

    # RDFa
    for el in soup.find_all(attrs={"typeof": True}):
        rtype = el.get("typeof", "")
        if rtype:
            for t in rtype.split():
                type_name = t.split(":")[-1] if ":" in t else t
                schema_info["rdfa_types"].append(type_name)

    all_types = list(set(
        schema_info["json_ld_types"] +
        schema_info["microdata_types"] +
        schema_info["rdfa_types"]
    ))
    schema_info["all_types"] = all_types
    schema_info["has_schema"] = len(all_types) > 0

    return schema_info


# ── E-E-A-T Signals Detection ───────────────────────────────────────────────
SOCIAL_PATTERNS = {
    "facebook": re.compile(r"facebook\.com/", re.I),
    "twitter": re.compile(r"(twitter\.com/|x\.com/)", re.I),
    "linkedin": re.compile(r"linkedin\.com/", re.I),
    "youtube": re.compile(r"youtube\.com/", re.I),
    "instagram": re.compile(r"instagram\.com/", re.I),
}

def detect_eeat(soup, schema_info, page_url):
    """Detect E-E-A-T signals on a page."""
    eeat = {
        "has_author": False,
        "author_name": "",
        "has_published_date": False,
        "has_modified_date": False,
        "has_about_link": False,
        "has_contact_link": False,
        "has_privacy_link": False,
        "has_terms_link": False,
        "has_org_schema": False,
        "social_links": [],
        "has_breadcrumb": False,
        "has_reviews_schema": False,
    }

    # Author detection
    # Check meta author
    author_meta = soup.find("meta", attrs={"name": re.compile(r"^author$", re.I)})
    if author_meta and author_meta.get("content"):
        eeat["has_author"] = True
        eeat["author_name"] = author_meta["content"].strip()

    # Check schema Person/author
    for item in schema_info.get("json_ld", []):
        if isinstance(item, dict):
            if item.get("@type") == "Person":
                eeat["has_author"] = True
                eeat["author_name"] = item.get("name", eeat["author_name"])
            if "author" in item:
                eeat["has_author"] = True
                author = item["author"]
                if isinstance(author, dict):
                    eeat["author_name"] = author.get("name", eeat["author_name"])
                elif isinstance(author, str):
                    eeat["author_name"] = author

    # Check byline patterns in HTML
    if not eeat["has_author"]:
        byline = soup.find(class_=re.compile(r"(author|byline|writer)", re.I))
        if byline:
            eeat["has_author"] = True
            eeat["author_name"] = byline.get_text(strip=True)[:80]
        # Also check rel="author"
        author_link = soup.find("a", rel="author")
        if author_link:
            eeat["has_author"] = True
            eeat["author_name"] = author_link.get_text(strip=True)[:80]

    # Published / Modified dates
    for item in schema_info.get("json_ld", []):
        if isinstance(item, dict):
            if item.get("datePublished"):
                eeat["has_published_date"] = True
            if item.get("dateModified"):
                eeat["has_modified_date"] = True

    date_meta = soup.find("meta", attrs={"property": re.compile(r"article:published_time", re.I)})
    if date_meta:
        eeat["has_published_date"] = True
    mod_meta = soup.find("meta", attrs={"property": re.compile(r"article:modified_time", re.I)})
    if mod_meta:
        eeat["has_modified_date"] = True

    # Check time tags
    time_tags = soup.find_all("time")
    if time_tags:
        for tt in time_tags:
            if tt.get("datetime"):
                eeat["has_published_date"] = True
                break

    # Trust page links
    all_links = soup.find_all("a", href=True)
    for a in all_links:
        href_lower = a["href"].lower()
        text_lower = a.get_text(strip=True).lower()
        combined = href_lower + " " + text_lower

        if any(kw in combined for kw in ["about", "about-us", "회사소개", "소개"]):
            eeat["has_about_link"] = True
        if any(kw in combined for kw in ["contact", "contact-us", "문의", "연락처"]):
            eeat["has_contact_link"] = True
        if any(kw in combined for kw in ["privacy", "privacy-policy", "개인정보", "개인정보처리방침"]):
            eeat["has_privacy_link"] = True
        if any(kw in combined for kw in ["terms", "terms-of-service", "tos", "이용약관", "서비스약관"]):
            eeat["has_terms_link"] = True

        # Social media links
        full_href = a.get("href", "")
        for platform, pattern in SOCIAL_PATTERNS.items():
            if pattern.search(full_href) and platform not in eeat["social_links"]:
                eeat["social_links"].append(platform)

    # Organization schema
    for t in schema_info.get("json_ld_types", []) + schema_info.get("microdata_types", []):
        if t in ("Organization", "Corporation", "LocalBusiness"):
            eeat["has_org_schema"] = True
            break

    # Breadcrumb
    if "BreadcrumbList" in schema_info.get("all_types", []):
        eeat["has_breadcrumb"] = True
    else:
        # Check for nav with breadcrumb class
        bc = soup.find(attrs={"class": re.compile(r"breadcrumb", re.I)})
        if bc:
            eeat["has_breadcrumb"] = True
        bc_nav = soup.find("nav", attrs={"aria-label": re.compile(r"breadcrumb", re.I)})
        if bc_nav:
            eeat["has_breadcrumb"] = True

    # Reviews/Testimonials schema
    for t in schema_info.get("all_types", []):
        if t in ("Review", "AggregateRating"):
            eeat["has_reviews_schema"] = True
            break

    return eeat


# ── Technical SEO Detection ─────────────────────────────────────────────────
def detect_technical_seo(soup, response, page_url):
    """Detect technical SEO signals."""
    tech = {
        "meta_robots": "",
        "x_robots_tag": "",
        "is_noindex": False,
        "is_nofollow": False,
        "hreflang_tags": [],
        "hreflang_self_ref": False,
        "og_title": "",
        "og_description": "",
        "og_image": "",
        "og_url": "",
        "og_type": "",
        "twitter_card": "",
        "twitter_title": "",
        "twitter_description": "",
        "twitter_image": "",
        "has_viewport": False,
        "viewport_content": "",
        "charset": "",
        "lang": "",
        "content_type": "",
        "is_redirect": False,
        "redirect_url": "",
        "url_length": len(page_url),
        "has_query_params": bool(urlparse(page_url).query),
        "query_params": urlparse(page_url).query,
        "headings": {"h1": 0, "h2": 0, "h3": 0, "h4": 0, "h5": 0, "h6": 0},
        "heading_hierarchy_ok": True,
        "heading_issues": [],
        "has_iframes": False,
        "iframe_count": 0,
        "has_noscript": False,
        "canonical": "",
        "rel_next": "",
        "rel_prev": "",
    }

    # Meta robots
    robots_meta = soup.find("meta", attrs={"name": re.compile(r"^robots$", re.I)})
    if robots_meta and robots_meta.get("content"):
        tech["meta_robots"] = robots_meta["content"].strip()
        content_lower = tech["meta_robots"].lower()
        tech["is_noindex"] = "noindex" in content_lower
        tech["is_nofollow"] = "nofollow" in content_lower

    # X-Robots-Tag header
    xrobots = response.headers.get("X-Robots-Tag", "")
    tech["x_robots_tag"] = xrobots
    if "noindex" in xrobots.lower():
        tech["is_noindex"] = True

    # Hreflang
    for link in soup.find_all("link", rel="alternate"):
        hreflang = link.get("hreflang")
        if hreflang:
            href = link.get("href", "")
            tech["hreflang_tags"].append({"lang": hreflang, "href": href})
            if normalize_url(href) == normalize_url(page_url):
                tech["hreflang_self_ref"] = True

    # Open Graph
    og_map = {"og:title": "og_title", "og:description": "og_description",
              "og:image": "og_image", "og:url": "og_url", "og:type": "og_type"}
    for prop, key in og_map.items():
        tag = soup.find("meta", attrs={"property": prop})
        if tag and tag.get("content"):
            tech[key] = tag["content"].strip()

    # Twitter Card
    tw_map = {"twitter:card": "twitter_card", "twitter:title": "twitter_title",
              "twitter:description": "twitter_description", "twitter:image": "twitter_image"}
    for name, key in tw_map.items():
        tag = soup.find("meta", attrs={"name": name})
        if not tag:
            tag = soup.find("meta", attrs={"property": name})
        if tag and tag.get("content"):
            tech[key] = tag["content"].strip()

    # Viewport
    viewport = soup.find("meta", attrs={"name": re.compile(r"^viewport$", re.I)})
    if viewport and viewport.get("content"):
        tech["has_viewport"] = True
        tech["viewport_content"] = viewport["content"].strip()

    # Charset
    charset_meta = soup.find("meta", charset=True)
    if charset_meta:
        tech["charset"] = charset_meta["charset"]
    else:
        ct_meta = soup.find("meta", attrs={"http-equiv": re.compile(r"content-type", re.I)})
        if ct_meta and ct_meta.get("content"):
            m = re.search(r"charset=([^\s;]+)", ct_meta["content"], re.I)
            if m:
                tech["charset"] = m.group(1)

    # Language
    html_tag = soup.find("html")
    if html_tag:
        tech["lang"] = html_tag.get("lang", "")

    # Content-Type header
    tech["content_type"] = response.headers.get("Content-Type", "")

    # Redirect detection
    if response.history:
        tech["is_redirect"] = True
        tech["redirect_url"] = response.url

    # Headings hierarchy
    for level in range(1, 7):
        tag_name = f"h{level}"
        count = len(soup.find_all(tag_name))
        tech["headings"][tag_name] = count

    # Check heading hierarchy
    if tech["headings"]["h1"] == 0:
        tech["heading_hierarchy_ok"] = False
        tech["heading_issues"].append("H1 태그 없음")
    elif tech["headings"]["h1"] > 1:
        tech["heading_issues"].append(f"H1 태그 {tech['headings']['h1']}개 (1개 권장)")

    # Check for skipped levels
    found_levels = [i for i in range(1, 7) if tech["headings"][f"h{i}"] > 0]
    if found_levels:
        for i in range(len(found_levels) - 1):
            if found_levels[i + 1] - found_levels[i] > 1:
                tech["heading_hierarchy_ok"] = False
                tech["heading_issues"].append(
                    f"H{found_levels[i]} → H{found_levels[i+1]} (H{found_levels[i]+1} 건너뜀)"
                )

    # iframes
    iframes = soup.find_all("iframe")
    tech["has_iframes"] = len(iframes) > 0
    tech["iframe_count"] = len(iframes)

    # noscript
    tech["has_noscript"] = soup.find("noscript") is not None

    # Canonical
    canon = soup.find("link", attrs={"rel": "canonical"})
    if canon and canon.get("href"):
        tech["canonical"] = canon["href"].strip()

    # Pagination
    rel_next = soup.find("link", attrs={"rel": "next"})
    if rel_next and rel_next.get("href"):
        tech["rel_next"] = rel_next["href"]
    rel_prev = soup.find("link", attrs={"rel": "prev"})
    if rel_prev and rel_prev.get("href"):
        tech["rel_prev"] = rel_prev["href"]

    return tech


# ── Security Headers Detection ──────────────────────────────────────────────
def detect_security(response, soup, page_url):
    """Detect security headers and mixed content."""
    sec = {
        "is_https": urlparse(page_url).scheme == "https",
        "has_hsts": False,
        "hsts_value": "",
        "has_xcto": False,
        "xcto_value": "",
        "has_xfo": False,
        "xfo_value": "",
        "has_csp": False,
        "csp_value": "",
        "has_referrer_policy": False,
        "referrer_policy": "",
        "has_permissions_policy": False,
        "permissions_policy": "",
        "mixed_content": [],
        "mixed_content_count": 0,
    }

    headers = response.headers

    # HSTS
    hsts = headers.get("Strict-Transport-Security", "")
    if hsts:
        sec["has_hsts"] = True
        sec["hsts_value"] = hsts

    # X-Content-Type-Options
    xcto = headers.get("X-Content-Type-Options", "")
    if xcto:
        sec["has_xcto"] = True
        sec["xcto_value"] = xcto

    # X-Frame-Options
    xfo = headers.get("X-Frame-Options", "")
    if xfo:
        sec["has_xfo"] = True
        sec["xfo_value"] = xfo

    # Content-Security-Policy
    csp = headers.get("Content-Security-Policy", "")
    if csp:
        sec["has_csp"] = True
        sec["csp_value"] = csp[:200]

    # Referrer-Policy
    rp = headers.get("Referrer-Policy", "")
    if rp:
        sec["has_referrer_policy"] = True
        sec["referrer_policy"] = rp

    # Permissions-Policy
    pp = headers.get("Permissions-Policy", "")
    if pp:
        sec["has_permissions_policy"] = True
        sec["permissions_policy"] = pp[:200]

    # Mixed content detection (http:// resources on https:// page)
    if sec["is_https"]:
        resource_attrs = [
            ("img", "src"), ("script", "src"), ("link", "href"),
            ("iframe", "src"), ("video", "src"), ("audio", "src"),
            ("source", "src"), ("embed", "src"), ("object", "data"),
        ]
        for tag_name, attr in resource_attrs:
            for el in soup.find_all(tag_name):
                val = el.get(attr, "")
                if val.startswith("http://"):
                    sec["mixed_content"].append(f"<{tag_name}> {val[:100]}")

        mixed_in_css = []
        for style_tag in soup.find_all("style"):
            if style_tag.string and "http://" in style_tag.string:
                mixed_in_css.append("inline <style>")
        sec["mixed_content"].extend(mixed_in_css)
        sec["mixed_content_count"] = len(sec["mixed_content"])

    return sec


# ── Performance Hints Detection ─────────────────────────────────────────────
def detect_performance(soup, response):
    """Detect performance-related hints."""
    perf = {
        "html_size_bytes": 0,
        "html_size_kb": 0,
        "external_scripts": 0,
        "external_stylesheets": 0,
        "inline_css_count": 0,
        "inline_js_count": 0,
        "inline_css_size": 0,
        "inline_js_size": 0,
        "image_count": 0,
        "images_no_lazy": 0,
        "has_compression": False,
        "compression_type": "",
    }

    # HTML size
    content_length = len(response.content)
    perf["html_size_bytes"] = content_length
    perf["html_size_kb"] = round(content_length / 1024, 1)

    # External scripts
    external_scripts = [s for s in soup.find_all("script", src=True)]
    perf["external_scripts"] = len(external_scripts)

    # External stylesheets
    external_css = [l for l in soup.find_all("link", rel="stylesheet")]
    perf["external_stylesheets"] = len(external_css)

    # Inline CSS/JS
    inline_styles = soup.find_all("style")
    perf["inline_css_count"] = len(inline_styles)
    perf["inline_css_size"] = sum(len(s.string or "") for s in inline_styles)

    inline_scripts = [s for s in soup.find_all("script") if not s.get("src")]
    perf["inline_js_count"] = len(inline_scripts)
    perf["inline_js_size"] = sum(len(s.string or "") for s in inline_scripts)

    # Images and lazy loading
    images = soup.find_all("img")
    perf["image_count"] = len(images)
    no_lazy = 0
    for img in images:
        loading = img.get("loading", "").lower()
        if loading != "lazy":
            no_lazy += 1
    perf["images_no_lazy"] = no_lazy

    # Compression
    encoding = response.headers.get("Content-Encoding", "")
    if encoding:
        perf["has_compression"] = True
        perf["compression_type"] = encoding

    return perf


# ── Content Quality Detection ───────────────────────────────────────────────
def detect_content_quality(soup, page_url, base_domain):
    """Detect content quality signals."""
    cq = {
        "external_links": [],
        "external_links_count": 0,
        "nofollow_links_count": 0,
        "total_links_count": 0,
        "nofollow_ratio": 0.0,
        "text_to_html_ratio": 0.0,
        "word_count": 0,
    }

    body = soup.find("body")
    html_text = str(soup)
    body_text = body.get_text(separator=" ", strip=True) if body else ""
    cq["word_count"] = len(body_text.split())

    # Text-to-HTML ratio
    if len(html_text) > 0:
        cq["text_to_html_ratio"] = round(len(body_text) / len(html_text) * 100, 1)

    # Links analysis
    all_links = soup.find_all("a", href=True)
    cq["total_links_count"] = len(all_links)
    nofollow_count = 0
    external = []

    for a in all_links:
        href = a["href"].strip()
        rel = a.get("rel", [])
        if isinstance(rel, str):
            rel = rel.split()
        if "nofollow" in [r.lower() for r in rel]:
            nofollow_count += 1

        full_url = urljoin(page_url, href)
        parsed = urlparse(full_url)
        if parsed.scheme in ("http", "https") and parsed.netloc and parsed.netloc != base_domain:
            external.append(full_url)

    cq["external_links"] = external[:50]  # limit stored
    cq["external_links_count"] = len(external)
    cq["nofollow_links_count"] = nofollow_count
    if cq["total_links_count"] > 0:
        cq["nofollow_ratio"] = round(nofollow_count / cq["total_links_count"] * 100, 1)

    return cq


# ── 페이지 분석 (Comprehensive) ─────────────────────────────────────────────
def analyze_page(url, session, crawl_depth=0):
    result = {
        "URL": url,
        "Status": None,
        "Title": "",
        "Title Len": 0,
        "Meta Desc": "",
        "Desc Len": 0,
        "H1": "",
        "H1 Len": 0,
        "Canonical": "",
        "Words": 0,
        "Images": 0,
        "Alt Missing": 0,
        "Outlinks": 0,
        "Load (s)": 0.0,
        "Error": "",
        "Crawl Depth": crawl_depth,
        "_internal_links": [],
        # Schema
        "_schema": {},
        "Schema Types": "",
        "Has Schema": False,
        # E-E-A-T
        "_eeat": {},
        "Author": "",
        "Has Date": False,
        # Technical
        "_tech": {},
        "Meta Robots": "",
        "Noindex": False,
        "Viewport": False,
        "Lang": "",
        "OG Image": "",
        "Twitter Card": "",
        "Hreflang": 0,
        # Security
        "_security": {},
        "HTTPS": False,
        "HSTS": False,
        "Mixed Content": 0,
        # Performance
        "_perf": {},
        "HTML KB": 0,
        "Ext Scripts": 0,
        "Ext CSS": 0,
        "Img No Lazy": 0,
        "Compression": "",
        # Content
        "_content": {},
        "Ext Links": 0,
        "Text/HTML %": 0.0,
        # Headings
        "H1s": 0, "H2s": 0, "H3s": 0, "H4s": 0, "H5s": 0, "H6s": 0,
    }
    try:
        start = time.time()
        r = session.get(url, timeout=REQUEST_TIMEOUT)
        result["Load (s)"] = round(time.time() - start, 2)
        result["Status"] = r.status_code

        if r.status_code != 200 and not (300 <= r.status_code < 400):
            return result

        soup = BeautifulSoup(r.content, "html.parser")

        # ── Basic SEO ──
        # Title
        tag = soup.find("title")
        if tag and tag.string:
            result["Title"] = tag.string.strip()
            result["Title Len"] = len(result["Title"])

        # Meta Description
        meta = soup.find("meta", attrs={"name": re.compile(r"^description$", re.I)})
        if meta and meta.get("content"):
            result["Meta Desc"] = meta["content"].strip()
            result["Desc Len"] = len(result["Meta Desc"])

        # H1
        h1 = soup.find("h1")
        if h1:
            result["H1"] = h1.get_text(strip=True)
            result["H1 Len"] = len(result["H1"])

        # Canonical
        canon = soup.find("link", attrs={"rel": "canonical"})
        if canon and canon.get("href"):
            result["Canonical"] = canon["href"].strip()

        # Word count
        body = soup.find("body")
        if body:
            text = body.get_text(separator=" ", strip=True)
            result["Words"] = len(text.split())

        # Images
        imgs = soup.find_all("img")
        result["Images"] = len(imgs)
        result["Alt Missing"] = sum(1 for img in imgs if not img.get("alt", "").strip())

        # Internal + external links
        base_domain = urlparse(url).netloc
        internal_links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            full = urljoin(url, href)
            normalized = normalize_url(full)
            if is_same_domain(normalized, base_domain):
                internal_links.add(normalized)
        result["_internal_links"] = sorted(internal_links)
        result["Outlinks"] = len(internal_links)

        # ── Schema / Structured Data ──
        schema_info = detect_schema(soup, url)
        result["_schema"] = schema_info
        result["Schema Types"] = ", ".join(schema_info["all_types"]) if schema_info["all_types"] else ""
        result["Has Schema"] = schema_info["has_schema"]

        # ── E-E-A-T ──
        eeat = detect_eeat(soup, schema_info, url)
        result["_eeat"] = eeat
        result["Author"] = eeat["author_name"][:40] if eeat["author_name"] else ""
        result["Has Date"] = eeat["has_published_date"] or eeat["has_modified_date"]

        # ── Technical SEO ──
        tech = detect_technical_seo(soup, r, url)
        result["_tech"] = tech
        result["Meta Robots"] = tech["meta_robots"]
        result["Noindex"] = tech["is_noindex"]
        result["Viewport"] = tech["has_viewport"]
        result["Lang"] = tech["lang"]
        result["OG Image"] = "Y" if tech["og_image"] else "N"
        result["Twitter Card"] = tech["twitter_card"] if tech["twitter_card"] else ""
        result["Hreflang"] = len(tech["hreflang_tags"])
        result["H1s"] = tech["headings"]["h1"]
        result["H2s"] = tech["headings"]["h2"]
        result["H3s"] = tech["headings"]["h3"]
        result["H4s"] = tech["headings"]["h4"]
        result["H5s"] = tech["headings"]["h5"]
        result["H6s"] = tech["headings"]["h6"]

        # ── Security ──
        sec = detect_security(r, soup, url)
        result["_security"] = sec
        result["HTTPS"] = sec["is_https"]
        result["HSTS"] = sec["has_hsts"]
        result["Mixed Content"] = sec["mixed_content_count"]

        # ── Performance ──
        perf = detect_performance(soup, r)
        result["_perf"] = perf
        result["HTML KB"] = perf["html_size_kb"]
        result["Ext Scripts"] = perf["external_scripts"]
        result["Ext CSS"] = perf["external_stylesheets"]
        result["Img No Lazy"] = perf["images_no_lazy"]
        result["Compression"] = perf["compression_type"] if perf["compression_type"] else "None"

        # ── Content Quality ──
        cq = detect_content_quality(soup, url, base_domain)
        result["_content"] = cq
        result["Ext Links"] = cq["external_links_count"]
        result["Text/HTML %"] = cq["text_to_html_ratio"]

    except Exception as e:
        result["Error"] = str(e)[:120]

    return result


# ── PageSpeed ─────────────────────────────────────────────────────────────────
def get_pagespeed_score(url, api_key):
    if not api_key:
        return None
    try:
        endpoint = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
        r = requests.get(
            endpoint,
            params={"url": url, "strategy": "mobile", "key": api_key},
            timeout=60,
        )
        data = r.json()
        score = data["lighthouseResult"]["categories"]["performance"]["score"]
        return int(score * 100)
    except Exception:
        return None


# ── 사이트 구조 트리 ─────────────────────────────────────────────────────────
def build_tree_string(urls, base_domain):
    tree = {}
    for url in sorted(urls):
        parsed = urlparse(url)
        parts = [p for p in parsed.path.split("/") if p]
        if not parts:
            parts = ["(root)"]
        node = tree
        for part in parts:
            node = node.setdefault(part, {})
    lines = [f"{base_domain}/"]
    def _render(node, prefix=""):
        items = sorted(node.keys())
        for i, name in enumerate(items):
            is_last = i == len(items) - 1
            connector = "└── " if is_last else "├── "
            lines.append(f"{prefix}{connector}{name}")
            extension = "    " if is_last else "│   "
            _render(node[name], prefix + extension)
    _render(tree)
    return "\n".join(lines)


# ── 진단 엔진 (Comprehensive) ──────────────────────────────────────────────
def run_diagnostics(pages, incoming_map, pagespeed_scores):
    issues = []

    # 중복 Title
    title_map = defaultdict(list)
    for p in pages:
        if p["Title"]:
            title_map[p["Title"]].append(p["URL"])
    for title, urls in title_map.items():
        if len(urls) > 1:
            issues.append({
                "type": "중복 Title",
                "severity": "HIGH",
                "detail": f'"{title[:40]}..." → {len(urls)}개 페이지 중복',
                "pages": urls,
                "fix": "각 페이지마다 고유한 Title을 작성하세요.",
            })

    # Check if homepage has Organization schema
    homepage_urls = [p["URL"] for p in pages if urlparse(p["URL"]).path in ("/", "")]
    for hp_url in homepage_urls:
        hp = next((p for p in pages if p["URL"] == hp_url), None)
        if hp and hp.get("_schema"):
            schema_types = hp["_schema"].get("all_types", [])
            has_org = any(t in ("Organization", "Corporation", "LocalBusiness") for t in schema_types)
            if not has_org:
                issues.append({
                    "type": "Organization 스키마 없음",
                    "severity": "MEDIUM",
                    "detail": "홈페이지에 Organization 스키마 없음",
                    "pages": [hp_url],
                    "fix": "홈페이지에 Organization 스키마를 추가하세요.",
                })

    for p in pages:
        url = p["URL"]
        tl = p["Title Len"]
        dl = p["Desc Len"]
        status = p["Status"]

        # Skip non-200 for most checks
        is_ok = status == 200

        # ── Basic SEO checks ──
        if tl == 0:
            issues.append({"type": "Title 없음", "severity": "HIGH", "detail": "Title 태그 없음", "pages": [url], "fix": "고유하고 설명적인 Title 태그를 추가하세요."})
        elif tl < TITLE_MIN or tl > TITLE_MAX:
            issues.append({"type": "Title 길이", "severity": "MEDIUM", "detail": f"Title {tl}자 → {TITLE_MIN}~{TITLE_MAX}자 권장", "pages": [url], "fix": f"현재 {tl}자. {TITLE_MIN}~{TITLE_MAX}자로 조절하세요."})

        if dl == 0:
            issues.append({"type": "Description 없음", "severity": "MEDIUM", "detail": "Meta Description 없음", "pages": [url], "fix": "핵심 키워드와 CTA를 포함한 설명을 추가하세요."})
        elif dl < DESC_MIN or dl > DESC_MAX:
            issues.append({"type": "Description 길이", "severity": "MEDIUM", "detail": f"Description {dl}자 → {DESC_MIN}~{DESC_MAX}자 권장", "pages": [url], "fix": f"현재 {dl}자. {DESC_MIN}~{DESC_MAX}자로 조절하세요."})

        if p["H1 Len"] == 0:
            issues.append({"type": "H1 없음", "severity": "HIGH", "detail": "H1 태그 없음", "pages": [url], "fix": "페이지당 하나의 H1 태그 + 핵심 키워드 포함."})

        if p["Words"] < THIN_CONTENT_THRESHOLD and is_ok:
            issues.append({"type": "Thin Content", "severity": "MEDIUM", "detail": f"단어 수 {p['Words']}개 (최소 {THIN_CONTENT_THRESHOLD} 권장)", "pages": [url], "fix": "FAQ, 관련 정보, 사용자 후기 등으로 콘텐츠 보강."})

        if p["Alt Missing"] > 0:
            issues.append({"type": "Alt 누락", "severity": "LOW", "detail": f"Alt 없는 이미지 {p['Alt Missing']}/{p['Images']}개", "pages": [url], "fix": "모든 이미지에 설명적인 alt 텍스트를 추가하세요."})

        if status and status >= 400:
            issues.append({"type": f"HTTP {status}", "severity": "HIGH", "detail": f"HTTP {status} 에러", "pages": [url], "fix": "깨진 링크 수정 또는 301 리다이렉트 설정."})

        inc = incoming_map.get(url, 0)
        if inc < MIN_INCOMING_LINKS and is_ok:
            issues.append({"type": "Inlinks 부족", "severity": "MEDIUM", "detail": f"들어오는 링크 {inc}개 (최소 {MIN_INCOMING_LINKS} 권장)", "pages": [url], "fix": "내부 링크 추가 (실크 로드 전략 권장)."})

        score = pagespeed_scores.get(url)
        if score is not None and score < PAGESPEED_THRESHOLD:
            issues.append({"type": "PageSpeed 낮음", "severity": "HIGH" if score < 50 else "MEDIUM", "detail": f"Mobile 점수: {score}/100", "pages": [url], "fix": "이미지 최적화, Lazy Loading, 렌더링 차단 리소스 제거."})

        if p["Load (s)"] > 3.0:
            issues.append({"type": "느린 로딩", "severity": "MEDIUM", "detail": f"로드 시간 {p['Load (s)']}초", "pages": [url], "fix": "서버 응답 시간, CDN, 캐싱 설정 점검."})

        # ── Schema checks ──
        schema = p.get("_schema", {})
        if is_ok and not schema.get("has_schema"):
            issues.append({"type": "구조화 데이터 없음", "severity": "MEDIUM", "detail": "JSON-LD/Microdata/RDFa 없음", "pages": [url], "fix": "구조화된 데이터를 추가하세요 (JSON-LD 권장)"})

        if schema.get("has_schema"):
            all_types = schema.get("all_types", [])
            if not any(t == "BreadcrumbList" for t in all_types):
                if is_ok:
                    issues.append({"type": "BreadcrumbList 없음", "severity": "LOW", "detail": "BreadcrumbList 스키마 없음", "pages": [url], "fix": "BreadcrumbList 스키마로 사이트 구조를 표현하세요"})
            for vi in schema.get("validation_issues", []):
                issues.append({"type": "스키마 속성 누락", "severity": "LOW", "detail": vi, "pages": [url], "fix": f"스키마 필수 속성을 추가하세요: {vi}"})

        # ── E-E-A-T checks ──
        eeat = p.get("_eeat", {})
        if is_ok:
            if not eeat.get("has_author"):
                issues.append({"type": "저자 정보 없음", "severity": "LOW", "detail": "author 메타/스키마/바이라인 없음", "pages": [url], "fix": "E-E-A-T: 저자 정보를 추가하세요"})
            if not eeat.get("has_published_date") and not eeat.get("has_modified_date"):
                issues.append({"type": "날짜 정보 없음", "severity": "LOW", "detail": "게시일/수정일 없음", "pages": [url], "fix": "E-E-A-T: 게시일/수정일을 표시하세요"})
            if not eeat.get("has_about_link") and not eeat.get("has_contact_link"):
                issues.append({"type": "About/Contact 없음", "severity": "LOW", "detail": "About/Contact 페이지 링크 없음", "pages": [url], "fix": "E-E-A-T: About/Contact 페이지 링크를 추가하세요"})
            if not eeat.get("has_privacy_link"):
                issues.append({"type": "개인정보처리방침 없음", "severity": "LOW", "detail": "Privacy Policy 링크 없음", "pages": [url], "fix": "E-E-A-T: 개인정보 처리방침 링크를 추가하세요"})

        # ── Technical SEO checks ──
        tech = p.get("_tech", {})
        if is_ok:
            if tech.get("is_noindex"):
                issues.append({"type": "Noindex 설정", "severity": "HIGH", "detail": f"meta robots: {tech.get('meta_robots', '')}", "pages": [url], "fix": "이 페이지가 noindex 설정되어 있습니다"})

            if not tech.get("og_image"):
                issues.append({"type": "OG Image 없음", "severity": "MEDIUM", "detail": "og:image 메타 태그 없음", "pages": [url], "fix": "SNS 공유를 위해 Open Graph 이미지를 설정하세요"})

            if not tech.get("has_viewport"):
                issues.append({"type": "Viewport 없음", "severity": "HIGH", "detail": "viewport 메타 태그 없음", "pages": [url], "fix": "모바일 호환성을 위해 viewport 메타 태그를 추가하세요"})

            if not tech.get("twitter_card"):
                issues.append({"type": "Twitter Card 없음", "severity": "LOW", "detail": "twitter:card 메타 태그 없음", "pages": [url], "fix": "Twitter Card 메타 태그를 추가하세요"})

            if tech.get("url_length", 0) > URL_MAX_LENGTH:
                issues.append({"type": "URL 길이 초과", "severity": "MEDIUM", "detail": f"URL {tech['url_length']}자 (100자 이내 권장)", "pages": [url], "fix": "URL 길이를 100자 이내로 줄이세요"})

            if not tech.get("heading_hierarchy_ok"):
                for hi in tech.get("heading_issues", []):
                    issues.append({"type": "Heading 계층 문제", "severity": "MEDIUM", "detail": hi, "pages": [url], "fix": "H1→H2→H3 순서로 계층 구조를 지키세요"})

            if tech.get("hreflang_tags") and not tech.get("hreflang_self_ref"):
                issues.append({"type": "Hreflang Self-ref 없음", "severity": "LOW", "detail": "hreflang에 자기 참조 없음", "pages": [url], "fix": "hreflang 태그에 자기 참조를 추가하세요"})

        # ── Security checks ──
        sec = p.get("_security", {})
        if is_ok:
            if not sec.get("is_https"):
                issues.append({"type": "HTTPS 미사용", "severity": "HIGH", "detail": "HTTP 사용 중", "pages": [url], "fix": "HTTPS로 전환하세요"})
            elif not sec.get("has_hsts"):
                issues.append({"type": "HSTS 없음", "severity": "MEDIUM", "detail": "Strict-Transport-Security 헤더 없음", "pages": [url], "fix": "HSTS 헤더를 추가하세요"})

            if sec.get("mixed_content_count", 0) > 0:
                issues.append({"type": "Mixed Content", "severity": "HIGH", "detail": f"HTTP 리소스 {sec['mixed_content_count']}개 감지", "pages": [url], "fix": "HTTPS 페이지에서 HTTP 리소스를 로드하고 있습니다"})

        # ── Performance checks ──
        perf = p.get("_perf", {})
        if is_ok:
            if perf.get("html_size_bytes", 0) > PAGE_SIZE_WARN:
                size_mb = round(perf["html_size_bytes"] / (1024 * 1024), 1)
                issues.append({"type": "페이지 용량 과다", "severity": "HIGH", "detail": f"HTML {size_mb}MB (3MB 이하 권장)", "pages": [url], "fix": "페이지 용량이 큽니다. 최적화하세요"})

            if perf.get("images_no_lazy", 0) > 3:
                issues.append({"type": "Lazy Loading 없음", "severity": "LOW", "detail": f"lazy loading 없는 이미지 {perf['images_no_lazy']}개", "pages": [url], "fix": "이미지에 loading='lazy' 속성을 추가하세요"})

            if perf.get("external_scripts", 0) > MAX_EXTERNAL_SCRIPTS:
                issues.append({"type": "외부 스크립트 과다", "severity": "MEDIUM", "detail": f"외부 스크립트 {perf['external_scripts']}개", "pages": [url], "fix": "외부 스크립트가 많습니다. 번들링을 고려하세요"})

            if not perf.get("has_compression"):
                issues.append({"type": "압축 없음", "severity": "MEDIUM", "detail": "Gzip/Brotli 압축 미적용", "pages": [url], "fix": "Gzip/Brotli 압축을 활성화하세요"})

    severity_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    issues.sort(key=lambda x: severity_order.get(x["severity"], 9))
    return issues


# ══════════════════════════════════════════════════════════════════════════════
# UI
# ══════════════════════════════════════════════════════════════════════════════

# ── 헤더 ─────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="sf-header">
    <h1>🔍 SEO Diagnostic Pro</h1>
    <span>Comprehensive SEO Diagnostic &middot; v3.0</span>
</div>
""", unsafe_allow_html=True)

# ── 상단 입력바 ──────────────────────────────────────────────────────────────
with st.container():
    col_url, col_mode, col_max, col_delay, col_btn = st.columns([4, 2, 1, 1, 1])

    with col_url:
        base_url = st.text_input("URL", placeholder="https://example.com", label_visibility="collapsed")
    with col_mode:
        mode = st.selectbox("모드", ["크롤링", "사이트맵"], label_visibility="collapsed")
    with col_max:
        max_pages = st.number_input("Max", min_value=5, max_value=1000, value=50, step=5, label_visibility="collapsed")
    with col_delay:
        crawl_delay = st.number_input("Delay", min_value=0.1, max_value=3.0, value=0.5, step=0.1, label_visibility="collapsed")
    with col_btn:
        run_btn = st.button("▶ Start", use_container_width=True, type="primary")

# ── 사이드바: 설정 ────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ⚙️ 설정")
    api_key = st.text_input(
        "PageSpeed API 키 (선택)",
        placeholder="비워두면 스킵",
        help="Google PageSpeed Insights API 키",
    )
    st.divider()
    st.markdown("### 📌 진단 기준")
    st.caption(f"Title: {TITLE_MIN}~{TITLE_MAX}자")
    st.caption(f"Description: {DESC_MIN}~{DESC_MAX}자")
    st.caption(f"Thin Content: {THIN_CONTENT_THRESHOLD}단어 미만")
    st.caption(f"내부 링크 최소: {MIN_INCOMING_LINKS}개")
    st.caption(f"PageSpeed 기준: {PAGESPEED_THRESHOLD}점")
    st.caption(f"URL 최대 길이: {URL_MAX_LENGTH}자")
    st.caption(f"페이지 용량 경고: 3MB 초과")
    st.caption(f"외부 스크립트 경고: {MAX_EXTERNAL_SCRIPTS}개 초과")
    st.divider()
    st.markdown("### 📖 진단 항목")
    st.markdown("""
    - Schema / 구조화 데이터
    - E-E-A-T 신뢰 신호
    - Technical SEO
    - Security Headers
    - Performance Hints
    - Content Quality
    - Links Analysis
    """)

# ── 메인: 대기 화면 ──────────────────────────────────────────────────────────
if not run_btn:
    st.markdown("---")
    c1, c2, c3, c4 = st.columns(4)
    c1.markdown("""
    #### 📊 실시간 크롤링
    수집과 동시에 데이터 테이블이
    업데이트됩니다
    """)
    c2.markdown("""
    #### 🏷️ Schema & E-E-A-T
    구조화 데이터, 저자 정보,
    신뢰 신호 분석
    """)
    c3.markdown("""
    #### ⚙️ Technical & Security
    Meta Robots, OG, Hreflang,
    HTTPS, HSTS, CSP
    """)
    c4.markdown("""
    #### 📈 Performance
    페이지 크기, 스크립트, 압축,
    이미지 최적화
    """)
    st.stop()

# ── 입력 검증 ─────────────────────────────────────────────────────────────────
if not base_url:
    st.error("URL을 입력해주세요.")
    st.stop()

if not base_url.startswith(("http://", "https://")):
    base_url = "https://" + base_url
base_url = base_url.rstrip("/")
base_domain = urlparse(base_url).netloc
is_sitemap_mode = mode == "사이트맵"
session = build_session()


# ══════════════════════════════════════════════════════════════════════════════
# 실시간 크롤링
# ══════════════════════════════════════════════════════════════════════════════

st.markdown("---")

# 실시간 상태 표시 영역
status_container = st.container()
with status_container:
    stat_cols = st.columns([1, 1, 1, 1, 1])
    ph_crawled = stat_cols[0].empty()
    ph_queued = stat_cols[1].empty()
    ph_elapsed = stat_cols[2].empty()
    ph_eta = stat_cols[3].empty()
    ph_speed = stat_cols[4].empty()

progress_bar = st.progress(0.0)
status_line = st.empty()

# 실시간 데이터 테이블 영역
st.markdown("### 📋 수집 데이터 (실시간)")
table_placeholder = st.empty()

pages = []
all_internal_links = []
crawl_start_time = time.time()
depth_map = {}  # url -> crawl depth


def update_live_display(pages_so_far, current_url, total_target, queue_size):
    """실시간 UI 업데이트"""
    count = len(pages_so_far)
    elapsed = time.time() - crawl_start_time

    speed = count / elapsed if elapsed > 0 else 0
    eta = (total_target - count) / speed if speed > 0 and count < total_target else 0

    pct = min(count / total_target, 1.0) if total_target > 0 else 0
    progress_bar.progress(pct)

    ph_crawled.metric("Crawled", f"{count}/{total_target}")
    ph_queued.metric("Queue", queue_size)
    ph_elapsed.metric("Elapsed", fmt_time(elapsed))
    ph_eta.metric("ETA", fmt_time(eta))
    ph_speed.metric("Speed", f"{speed:.1f} pg/s")

    short_url = current_url if len(current_url) < 80 else current_url[:77] + "..."
    status_line.markdown(
        f'<div class="crawl-status">'
        f'<span class="count">[{count}/{total_target}]</span> '
        f'<span class="url">{short_url}</span> '
        f'<span class="eta">ETA {fmt_time(eta)}</span> '
        f'({pct*100:.0f}%)'
        f'</div>',
        unsafe_allow_html=True,
    )

    if pages_so_far:
        display_cols = ["URL", "Status", "Title", "Title Len", "Desc Len",
                        "H1", "Words", "Outlinks", "Schema Types", "HTTPS",
                        "HTML KB", "Ext Scripts", "Load (s)"]
        df = pd.DataFrame(pages_so_far)
        available = [c for c in display_cols if c in df.columns]
        df_disp = df[available].copy()
        df_disp["URL"] = df_disp["URL"].apply(lambda x: urlparse(x).path or "/")
        table_placeholder.dataframe(df_disp, use_container_width=True, hide_index=True, height=400)


# ── 사이트맵 모드 ────────────────────────────────────────────────────────────
if is_sitemap_mode:
    status_line.markdown('<div class="crawl-status">사이트맵 탐색 중...</div>', unsafe_allow_html=True)
    sitemaps = discover_sitemaps(base_url, session)
    if sitemaps:
        all_urls = set()
        for sm in sitemaps:
            all_urls |= parse_sitemap(sm, session, max_pages, base_domain)
        all_urls = sorted(all_urls)[:max_pages]
        total = len(all_urls)
        status_line.markdown(f'<div class="crawl-status">사이트맵에서 <span class="count">{total}</span>개 URL 발견. 분석 시작...</div>', unsafe_allow_html=True)

        for i, url in enumerate(all_urls):
            page = analyze_page(url, session, crawl_depth=0)
            pages.append(page)
            all_internal_links.extend([(url, link) for link in page["_internal_links"]])
            update_live_display(pages, url, total, total - i - 1)
            time.sleep(crawl_delay)
    else:
        st.warning("사이트맵을 찾지 못했습니다. 크롤링 모드로 전환합니다.")
        is_sitemap_mode = False

# ── 크롤링 모드 ──────────────────────────────────────────────────────────────
if not is_sitemap_mode and not pages:
    visited = set()
    queue = [normalize_url(base_url)]
    depth_map[normalize_url(base_url)] = 0

    while queue and len(visited) < max_pages:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)

        current_depth = depth_map.get(url, 0)
        page = analyze_page(url, session, crawl_depth=current_depth)
        pages.append(page)
        all_internal_links.extend([(url, link) for link in page["_internal_links"]])

        for link in page["_internal_links"]:
            if link not in visited:
                queue.append(link)
                if link not in depth_map:
                    depth_map[link] = current_depth + 1

        update_live_display(pages, url, max_pages, len(queue))
        time.sleep(crawl_delay)

# ── 크롤링 완료 ──────────────────────────────────────────────────────────────
elapsed_total = time.time() - crawl_start_time
progress_bar.progress(1.0)
status_line.markdown(
    f'<div class="crawl-status" style="border-color:#3fb950;">'
    f'✅ <span class="count">크롤링 완료!</span> '
    f'{len(pages)}개 페이지 · {fmt_time(elapsed_total)} 소요'
    f'</div>',
    unsafe_allow_html=True,
)

if not pages:
    st.error("분석할 페이지가 없습니다. URL을 확인해주세요.")
    st.stop()

# ── 들어오는 내부 링크 계산 ───────────────────────────────────────────────────
incoming_map = defaultdict(int)
for p in pages:
    for link in p["_internal_links"]:
        incoming_map[link] += 1

for p in pages:
    p["Inlinks"] = incoming_map.get(p["URL"], 0)

# ── PageSpeed 수집 ────────────────────────────────────────────────────────────
pagespeed_scores = {}
if api_key:
    st.markdown("### ⚡ PageSpeed Insights 수집 중...")
    ps_bar = st.progress(0)
    ps_status = st.empty()
    ok_pages = [p for p in pages if p["Status"] == 200]
    for i, p in enumerate(ok_pages, 1):
        ps_status.text(f"[{i}/{len(ok_pages)}] {p['URL']}")
        ps_bar.progress(i / len(ok_pages))
        score = get_pagespeed_score(p["URL"], api_key)
        if score is not None:
            pagespeed_scores[p["URL"]] = score
            p["PageSpeed"] = score
    ps_bar.empty()
    ps_status.empty()


# ══════════════════════════════════════════════════════════════════════════════
# 결과 대시보드
# ══════════════════════════════════════════════════════════════════════════════

issues = run_diagnostics(pages, incoming_map, pagespeed_scores)

st.markdown("---")

# ── 요약 카드 ─────────────────────────────────────────────────────────────────
high = sum(1 for i in issues if i["severity"] == "HIGH")
med = sum(1 for i in issues if i["severity"] == "MEDIUM")
low = sum(1 for i in issues if i["severity"] == "LOW")
avg_load = round(sum(p["Load (s)"] for p in pages) / len(pages), 2)
avg_words = round(sum(p["Words"] for p in pages) / len(pages))
schema_count = sum(1 for p in pages if p.get("Has Schema"))
https_count = sum(1 for p in pages if p.get("HTTPS"))

cols = st.columns(8)
metrics = [
    ("총 페이지", len(pages), ""),
    ("총 이슈", len(issues), ""),
    ("🔴 HIGH", high, "red"),
    ("🟡 MEDIUM", med, "yellow"),
    ("🟢 LOW", low, "green"),
    ("평균 로딩", f"{avg_load}s", ""),
    ("Schema 있음", f"{schema_count}/{len(pages)}", ""),
    ("HTTPS", f"{https_count}/{len(pages)}", ""),
]
for col, (label, val, cls) in zip(cols, metrics):
    col.markdown(
        f'<div class="summary-card {cls}"><div class="num">{val}</div><div class="label">{label}</div></div>',
        unsafe_allow_html=True,
    )

st.markdown("")

# ── 탭 구성 ──────────────────────────────────────────────────────────────────
tab_all, tab_td, tab_links, tab_schema, tab_eeat, tab_tech, tab_sec, tab_perf, tab_tree, tab_issues, tab_export = st.tabs([
    "📋 All Pages",
    "🏷️ Title & Description",
    "🔗 Links Analysis",
    "📊 Schema Data",
    "🛡️ E-E-A-T",
    "⚙️ Technical SEO",
    "🔒 Security",
    "📈 Performance",
    "🌳 Site Structure",
    "⚠️ All Issues",
    "💾 Export",
])

# ── TAB: All Pages ──────────────────────────────────────────────────────────
with tab_all:
    st.subheader("All Pages")
    display_cols = ["URL", "Status", "Title", "Title Len", "Meta Desc", "Desc Len",
                    "H1", "H1 Len", "Canonical", "Words", "Inlinks", "Outlinks",
                    "Images", "Alt Missing", "Schema Types", "HTTPS", "Noindex",
                    "HTML KB", "Ext Scripts", "Compression", "Load (s)", "Error"]
    if pagespeed_scores:
        for p in pages:
            if "PageSpeed" not in p:
                p["PageSpeed"] = "-"
        display_cols.append("PageSpeed")

    df_all = pd.DataFrame(pages)
    available_cols = [c for c in display_cols if c in df_all.columns]
    st.dataframe(df_all[available_cols], use_container_width=True, hide_index=True, height=500)
    st.caption(f"총 {len(pages)}개 페이지 | 평균 로드 {avg_load}s | 평균 {avg_words}단어 | Schema {schema_count}개 | HTTPS {https_count}개")

# ── TAB: Title & Description ────────────────────────────────────────────────
with tab_td:
    st.subheader("Title & Description Overview")

    td_data = []
    for p in pages:
        tl = p["Title Len"]
        if tl == 0:
            t_status = "❌ 없음"
        elif tl < TITLE_MIN:
            t_status = f"⚠️ 짧음 ({tl}자)"
        elif tl > TITLE_MAX:
            t_status = f"⚠️ 김 ({tl}자)"
        else:
            t_status = f"✅ 적정 ({tl}자)"

        dl = p["Desc Len"]
        if dl == 0:
            d_status = "❌ 없음"
        elif dl < DESC_MIN:
            d_status = f"⚠️ 짧음 ({dl}자)"
        elif dl > DESC_MAX:
            d_status = f"⚠️ 김 ({dl}자)"
        else:
            d_status = f"✅ 적정 ({dl}자)"

        h1_status = f"✅ {p['H1'][:30]}" if p["H1"] else "❌ 없음"

        canon = p["Canonical"]
        if not canon:
            c_status = "❌ 없음"
        elif canon == p["URL"]:
            c_status = "✅ Self"
        else:
            c_status = f"↗️ {canon[:40]}"

        td_data.append({
            "URL": urlparse(p["URL"]).path or "/",
            "Title 상태": t_status,
            "Title": p["Title"][:60],
            "Desc 상태": d_status,
            "Description": p["Meta Desc"][:60],
            "H1 상태": h1_status,
            "Canonical": c_status,
        })

    df_td = pd.DataFrame(td_data)
    st.dataframe(df_td, use_container_width=True, hide_index=True, height=500)

    t_ok = sum(1 for p in pages if TITLE_MIN <= p["Title Len"] <= TITLE_MAX)
    d_ok = sum(1 for p in pages if DESC_MIN <= p["Desc Len"] <= DESC_MAX)
    h_ok = sum(1 for p in pages if p["H1 Len"] > 0)
    c_ok = sum(1 for p in pages if p["Canonical"])

    sc1, sc2, sc3, sc4 = st.columns(4)
    sc1.metric("Title 적정", f"{t_ok}/{len(pages)}")
    sc2.metric("Description 적정", f"{d_ok}/{len(pages)}")
    sc3.metric("H1 있음", f"{h_ok}/{len(pages)}")
    sc4.metric("Canonical 있음", f"{c_ok}/{len(pages)}")

# ── TAB: Links Analysis ─────────────────────────────────────────────────────
with tab_links:
    st.subheader("Internal & External Links")

    link_data = []
    for p in pages:
        url = p["URL"]
        inc = incoming_map.get(url, 0)
        if inc == 0:
            status = "🚨 고아"
        elif inc < MIN_INCOMING_LINKS:
            status = "⚠️ 부족"
        else:
            status = "✅ 양호"

        cq = p.get("_content", {})
        link_data.append({
            "URL": urlparse(url).path or "/",
            "Inlinks": inc,
            "Outlinks (Internal)": p["Outlinks"],
            "External Links": cq.get("external_links_count", 0),
            "Nofollow Links": cq.get("nofollow_links_count", 0),
            "Nofollow %": cq.get("nofollow_ratio", 0),
            "Total Links": cq.get("total_links_count", 0),
            "상태": status,
        })

    df_links = pd.DataFrame(link_data)
    df_links = df_links.sort_values("Inlinks", ascending=True)
    st.dataframe(df_links, use_container_width=True, hide_index=True, height=400)

    orphan_count = sum(1 for p in pages if incoming_map.get(p["URL"], 0) == 0)
    weak_count = sum(1 for p in pages if 0 < incoming_map.get(p["URL"], 0) < MIN_INCOMING_LINKS)
    total_ext = sum(p.get("_content", {}).get("external_links_count", 0) for p in pages)

    lc1, lc2, lc3, lc4 = st.columns(4)
    lc1.metric("🚨 고아 페이지", orphan_count)
    lc2.metric("⚠️ 링크 부족", weak_count)
    lc3.metric("✅ 양호", len(pages) - orphan_count - weak_count)
    lc4.metric("🔗 외부 링크 (전체)", total_ext)

    # External links detail
    with st.expander("외부 링크 상세 목록"):
        ext_detail = []
        for p in pages:
            cq = p.get("_content", {})
            for elink in cq.get("external_links", []):
                ext_detail.append({
                    "Source": urlparse(p["URL"]).path or "/",
                    "External URL": elink[:120],
                })
        if ext_detail:
            st.dataframe(pd.DataFrame(ext_detail), use_container_width=True, hide_index=True, height=300)
        else:
            st.info("외부 링크가 없습니다.")

# ── TAB: Schema Data ────────────────────────────────────────────────────────
with tab_schema:
    st.subheader("Schema / Structured Data")

    schema_data = []
    for p in pages:
        schema = p.get("_schema", {})
        json_ld_count = len(schema.get("json_ld", []))
        micro_count = len(schema.get("microdata_types", []))
        rdfa_count = len(schema.get("rdfa_types", []))

        if schema.get("has_schema"):
            status = "✅ 있음"
        else:
            status = "❌ 없음"

        schema_data.append({
            "URL": urlparse(p["URL"]).path or "/",
            "상태": status,
            "Schema Types": ", ".join(schema.get("all_types", [])) or "-",
            "JSON-LD": json_ld_count,
            "Microdata": micro_count,
            "RDFa": rdfa_count,
            "검증 이슈": "; ".join(schema.get("validation_issues", [])) or "-",
        })

    df_schema = pd.DataFrame(schema_data)
    st.dataframe(df_schema, use_container_width=True, hide_index=True, height=400)

    # Schema type distribution
    all_schema_types = []
    for p in pages:
        all_schema_types.extend(p.get("_schema", {}).get("all_types", []))

    if all_schema_types:
        st.markdown("#### Schema Type 분포")
        type_counts = defaultdict(int)
        for t in all_schema_types:
            type_counts[t] += 1
        type_df = pd.DataFrame([
            {"Type": k, "Count": v} for k, v in sorted(type_counts.items(), key=lambda x: -x[1])
        ])
        st.dataframe(type_df, use_container_width=True, hide_index=True)

    no_schema = sum(1 for p in pages if not p.get("Has Schema") and p.get("Status") == 200)
    has_schema_n = sum(1 for p in pages if p.get("Has Schema"))
    sc1, sc2 = st.columns(2)
    sc1.metric("✅ Schema 있음", has_schema_n)
    sc2.metric("❌ Schema 없음 (200 OK)", no_schema)

# ── TAB: E-E-A-T ────────────────────────────────────────────────────────────
with tab_eeat:
    st.subheader("E-E-A-T Trust Signals")

    eeat_data = []
    for p in pages:
        eeat = p.get("_eeat", {})
        social = ", ".join(eeat.get("social_links", [])) or "-"

        eeat_data.append({
            "URL": urlparse(p["URL"]).path or "/",
            "Author": eeat.get("author_name", "")[:30] or "❌",
            "Published Date": "✅" if eeat.get("has_published_date") else "❌",
            "Modified Date": "✅" if eeat.get("has_modified_date") else "❌",
            "About Link": "✅" if eeat.get("has_about_link") else "❌",
            "Contact Link": "✅" if eeat.get("has_contact_link") else "❌",
            "Privacy Policy": "✅" if eeat.get("has_privacy_link") else "❌",
            "Terms of Service": "✅" if eeat.get("has_terms_link") else "❌",
            "Org Schema": "✅" if eeat.get("has_org_schema") else "❌",
            "Breadcrumb": "✅" if eeat.get("has_breadcrumb") else "❌",
            "Reviews Schema": "✅" if eeat.get("has_reviews_schema") else "❌",
            "Social Links": social,
        })

    df_eeat = pd.DataFrame(eeat_data)
    st.dataframe(df_eeat, use_container_width=True, hide_index=True, height=400)

    # E-E-A-T summary
    ok_pages_list = [p for p in pages if p.get("Status") == 200]
    total_ok = len(ok_pages_list)
    if total_ok > 0:
        author_pct = round(sum(1 for p in ok_pages_list if p.get("_eeat", {}).get("has_author")) / total_ok * 100)
        date_pct = round(sum(1 for p in ok_pages_list if p.get("_eeat", {}).get("has_published_date")) / total_ok * 100)
        privacy_pct = round(sum(1 for p in ok_pages_list if p.get("_eeat", {}).get("has_privacy_link")) / total_ok * 100)
        breadcrumb_pct = round(sum(1 for p in ok_pages_list if p.get("_eeat", {}).get("has_breadcrumb")) / total_ok * 100)

        ec1, ec2, ec3, ec4 = st.columns(4)
        ec1.metric("저자 정보", f"{author_pct}%")
        ec2.metric("게시일 정보", f"{date_pct}%")
        ec3.metric("개인정보처리방침", f"{privacy_pct}%")
        ec4.metric("Breadcrumb", f"{breadcrumb_pct}%")

# ── TAB: Technical SEO ──────────────────────────────────────────────────────
with tab_tech:
    st.subheader("Technical SEO")

    tech_data = []
    for p in pages:
        tech = p.get("_tech", {})

        tech_data.append({
            "URL": urlparse(p["URL"]).path or "/",
            "Meta Robots": tech.get("meta_robots", "") or "-",
            "Noindex": "⚠️ YES" if tech.get("is_noindex") else "✅ No",
            "Viewport": "✅" if tech.get("has_viewport") else "❌",
            "Lang": tech.get("lang", "") or "-",
            "Charset": tech.get("charset", "") or "-",
            "Canonical": tech.get("canonical", "")[:40] or "-",
            "OG Title": "✅" if tech.get("og_title") else "❌",
            "OG Image": "✅" if tech.get("og_image") else "❌",
            "OG Type": tech.get("og_type", "") or "-",
            "Twitter Card": tech.get("twitter_card", "") or "-",
            "Hreflang": len(tech.get("hreflang_tags", [])),
            "Self-ref Hreflang": "✅" if tech.get("hreflang_self_ref") else ("-" if not tech.get("hreflang_tags") else "❌"),
            "URL Length": tech.get("url_length", 0),
            "Query Params": "Yes" if tech.get("has_query_params") else "No",
            "Redirect": "Yes" if tech.get("is_redirect") else "No",
            "iframes": tech.get("iframe_count", 0),
            "noscript": "Yes" if tech.get("has_noscript") else "No",
        })

    df_tech = pd.DataFrame(tech_data)
    st.dataframe(df_tech, use_container_width=True, hide_index=True, height=400)

    # Heading hierarchy detail
    st.markdown("#### Heading Hierarchy (H1-H6)")
    heading_data = []
    for p in pages:
        tech = p.get("_tech", {})
        headings = tech.get("headings", {})
        hierarchy_ok = tech.get("heading_hierarchy_ok", True)
        heading_issues = tech.get("heading_issues", [])

        heading_data.append({
            "URL": urlparse(p["URL"]).path or "/",
            "H1": headings.get("h1", 0),
            "H2": headings.get("h2", 0),
            "H3": headings.get("h3", 0),
            "H4": headings.get("h4", 0),
            "H5": headings.get("h5", 0),
            "H6": headings.get("h6", 0),
            "계층 구조": "✅" if hierarchy_ok else "❌",
            "이슈": "; ".join(heading_issues) or "-",
        })

    df_headings = pd.DataFrame(heading_data)
    st.dataframe(df_headings, use_container_width=True, hide_index=True, height=300)

    # Open Graph detail
    with st.expander("Open Graph & Twitter Card 상세"):
        og_data = []
        for p in pages:
            tech = p.get("_tech", {})
            og_data.append({
                "URL": urlparse(p["URL"]).path or "/",
                "og:title": tech.get("og_title", "")[:50] or "-",
                "og:description": tech.get("og_description", "")[:50] or "-",
                "og:image": tech.get("og_image", "")[:60] or "-",
                "og:url": tech.get("og_url", "")[:60] or "-",
                "og:type": tech.get("og_type", "") or "-",
                "twitter:card": tech.get("twitter_card", "") or "-",
                "twitter:title": tech.get("twitter_title", "")[:50] or "-",
                "twitter:image": tech.get("twitter_image", "")[:60] or "-",
            })
        st.dataframe(pd.DataFrame(og_data), use_container_width=True, hide_index=True, height=300)

    # Technical summary
    noindex_count = sum(1 for p in pages if p.get("_tech", {}).get("is_noindex"))
    viewport_count = sum(1 for p in pages if p.get("_tech", {}).get("has_viewport"))
    og_image_count = sum(1 for p in pages if p.get("_tech", {}).get("og_image"))

    tc1, tc2, tc3, tc4 = st.columns(4)
    tc1.metric("⚠️ Noindex", noindex_count)
    tc2.metric("Viewport 설정", f"{viewport_count}/{len(pages)}")
    tc3.metric("OG Image 설정", f"{og_image_count}/{len(pages)}")
    tc4.metric("평균 URL 길이", round(sum(p.get("_tech", {}).get("url_length", 0) for p in pages) / len(pages)))

# ── TAB: Security ────────────────────────────────────────────────────────────
with tab_sec:
    st.subheader("Security Headers & HTTPS")

    sec_data = []
    for p in pages:
        sec = p.get("_security", {})

        sec_data.append({
            "URL": urlparse(p["URL"]).path or "/",
            "HTTPS": "✅" if sec.get("is_https") else "❌",
            "HSTS": "✅" if sec.get("has_hsts") else "❌",
            "X-Content-Type-Options": "✅" if sec.get("has_xcto") else "❌",
            "X-Frame-Options": "✅" if sec.get("has_xfo") else "❌",
            "CSP": "✅" if sec.get("has_csp") else "❌",
            "Referrer-Policy": "✅" if sec.get("has_referrer_policy") else "❌",
            "Permissions-Policy": "✅" if sec.get("has_permissions_policy") else "❌",
            "Mixed Content": sec.get("mixed_content_count", 0),
        })

    df_sec = pd.DataFrame(sec_data)
    st.dataframe(df_sec, use_container_width=True, hide_index=True, height=400)

    # Mixed content details
    mixed_pages = [p for p in pages if p.get("_security", {}).get("mixed_content_count", 0) > 0]
    if mixed_pages:
        with st.expander(f"Mixed Content 상세 ({len(mixed_pages)}개 페이지)"):
            mc_detail = []
            for p in mixed_pages:
                for mc in p["_security"].get("mixed_content", []):
                    mc_detail.append({
                        "Page": urlparse(p["URL"]).path or "/",
                        "HTTP Resource": mc,
                    })
            st.dataframe(pd.DataFrame(mc_detail), use_container_width=True, hide_index=True, height=300)

    # Security summary
    ok_sec_pages = [p for p in pages if p.get("Status") == 200]
    total_sec = len(ok_sec_pages) if ok_sec_pages else 1

    https_n = sum(1 for p in ok_sec_pages if p.get("_security", {}).get("is_https"))
    hsts_n = sum(1 for p in ok_sec_pages if p.get("_security", {}).get("has_hsts"))
    csp_n = sum(1 for p in ok_sec_pages if p.get("_security", {}).get("has_csp"))
    mixed_n = sum(1 for p in ok_sec_pages if p.get("_security", {}).get("mixed_content_count", 0) > 0)

    sc1, sc2, sc3, sc4 = st.columns(4)
    sc1.metric("HTTPS", f"{https_n}/{total_sec}")
    sc2.metric("HSTS", f"{hsts_n}/{total_sec}")
    sc3.metric("CSP", f"{csp_n}/{total_sec}")
    sc4.metric("🚨 Mixed Content", mixed_n)

# ── TAB: Performance ─────────────────────────────────────────────────────────
with tab_perf:
    st.subheader("Performance Hints")

    perf_data = []
    for p in pages:
        perf = p.get("_perf", {})

        perf_data.append({
            "URL": urlparse(p["URL"]).path or "/",
            "HTML Size (KB)": perf.get("html_size_kb", 0),
            "External Scripts": perf.get("external_scripts", 0),
            "External CSS": perf.get("external_stylesheets", 0),
            "Inline CSS": perf.get("inline_css_count", 0),
            "Inline JS": perf.get("inline_js_count", 0),
            "Inline CSS Size": f"{round(perf.get('inline_css_size', 0)/1024, 1)}KB",
            "Inline JS Size": f"{round(perf.get('inline_js_size', 0)/1024, 1)}KB",
            "Images": perf.get("image_count", 0),
            "No Lazy Loading": perf.get("images_no_lazy", 0),
            "Compression": perf.get("compression_type", "") or "❌ None",
            "Load (s)": p.get("Load (s)", 0),
        })

    df_perf = pd.DataFrame(perf_data)
    st.dataframe(df_perf, use_container_width=True, hide_index=True, height=400)

    # Performance summary
    avg_size = round(sum(p.get("_perf", {}).get("html_size_kb", 0) for p in pages) / len(pages), 1)
    avg_scripts = round(sum(p.get("_perf", {}).get("external_scripts", 0) for p in pages) / len(pages), 1)
    compressed_n = sum(1 for p in pages if p.get("_perf", {}).get("has_compression"))
    lazy_issue_n = sum(1 for p in pages if p.get("_perf", {}).get("images_no_lazy", 0) > 3)

    pc1, pc2, pc3, pc4 = st.columns(4)
    pc1.metric("평균 HTML 크기", f"{avg_size} KB")
    pc2.metric("평균 외부 스크립트", f"{avg_scripts}")
    pc3.metric("압축 적용", f"{compressed_n}/{len(pages)}")
    pc4.metric("Lazy Loading 필요", lazy_issue_n)

# ── TAB: Site Structure ──────────────────────────────────────────────────────
with tab_tree:
    st.subheader("Site Structure")
    tree_str = build_tree_string([p["URL"] for p in pages], base_domain)
    st.code(tree_str, language=None)

    # Crawl depth info
    if any(p.get("Crawl Depth", 0) > 0 for p in pages):
        st.markdown("#### Crawl Depth 분포")
        depth_dist = defaultdict(int)
        for p in pages:
            depth_dist[p.get("Crawl Depth", 0)] += 1
        depth_df = pd.DataFrame([
            {"Depth": k, "Pages": v} for k, v in sorted(depth_dist.items())
        ])
        st.dataframe(depth_df, use_container_width=True, hide_index=True)

# ── TAB: All Issues (진단 리포트) ───────────────────────────────────────────
with tab_issues:
    st.subheader(f"Issues — {len(issues)}건")

    fc1, fc2 = st.columns([1, 3])
    with fc1:
        sev_filter = st.multiselect("심각도", ["HIGH", "MEDIUM", "LOW"], default=["HIGH", "MEDIUM", "LOW"])
    with fc2:
        type_options = sorted(set(i["type"] for i in issues))
        type_filter = st.multiselect("유형", type_options, default=type_options)

    filtered = [i for i in issues if i["severity"] in sev_filter and i["type"] in type_filter]

    issue_table = []
    for i in filtered:
        badge = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}.get(i["severity"], "")
        issue_table.append({
            "심각도": f"{badge} {i['severity']}",
            "유형": i["type"],
            "내용": i["detail"],
            "조치": i["fix"],
            "URL": urlparse(i["pages"][0]).path if i["pages"] else "",
            "영향 페이지": len(i["pages"]),
        })

    if issue_table:
        df_issues = pd.DataFrame(issue_table)
        st.dataframe(df_issues, use_container_width=True, hide_index=True, height=500)
    else:
        st.success("이슈가 없습니다!")

    st.markdown("#### 유형별 요약")
    type_summary = defaultdict(lambda: {"HIGH": 0, "MEDIUM": 0, "LOW": 0, "total": 0})
    for i in issues:
        type_summary[i["type"]][i["severity"]] += 1
        type_summary[i["type"]]["total"] += 1

    summary_rows = []
    for t, counts in sorted(type_summary.items(), key=lambda x: -x[1]["total"]):
        summary_rows.append({
            "유형": t,
            "🔴 HIGH": counts["HIGH"],
            "🟡 MEDIUM": counts["MEDIUM"],
            "🟢 LOW": counts["LOW"],
            "합계": counts["total"],
        })
    if summary_rows:
        st.dataframe(pd.DataFrame(summary_rows), use_container_width=True, hide_index=True)

# ── TAB: Export ──────────────────────────────────────────────────────────────
with tab_export:
    st.subheader("Export")

    # Prepare export-safe pages (remove internal objects)
    def clean_page_for_export(p):
        clean = {}
        for k, v in p.items():
            if k.startswith("_"):
                continue
            clean[k] = v
        # Add flattened schema/eeat/tech/security/performance info
        schema = p.get("_schema", {})
        clean["Schema_Types"] = ", ".join(schema.get("all_types", []))
        clean["Schema_JSON_LD_Count"] = len(schema.get("json_ld", []))
        clean["Schema_Microdata_Count"] = len(schema.get("microdata_types", []))
        clean["Schema_RDFa_Count"] = len(schema.get("rdfa_types", []))

        eeat = p.get("_eeat", {})
        clean["EEAT_Author"] = eeat.get("author_name", "")
        clean["EEAT_Has_Date"] = eeat.get("has_published_date", False)
        clean["EEAT_About_Link"] = eeat.get("has_about_link", False)
        clean["EEAT_Contact_Link"] = eeat.get("has_contact_link", False)
        clean["EEAT_Privacy_Link"] = eeat.get("has_privacy_link", False)
        clean["EEAT_Social_Links"] = ", ".join(eeat.get("social_links", []))
        clean["EEAT_Breadcrumb"] = eeat.get("has_breadcrumb", False)

        tech = p.get("_tech", {})
        clean["Tech_Meta_Robots"] = tech.get("meta_robots", "")
        clean["Tech_OG_Title"] = tech.get("og_title", "")
        clean["Tech_OG_Image"] = tech.get("og_image", "")
        clean["Tech_Twitter_Card"] = tech.get("twitter_card", "")
        clean["Tech_Hreflang_Count"] = len(tech.get("hreflang_tags", []))
        clean["Tech_Heading_H1"] = tech.get("headings", {}).get("h1", 0)
        clean["Tech_Heading_H2"] = tech.get("headings", {}).get("h2", 0)

        sec = p.get("_security", {})
        clean["Sec_HTTPS"] = sec.get("is_https", False)
        clean["Sec_HSTS"] = sec.get("has_hsts", False)
        clean["Sec_CSP"] = sec.get("has_csp", False)
        clean["Sec_Mixed_Content"] = sec.get("mixed_content_count", 0)

        perf = p.get("_perf", {})
        clean["Perf_HTML_KB"] = perf.get("html_size_kb", 0)
        clean["Perf_Ext_Scripts"] = perf.get("external_scripts", 0)
        clean["Perf_Ext_CSS"] = perf.get("external_stylesheets", 0)
        clean["Perf_Images_No_Lazy"] = perf.get("images_no_lazy", 0)
        clean["Perf_Compression"] = perf.get("compression_type", "")

        cq = p.get("_content", {})
        clean["Content_Ext_Links"] = cq.get("external_links_count", 0)
        clean["Content_Text_HTML_Ratio"] = cq.get("text_to_html_ratio", 0)

        return clean

    report = {
        "generated_at": datetime.now().isoformat(),
        "base_url": base_url,
        "total_pages": len(pages),
        "crawl_time_seconds": round(elapsed_total, 1),
        "mode": "sitemap" if is_sitemap_mode else "crawl",
        "summary": {"high": high, "medium": med, "low": low, "total": len(issues)},
        "pages": [clean_page_for_export(p) for p in pages],
        "issues": issues,
    }

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")

    ec1, ec2 = st.columns(2)

    with ec1:
        json_str = json.dumps(report, ensure_ascii=False, indent=2, default=str)
        st.download_button(
            "📥 JSON 다운로드",
            data=json_str,
            file_name=f"seo_report_{timestamp}.json",
            mime="application/json",
            use_container_width=True,
        )

    with ec2:
        export_pages = [clean_page_for_export(p) for p in pages]
        df_export = pd.DataFrame(export_pages)
        csv_str = df_export.to_csv(index=False)
        st.download_button(
            "📥 CSV 다운로드",
            data=csv_str,
            file_name=f"seo_report_{timestamp}.csv",
            mime="text/csv",
            use_container_width=True,
        )

    with st.expander("JSON 미리보기"):
        st.json(report)
