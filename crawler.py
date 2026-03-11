#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
crawler.py — SEO Diagnostic Pro 크롤링/분석 엔진
UI와 완전히 분리된 재사용 가능한 모듈.
모든 분석 로직을 포함하며, progress_callback을 통해 UI 업데이트 가능.
"""

import re
import json
import time
import math
import os
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urlparse, urljoin, urldefrag, parse_qs
from collections import defaultdict

# Playwright (헤드리스 브라우저) — WAF 차단 사이트 대응
_PLAYWRIGHT_AVAILABLE = False
try:
    from playwright.sync_api import sync_playwright
    _PLAYWRIGHT_AVAILABLE = True
except ImportError:
    pass

# Streamlit Cloud 등 서버 환경에서 Playwright 브라우저 자동 설치
if _PLAYWRIGHT_AVAILABLE:
    import subprocess
    try:
        subprocess.run(
            ["playwright", "install", "chromium"],
            capture_output=True, timeout=120
        )
    except Exception:
        pass


_WAF_SIGNATURES = [
    "unable to give you access",
    "access denied",
    "just a moment",          # Cloudflare
    "checking your browser",  # Cloudflare
    "akamai",
    "incapsula",
    "blocked",
    "captcha",
    "challenge",
    "bot detection",
    "human verification",
]


def _is_waf_page(html):
    """HTML이 WAF 챌린지/차단 페이지인지 감지"""
    if not html:
        return True
    lower = html.lower()
    # 콘텐츠가 너무 짧으면 WAF 가능성 높음
    if len(html) < 2000:
        return True
    for sig in _WAF_SIGNATURES:
        if sig in lower:
            # 실제 콘텐츠에 이 단어가 있을 수 있으므로 body 텍스트 길이도 확인
            soup = BeautifulSoup(html, "html.parser")
            body = soup.find("body")
            if body and len(body.get_text(strip=True)) < 200:
                return True
    return False


def _fetch_with_playwright(url, timeout=30, max_retries=2):
    """Playwright 헤드리스 브라우저로 페이지를 가져옵니다. WAF 차단 우회용."""
    if not _PLAYWRIGHT_AVAILABLE:
        return None, 0
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                ]
            )
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                locale="ko-KR",
                viewport={"width": 1920, "height": 1080},
                java_script_enabled=True,
            )
            # 자동화 감지 우회
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                Object.defineProperty(navigator, 'languages', {get: () => ['ko-KR', 'ko', 'en-US', 'en']});
                window.chrome = {runtime: {}};
            """)
            page = context.new_page()
            start = time.time()
            page.goto(url, wait_until="domcontentloaded", timeout=timeout * 1000)

            # WAF JS 챌린지 통과 대기 — 단계적으로 대기 시간 늘림
            html = None
            for wait_sec in [3, 5, 8]:
                page.wait_for_timeout(wait_sec * 1000)
                html = page.content()
                if not _is_waf_page(html):
                    break

            load_time = round(time.time() - start, 2)
            browser.close()

            if html and not _is_waf_page(html):
                return html, load_time
            # WAF를 못 뚫었어도 받은 HTML은 반환 (일부 분석 가능)
            if html and len(html) > 500:
                return html, load_time
            return None, 0
    except Exception:
        return None, 0


# ══════════════════════════════════════════════════════════════════════════════
# 1. Constants — 설정 상수
# ══════════════════════════════════════════════════════════════════════════════

# 크롤 봇 User-Agent 사전
USER_AGENTS = {
    "Chrome (기본)": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Googlebot": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Googlebot (스마트폰)": "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Bingbot": "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
    "GPTBot (OpenAI)": "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; GPTBot/1.2; +https://openai.com/gptbot)",
    "ChatGPT-User": "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; ChatGPT-User/1.0; +https://openai.com/bot)",
    "ClaudeBot (Anthropic)": "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; ClaudeBot/1.0; +https://www.anthropic.com)",
    "Google-Extended (AI학습)": "Mozilla/5.0 (compatible; Google-Extended; +https://developers.google.com/search/docs/crawling-indexing/overview-google-crawlers)",
    "Yandex Bot": "Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)",
    "Naver Bot": "Mozilla/5.0 (compatible; Yeti/1.1; +http://naver.me/spd)",
    "Samsung Internet": "Mozilla/5.0 (Linux; Android 13; SAMSUNG SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/23.0 Chrome/115.0.0.0 Mobile Safari/537.36",
    "Safari (iPhone)": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Safari (Mac)": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Firefox": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Edge": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
}

USER_AGENT = USER_AGENTS["Chrome (기본)"]
REQUEST_TIMEOUT = 15
TITLE_MIN, TITLE_MAX = 30, 60
DESC_MIN, DESC_MAX = 120, 160
THIN_CONTENT_THRESHOLD = 300
MIN_INCOMING_LINKS = 3
PAGESPEED_THRESHOLD = 90
URL_MAX_LENGTH = 100
PAGE_SIZE_WARN = 3 * 1024 * 1024  # 3MB
MAX_EXTERNAL_SCRIPTS = 15

# Schema 필수 속성 매핑
COMMON_SCHEMA_REQUIRED = {
    "Article": ["headline", "author", "datePublished"],
    "Product": ["name", "image"],
    "LocalBusiness": ["name", "address"],
    "Organization": ["name", "url"],
    "BreadcrumbList": ["itemListElement"],
    "FAQPage": ["mainEntity"],
    "WebSite": ["name", "url"],
    "Person": ["name"],
}

# E-E-A-T 소셜 패턴
SOCIAL_PATTERNS = {
    "facebook": re.compile(r"facebook\.com/", re.I),
    "twitter": re.compile(r"(twitter|x)\.com/", re.I),
    "linkedin": re.compile(r"linkedin\.com/", re.I),
    "youtube": re.compile(r"youtube\.com/", re.I),
    "instagram": re.compile(r"instagram\.com/", re.I),
}


# ══════════════════════════════════════════════════════════════════════════════
# 2. Utility functions — 유틸리티
# ══════════════════════════════════════════════════════════════════════════════

def normalize_url(url):
    """URL 정규화: fragment 제거, trailing slash 정리"""
    url, _ = urldefrag(url)
    parsed = urlparse(url)
    path = parsed.path.rstrip("/") or "/"
    return f"{parsed.scheme}://{parsed.netloc}{path}" + (f"?{parsed.query}" if parsed.query else "")


def is_same_domain(url, base_domain):
    """URL이 동일 도메인인지 확인"""
    try:
        return urlparse(url).netloc == base_domain
    except:
        return False


def build_session(user_agent=None):
    """크롤링용 requests.Session 생성 — 실제 브라우저처럼 동작"""
    ua = user_agent or USER_AGENT
    s = requests.Session()
    s.headers.update({
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    })
    s.verify = False
    return s


def fmt_time(seconds):
    """초를 MM:SS 형식으로 변환"""
    if seconds < 0:
        return "--:--"
    m, s = divmod(int(seconds), 60)
    return f"{m:02d}:{s:02d}"


# ══════════════════════════════════════════════════════════════════════════════
# 3. Discovery functions — 사이트맵 탐색
# ══════════════════════════════════════════════════════════════════════════════

def discover_sitemaps(base_url, session):
    """robots.txt에서 사이트맵 URL을 찾고, 없으면 기본 경로를 시도"""
    sitemaps = []
    try:
        r = session.get(urljoin(base_url, "/robots.txt"), timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            for line in r.text.splitlines():
                if line.strip().lower().startswith("sitemap:"):
                    sitemaps.append(line.split(":", 1)[1].strip())
    except:
        pass
    if not sitemaps:
        for path in ["/sitemap.xml", "/sitemap_index.xml"]:
            try:
                r = session.head(urljoin(base_url, path), timeout=REQUEST_TIMEOUT)
                if r.status_code == 200:
                    sitemaps.append(urljoin(base_url, path))
                    break
            except:
                pass
    return sitemaps


def parse_sitemap(sitemap_url, session, max_pages, base_domain):
    """사이트맵 XML을 파싱하여 URL 집합 반환 (재귀적으로 sitemap index 처리)"""
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
                    n = normalize_url(loc.text.strip())
                    if is_same_domain(n, base_domain):
                        urls.add(n)
        except:
            pass

    _parse(sitemap_url)
    return urls


# ══════════════════════════════════════════════════════════════════════════════
# 4. Detection functions — SEO 분석 감지 로직
# ══════════════════════════════════════════════════════════════════════════════

def detect_schema(soup, url):
    """구조화 데이터 (JSON-LD, Microdata, RDFa) 감지 및 검증"""
    info = {
        "json_ld": [], "json_ld_types": [], "microdata_types": [],
        "rdfa_types": [], "all_types": [], "has_schema": False,
        "validation_issues": [],
    }
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            items = data if isinstance(data, list) else [data]
            expanded = []
            for item in items:
                if isinstance(item, dict) and "@graph" in item:
                    expanded.extend(item["@graph"])
                else:
                    expanded.append(item)
            for item in expanded:
                if not isinstance(item, dict):
                    continue
                st_type = item.get("@type", "Unknown")
                types = st_type if isinstance(st_type, list) else [st_type]
                info["json_ld_types"].extend(types)
                info["json_ld"].append(item)
                for t in types:
                    if t in COMMON_SCHEMA_REQUIRED:
                        for prop in COMMON_SCHEMA_REQUIRED[t]:
                            if prop not in item:
                                info["validation_issues"].append(f"{t}: '{prop}' 누락")
        except:
            pass
    for el in soup.find_all(attrs={"itemtype": True}):
        itype = el.get("itemtype", "").rstrip("/").split("/")[-1]
        if itype:
            info["microdata_types"].append(itype)
    for el in soup.find_all(attrs={"typeof": True}):
        for t in el.get("typeof", "").split():
            info["rdfa_types"].append(t.split(":")[-1] if ":" in t else t)
    info["all_types"] = list(set(info["json_ld_types"] + info["microdata_types"] + info["rdfa_types"]))
    info["has_schema"] = len(info["all_types"]) > 0
    return info


def detect_eeat(soup, schema_info, url):
    """E-E-A-T (경험, 전문성, 권위, 신뢰성) 신호 감지"""
    e = {
        "has_author": False, "author_name": "", "has_published_date": False,
        "has_modified_date": False, "has_about_link": False, "has_contact_link": False,
        "has_privacy_link": False, "has_terms_link": False, "has_org_schema": False,
        "social_links": [], "has_breadcrumb": False, "has_reviews_schema": False,
    }
    am = soup.find("meta", attrs={"name": re.compile(r"^author$", re.I)})
    if am and am.get("content"):
        e["has_author"] = True
        e["author_name"] = am["content"].strip()
    for item in schema_info.get("json_ld", []):
        if isinstance(item, dict):
            if item.get("@type") == "Person":
                e["has_author"] = True
                e["author_name"] = item.get("name", e["author_name"])
            if "author" in item:
                e["has_author"] = True
                a = item["author"]
                if isinstance(a, dict):
                    e["author_name"] = a.get("name", e["author_name"])
                elif isinstance(a, str):
                    e["author_name"] = a
            if item.get("datePublished"):
                e["has_published_date"] = True
            if item.get("dateModified"):
                e["has_modified_date"] = True
    if not e["has_author"]:
        bl = soup.find(class_=re.compile(r"(author|byline|writer)", re.I))
        if bl:
            e["has_author"] = True
            e["author_name"] = bl.get_text(strip=True)[:80]
        al = soup.find("a", rel="author")
        if al:
            e["has_author"] = True
            e["author_name"] = al.get_text(strip=True)[:80]
    if soup.find("meta", attrs={"property": re.compile(r"article:published_time", re.I)}):
        e["has_published_date"] = True
    if soup.find("meta", attrs={"property": re.compile(r"article:modified_time", re.I)}):
        e["has_modified_date"] = True
    if not e["has_published_date"]:
        for tt in soup.find_all("time"):
            if tt.get("datetime"):
                e["has_published_date"] = True
                break
    for a in soup.find_all("a", href=True):
        h = a["href"].lower()
        t = a.get_text(strip=True).lower()
        c = h + " " + t
        if any(k in c for k in ["about", "about-us", "회사소개", "소개"]):
            e["has_about_link"] = True
        if any(k in c for k in ["contact", "contact-us", "문의", "연락처"]):
            e["has_contact_link"] = True
        if any(k in c for k in ["privacy", "privacy-policy", "개인정보", "개인정보처리방침"]):
            e["has_privacy_link"] = True
        if any(k in c for k in ["terms", "terms-of-service", "tos", "이용약관"]):
            e["has_terms_link"] = True
        for plat, pat in SOCIAL_PATTERNS.items():
            if pat.search(a.get("href", "")) and plat not in e["social_links"]:
                e["social_links"].append(plat)
    for t in schema_info.get("all_types", []):
        if t in ("Organization", "Corporation", "LocalBusiness"):
            e["has_org_schema"] = True
            break
    if "BreadcrumbList" in schema_info.get("all_types", []):
        e["has_breadcrumb"] = True
    elif soup.find(attrs={"class": re.compile(r"breadcrumb", re.I)}):
        e["has_breadcrumb"] = True
    elif soup.find("nav", attrs={"aria-label": re.compile(r"breadcrumb", re.I)}):
        e["has_breadcrumb"] = True
    for t in schema_info.get("all_types", []):
        if t in ("Review", "AggregateRating"):
            e["has_reviews_schema"] = True
            break
    return e


def detect_technical_seo(soup, response, url):
    """기술적 SEO 요소 감지: meta robots, OG, viewport, headings, canonical 등"""
    t = {
        "meta_robots": "", "x_robots_tag": "", "is_noindex": False,
        "is_nofollow": False, "hreflang_tags": [], "hreflang_self_ref": False,
        "og_title": "", "og_description": "", "og_image": "", "og_url": "",
        "og_type": "", "twitter_card": "", "twitter_title": "",
        "twitter_description": "", "twitter_image": "", "has_viewport": False,
        "viewport_content": "", "charset": "", "lang": "", "content_type": "",
        "is_redirect": False, "redirect_url": "", "url_length": len(url),
        "has_query_params": bool(urlparse(url).query),
        "headings": {"h1": 0, "h2": 0, "h3": 0, "h4": 0, "h5": 0, "h6": 0},
        "heading_hierarchy_ok": True, "heading_issues": [],
        "has_iframes": False, "iframe_count": 0, "has_noscript": False,
        "canonical": "", "rel_next": "", "rel_prev": "",
    }
    rm = soup.find("meta", attrs={"name": re.compile(r"^robots$", re.I)})
    if rm and rm.get("content"):
        t["meta_robots"] = rm["content"].strip()
        cl = t["meta_robots"].lower()
        t["is_noindex"] = "noindex" in cl
        t["is_nofollow"] = "nofollow" in cl
    xr = response.headers.get("X-Robots-Tag", "")
    t["x_robots_tag"] = xr
    if "noindex" in xr.lower():
        t["is_noindex"] = True
    for link in soup.find_all("link", rel="alternate"):
        hl = link.get("hreflang")
        if hl:
            href = link.get("href", "")
            t["hreflang_tags"].append({"lang": hl, "href": href})
            if normalize_url(href) == normalize_url(url):
                t["hreflang_self_ref"] = True
    for prop, key in {
        "og:title": "og_title", "og:description": "og_description",
        "og:image": "og_image", "og:url": "og_url", "og:type": "og_type",
    }.items():
        tag = soup.find("meta", attrs={"property": prop})
        if tag and tag.get("content"):
            t[key] = tag["content"].strip()
    for name, key in {
        "twitter:card": "twitter_card", "twitter:title": "twitter_title",
        "twitter:description": "twitter_description", "twitter:image": "twitter_image",
    }.items():
        tag = soup.find("meta", attrs={"name": name}) or soup.find("meta", attrs={"property": name})
        if tag and tag.get("content"):
            t[key] = tag["content"].strip()
    vp = soup.find("meta", attrs={"name": re.compile(r"^viewport$", re.I)})
    if vp and vp.get("content"):
        t["has_viewport"] = True
        t["viewport_content"] = vp["content"].strip()
    cm = soup.find("meta", charset=True)
    if cm:
        t["charset"] = cm["charset"]
    else:
        ct = soup.find("meta", attrs={"http-equiv": re.compile(r"content-type", re.I)})
        if ct and ct.get("content"):
            m = re.search(r"charset=([^\s;]+)", ct["content"], re.I)
            if m:
                t["charset"] = m.group(1)
    html_tag = soup.find("html")
    if html_tag:
        t["lang"] = html_tag.get("lang", "")
    t["content_type"] = response.headers.get("Content-Type", "")
    if response.history:
        t["is_redirect"] = True
        t["redirect_url"] = response.url
    for lv in range(1, 7):
        t["headings"][f"h{lv}"] = len(soup.find_all(f"h{lv}"))
    if t["headings"]["h1"] == 0:
        t["heading_hierarchy_ok"] = False
        t["heading_issues"].append("H1 없음")
    elif t["headings"]["h1"] > 1:
        t["heading_issues"].append(f"H1 {t['headings']['h1']}개")
    found = [i for i in range(1, 7) if t["headings"][f"h{i}"] > 0]
    for i in range(len(found) - 1):
        if found[i + 1] - found[i] > 1:
            t["heading_hierarchy_ok"] = False
            t["heading_issues"].append(f"H{found[i]}→H{found[i + 1]} 건너뜀")
    t["has_iframes"] = len(soup.find_all("iframe")) > 0
    t["iframe_count"] = len(soup.find_all("iframe"))
    t["has_noscript"] = soup.find("noscript") is not None
    cn = soup.find("link", attrs={"rel": "canonical"})
    if cn and cn.get("href"):
        t["canonical"] = cn["href"].strip()
    rn = soup.find("link", attrs={"rel": "next"})
    if rn and rn.get("href"):
        t["rel_next"] = rn["href"]
    rp = soup.find("link", attrs={"rel": "prev"})
    if rp and rp.get("href"):
        t["rel_prev"] = rp["href"]
    return t


def detect_security(response, soup, url):
    """보안 헤더 및 Mixed Content 감지"""
    s = {
        "is_https": urlparse(url).scheme == "https", "has_hsts": False,
        "hsts_value": "", "has_xcto": False, "has_xfo": False,
        "has_csp": False, "csp_value": "", "has_referrer_policy": False,
        "has_permissions_policy": False, "mixed_content": [],
        "mixed_content_count": 0,
    }
    h = response.headers
    if h.get("Strict-Transport-Security"):
        s["has_hsts"] = True
        s["hsts_value"] = h["Strict-Transport-Security"]
    if h.get("X-Content-Type-Options"):
        s["has_xcto"] = True
    if h.get("X-Frame-Options"):
        s["has_xfo"] = True
    csp = h.get("Content-Security-Policy", "")
    if csp:
        s["has_csp"] = True
        s["csp_value"] = csp[:200]
    if h.get("Referrer-Policy"):
        s["has_referrer_policy"] = True
    if h.get("Permissions-Policy"):
        s["has_permissions_policy"] = True
    if s["is_https"]:
        for tag, attr in [
            ("img", "src"), ("script", "src"), ("link", "href"),
            ("iframe", "src"), ("video", "src"), ("audio", "src"),
            ("source", "src"), ("embed", "src"), ("object", "data"),
        ]:
            for el in soup.find_all(tag):
                v = el.get(attr, "")
                if v.startswith("http://"):
                    s["mixed_content"].append(f"<{tag}> {v[:100]}")
        s["mixed_content_count"] = len(s["mixed_content"])
    return s


def detect_performance(soup, response):
    """페이지 성능 관련 요소 감지"""
    p = {
        "html_size_bytes": len(response.content),
        "html_size_kb": round(len(response.content) / 1024, 1),
        "external_scripts": len(soup.find_all("script", src=True)),
        "external_stylesheets": len(soup.find_all("link", rel="stylesheet")),
        "inline_css_count": 0, "inline_js_count": 0,
        "image_count": 0, "images_no_lazy": 0,
        "has_compression": False, "compression_type": "",
    }
    p["inline_css_count"] = len(soup.find_all("style"))
    p["inline_js_count"] = len([s for s in soup.find_all("script") if not s.get("src")])
    imgs = soup.find_all("img")
    p["image_count"] = len(imgs)
    p["images_no_lazy"] = sum(1 for img in imgs if img.get("loading", "").lower() != "lazy")
    enc = response.headers.get("Content-Encoding", "")
    if enc:
        p["has_compression"] = True
        p["compression_type"] = enc
    return p


def detect_content_quality(soup, url, base_domain):
    """콘텐츠 품질 분석: 외부 링크, nofollow 비율, 텍스트/HTML 비율"""
    c = {
        "external_links": [], "external_links_count": 0,
        "nofollow_links_count": 0, "total_links_count": 0,
        "nofollow_ratio": 0.0, "text_to_html_ratio": 0.0, "word_count": 0,
    }
    body = soup.find("body")
    html_text = str(soup)
    body_text = body.get_text(separator=" ", strip=True) if body else ""
    c["word_count"] = len(body_text.split())
    if len(html_text) > 0:
        c["text_to_html_ratio"] = round(len(body_text) / len(html_text) * 100, 1)
    all_a = soup.find_all("a", href=True)
    c["total_links_count"] = len(all_a)
    nf = 0
    ext = []
    for a in all_a:
        rel = a.get("rel", [])
        rel = rel.split() if isinstance(rel, str) else rel
        if "nofollow" in [r.lower() for r in rel]:
            nf += 1
        full = urljoin(url, a["href"].strip())
        p = urlparse(full)
        if p.scheme in ("http", "https") and p.netloc and p.netloc != base_domain:
            ext.append(full)
    c["external_links"] = ext[:50]
    c["external_links_count"] = len(ext)
    c["nofollow_links_count"] = nf
    if c["total_links_count"] > 0:
        c["nofollow_ratio"] = round(nf / c["total_links_count"] * 100, 1)
    return c


# ══════════════════════════════════════════════════════════════════════════════
# 5. Page analysis — 단일 페이지 분석
# ══════════════════════════════════════════════════════════════════════════════

def analyze_page(url, session, crawl_depth=0, js_rendering=False):
    """단일 URL을 가져와 모든 SEO 요소를 분석하여 딕셔너리로 반환"""
    r_dict = {
        "URL": url, "Status": None, "Title": "", "Title Len": 0,
        "Meta Desc": "", "Desc Len": 0, "H1": "", "H1 Len": 0,
        "Canonical": "", "Words": 0, "Images": 0, "Alt Missing": 0,
        "Outlinks": 0, "Load (s)": 0.0, "Error": "", "Crawl Depth": crawl_depth,
        "_internal_links": [], "_schema": {}, "Schema Types": "",
        "Has Schema": False, "_eeat": {}, "Author": "", "Has Date": False,
        "_tech": {}, "Meta Robots": "", "Noindex": False, "Viewport": False,
        "Lang": "", "OG Image": "", "Twitter Card": "", "Hreflang": 0,
        "_security": {}, "HTTPS": False, "HSTS": False, "Mixed Content": 0,
        "_perf": {}, "HTML KB": 0, "Ext Scripts": 0, "Ext CSS": 0,
        "Img No Lazy": 0, "Compression": "", "_content": {},
        "Ext Links": 0, "Text/HTML %": 0.0,
        "H1s": 0, "H2s": 0, "H3s": 0, "H4s": 0, "H5s": 0, "H6s": 0,
    }
    try:
        pw_used = False

        # JS 렌더링 모드: Playwright로 직접 수집
        if js_rendering and _PLAYWRIGHT_AVAILABLE:
            pw_html, pw_load = _fetch_with_playwright(url)
            if pw_html and len(pw_html) > 500:
                r_dict["Load (s)"] = pw_load
                r_dict["Status"] = 200
                soup = BeautifulSoup(pw_html, "html.parser")
                pw_used = True

        if not pw_used:
            start = time.time()
            r = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            if r.status_code == 403:
                session.headers.update({"Referer": url, "DNT": "1"})
                r = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            r_dict["Load (s)"] = round(time.time() - start, 2)
            r_dict["Status"] = r.status_code

            # WAF 감지 시 Playwright fallback
            need_playwright = (
                r.status_code in (403, 406, 429, 503)
                or (r.status_code == 200 and _is_waf_page(r.text))
            )
            if need_playwright and _PLAYWRIGHT_AVAILABLE:
                pw_html, pw_load = _fetch_with_playwright(url)
                if pw_html and len(pw_html) > 500:
                    r_dict["Load (s)"] = pw_load
                    r_dict["Status"] = 200
                    soup = BeautifulSoup(pw_html, "html.parser")
                    pw_used = True

            if not pw_used:
                if r.status_code != 200 and not (300 <= r.status_code < 400):
                    return r_dict
                soup = BeautifulSoup(r.content, "html.parser")
        tag = soup.find("title")
        if tag and tag.string:
            r_dict["Title"] = tag.string.strip()
            r_dict["Title Len"] = len(r_dict["Title"])
        meta = soup.find("meta", attrs={"name": re.compile(r"^description$", re.I)})
        if meta and meta.get("content"):
            r_dict["Meta Desc"] = meta["content"].strip()
            r_dict["Desc Len"] = len(r_dict["Meta Desc"])
        h1 = soup.find("h1")
        if h1:
            r_dict["H1"] = h1.get_text(strip=True)
            r_dict["H1 Len"] = len(r_dict["H1"])
        cn = soup.find("link", attrs={"rel": "canonical"})
        if cn and cn.get("href"):
            r_dict["Canonical"] = cn["href"].strip()
        body = soup.find("body")
        if body:
            r_dict["Words"] = len(body.get_text(separator=" ", strip=True).split())
        imgs = soup.find_all("img")
        r_dict["Images"] = len(imgs)
        r_dict["Alt Missing"] = sum(1 for img in imgs if not img.get("alt", "").strip())
        bd = urlparse(url).netloc
        il = set()
        for a in soup.find_all("a", href=True):
            full = urljoin(url, a["href"].strip())
            n = normalize_url(full)
            if is_same_domain(n, bd):
                il.add(n)
        r_dict["_internal_links"] = sorted(il)
        r_dict["Outlinks"] = len(il)
        # Schema
        si = detect_schema(soup, url)
        r_dict["_schema"] = si
        r_dict["Schema Types"] = ", ".join(si["all_types"])
        r_dict["Has Schema"] = si["has_schema"]
        # E-E-A-T
        ee = detect_eeat(soup, si, url)
        r_dict["_eeat"] = ee
        r_dict["Author"] = ee["author_name"][:40]
        r_dict["Has Date"] = ee["has_published_date"] or ee["has_modified_date"]
        # Technical SEO
        tc = detect_technical_seo(soup, r, url)
        r_dict["_tech"] = tc
        r_dict["Meta Robots"] = tc["meta_robots"]
        r_dict["Noindex"] = tc["is_noindex"]
        r_dict["Viewport"] = tc["has_viewport"]
        r_dict["Lang"] = tc["lang"]
        r_dict["OG Image"] = "Y" if tc["og_image"] else "N"
        r_dict["Twitter Card"] = tc["twitter_card"] or ""
        r_dict["Hreflang"] = len(tc["hreflang_tags"])
        for i in range(1, 7):
            r_dict[f"H{i}s"] = tc["headings"][f"h{i}"]
        # Security
        sc = detect_security(r, soup, url)
        r_dict["_security"] = sc
        r_dict["HTTPS"] = sc["is_https"]
        r_dict["HSTS"] = sc["has_hsts"]
        r_dict["Mixed Content"] = sc["mixed_content_count"]
        # Performance
        pf = detect_performance(soup, r)
        r_dict["_perf"] = pf
        r_dict["HTML KB"] = pf["html_size_kb"]
        r_dict["Ext Scripts"] = pf["external_scripts"]
        r_dict["Ext CSS"] = pf["external_stylesheets"]
        r_dict["Img No Lazy"] = pf["images_no_lazy"]
        r_dict["Compression"] = pf["compression_type"] or "None"
        # Content Quality
        cq = detect_content_quality(soup, url, bd)
        r_dict["_content"] = cq
        r_dict["Ext Links"] = cq["external_links_count"]
        r_dict["Text/HTML %"] = cq["text_to_html_ratio"]
    except Exception as e:
        r_dict["Error"] = str(e)[:120]
    return r_dict


# ══════════════════════════════════════════════════════════════════════════════
# 6. PageSpeed — Google PageSpeed Insights
# ══════════════════════════════════════════════════════════════════════════════

def get_pagespeed_score(url, api_key):
    """Google PageSpeed Insights API로 모바일 성능 점수 조회"""
    if not api_key:
        return None
    try:
        r = requests.get(
            "https://www.googleapis.com/pagespeedonline/v5/runPagespeed",
            params={"url": url, "strategy": "mobile", "key": api_key},
            timeout=60,
        )
        return int(r.json()["lighthouseResult"]["categories"]["performance"]["score"] * 100)
    except:
        return None


# ══════════════════════════════════════════════════════════════════════════════
# 7. Site Tree — 사이트 구조 트리
# ══════════════════════════════════════════════════════════════════════════════

def build_tree_string(urls, base_domain):
    """URL 목록에서 트리 구조 문자열 생성"""
    tree = {}
    for url in sorted(urls):
        parts = [p for p in urlparse(url).path.split("/") if p] or ["(root)"]
        node = tree
        for part in parts:
            node = node.setdefault(part, {})
    lines = [f"{base_domain}/"]

    def _r(node, prefix=""):
        items = sorted(node.keys())
        for i, name in enumerate(items):
            last = i == len(items) - 1
            lines.append(f"{prefix}{'└── ' if last else '├── '}{name}")
            _r(node[name], prefix + ("    " if last else "│   "))

    _r(tree)
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# 8. Diagnostics — SEO 진단 엔진
# ══════════════════════════════════════════════════════════════════════════════

def run_diagnostics(pages, incoming_map, pagespeed_scores):
    """모든 페이지 데이터를 분석하여 이슈 목록 반환"""
    issues = []
    title_map = defaultdict(list)
    for p in pages:
        if p["Title"]:
            title_map[p["Title"]].append(p["URL"])
    for title, urls in title_map.items():
        if len(urls) > 1:
            issues.append({
                "type": "중복 Title", "severity": "HIGH",
                "detail": f'"{title[:40]}..." → {len(urls)}개 중복',
                "pages": urls,
                "fix": "각 페이지마다 고유한 Title을 작성하세요.",
            })
    hp = [p for p in pages if urlparse(p["URL"]).path in ("/", "")]
    for h in hp:
        st_types = h.get("_schema", {}).get("all_types", [])
        if not any(t in ("Organization", "Corporation", "LocalBusiness") for t in st_types):
            issues.append({
                "type": "Organization 스키마 없음", "severity": "MEDIUM",
                "detail": "홈페이지에 Organization 스키마 없음",
                "pages": [h["URL"]],
                "fix": "홈페이지에 Organization 스키마를 추가하세요.",
            })
    for p in pages:
        url = p["URL"]
        tl = p["Title Len"]
        dl = p["Desc Len"]
        ok = p["Status"] == 200
        if tl == 0:
            issues.append({
                "type": "Title 없음", "severity": "HIGH",
                "detail": "Title 태그 없음", "pages": [url],
                "fix": "고유하고 설명적인 Title 태그를 추가하세요.",
            })
        elif tl < TITLE_MIN or tl > TITLE_MAX:
            issues.append({
                "type": "Title 길이", "severity": "MEDIUM",
                "detail": f"Title {tl}자 → {TITLE_MIN}~{TITLE_MAX}자 권장",
                "pages": [url],
                "fix": f"현재 {tl}자. {TITLE_MIN}~{TITLE_MAX}자로 조절하세요.",
            })
        if dl == 0:
            issues.append({
                "type": "Description 없음", "severity": "MEDIUM",
                "detail": "Meta Description 없음", "pages": [url],
                "fix": "핵심 키워드와 CTA를 포함한 설명을 추가하세요.",
            })
        elif dl < DESC_MIN or dl > DESC_MAX:
            issues.append({
                "type": "Description 길이", "severity": "MEDIUM",
                "detail": f"Description {dl}자 → {DESC_MIN}~{DESC_MAX}자 권장",
                "pages": [url],
                "fix": f"현재 {dl}자. {DESC_MIN}~{DESC_MAX}자로 조절하세요.",
            })
        if p["H1 Len"] == 0:
            issues.append({
                "type": "H1 없음", "severity": "HIGH",
                "detail": "H1 태그 없음", "pages": [url],
                "fix": "페이지당 하나의 H1 태그 + 핵심 키워드 포함.",
            })
        if p["Words"] < THIN_CONTENT_THRESHOLD and ok:
            issues.append({
                "type": "Thin Content", "severity": "MEDIUM",
                "detail": f"단어 수 {p['Words']}개 (최소 {THIN_CONTENT_THRESHOLD} 권장)",
                "pages": [url],
                "fix": "FAQ, 관련 정보, 사용자 후기 등으로 콘텐츠 보강.",
            })
        if p["Alt Missing"] > 0:
            issues.append({
                "type": "Alt 누락", "severity": "LOW",
                "detail": f"Alt 없는 이미지 {p['Alt Missing']}/{p['Images']}개",
                "pages": [url],
                "fix": "모든 이미지에 설명적인 alt 텍스트를 추가하세요.",
            })
        if p["Status"] and p["Status"] >= 400:
            issues.append({
                "type": f"HTTP {p['Status']}", "severity": "HIGH",
                "detail": f"HTTP {p['Status']} 에러", "pages": [url],
                "fix": "깨진 링크 수정 또는 301 리다이렉트 설정.",
            })
        inc = incoming_map.get(url, 0)
        if inc < MIN_INCOMING_LINKS and ok:
            issues.append({
                "type": "Inlinks 부족", "severity": "MEDIUM",
                "detail": f"들어오는 링크 {inc}개 (최소 {MIN_INCOMING_LINKS} 권장)",
                "pages": [url],
                "fix": "내부 링크 추가 (실크 로드 전략 권장).",
            })
        sc = pagespeed_scores.get(url)
        if sc is not None and sc < PAGESPEED_THRESHOLD:
            issues.append({
                "type": "PageSpeed 낮음",
                "severity": "HIGH" if sc < 50 else "MEDIUM",
                "detail": f"Mobile 점수: {sc}/100", "pages": [url],
                "fix": "이미지 최적화, Lazy Loading, 렌더링 차단 리소스 제거.",
            })
        if p["Load (s)"] > 3.0:
            issues.append({
                "type": "느린 로딩", "severity": "MEDIUM",
                "detail": f"로드 시간 {p['Load (s)']}초", "pages": [url],
                "fix": "서버 응답 시간, CDN, 캐싱 설정 점검.",
            })
        schema = p.get("_schema", {})
        if ok and not schema.get("has_schema"):
            issues.append({
                "type": "구조화 데이터 없음", "severity": "MEDIUM",
                "detail": "JSON-LD/Microdata/RDFa 없음", "pages": [url],
                "fix": "구조화된 데이터를 추가하세요 (JSON-LD 권장)",
            })
        if schema.get("has_schema") and ok:
            if "BreadcrumbList" not in schema.get("all_types", []):
                issues.append({
                    "type": "BreadcrumbList 없음", "severity": "LOW",
                    "detail": "BreadcrumbList 스키마 없음", "pages": [url],
                    "fix": "BreadcrumbList 스키마로 사이트 구조를 표현하세요",
                })
            for vi in schema.get("validation_issues", []):
                issues.append({
                    "type": "스키마 속성 누락", "severity": "LOW",
                    "detail": vi, "pages": [url],
                    "fix": f"스키마 필수 속성을 추가하세요: {vi}",
                })
        eeat = p.get("_eeat", {})
        if ok:
            if not eeat.get("has_author"):
                issues.append({
                    "type": "저자 정보 없음", "severity": "LOW",
                    "detail": "author 메타/스키마/바이라인 없음", "pages": [url],
                    "fix": "E-E-A-T: 저자 정보를 추가하세요",
                })
            if not eeat.get("has_published_date") and not eeat.get("has_modified_date"):
                issues.append({
                    "type": "날짜 정보 없음", "severity": "LOW",
                    "detail": "게시일/수정일 없음", "pages": [url],
                    "fix": "E-E-A-T: 게시일/수정일을 표시하세요",
                })
            if not eeat.get("has_about_link") and not eeat.get("has_contact_link"):
                issues.append({
                    "type": "About/Contact 없음", "severity": "LOW",
                    "detail": "About/Contact 링크 없음", "pages": [url],
                    "fix": "E-E-A-T: About/Contact 페이지 링크를 추가하세요",
                })
            if not eeat.get("has_privacy_link"):
                issues.append({
                    "type": "개인정보처리방침 없음", "severity": "LOW",
                    "detail": "Privacy Policy 링크 없음", "pages": [url],
                    "fix": "E-E-A-T: 개인정보 처리방침 링크를 추가하세요",
                })
        tech = p.get("_tech", {})
        if ok:
            if tech.get("is_noindex"):
                issues.append({
                    "type": "Noindex 설정", "severity": "HIGH",
                    "detail": f"meta robots: {tech.get('meta_robots', '')}",
                    "pages": [url],
                    "fix": "이 페이지가 noindex 설정되어 있습니다",
                })
            if not tech.get("og_image"):
                issues.append({
                    "type": "OG Image 없음", "severity": "MEDIUM",
                    "detail": "og:image 없음", "pages": [url],
                    "fix": "SNS 공유를 위해 Open Graph 이미지를 설정하세요",
                })
            if not tech.get("has_viewport"):
                issues.append({
                    "type": "Viewport 없음", "severity": "HIGH",
                    "detail": "viewport 메타 없음", "pages": [url],
                    "fix": "모바일 호환성을 위해 viewport 메타 태그를 추가하세요",
                })
            if not tech.get("twitter_card"):
                issues.append({
                    "type": "Twitter Card 없음", "severity": "LOW",
                    "detail": "twitter:card 없음", "pages": [url],
                    "fix": "Twitter Card 메타 태그를 추가하세요",
                })
            if tech.get("url_length", 0) > URL_MAX_LENGTH:
                issues.append({
                    "type": "URL 길이 초과", "severity": "MEDIUM",
                    "detail": f"URL {tech['url_length']}자", "pages": [url],
                    "fix": "URL 길이를 100자 이내로 줄이세요",
                })
            if not tech.get("heading_hierarchy_ok"):
                for hi in tech.get("heading_issues", []):
                    issues.append({
                        "type": "Heading 계층 문제", "severity": "MEDIUM",
                        "detail": hi, "pages": [url],
                        "fix": "H1→H2→H3 순서로 계층 구조를 지키세요",
                    })
        sec = p.get("_security", {})
        if ok:
            if not sec.get("is_https"):
                issues.append({
                    "type": "HTTPS 미사용", "severity": "HIGH",
                    "detail": "HTTP 사용 중", "pages": [url],
                    "fix": "HTTPS로 전환하세요",
                })
            elif not sec.get("has_hsts"):
                issues.append({
                    "type": "HSTS 없음", "severity": "MEDIUM",
                    "detail": "HSTS 헤더 없음", "pages": [url],
                    "fix": "HSTS 헤더를 추가하세요",
                })
            if sec.get("mixed_content_count", 0) > 0:
                issues.append({
                    "type": "Mixed Content", "severity": "HIGH",
                    "detail": f"HTTP 리소스 {sec['mixed_content_count']}개",
                    "pages": [url],
                    "fix": "HTTPS 페이지에서 HTTP 리소스를 로드하고 있습니다",
                })
        perf = p.get("_perf", {})
        if ok:
            if perf.get("html_size_bytes", 0) > PAGE_SIZE_WARN:
                issues.append({
                    "type": "페이지 용량 과다", "severity": "HIGH",
                    "detail": f"HTML {round(perf['html_size_bytes'] / (1024 * 1024), 1)}MB",
                    "pages": [url],
                    "fix": "페이지 용량이 큽니다. 최적화하세요",
                })
            if perf.get("images_no_lazy", 0) > 3:
                issues.append({
                    "type": "Lazy Loading 없음", "severity": "LOW",
                    "detail": f"lazy loading 없는 이미지 {perf['images_no_lazy']}개",
                    "pages": [url],
                    "fix": "이미지에 loading='lazy' 속성을 추가하세요",
                })
            if perf.get("external_scripts", 0) > MAX_EXTERNAL_SCRIPTS:
                issues.append({
                    "type": "외부 스크립트 과다", "severity": "MEDIUM",
                    "detail": f"외부 스크립트 {perf['external_scripts']}개",
                    "pages": [url],
                    "fix": "외부 스크립트가 많습니다. 번들링을 고려하세요",
                })
            if not perf.get("has_compression"):
                issues.append({
                    "type": "압축 없음", "severity": "MEDIUM",
                    "detail": "Gzip/Brotli 미적용", "pages": [url],
                    "fix": "Gzip/Brotli 압축을 활성화하세요",
                })
    issues.sort(key=lambda x: {"HIGH": 0, "MEDIUM": 1, "LOW": 2}.get(x["severity"], 9))
    return issues


# ══════════════════════════════════════════════════════════════════════════════
# 9. Quick Scan — 무료 랜딩 페이지용 간단 스캔
# ══════════════════════════════════════════════════════════════════════════════

def quick_scan(url: str) -> dict:
    """
    무료 랜딩 페이지용 단일 URL 경량 스캔.
    최소 리소스로 핵심 SEO 지표를 빠르게 분석합니다.

    Returns:
        dict with keys:
        - title, title_len
        - meta_description, desc_len
        - h1
        - has_schema, schema_types
        - is_https
        - has_viewport
        - load_time
        - word_count
        - image_count, images_without_alt
        - og_image (bool)
        - issues_preview: 상위 5개 핵심 이슈
        - score: 종합 SEO 점수 (0-100)
    """
    # URL 정규화
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    url = url.rstrip("/")

    result = {
        "url": url,
        "title": "", "title_len": 0,
        "meta_description": "", "desc_len": 0,
        "h1": "",
        "has_schema": False, "schema_types": [],
        "is_https": urlparse(url).scheme == "https",
        "has_viewport": False,
        "load_time": 0.0,
        "word_count": 0,
        "image_count": 0, "images_without_alt": 0,
        "og_image": False,
        "issues_preview": [],
        "score": 0,
        "error": "",
    }

    session = build_session()
    all_issues = []  # (severity_order, severity, message)

    try:
        start = time.time()
        r = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        result["load_time"] = round(time.time() - start, 2)

        # 403/406 등 차단 시 Referer 헤더 추가 후 재시도
        if r.status_code in (403, 406, 429):
            session.headers.update({
                "Referer": url,
                "DNT": "1",
            })
            time.sleep(1)
            start2 = time.time()
            r = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            result["load_time"] = round(time.time() - start2, 2)

        # 여전히 차단되면 다른 User-Agent로 재시도
        if r.status_code in (403, 406, 429):
            session.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            time.sleep(1)
            start3 = time.time()
            r = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            result["load_time"] = round(time.time() - start3, 2)

        if r.status_code != 200:
            if r.status_code in (403, 406):
                # Playwright 헤드리스 브라우저로 재시도
                pw_html, pw_load = _fetch_with_playwright(url)
                if pw_html and len(pw_html) > 500:
                    result["load_time"] = pw_load
                    result["waf_bypassed"] = True
                    soup = BeautifulSoup(pw_html, "html.parser")
                    # Playwright 성공 — 아래 정상 분석 흐름으로 진행
                else:
                    # Playwright도 실패 — WAF 안내
                    result["error"] = ""
                    result["waf_blocked"] = True
                    soup_403 = BeautifulSoup(r.content, "html.parser")
                    title_tag = soup_403.find("title")
                    if title_tag and title_tag.string:
                        result["title"] = title_tag.string.strip()
                        result["title_len"] = len(result["title"])
                    result["score"] = 0
                    result["issues_preview"] = [{
                        "severity": "HIGH",
                        "message": "WAF(웹 방화벽)에 의해 크롤러 접근이 차단되었습니다"
                    }, {
                        "severity": "MEDIUM",
                        "message": "헤드리스 브라우저로도 접근이 차단되었습니다"
                    }, {
                        "severity": "MEDIUM",
                        "message": "프로젝트에서 전체 크롤링을 시도해보세요"
                    }]
                    return result
            else:
                result["error"] = f"HTTP {r.status_code}"
                return result
        else:
            soup = BeautifulSoup(r.content, "html.parser")

        # Title
        tag = soup.find("title")
        if tag and tag.string:
            result["title"] = tag.string.strip()
            result["title_len"] = len(result["title"])

        # Meta Description
        meta = soup.find("meta", attrs={"name": re.compile(r"^description$", re.I)})
        if meta and meta.get("content"):
            result["meta_description"] = meta["content"].strip()
            result["desc_len"] = len(result["meta_description"])

        # H1
        h1 = soup.find("h1")
        if h1:
            result["h1"] = h1.get_text(strip=True)

        # Schema (간략하게 JSON-LD 타입만 추출)
        schema_types = []
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if isinstance(item, dict):
                        if "@graph" in item:
                            for g in item["@graph"]:
                                if isinstance(g, dict):
                                    st = g.get("@type", "")
                                    if st:
                                        types = st if isinstance(st, list) else [st]
                                        schema_types.extend(types)
                        else:
                            st = item.get("@type", "")
                            if st:
                                types = st if isinstance(st, list) else [st]
                                schema_types.extend(types)
            except:
                pass
        # Microdata도 체크
        for el in soup.find_all(attrs={"itemtype": True}):
            itype = el.get("itemtype", "").rstrip("/").split("/")[-1]
            if itype:
                schema_types.append(itype)
        result["schema_types"] = list(set(schema_types))
        result["has_schema"] = len(result["schema_types"]) > 0

        # Viewport
        vp = soup.find("meta", attrs={"name": re.compile(r"^viewport$", re.I)})
        if vp and vp.get("content"):
            result["has_viewport"] = True

        # Word count
        body = soup.find("body")
        if body:
            result["word_count"] = len(body.get_text(separator=" ", strip=True).split())

        # Images
        imgs = soup.find_all("img")
        result["image_count"] = len(imgs)
        result["images_without_alt"] = sum(1 for img in imgs if not img.get("alt", "").strip())

        # OG Image
        og_img = soup.find("meta", attrs={"property": "og:image"})
        result["og_image"] = bool(og_img and og_img.get("content"))

        # ── 점수 계산 (총 100점) ──
        score = 0

        # Title (15점)
        tl = result["title_len"]
        if tl > 0:
            if TITLE_MIN <= tl <= TITLE_MAX:
                score += 15
            else:
                score += 8  # 있긴 하지만 길이 부적합
        if tl == 0:
            all_issues.append((0, "HIGH", "Title 태그가 없습니다. 검색 결과에 표시될 고유한 제목을 추가하세요."))
        elif tl < TITLE_MIN:
            all_issues.append((1, "MEDIUM", f"Title이 너무 짧습니다 ({tl}자). {TITLE_MIN}~{TITLE_MAX}자를 권장합니다."))
        elif tl > TITLE_MAX:
            all_issues.append((1, "MEDIUM", f"Title이 너무 깁니다 ({tl}자). 검색 결과에서 잘릴 수 있습니다."))

        # Meta Description (15점)
        dl = result["desc_len"]
        if dl > 0:
            if DESC_MIN <= dl <= DESC_MAX:
                score += 15
            else:
                score += 8
        if dl == 0:
            all_issues.append((0, "HIGH", "Meta Description이 없습니다. 클릭률(CTR) 향상을 위해 추가하세요."))
        elif dl < DESC_MIN:
            all_issues.append((1, "MEDIUM", f"Meta Description이 너무 짧습니다 ({dl}자). {DESC_MIN}~{DESC_MAX}자를 권장합니다."))
        elif dl > DESC_MAX:
            all_issues.append((1, "MEDIUM", f"Meta Description이 너무 깁니다 ({dl}자). 검색 결과에서 잘릴 수 있습니다."))

        # H1 (10점)
        if result["h1"]:
            score += 10
        else:
            all_issues.append((0, "HIGH", "H1 태그가 없습니다. 페이지의 핵심 주제를 H1으로 명시하세요."))

        # HTTPS (10점)
        if result["is_https"]:
            score += 10
        else:
            all_issues.append((0, "HIGH", "HTTPS를 사용하지 않습니다. 보안과 SEO를 위해 HTTPS로 전환하세요."))

        # Schema (10점)
        if result["has_schema"]:
            score += 10
        else:
            all_issues.append((1, "MEDIUM", "구조화 데이터(Schema)가 없습니다. 리치 결과를 위해 JSON-LD를 추가하세요."))

        # Viewport (5점)
        if result["has_viewport"]:
            score += 5
        else:
            all_issues.append((0, "HIGH", "Viewport 메타 태그가 없습니다. 모바일 호환성에 문제가 생깁니다."))

        # Load time (10점) — 1초 이하 만점, 3초 초과 0점
        lt = result["load_time"]
        if lt <= 1.0:
            score += 10
        elif lt <= 2.0:
            score += 7
        elif lt <= 3.0:
            score += 4
        else:
            all_issues.append((1, "MEDIUM", f"로딩 시간이 {lt}초로 느립니다. 3초 이내를 목표로 최적화하세요."))

        # Alt tags (10점) — 이미지 없으면 만점, 있으면 비율로 계산
        if result["image_count"] == 0:
            score += 10
        else:
            alt_ratio = 1 - (result["images_without_alt"] / result["image_count"])
            score += round(10 * alt_ratio)
            if result["images_without_alt"] > 0:
                all_issues.append((2, "LOW", f"Alt 텍스트 없는 이미지가 {result['images_without_alt']}개 있습니다."))

        # OG Image (5점)
        if result["og_image"]:
            score += 5
        else:
            all_issues.append((1, "MEDIUM", "OG Image가 없습니다. SNS 공유 시 미리보기 이미지가 표시되지 않습니다."))

        # Content length (10점) — 단어 수 기준
        wc = result["word_count"]
        if wc >= 600:
            score += 10
        elif wc >= THIN_CONTENT_THRESHOLD:
            score += 7
        elif wc >= 100:
            score += 3
        else:
            pass
        if wc < THIN_CONTENT_THRESHOLD:
            all_issues.append((1, "MEDIUM", f"콘텐츠가 부족합니다 ({wc}단어). 최소 {THIN_CONTENT_THRESHOLD}단어 이상을 권장합니다."))

        result["score"] = min(score, 100)

        # 심각도 순 정렬 후 상위 5개만 반환
        all_issues.sort(key=lambda x: x[0])
        result["issues_preview"] = [
            {"severity": sev, "message": msg}
            for _, sev, msg in all_issues[:5]
        ]

    except Exception as e:
        result["error"] = str(e)[:200]

    return result


# ══════════════════════════════════════════════════════════════════════════════
# 10. Crawl Engine — 크롤 엔진 (UI 독립)
# ══════════════════════════════════════════════════════════════════════════════

def _compute_incoming_map(pages):
    """페이지 목록에서 내부 링크 수신 맵 계산"""
    incoming_map = defaultdict(int)
    for p in pages:
        for link in p["_internal_links"]:
            incoming_map[link] += 1
    # 각 페이지에 Inlinks 값 추가
    for p in pages:
        p["Inlinks"] = incoming_map.get(p["URL"], 0)
    return incoming_map


def _build_crawl_result(pages, incoming_map, issues, crawl_start):
    """크롤 결과를 표준 형식으로 패키징"""
    elapsed = round(time.time() - crawl_start, 2)

    high = sum(1 for i in issues if i["severity"] == "HIGH")
    med = sum(1 for i in issues if i["severity"] == "MEDIUM")
    low = sum(1 for i in issues if i["severity"] == "LOW")

    return {
        "pages": pages,
        "issues": issues,
        "incoming_map": dict(incoming_map),
        "elapsed": elapsed,
        "summary": {
            "total_pages": len(pages),
            "total_issues": len(issues),
            "high": high,
            "medium": med,
            "low": low,
        },
    }


def _analyze_robots_txt(base_url, session):
    """robots.txt를 분석하여 크롤링 제한 정보를 반환"""
    info = {
        "exists": False,
        "raw_content": "",
        "disallowed_paths": [],
        "allowed_paths": [],
        "crawl_delay": None,
        "sitemaps": [],
        "is_fully_blocked": False,
        "warnings": [],
    }
    try:
        r = session.get(urljoin(base_url, "/robots.txt"), timeout=REQUEST_TIMEOUT)
        if r.status_code == 200 and "user-agent" in r.text.lower():
            info["exists"] = True
            info["raw_content"] = r.text[:5000]

            current_agent_applies = False
            for line in r.text.splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue

                if line.lower().startswith("user-agent:"):
                    agent = line.split(":", 1)[1].strip().lower()
                    current_agent_applies = agent == "*" or "bot" in agent or "crawl" in agent or "spider" in agent
                elif current_agent_applies:
                    if line.lower().startswith("disallow:"):
                        path = line.split(":", 1)[1].strip()
                        if path:
                            info["disallowed_paths"].append(path)
                            if path == "/":
                                info["is_fully_blocked"] = True
                    elif line.lower().startswith("allow:"):
                        path = line.split(":", 1)[1].strip()
                        if path:
                            info["allowed_paths"].append(path)
                    elif line.lower().startswith("crawl-delay:"):
                        try:
                            info["crawl_delay"] = float(line.split(":", 1)[1].strip())
                        except ValueError:
                            pass
                    elif line.lower().startswith("sitemap:"):
                        info["sitemaps"].append(line.split(":", 1)[1].strip())

            # 경고 메시지 생성
            if info["is_fully_blocked"]:
                info["warnings"].append("robots.txt에서 모든 크롤러의 접근을 차단(Disallow: /)하고 있습니다. 강제로 크롤링하였습니다.")
            if info["disallowed_paths"]:
                info["warnings"].append(f"robots.txt에서 {len(info['disallowed_paths'])}개 경로가 차단되어 있습니다. 강제로 수집하였습니다.")
            if info["crawl_delay"]:
                info["warnings"].append(f"robots.txt에서 권장 크롤링 딜레이: {info['crawl_delay']}초")
    except Exception:
        pass
    return info


def run_full_crawl(base_url, max_pages, delay, progress_callback=None,
                   user_agent=None, js_rendering=False):
    """
    BFS 전체 크롤링.
    """
    if not base_url.startswith(("http://", "https://")):
        base_url = "https://" + base_url
    base_url = base_url.rstrip("/")
    base_domain = urlparse(base_url).netloc
    session = build_session(user_agent=user_agent)
    crawl_start = time.time()

    robots_info = _analyze_robots_txt(base_url, session)

    visited = set()
    queue = [normalize_url(base_url)]
    depth_map = {normalize_url(base_url): 0}
    pages = []

    while queue and len(visited) < max_pages:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)
        cd = depth_map.get(url, 0)
        page = analyze_page(url, session, cd, js_rendering=js_rendering)
        pages.append(page)
        for link in page["_internal_links"]:
            if link not in visited:
                queue.append(link)
                if link not in depth_map:
                    depth_map[link] = cd + 1
        if progress_callback:
            progress_callback(len(pages), max_pages, url)
        time.sleep(delay)

    # WAF 감지: 1페이지만 수집되고 내부 링크가 없으면 sitemap fallback
    if len(pages) <= 1 and max_pages > 1:
        sitemaps = discover_sitemaps(base_url, session)
        if sitemaps:
            sitemap_urls = set()
            for sm in sitemaps:
                sitemap_urls |= parse_sitemap(sm, session, max_pages, base_domain)
            new_urls = sorted(sitemap_urls - visited)[:max_pages - len(pages)]
            if new_urls and progress_callback:
                progress_callback(len(pages), len(new_urls) + len(pages),
                                  f"[Sitemap fallback] {len(new_urls)}개 URL 발견")
            for i, surl in enumerate(new_urls):
                if len(pages) >= max_pages:
                    break
                page = analyze_page(surl, session, 1, js_rendering=js_rendering)
                pages.append(page)
                if progress_callback:
                    progress_callback(len(pages), len(new_urls) + 1, surl)
                time.sleep(delay)

    incoming_map = _compute_incoming_map(pages)
    issues = run_diagnostics(pages, incoming_map, {})
    result = _build_crawl_result(pages, incoming_map, issues, crawl_start)
    result["robots_info"] = robots_info
    return result


def run_sitemap_crawl(base_url, max_pages, delay, progress_callback=None,
                      user_agent=None, js_rendering=False):
    """사이트맵 기반 크롤링."""
    if not base_url.startswith(("http://", "https://")):
        base_url = "https://" + base_url
    base_url = base_url.rstrip("/")
    base_domain = urlparse(base_url).netloc
    session = build_session(user_agent=user_agent)
    crawl_start = time.time()

    sitemaps = discover_sitemaps(base_url, session)
    pages = []

    if not sitemaps:
        incoming_map = _compute_incoming_map(pages)
        issues = run_diagnostics(pages, incoming_map, {})
        return _build_crawl_result(pages, incoming_map, issues, crawl_start)

    all_urls = set()
    for sm in sitemaps:
        all_urls |= parse_sitemap(sm, session, max_pages, base_domain)
    all_urls = sorted(all_urls)[:max_pages]
    total = len(all_urls)

    for i, url in enumerate(all_urls):
        page = analyze_page(url, session, 0, js_rendering=js_rendering)
        pages.append(page)
        if progress_callback:
            progress_callback(len(pages), total, url)
        time.sleep(delay)

    incoming_map = _compute_incoming_map(pages)
    issues = run_diagnostics(pages, incoming_map, {})
    return _build_crawl_result(pages, incoming_map, issues, crawl_start)


def run_path_crawl(base_url, path, max_pages, delay, progress_callback=None,
                   user_agent=None, js_rendering=False):
    """특정 경로 하위만 크롤링."""
    if not base_url.startswith(("http://", "https://")):
        base_url = "https://" + base_url
    base_url = base_url.rstrip("/")
    session = build_session(user_agent=user_agent)
    crawl_start = time.time()

    path_prefix = path.strip() or "/"
    if not path_prefix.startswith("/"):
        path_prefix = "/" + path_prefix
    start_url = base_url + path_prefix.rstrip("/")

    visited = set()
    queue = [normalize_url(start_url)]
    depth_map = {normalize_url(start_url): 0}
    pages = []

    while queue and len(visited) < max_pages:
        url = queue.pop(0)
        if url in visited:
            continue
        if path_prefix and not urlparse(url).path.startswith(path_prefix):
            continue
        visited.add(url)
        cd = depth_map.get(url, 0)
        page = analyze_page(url, session, cd, js_rendering=js_rendering)
        pages.append(page)
        for link in page["_internal_links"]:
            if link not in visited:
                if not path_prefix or urlparse(link).path.startswith(path_prefix):
                    queue.append(link)
                    if link not in depth_map:
                        depth_map[link] = cd + 1
        if progress_callback:
            progress_callback(len(pages), max_pages, url)
        time.sleep(delay)

    incoming_map = _compute_incoming_map(pages)
    issues = run_diagnostics(pages, incoming_map, {})
    return _build_crawl_result(pages, incoming_map, issues, crawl_start)


def run_mixed_crawl(base_url, max_pages, delay, progress_callback=None,
                    user_agent=None, js_rendering=False):
    """사이트맵 + 크롤링 혼합 모드."""
    if not base_url.startswith(("http://", "https://")):
        base_url = "https://" + base_url
    base_url = base_url.rstrip("/")
    base_domain = urlparse(base_url).netloc
    session = build_session(user_agent=user_agent)
    crawl_start = time.time()

    sitemaps = discover_sitemaps(base_url, session)
    pages = []

    if sitemaps:
        all_urls = set()
        for sm in sitemaps:
            all_urls |= parse_sitemap(sm, session, max_pages, base_domain)
        all_urls = sorted(all_urls)[:max_pages]

        for i, url in enumerate(all_urls):
            page = analyze_page(url, session, 0, js_rendering=js_rendering)
            pages.append(page)
            if progress_callback:
                progress_callback(len(pages), max_pages, url)
            time.sleep(delay)

    sitemap_urls = {p["URL"] for p in pages}
    remaining = max_pages - len(pages)
    if remaining > 0:
        visited = set(sitemap_urls)
        queue = []
        for p in pages:
            for link in p["_internal_links"]:
                if link not in visited:
                    queue.append(link)
        while queue and len(pages) < max_pages:
            url = queue.pop(0)
            if url in visited:
                continue
            visited.add(url)
            page = analyze_page(url, session, 1, js_rendering=js_rendering)
            pages.append(page)
            for link in page["_internal_links"]:
                if link not in visited:
                    queue.append(link)
            if progress_callback:
                progress_callback(len(pages), max_pages, url)
            time.sleep(delay)

    incoming_map = _compute_incoming_map(pages)
    issues = run_diagnostics(pages, incoming_map, {})
    return _build_crawl_result(pages, incoming_map, issues, crawl_start)


def run_crawl(base_url, mode, max_pages, delay, path='', progress_callback=None,
              user_agent=None, js_rendering=False):
    """
    메인 크롤 엔트리 포인트 — 모드에 따라 적절한 크롤 함수로 디스패치.

    Args:
        base_url: 기준 URL
        mode: 'full', 'sitemap', 'path', 'mixed'
        max_pages: 최대 수집 페이지 수
        delay: 요청 간 딜레이(초)
        path: 'path' 모드일 때 크롤링할 경로 (예: '/blog/')
        progress_callback: 진행 상황 콜백
        user_agent: 사용할 User-Agent 문자열 (None이면 기본 Chrome)
        js_rendering: True면 모든 페이지를 Playwright로 렌더링

    Returns:
        dict: {
            'pages': list,        # 분석된 페이지 데이터 목록
            'issues': list,       # 진단 이슈 목록
            'incoming_map': dict, # URL → 수신 내부 링크 수
            'elapsed': float,     # 총 소요 시간(초)
            'summary': dict,      # 요약 통계 {total_pages, total_issues, high, medium, low}
        }
    """
    mode = mode.lower().strip()

    if mode == 'full':
        return run_full_crawl(base_url, max_pages, delay, progress_callback,
                              user_agent=user_agent, js_rendering=js_rendering)
    elif mode == 'sitemap':
        result = run_sitemap_crawl(base_url, max_pages, delay, progress_callback,
                                   user_agent=user_agent, js_rendering=js_rendering)
        if not result["pages"]:
            return run_full_crawl(base_url, max_pages, delay, progress_callback,
                                  user_agent=user_agent, js_rendering=js_rendering)
        return result
    elif mode == 'path':
        return run_path_crawl(base_url, path, max_pages, delay, progress_callback,
                              user_agent=user_agent, js_rendering=js_rendering)
    elif mode == 'mixed':
        return run_mixed_crawl(base_url, max_pages, delay, progress_callback,
                               user_agent=user_agent, js_rendering=js_rendering)
    else:
        raise ValueError(f"Unknown crawl mode: '{mode}'. Use 'full', 'sitemap', 'path', or 'mixed'.")


# ══════════════════════════════════════════════════════════════════════════════
# 10. AI/GEO — AI 검색 최적화 분석 엔진
# ══════════════════════════════════════════════════════════════════════════════

AI_BOT_AGENTS = {
    # ── 학습용 (Training) ──
    "GPTBot": {"company": "OpenAI", "tier": "학습", "purpose": "GPT 모델 학습 데이터 수집"},
    "Google-Extended": {"company": "Google", "tier": "학습", "purpose": "Gemini AI 학습 데이터"},
    "ClaudeBot": {"company": "Anthropic", "tier": "학습", "purpose": "Claude 모델 학습"},
    "CCBot": {"company": "Common Crawl", "tier": "학습", "purpose": "오픈 데이터셋 (다수 AI 사용)"},
    "Bytespider": {"company": "ByteDance", "tier": "학습", "purpose": "TikTok/모델 학습"},
    "cohere-ai": {"company": "Cohere", "tier": "학습", "purpose": "Cohere 언어 모델 학습"},
    "Diffbot": {"company": "Diffbot", "tier": "학습", "purpose": "구조화 데이터 추출"},
    "Applebot-Extended": {"company": "Apple", "tier": "학습", "purpose": "Apple Intelligence/Siri"},
    "meta-externalagent": {"company": "Meta", "tier": "학습", "purpose": "Meta AI 학습"},
    "AI2Bot": {"company": "Allen AI", "tier": "학습", "purpose": "학술 AI 연구"},
    # ── 검색용 (Search/Citation) ──
    "OAI-SearchBot": {"company": "OpenAI", "tier": "검색", "purpose": "ChatGPT 검색 인용"},
    "Claude-SearchBot": {"company": "Anthropic", "tier": "검색", "purpose": "Claude 검색 인덱싱"},
    "PerplexityBot": {"company": "Perplexity", "tier": "검색", "purpose": "Perplexity AI 검색"},
    "DuckAssistBot": {"company": "DuckDuckGo", "tier": "검색", "purpose": "DuckAssist AI 답변"},
    "YouBot": {"company": "You.com", "tier": "검색", "purpose": "You.com AI 검색"},
    "Amazonbot": {"company": "Amazon", "tier": "검색", "purpose": "Alexa/Amazon 기능"},
    # ── 사용자용 (User-Initiated) ──
    "ChatGPT-User": {"company": "OpenAI", "tier": "사용자", "purpose": "ChatGPT 사용자 링크 조회"},
    "Claude-User": {"company": "Anthropic", "tier": "사용자", "purpose": "Claude 사용자 콘텐츠 조회"},
    "Perplexity-User": {"company": "Perplexity", "tier": "사용자", "purpose": "Perplexity 사용자 인용"},
}


def analyze_ai_bot_access(base_url, session=None):
    """robots.txt에서 AI 봇별 접근 허용/차단 상태 분석"""
    if session is None:
        session = build_session()

    results = {}
    robots_text = ""

    try:
        r = session.get(urljoin(base_url, "/robots.txt"), timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            for bot, info in AI_BOT_AGENTS.items():
                results[bot] = {**info, "status": "허용", "reason": "robots.txt 없음"}
            return {"bots": results, "robots_exists": False, "robots_text": ""}
        robots_text = r.text
    except Exception:
        for bot, info in AI_BOT_AGENTS.items():
            results[bot] = {**info, "status": "확인불가", "reason": "robots.txt 접근 실패"}
        return {"bots": results, "robots_exists": False, "robots_text": ""}

    # Parse robots.txt into agent-specific rules
    agent_rules = {}  # agent_name -> list of (directive, path)
    current_agents = []

    for line in robots_text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.lower().startswith("user-agent:"):
            agent_name = line.split(":", 1)[1].strip()
            if not current_agents or (current_agents and agent_rules.get(current_agents[-1])):
                current_agents = [agent_name]
            else:
                current_agents.append(agent_name)
            if agent_name not in agent_rules:
                agent_rules[agent_name] = []
        elif current_agents:
            if line.lower().startswith(("disallow:", "allow:")):
                directive = line.split(":", 1)[0].strip().lower()
                path = line.split(":", 1)[1].strip()
                for ag in current_agents:
                    agent_rules.setdefault(ag, []).append((directive, path))

    # Check each AI bot
    for bot, info in AI_BOT_AGENTS.items():
        bot_lower = bot.lower()
        is_blocked = False
        block_reason = ""

        # Check bot-specific rules first
        specific_rules = None
        for agent_name, rules in agent_rules.items():
            if agent_name.lower() == bot_lower:
                specific_rules = rules
                break

        if specific_rules is not None:
            for directive, path in specific_rules:
                if directive == "disallow" and path == "/":
                    is_blocked = True
                    block_reason = f"User-agent: {bot} → Disallow: /"
                    break
                elif directive == "disallow" and path:
                    is_blocked = True
                    block_reason = f"User-agent: {bot} → Disallow: {path}"
                elif directive == "allow" and path == "/":
                    is_blocked = False
                    block_reason = ""
        else:
            # Fall back to wildcard rules
            wildcard_rules = agent_rules.get("*", [])
            for directive, path in wildcard_rules:
                if directive == "disallow" and path == "/":
                    is_blocked = True
                    block_reason = "User-agent: * → Disallow: /"
                    break

        if is_blocked:
            results[bot] = {**info, "status": "차단", "reason": block_reason}
        else:
            results[bot] = {**info, "status": "허용", "reason": "접근 허용"}

    return {"bots": results, "robots_exists": True, "robots_text": robots_text[:5000]}


def check_llms_txt(base_url, session=None):
    """llms.txt 파일 존재 및 유효성 검사"""
    if session is None:
        session = build_session()

    result = {
        "exists": False,
        "url": urljoin(base_url.rstrip("/") + "/", "llms.txt"),
        "content": "",
        "has_h1": False,
        "has_blockquote": False,
        "has_sections": False,
        "has_links": False,
        "issues": [],
        "score": 0,
    }

    try:
        r = session.get(result["url"], timeout=REQUEST_TIMEOUT)
        if r.status_code == 200 and len(r.text.strip()) > 10:
            content = r.text
            # Verify it looks like markdown, not HTML
            if "<html" in content.lower()[:200] or "<head" in content.lower()[:200]:
                return result

            result["exists"] = True
            result["content"] = content[:5000]

            lines = content.strip().split("\n")
            result["has_h1"] = any(l.strip().startswith("# ") for l in lines)
            result["has_blockquote"] = any(l.strip().startswith("> ") for l in lines)
            h2_count = sum(1 for l in lines if l.strip().startswith("## "))
            result["has_sections"] = h2_count > 0
            result["has_links"] = bool(re.search(r'\[.*?\]\(.*?\)', content))

            score = 0
            if result["has_h1"]:
                score += 25
            else:
                result["issues"].append("H1 제목(# 사이트명)이 없습니다")
            if result["has_blockquote"]:
                score += 25
            else:
                result["issues"].append("사이트 설명 blockquote(> 설명)가 없습니다")
            if result["has_sections"]:
                score += 25
            else:
                result["issues"].append("콘텐츠 섹션(## 카테고리)이 없습니다")
            if result["has_links"]:
                score += 25
            else:
                result["issues"].append("주요 콘텐츠 링크가 없습니다")
            result["score"] = score
    except Exception:
        pass

    return result


def calculate_eeat_score(eeat_info, schema_info, security_info=None, content_info=None):
    """
    E-E-A-T 시그널을 0~100 점수로 변환.
    각 축 25점 만점.
    """
    details = {}

    # ── Experience (경험) 25점 ──
    exp = 0
    exp_items = []
    if eeat_info.get("has_author"):
        exp += 8
        exp_items.append(f"✅ 저자: {eeat_info.get('author_name', '있음')[:30]}")
    else:
        exp_items.append("❌ 저자 정보 없음")
    if eeat_info.get("has_published_date"):
        exp += 7
        exp_items.append("✅ 게시일 표시")
    else:
        exp_items.append("❌ 게시일 없음")
    if eeat_info.get("has_modified_date"):
        exp += 5
        exp_items.append("✅ 수정일 표시")
    else:
        exp_items.append("❌ 수정일 없음")
    if eeat_info.get("has_reviews_schema"):
        exp += 5
        exp_items.append("✅ 리뷰/평점 스키마")
    exp = min(exp, 25)
    details["경험(Experience)"] = {"score": exp, "items": exp_items}

    # ── Expertise (전문성) 25점 ──
    expt = 0
    expt_items = []
    all_types = schema_info.get("all_types", [])
    if schema_info.get("has_schema"):
        expt += 5
        expt_items.append(f"✅ 구조화 데이터: {', '.join(all_types[:5])}")
    else:
        expt_items.append("❌ 구조화 데이터 없음")
    if any(t in all_types for t in ("Article", "BlogPosting", "NewsArticle", "TechArticle")):
        expt += 4
        expt_items.append("✅ 콘텐츠 타입 스키마")
    if "FAQPage" in all_types:
        expt += 5
        expt_items.append("✅ FAQ 스키마 (AI 인용에 매우 유리)")
    if "HowTo" in all_types:
        expt += 3
        expt_items.append("✅ HowTo 스키마")
    wc = content_info.get("word_count", 0) if content_info else 0
    if wc >= 2000:
        expt += 5
        expt_items.append(f"✅ 충분한 콘텐츠 ({wc}단어)")
    elif wc >= 1000:
        expt += 3
        expt_items.append(f"⚠️ 보통 콘텐츠 ({wc}단어)")
    elif wc >= 300:
        expt += 1
        expt_items.append(f"❌ 짧은 콘텐츠 ({wc}단어)")
    else:
        expt_items.append(f"❌ 매우 짧은 콘텐츠 ({wc}단어)")
    vi = schema_info.get("validation_issues", [])
    if not vi:
        expt += 3
        expt_items.append("✅ 스키마 유효성 통과")
    else:
        expt_items.append(f"⚠️ 스키마 오류 {len(vi)}건")
    expt = min(expt, 25)
    details["전문성(Expertise)"] = {"score": expt, "items": expt_items}

    # ── Authoritativeness (권위) 25점 ──
    auth = 0
    auth_items = []
    if eeat_info.get("has_org_schema"):
        auth += 6
        auth_items.append("✅ Organization 스키마")
    else:
        auth_items.append("❌ Organization 스키마 없음")
    if eeat_info.get("has_breadcrumb"):
        auth += 4
        auth_items.append("✅ 브레드크럼 내비게이션")
    else:
        auth_items.append("❌ 브레드크럼 없음")
    socials = eeat_info.get("social_links", [])
    if socials:
        auth += min(len(socials) * 2, 8)
        auth_items.append(f"✅ 소셜 링크: {', '.join(socials)}")
    else:
        auth_items.append("❌ 소셜 미디어 링크 없음")
    ext_count = content_info.get("external_links_count", 0) if content_info else 0
    if ext_count >= 3:
        auth += 4
        auth_items.append(f"✅ 외부 인용 {ext_count}개")
    elif ext_count >= 1:
        auth += 2
        auth_items.append(f"⚠️ 외부 인용 {ext_count}개 (3개 이상 권장)")
    else:
        auth_items.append("❌ 외부 인용 링크 없음")
    if "Person" in all_types:
        auth += 3
        auth_items.append("✅ Person 스키마")
    auth = min(auth, 25)
    details["권위(Authoritativeness)"] = {"score": auth, "items": auth_items}

    # ── Trust (신뢰) 25점 ──
    trust = 0
    trust_items = []
    if eeat_info.get("has_about_link"):
        trust += 5
        trust_items.append("✅ About 페이지 링크")
    else:
        trust_items.append("❌ About 페이지 없음")
    if eeat_info.get("has_contact_link"):
        trust += 5
        trust_items.append("✅ Contact 페이지 링크")
    else:
        trust_items.append("❌ Contact 페이지 없음")
    if eeat_info.get("has_privacy_link"):
        trust += 5
        trust_items.append("✅ 개인정보처리방침")
    else:
        trust_items.append("❌ 개인정보처리방침 없음")
    if eeat_info.get("has_terms_link"):
        trust += 4
        trust_items.append("✅ 이용약관")
    else:
        trust_items.append("❌ 이용약관 없음")
    if security_info:
        if security_info.get("is_https"):
            trust += 3
            trust_items.append("✅ HTTPS")
        else:
            trust_items.append("❌ HTTPS 미사용")
        if security_info.get("has_hsts"):
            trust += 3
            trust_items.append("✅ HSTS 헤더")
    trust = min(trust, 25)
    details["신뢰(Trust)"] = {"score": trust, "items": trust_items}

    total = details["경험(Experience)"]["score"] + details["전문성(Expertise)"]["score"] + \
            details["권위(Authoritativeness)"]["score"] + details["신뢰(Trust)"]["score"]

    if total >= 80:
        grade, color = "A", "#3fb950"
    elif total >= 60:
        grade, color = "B", "#58a6ff"
    elif total >= 40:
        grade, color = "C", "#d29922"
    elif total >= 20:
        grade, color = "D", "#f0883e"
    else:
        grade, color = "F", "#f85149"

    return {"score": total, "grade": grade, "color": color, "details": details}


def calculate_ai_readiness(soup, schema_info, eeat_info, content_info, tech_info):
    """
    AI 검색 최적화 준비도 점수 (0~100).
    6가지 차원으로 평가:
    - 인용 준비도 (25%)
    - 답변 적합성 (20%)
    - 콘텐츠 권위 (20%)
    - 지식 그래프 연동 (15%)
    - 기술적 접근성 (10%)
    - 차별화 (10%)
    """
    scores = {}
    details = {}

    body = soup.find("body") if soup else None
    body_text = body.get_text(separator="\n", strip=True) if body else ""

    # ══ 1. 인용 준비도 (Citation Readiness) — 25점 ══
    cr = 0
    cr_items = []

    # 인용 가능한 문장 (20~150자, 팩트성)
    sentences = [s.strip() for s in re.split(r'[.!?。！？]\s', body_text) if 20 <= len(s.strip()) <= 150]
    quotable = len(sentences)
    if quotable >= 10:
        cr += 8
    elif quotable >= 5:
        cr += 5
    elif quotable >= 2:
        cr += 3
    cr_items.append(f"인용 가능 문장: {quotable}개")

    # 통계/수치 데이터
    stats = re.findall(r'\d+[%％]|\d{1,3}(,\d{3})+|\d+\.\d+', body_text)
    stat_count = len(stats)
    if stat_count >= 5:
        cr += 7
    elif stat_count >= 2:
        cr += 4
    elif stat_count >= 1:
        cr += 2
    cr_items.append(f"통계/수치 데이터: {stat_count}개")

    # 구조화된 목록
    lists = soup.find_all(["ul", "ol"]) if soup else []
    list_items = sum(len(l.find_all("li")) for l in lists)
    if list_items >= 10:
        cr += 6
    elif list_items >= 5:
        cr += 4
    elif list_items >= 1:
        cr += 2
    cr_items.append(f"목록 항목: {list_items}개")

    # 테이블 (AI가 매우 선호)
    tables = soup.find_all("table") if soup else []
    if tables:
        cr += 4
    cr_items.append(f"데이터 테이블: {len(tables)}개")

    scores["인용 준비도"] = min(cr, 25)
    details["인용 준비도"] = cr_items

    # ══ 2. 답변 적합성 (Answer Alignment) — 20점 ══
    aa = 0
    aa_items = []

    headings = soup.find_all(["h2", "h3"]) if soup else []
    # 질문형 헤딩 체크
    q_pattern = re.compile(
        r'(무엇|어떻게|왜|언제|어디|누구|몇|할까|인가|일까|인지|방법|가이드|차이|비교|장단점|what|how|why|when|where|who|which|guide|tips|vs)\b',
        re.I
    )
    question_headings = [h for h in headings if
                         h.get_text(strip=True).rstrip().endswith(("?", "？")) or
                         q_pattern.search(h.get_text(strip=True))]
    q_ratio = len(question_headings) / max(len(headings), 1)
    if q_ratio >= 0.4:
        aa += 6
    elif q_ratio >= 0.2:
        aa += 4
    elif q_ratio >= 0.1:
        aa += 2
    aa_items.append(f"질문형 헤딩: {len(question_headings)}/{len(headings)}개 ({round(q_ratio * 100)}%)")

    # 첫 200자에 핵심 답변 포함
    first_content = body_text[:200] if body_text else ""
    if len(first_content) >= 100:
        aa += 4
        aa_items.append("✅ 도입부 100자 이상 (직접 답변 가능)")
    else:
        aa_items.append("❌ 도입부 콘텐츠 부족")

    # 짧은 단락 비율 (2~3문장, AI 친화적)
    paras = soup.find_all("p") if soup else []
    short_paras = [p for p in paras if 20 < len(p.get_text(strip=True)) and
                   len(p.get_text(strip=True).split(". ")) <= 3]
    sp_ratio = len(short_paras) / max(len(paras), 1)
    if sp_ratio >= 0.5:
        aa += 4
    elif sp_ratio >= 0.3:
        aa += 2
    aa_items.append(f"짧은 단락 비율: {round(sp_ratio * 100)}%")

    # 코드 블록/pre (기술 콘텐츠에서 AI 인용 증가)
    code_blocks = soup.find_all(["code", "pre"]) if soup else []
    if code_blocks:
        aa += 3
        aa_items.append(f"코드/예시 블록: {len(code_blocks)}개")

    # 정의형 구조 (dl/dt/dd)
    defs = soup.find_all("dl") if soup else []
    if defs:
        aa += 3
        aa_items.append(f"정의 목록: {len(defs)}개")

    scores["답변 적합성"] = min(aa, 20)
    details["답변 적합성"] = aa_items

    # ══ 3. 콘텐츠 권위 (Content Authority) — 20점 ══
    ca = 0
    ca_items = []

    if eeat_info.get("has_author"):
        ca += 6
        ca_items.append(f"✅ 저자: {eeat_info.get('author_name', '')[:30]}")
    else:
        ca_items.append("❌ 저자 정보 없음")

    if eeat_info.get("has_published_date"):
        ca += 4
        ca_items.append("✅ 게시일 표시")
    else:
        ca_items.append("❌ 게시일 없음")

    if eeat_info.get("has_modified_date"):
        ca += 4
        ca_items.append("✅ 수정일 표시 (최신성 신호)")
    else:
        ca_items.append("❌ 수정일 없음")

    ext_links = content_info.get("external_links_count", 0) if content_info else 0
    if ext_links >= 5:
        ca += 4
        ca_items.append(f"✅ 외부 출처 인용: {ext_links}개")
    elif ext_links >= 2:
        ca += 2
        ca_items.append(f"⚠️ 외부 출처 인용: {ext_links}개 (5개+ 권장)")
    else:
        ca_items.append(f"❌ 외부 출처 인용 부족: {ext_links}개")

    # 1인칭 경험 표현 감지
    exp_patterns = re.compile(r'(제가|저는|우리는|직접|테스트|경험|실제로|사용해|써본|I tested|we found|in my experience)', re.I)
    exp_matches = exp_patterns.findall(body_text[:3000])
    if exp_matches:
        ca += 2
        ca_items.append(f"✅ 1인칭 경험 표현: {len(exp_matches)}개")

    scores["콘텐츠 권위"] = min(ca, 20)
    details["콘텐츠 권위"] = ca_items

    # ══ 4. 지식 그래프 연동 (Knowledge Graph) — 15점 ══
    kg = 0
    kg_items = []
    all_types = schema_info.get("all_types", [])

    if schema_info.get("has_schema"):
        kg += 3
    if "FAQPage" in all_types:
        kg += 4
        kg_items.append("✅ FAQPage 스키마 (AI 인용률 +41%)")
    if any(t in all_types for t in ("Article", "BlogPosting", "NewsArticle")):
        kg += 3
        kg_items.append("✅ Article 스키마")
    if "BreadcrumbList" in all_types:
        kg += 2
        kg_items.append("✅ BreadcrumbList")
    if "Organization" in all_types or "LocalBusiness" in all_types:
        kg += 2
        kg_items.append("✅ Organization 스키마")
    if "Person" in all_types:
        kg += 1
        kg_items.append("✅ Person 스키마")

    if not all_types:
        kg_items.append("❌ 구조화 데이터 없음")
    else:
        kg_items.insert(0, f"스키마 타입: {', '.join(all_types[:6])}")

    scores["지식 그래프 연동"] = min(kg, 15)
    details["지식 그래프 연동"] = kg_items

    # ══ 5. 기술적 AI 접근성 (Technical Accessibility) — 10점 ══
    ta = 0
    ta_items = []

    if not tech_info.get("is_noindex"):
        ta += 3
        ta_items.append("✅ 인덱싱 허용")
    else:
        ta_items.append("❌ noindex 설정 — AI 검색 노출 불가")

    if tech_info.get("heading_hierarchy_ok"):
        ta += 2
        ta_items.append("✅ 헤딩 계층 구조 정상")
    else:
        ta_items.append("❌ 헤딩 계층 구조 오류")

    if tech_info.get("has_viewport"):
        ta += 1
        ta_items.append("✅ 모바일 뷰포트")

    if not tech_info.get("is_nofollow"):
        ta += 2
        ta_items.append("✅ 링크 추적 허용")

    if tech_info.get("lang"):
        ta += 2
        ta_items.append(f"✅ 언어 명시: {tech_info['lang']}")
    else:
        ta_items.append("❌ html lang 속성 없음")

    scores["기술적 접근성"] = min(ta, 10)
    details["기술적 접근성"] = ta_items

    # ══ 6. 차별화 (Differentiation) — 10점 ══
    diff = 0
    diff_items = []

    wc = content_info.get("word_count", 0) if content_info else 0
    if wc >= 2000:
        diff += 3
    elif wc >= 1000:
        diff += 2
    elif wc >= 500:
        diff += 1
    diff_items.append(f"콘텐츠 길이: {wc}단어")

    imgs = soup.find_all("img") if soup else []
    imgs_with_alt = [i for i in imgs if i.get("alt", "").strip()]
    if len(imgs_with_alt) >= 3:
        diff += 3
    elif len(imgs_with_alt) >= 1:
        diff += 1
    diff_items.append(f"이미지(alt 포함): {len(imgs_with_alt)}개")

    if tables:
        diff += 2
    if code_blocks:
        diff += 2

    scores["차별화"] = min(diff, 10)
    details["차별화"] = diff_items

    # ══ 종합 ══
    total = sum(scores.values())
    if total >= 85:
        grade, grade_kr, color = "AI-Optimized", "AI 최적화 완료", "#3fb950"
    elif total >= 70:
        grade, grade_kr, color = "AI-Ready", "AI 준비 완료", "#58a6ff"
    elif total >= 55:
        grade, grade_kr, color = "Needs Work", "개선 필요", "#d29922"
    else:
        grade, grade_kr, color = "Not Optimized", "최적화 필요", "#f85149"

    return {
        "total_score": total,
        "grade": grade,
        "grade_kr": grade_kr,
        "color": color,
        "dimension_scores": scores,
        "details": details,
    }


def analyze_content_optimization(url, keyword, session=None):
    """
    키워드 기반 콘텐츠 최적화 분석.
    Surfer SEO 스타일의 콘텐츠 점수를 제공합니다.
    """
    if session is None:
        session = build_session()

    result = {
        "url": url, "keyword": keyword, "score": 0,
        "checks": [], "recommendations": [],
    }

    try:
        page = analyze_page(url, session)
        if page.get("Error") or not page.get("Status") or page["Status"] != 200:
            result["checks"].append({"name": "페이지 접근", "pass": False, "detail": f"페이지에 접근할 수 없습니다: {page.get('Error', 'Unknown')}"})
            return result

        soup = BeautifulSoup(
            session.get(url, timeout=REQUEST_TIMEOUT).content, "html.parser"
        )
        body = soup.find("body")
        body_text = body.get_text(separator=" ", strip=True).lower() if body else ""
        kw = keyword.lower().strip()
        score = 0

        # 1. Title에 키워드 포함 (15점)
        title = page.get("Title", "").lower()
        if kw in title:
            score += 15
            result["checks"].append({"name": "Title 키워드", "pass": True, "detail": f"Title에 '{keyword}' 포함"})
        else:
            result["checks"].append({"name": "Title 키워드", "pass": False, "detail": f"Title에 '{keyword}' 미포함"})
            result["recommendations"].append(f"Title에 '{keyword}'를 자연스럽게 포함하세요")

        # 2. H1에 키워드 포함 (10점)
        h1 = page.get("H1", "").lower()
        if kw in h1:
            score += 10
            result["checks"].append({"name": "H1 키워드", "pass": True, "detail": f"H1에 '{keyword}' 포함"})
        else:
            result["checks"].append({"name": "H1 키워드", "pass": False, "detail": f"H1에 '{keyword}' 미포함"})
            result["recommendations"].append(f"H1에 '{keyword}'를 포함하세요")

        # 3. Meta Description에 키워드 (10점)
        desc = page.get("Meta Desc", "").lower()
        if kw in desc:
            score += 10
            result["checks"].append({"name": "Description 키워드", "pass": True, "detail": "Meta Description에 키워드 포함"})
        else:
            result["checks"].append({"name": "Description 키워드", "pass": False, "detail": "Meta Description에 키워드 미포함"})
            result["recommendations"].append(f"Meta Description에 '{keyword}'를 포함하세요")

        # 4. H2/H3에 키워드 또는 관련어 (10점)
        sub_headings = [h.get_text(strip=True).lower() for h in soup.find_all(["h2", "h3"])]
        kw_in_headings = sum(1 for h in sub_headings if kw in h)
        if kw_in_headings >= 2:
            score += 10
            result["checks"].append({"name": "서브헤딩 키워드", "pass": True, "detail": f"H2/H3 중 {kw_in_headings}개에 키워드 포함"})
        elif kw_in_headings == 1:
            score += 5
            result["checks"].append({"name": "서브헤딩 키워드", "pass": True, "detail": f"H2/H3 중 {kw_in_headings}개에 키워드 포함 (2개+ 권장)"})
        else:
            result["checks"].append({"name": "서브헤딩 키워드", "pass": False, "detail": "H2/H3에 키워드 미포함"})
            result["recommendations"].append("H2 또는 H3 소제목에 키워드 또는 관련어를 포함하세요")

        # 5. 본문 키워드 밀도 (10점)
        words = body_text.split()
        word_count = len(words)
        kw_count = body_text.count(kw)
        density = (kw_count / max(word_count, 1)) * 100
        if 0.5 <= density <= 3.0:
            score += 10
            result["checks"].append({"name": "키워드 밀도", "pass": True, "detail": f"키워드 밀도 {density:.1f}% (적정 범위)"})
        elif density > 0:
            score += 5
            detail = f"키워드 밀도 {density:.1f}% ({'과다' if density > 3 else '부족'})"
            result["checks"].append({"name": "키워드 밀도", "pass": False, "detail": detail})
            if density > 3:
                result["recommendations"].append("키워드 밀도가 너무 높습니다. 자연스러운 문장으로 조절하세요")
            else:
                result["recommendations"].append("키워드 사용 빈도를 조금 더 높이세요 (0.5~3% 권장)")
        else:
            result["checks"].append({"name": "키워드 밀도", "pass": False, "detail": "본문에 키워드 없음"})
            result["recommendations"].append(f"본문에 '{keyword}'를 자연스럽게 포함하세요")

        # 6. 콘텐츠 길이 (10점)
        if word_count >= 2000:
            score += 10
            result["checks"].append({"name": "콘텐츠 길이", "pass": True, "detail": f"{word_count}단어 (충분)"})
        elif word_count >= 1000:
            score += 7
            result["checks"].append({"name": "콘텐츠 길이", "pass": True, "detail": f"{word_count}단어 (양호)"})
        elif word_count >= 500:
            score += 4
            result["checks"].append({"name": "콘텐츠 길이", "pass": False, "detail": f"{word_count}단어 (1000+ 권장)"})
            result["recommendations"].append("콘텐츠를 1000단어 이상으로 보강하세요")
        else:
            result["checks"].append({"name": "콘텐츠 길이", "pass": False, "detail": f"{word_count}단어 (매우 짧음)"})
            result["recommendations"].append("콘텐츠가 너무 짧습니다. 최소 1000단어 이상 작성하세요")

        # 7. URL에 키워드 (5점)
        url_path = urlparse(url).path.lower()
        kw_slug = kw.replace(" ", "-")
        if kw in url_path or kw_slug in url_path or any(w in url_path for w in kw.split() if len(w) > 2):
            score += 5
            result["checks"].append({"name": "URL 키워드", "pass": True, "detail": "URL 경로에 키워드 관련어 포함"})
        else:
            result["checks"].append({"name": "URL 키워드", "pass": False, "detail": "URL에 키워드 미포함"})

        # 8. 이미지 최적화 (10점)
        imgs = soup.find_all("img")
        imgs_with_kw_alt = [i for i in imgs if kw in (i.get("alt", "").lower())]
        if imgs and imgs_with_kw_alt:
            score += 10
            result["checks"].append({"name": "이미지 ALT 키워드", "pass": True, "detail": f"이미지 ALT에 키워드 포함 ({len(imgs_with_kw_alt)}개)"})
        elif imgs:
            score += 3
            result["checks"].append({"name": "이미지 ALT 키워드", "pass": False, "detail": "이미지 있지만 ALT에 키워드 미포함"})
            result["recommendations"].append("최소 1개 이미지의 ALT 텍스트에 키워드를 포함하세요")
        else:
            result["checks"].append({"name": "이미지", "pass": False, "detail": "이미지 없음"})
            result["recommendations"].append("관련 이미지를 추가하세요")

        # 9. 내부 링크 (5점)
        internal_links = len(page.get("_internal_links", []))
        if internal_links >= 5:
            score += 5
            result["checks"].append({"name": "내부 링크", "pass": True, "detail": f"내부 링크 {internal_links}개"})
        elif internal_links >= 2:
            score += 3
            result["checks"].append({"name": "내부 링크", "pass": True, "detail": f"내부 링크 {internal_links}개 (5개+ 권장)"})
        else:
            result["checks"].append({"name": "내부 링크", "pass": False, "detail": f"내부 링크 {internal_links}개"})
            result["recommendations"].append("관련 페이지로의 내부 링크를 더 추가하세요")

        # 10. 외부 인용 링크 (5점)
        ext_links = page.get("Ext Links", 0)
        if ext_links >= 2:
            score += 5
            result["checks"].append({"name": "외부 인용", "pass": True, "detail": f"외부 링크 {ext_links}개"})
        elif ext_links >= 1:
            score += 3
            result["checks"].append({"name": "외부 인용", "pass": True, "detail": f"외부 링크 {ext_links}개 (2개+ 권장)"})
        else:
            result["checks"].append({"name": "외부 인용", "pass": False, "detail": "외부 인용 링크 없음"})
            result["recommendations"].append("신뢰할 수 있는 출처를 인용하는 외부 링크를 추가하세요")

        # 11. Schema 마크업 (5점)
        if page.get("Has Schema"):
            score += 5
            result["checks"].append({"name": "구조화 데이터", "pass": True, "detail": f"Schema: {page.get('Schema Types', '')}"})
        else:
            result["checks"].append({"name": "구조화 데이터", "pass": False, "detail": "구조화 데이터 없음"})
            result["recommendations"].append("JSON-LD 구조화 데이터(Article, FAQPage 등)를 추가하세요")

        # 12. 첫 100단어에 키워드 (5점)
        first_100 = " ".join(words[:100]).lower()
        if kw in first_100:
            score += 5
            result["checks"].append({"name": "도입부 키워드", "pass": True, "detail": "첫 100단어 내 키워드 포함"})
        else:
            result["checks"].append({"name": "도입부 키워드", "pass": False, "detail": "첫 100단어에 키워드 미포함"})
            result["recommendations"].append("글의 도입부(첫 100단어)에 키워드를 자연스럽게 포함하세요")

        result["score"] = min(score, 100)
        result["word_count"] = word_count
        result["keyword_count"] = kw_count
        result["keyword_density"] = round(density, 2)
        result["heading_count"] = len(sub_headings)
        result["image_count"] = len(imgs)
        result["internal_links"] = internal_links
        result["external_links"] = ext_links

    except Exception as e:
        result["checks"].append({"name": "분석 오류", "pass": False, "detail": str(e)[:200]})

    return result
