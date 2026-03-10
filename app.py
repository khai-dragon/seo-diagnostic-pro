#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SEO Diagnostic Pro v2 — Screaming Frog Style
실시간 크롤링 + 데이터 테이블 + 진단 리포트
"""

import re
import json
import time
import pandas as pd
import streamlit as st
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urlparse, urljoin, urldefrag
from collections import defaultdict

# ── 설정 ─────────────────────────────────────────────────────────────────────
USER_AGENT = "SEODiagnosticPro/2.0 (+https://seodiagnosticpro.dev/bot)"
REQUEST_TIMEOUT = 15
TITLE_MIN, TITLE_MAX = 30, 60
DESC_MIN, DESC_MAX = 120, 160
THIN_CONTENT_THRESHOLD = 300
MIN_INCOMING_LINKS = 3
PAGESPEED_THRESHOLD = 90

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


# ── 페이지 분석 ──────────────────────────────────────────────────────────────
def analyze_page(url, session):
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
        "_internal_links": [],
    }
    try:
        start = time.time()
        r = session.get(url, timeout=REQUEST_TIMEOUT)
        result["Load (s)"] = round(time.time() - start, 2)
        result["Status"] = r.status_code

        if r.status_code != 200:
            return result

        soup = BeautifulSoup(r.content, "html.parser")

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

        # Internal links
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

    except Exception as e:
        result["Error"] = str(e)[:80]

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


# ── 진단 엔진 ────────────────────────────────────────────────────────────────
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

    for p in pages:
        url = p["URL"]
        tl = p["Title Len"]
        dl = p["Desc Len"]

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

        if p["Words"] < THIN_CONTENT_THRESHOLD and p["Status"] == 200:
            issues.append({"type": "Thin Content", "severity": "MEDIUM", "detail": f"단어 수 {p['Words']}개 (최소 {THIN_CONTENT_THRESHOLD} 권장)", "pages": [url], "fix": "FAQ, 관련 정보, 사용자 후기 등으로 콘텐츠 보강."})

        if p["Alt Missing"] > 0:
            issues.append({"type": "Alt 누락", "severity": "LOW", "detail": f"Alt 없는 이미지 {p['Alt Missing']}/{p['Images']}개", "pages": [url], "fix": "모든 이미지에 설명적인 alt 텍스트를 추가하세요."})

        if p["Status"] and p["Status"] >= 400:
            issues.append({"type": f"HTTP {p['Status']}", "severity": "HIGH", "detail": f"HTTP {p['Status']} 에러", "pages": [url], "fix": "깨진 링크 수정 또는 301 리다이렉트 설정."})

        inc = incoming_map.get(url, 0)
        if inc < MIN_INCOMING_LINKS and p["Status"] == 200:
            issues.append({"type": "Inlinks 부족", "severity": "MEDIUM", "detail": f"들어오는 링크 {inc}개 (최소 {MIN_INCOMING_LINKS} 권장)", "pages": [url], "fix": "내부 링크 추가 (실크 로드 전략 권장)."})

        score = pagespeed_scores.get(url)
        if score is not None and score < PAGESPEED_THRESHOLD:
            issues.append({"type": "PageSpeed 낮음", "severity": "HIGH" if score < 50 else "MEDIUM", "detail": f"Mobile 점수: {score}/100", "pages": [url], "fix": "이미지 최적화, Lazy Loading, 렌더링 차단 리소스 제거."})

        if p["Load (s)"] > 3.0:
            issues.append({"type": "느린 로딩", "severity": "MEDIUM", "detail": f"로드 시간 {p['Load (s)']}초", "pages": [url], "fix": "서버 응답 시간, CDN, 캐싱 설정 점검."})

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
    <span>Screaming Frog Style &middot; v2.0</span>
</div>
""", unsafe_allow_html=True)

# ── 상단 입력바 (Screaming Frog처럼 URL 바 상단에) ────────────────────────────
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
    st.divider()
    st.markdown("### 📖 사용법")
    st.markdown("""
    1. 상단에 URL 입력
    2. 모드/페이지수/딜레이 설정
    3. **▶ Start** 클릭
    4. 실시간으로 결과 확인!
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
    #### 🔍 Title & Description
    길이, 중복, 누락을
    자동 탐지합니다
    """)
    c3.markdown("""
    #### 🔗 내부 링크 분석
    Inlinks / Outlinks를
    한눈에 확인
    """)
    c4.markdown("""
    #### ⚡ PageSpeed
    Google PSI Mobile
    점수 자동 측정
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
# 실시간 크롤링 (Screaming Frog 스타일)
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
all_internal_links = []  # (source, target) 저장용
crawl_start_time = time.time()


def update_live_display(pages_so_far, current_url, total_target, queue_size):
    """실시간 UI 업데이트"""
    count = len(pages_so_far)
    elapsed = time.time() - crawl_start_time

    # 속도 계산
    speed = count / elapsed if elapsed > 0 else 0
    eta = (total_target - count) / speed if speed > 0 and count < total_target else 0

    # 진행률
    pct = min(count / total_target, 1.0) if total_target > 0 else 0
    progress_bar.progress(pct)

    # 상태 메트릭
    ph_crawled.metric("Crawled", f"{count}/{total_target}")
    ph_queued.metric("Queue", queue_size)
    ph_elapsed.metric("Elapsed", fmt_time(elapsed))
    ph_eta.metric("ETA", fmt_time(eta))
    ph_speed.metric("Speed", f"{speed:.1f} pg/s")

    # 현재 URL
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

    # 실시간 데이터 테이블
    if pages_so_far:
        display_cols = ["URL", "Status", "Title", "Title Len", "Meta Desc", "Desc Len",
                        "H1", "Canonical", "Words", "Outlinks", "Alt Missing", "Load (s)"]
        df = pd.DataFrame(pages_so_far)[display_cols]
        # URL 짧게
        df["URL"] = df["URL"].apply(lambda x: urlparse(x).path or "/")
        table_placeholder.dataframe(df, use_container_width=True, hide_index=True, height=400)


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
            page = analyze_page(url, session)
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

    while queue and len(visited) < max_pages:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)

        page = analyze_page(url, session)
        pages.append(page)
        all_internal_links.extend([(url, link) for link in page["_internal_links"]])

        for link in page["_internal_links"]:
            if link not in visited:
                queue.append(link)

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

# Inlinks 열 추가
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

cols = st.columns(7)
metrics = [
    ("총 페이지", len(pages), ""),
    ("총 이슈", len(issues), ""),
    ("🔴 HIGH", high, "red"),
    ("🟡 MEDIUM", med, "yellow"),
    ("🟢 LOW", low, "green"),
    ("평균 로딩", f"{avg_load}s", ""),
    ("평균 단어", avg_words, ""),
]
for col, (label, val, cls) in zip(cols, metrics):
    col.markdown(
        f'<div class="summary-card {cls}"><div class="num">{val}</div><div class="label">{label}</div></div>',
        unsafe_allow_html=True,
    )

st.markdown("")

# ── 탭 구성 ──────────────────────────────────────────────────────────────────
tab_all, tab_td, tab_links, tab_tree, tab_issues, tab_export = st.tabs([
    "📋 전체 데이터",
    "🏷️ Title & Description",
    "🔗 내부 링크",
    "🌳 사이트 구조",
    "⚠️ 진단 리포트",
    "💾 Export",
])

# ── TAB: 전체 데이터 (Screaming Frog 메인 뷰) ────────────────────────────────
with tab_all:
    st.subheader("All Pages")
    display_cols = ["URL", "Status", "Title", "Title Len", "Meta Desc", "Desc Len",
                    "H1", "H1 Len", "Canonical", "Words", "Inlinks", "Outlinks",
                    "Images", "Alt Missing", "Load (s)", "Error"]
    if pagespeed_scores:
        for p in pages:
            if "PageSpeed" not in p:
                p["PageSpeed"] = "-"
        display_cols.append("PageSpeed")

    df_all = pd.DataFrame(pages)
    available_cols = [c for c in display_cols if c in df_all.columns]
    st.dataframe(df_all[available_cols], use_container_width=True, hide_index=True, height=500)
    st.caption(f"총 {len(pages)}개 페이지 | 평균 로드 {avg_load}s | 평균 {avg_words}단어")

# ── TAB: Title & Description (한눈에 보기) ───────────────────────────────────
with tab_td:
    st.subheader("Title & Description Overview")

    td_data = []
    for p in pages:
        # Title 상태 판정
        tl = p["Title Len"]
        if tl == 0:
            t_status = "❌ 없음"
        elif tl < TITLE_MIN:
            t_status = f"⚠️ 짧음 ({tl}자)"
        elif tl > TITLE_MAX:
            t_status = f"⚠️ 김 ({tl}자)"
        else:
            t_status = f"✅ 적정 ({tl}자)"

        # Desc 상태 판정
        dl = p["Desc Len"]
        if dl == 0:
            d_status = "❌ 없음"
        elif dl < DESC_MIN:
            d_status = f"⚠️ 짧음 ({dl}자)"
        elif dl > DESC_MAX:
            d_status = f"⚠️ 김 ({dl}자)"
        else:
            d_status = f"✅ 적정 ({dl}자)"

        # H1 상태
        h1_status = f"✅ {p['H1'][:30]}" if p["H1"] else "❌ 없음"

        # Canonical 상태
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

    # 요약
    t_ok = sum(1 for p in pages if TITLE_MIN <= p["Title Len"] <= TITLE_MAX)
    d_ok = sum(1 for p in pages if DESC_MIN <= p["Desc Len"] <= DESC_MAX)
    h_ok = sum(1 for p in pages if p["H1 Len"] > 0)
    c_ok = sum(1 for p in pages if p["Canonical"])

    sc1, sc2, sc3, sc4 = st.columns(4)
    sc1.metric("Title 적정", f"{t_ok}/{len(pages)}")
    sc2.metric("Description 적정", f"{d_ok}/{len(pages)}")
    sc3.metric("H1 있음", f"{h_ok}/{len(pages)}")
    sc4.metric("Canonical 있음", f"{c_ok}/{len(pages)}")

# ── TAB: 내부 링크 ───────────────────────────────────────────────────────────
with tab_links:
    st.subheader("Internal Links (Inlinks / Outlinks)")

    link_data = []
    all_urls_set = {p["URL"] for p in pages}
    for p in pages:
        url = p["URL"]
        inc = incoming_map.get(url, 0)
        if inc == 0:
            status = "🚨 고아"
        elif inc < MIN_INCOMING_LINKS:
            status = "⚠️ 부족"
        else:
            status = "✅ 양호"
        link_data.append({
            "URL": urlparse(url).path or "/",
            "Inlinks": inc,
            "Outlinks": p["Outlinks"],
            "상태": status,
        })

    df_links = pd.DataFrame(link_data)
    df_links = df_links.sort_values("Inlinks", ascending=True)
    st.dataframe(df_links, use_container_width=True, hide_index=True, height=400)

    orphan_count = sum(1 for p in pages if incoming_map.get(p["URL"], 0) == 0)
    weak_count = sum(1 for p in pages if 0 < incoming_map.get(p["URL"], 0) < MIN_INCOMING_LINKS)
    lc1, lc2, lc3 = st.columns(3)
    lc1.metric("🚨 고아 페이지", orphan_count)
    lc2.metric("⚠️ 링크 부족", weak_count)
    lc3.metric("✅ 양호", len(pages) - orphan_count - weak_count)

# ── TAB: 사이트 구조 ─────────────────────────────────────────────────────────
with tab_tree:
    st.subheader("Site Structure")
    tree_str = build_tree_string([p["URL"] for p in pages], base_domain)
    st.code(tree_str, language=None)

# ── TAB: 진단 리포트 ─────────────────────────────────────────────────────────
with tab_issues:
    st.subheader(f"Issues — {len(issues)}건")

    # 필터
    fc1, fc2 = st.columns([1, 3])
    with fc1:
        sev_filter = st.multiselect("심각도", ["HIGH", "MEDIUM", "LOW"], default=["HIGH", "MEDIUM", "LOW"])
    with fc2:
        type_options = sorted(set(i["type"] for i in issues))
        type_filter = st.multiselect("유형", type_options, default=type_options)

    filtered = [i for i in issues if i["severity"] in sev_filter and i["type"] in type_filter]

    # 이슈 테이블
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

    # 이슈 유형별 요약
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
    st.dataframe(pd.DataFrame(summary_rows), use_container_width=True, hide_index=True)

# ── TAB: Export ──────────────────────────────────────────────────────────────
with tab_export:
    st.subheader("Export")

    report = {
        "generated_at": datetime.now().isoformat(),
        "base_url": base_url,
        "total_pages": len(pages),
        "crawl_time_seconds": round(elapsed_total, 1),
        "mode": "sitemap" if is_sitemap_mode else "crawl",
        "summary": {"high": high, "medium": med, "low": low, "total": len(issues)},
        "pages": [{k: v for k, v in p.items() if not k.startswith("_")} for p in pages],
        "issues": issues,
    }

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")

    ec1, ec2 = st.columns(2)

    with ec1:
        json_str = json.dumps(report, ensure_ascii=False, indent=2)
        st.download_button(
            "📥 JSON 다운로드",
            data=json_str,
            file_name=f"seo_report_{timestamp}.json",
            mime="application/json",
            use_container_width=True,
        )

    with ec2:
        # CSV 다운로드
        export_cols = ["URL", "Status", "Title", "Title Len", "Meta Desc", "Desc Len",
                       "H1", "H1 Len", "Canonical", "Words", "Inlinks", "Outlinks",
                       "Images", "Alt Missing", "Load (s)"]
        df_export = pd.DataFrame(pages)
        available_export = [c for c in export_cols if c in df_export.columns]
        csv_str = df_export[available_export].to_csv(index=False)
        st.download_button(
            "📥 CSV 다운로드",
            data=csv_str,
            file_name=f"seo_report_{timestamp}.csv",
            mime="text/csv",
            use_container_width=True,
        )

    with st.expander("JSON 미리보기"):
        st.json(report)
