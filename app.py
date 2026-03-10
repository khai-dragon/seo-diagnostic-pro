#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SEO Diagnostic Pro — Web Edition
Streamlit 기반 웹 인터페이스
"""

import re
import json
import time
import streamlit as st
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urlparse, urljoin, urldefrag
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── 설정 ─────────────────────────────────────────────────────────────────────
USER_AGENT = "SEODiagnosticPro/1.0 (+https://seodiagnosticpro.dev/bot)"
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


def count_words(text):
    return len(text.split())


def build_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
    })
    return s


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
        "url": url,
        "status_code": None,
        "title": "",
        "title_length": 0,
        "meta_description": "",
        "meta_description_length": 0,
        "h1": "",
        "h1_exists": False,
        "word_count": 0,
        "canonical": "",
        "images_missing_alt": 0,
        "total_images": 0,
        "outgoing_internal_links": [],
        "load_time": 0.0,
        "error": None,
    }
    try:
        start = time.time()
        r = session.get(url, timeout=REQUEST_TIMEOUT)
        result["load_time"] = round(time.time() - start, 3)
        result["status_code"] = r.status_code

        if r.status_code != 200:
            return result

        soup = BeautifulSoup(r.content, "html.parser")

        tag = soup.find("title")
        if tag and tag.string:
            result["title"] = tag.string.strip()
            result["title_length"] = len(result["title"])

        meta = soup.find("meta", attrs={"name": re.compile(r"^description$", re.I)})
        if meta and meta.get("content"):
            result["meta_description"] = meta["content"].strip()
            result["meta_description_length"] = len(result["meta_description"])

        h1 = soup.find("h1")
        if h1:
            result["h1"] = h1.get_text(strip=True)
            result["h1_exists"] = True

        canon = soup.find("link", attrs={"rel": "canonical"})
        if canon and canon.get("href"):
            result["canonical"] = canon["href"].strip()

        body = soup.find("body")
        if body:
            text = body.get_text(separator=" ", strip=True)
            result["word_count"] = count_words(text)

        imgs = soup.find_all("img")
        result["total_images"] = len(imgs)
        result["images_missing_alt"] = sum(
            1 for img in imgs if not img.get("alt", "").strip()
        )

        base_domain = urlparse(url).netloc
        internal_links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            full = urljoin(url, href)
            normalized = normalize_url(full)
            if is_same_domain(normalized, base_domain):
                internal_links.add(normalized)
        result["outgoing_internal_links"] = sorted(internal_links)

    except Exception as e:
        result["error"] = str(e)

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


# ── 크롤러 ───────────────────────────────────────────────────────────────────
def crawl_site(start_url, session, max_pages, base_domain, delay, progress_bar, status_text):
    visited = set()
    queue = [normalize_url(start_url)]
    results = []

    while queue and len(visited) < max_pages:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)

        status_text.text(f"크롤링 중... [{len(visited)}/{max_pages}] {url}")
        progress_bar.progress(len(visited) / max_pages)

        page = analyze_page(url, session)
        results.append(page)

        for link in page["outgoing_internal_links"]:
            if link not in visited:
                queue.append(link)

        time.sleep(delay)

    return results


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


# ── 진단 ─────────────────────────────────────────────────────────────────────
def run_diagnostics(pages, incoming_map, pagespeed_scores):
    issues = []

    title_map = defaultdict(list)
    for p in pages:
        if p["title"]:
            title_map[p["title"]].append(p["url"])
    for title, urls in title_map.items():
        if len(urls) > 1:
            issues.append({
                "type": "중복 Title",
                "severity": "HIGH",
                "icon": "🔴",
                "detail": f'Title "{title[:50]}" 이(가) {len(urls)}개 페이지에서 중복됩니다.',
                "pages": urls,
                "recommendation": "각 페이지마다 고유한 Title을 작성하세요.",
            })

    for p in pages:
        url = p["url"]

        tl = p["title_length"]
        if tl > 0 and (tl < TITLE_MIN or tl > TITLE_MAX):
            issues.append({
                "type": "Title 길이 부적절",
                "severity": "MEDIUM",
                "icon": "🟡",
                "detail": f"Title 길이 {tl}자 → {TITLE_MIN}~{TITLE_MAX}자로 수정하세요",
                "pages": [url],
                "recommendation": f"현재 {tl}자입니다. {TITLE_MIN}~{TITLE_MAX}자 사이로 조절하세요.",
            })
        elif tl == 0:
            issues.append({
                "type": "Title 없음",
                "severity": "HIGH",
                "icon": "🔴",
                "detail": "Title 태그가 비어 있거나 없습니다.",
                "pages": [url],
                "recommendation": "모든 페이지에 고유하고 설명적인 Title 태그를 추가하세요.",
            })

        dl = p["meta_description_length"]
        if dl > 0 and (dl < DESC_MIN or dl > DESC_MAX):
            issues.append({
                "type": "Meta Description 길이 부적절",
                "severity": "MEDIUM",
                "icon": "🟡",
                "detail": f"Meta Description 길이 {dl}자 → {DESC_MIN}~{DESC_MAX}자로 수정하세요",
                "pages": [url],
                "recommendation": f"현재 {dl}자입니다. {DESC_MIN}~{DESC_MAX}자 사이로 작성하세요.",
            })
        elif dl == 0:
            issues.append({
                "type": "Meta Description 없음",
                "severity": "MEDIUM",
                "icon": "🟡",
                "detail": "Meta Description이 없습니다.",
                "pages": [url],
                "recommendation": "핵심 키워드와 CTA를 포함한 설명을 추가하세요.",
            })

        if not p["h1_exists"]:
            issues.append({
                "type": "H1 태그 없음",
                "severity": "HIGH",
                "icon": "🔴",
                "detail": "H1 태그가 없습니다.",
                "pages": [url],
                "recommendation": "페이지당 하나의 H1 태그를 사용하고 핵심 키워드를 포함하세요.",
            })

        if p["word_count"] < THIN_CONTENT_THRESHOLD and p["status_code"] == 200:
            issues.append({
                "type": "얇은 콘텐츠 (Thin Content)",
                "severity": "MEDIUM",
                "icon": "🟡",
                "detail": f"단어 수 {p['word_count']}개 → 최소 {THIN_CONTENT_THRESHOLD}단어 이상 권장",
                "pages": [url],
                "recommendation": "콘텐츠를 보강하세요. FAQ, 관련 정보, 사용자 후기 등을 추가하면 효과적입니다.",
            })

        if p["images_missing_alt"] > 0:
            issues.append({
                "type": "이미지 Alt 텍스트 누락",
                "severity": "LOW",
                "icon": "🟢",
                "detail": f"Alt 없는 이미지 {p['images_missing_alt']}개 / 전체 {p['total_images']}개",
                "pages": [url],
                "recommendation": "모든 이미지에 설명적인 alt 텍스트를 추가하세요.",
            })

        if p["status_code"] and p["status_code"] >= 400:
            issues.append({
                "type": f"HTTP {p['status_code']} 에러",
                "severity": "HIGH",
                "icon": "🔴",
                "detail": f"HTTP 상태 코드 {p['status_code']}",
                "pages": [url],
                "recommendation": "깨진 링크를 수정하거나 301 리다이렉트를 설정하세요.",
            })

        inc = incoming_map.get(url, 0)
        if inc < MIN_INCOMING_LINKS and p["status_code"] == 200:
            issues.append({
                "type": "들어오는 내부 링크 부족",
                "severity": "MEDIUM",
                "icon": "🟡",
                "detail": f"들어오는 내부 링크 {inc}개 → 최소 {MIN_INCOMING_LINKS}개 이상 권장",
                "pages": [url],
                "recommendation": "이 페이지로 내부 링크를 더 추가하세요 (실크 로드 전략 권장).",
            })

        score = pagespeed_scores.get(url)
        if score is not None and score < PAGESPEED_THRESHOLD:
            issues.append({
                "type": "PageSpeed 점수 낮음",
                "severity": "HIGH" if score < 50 else "MEDIUM",
                "icon": "🔴" if score < 50 else "🟡",
                "detail": f"Mobile PageSpeed 점수: {score}/100",
                "pages": [url],
                "recommendation": "이미지 최적화, Lazy Loading, 렌더링 차단 리소스 제거 추천.",
            })

        if p["load_time"] > 3.0:
            issues.append({
                "type": "페이지 로드 시간 느림",
                "severity": "MEDIUM",
                "icon": "🟡",
                "detail": f"로드 시간: {p['load_time']}초",
                "pages": [url],
                "recommendation": "서버 응답 시간 점검, CDN 사용 및 캐싱 설정을 확인하세요.",
            })

    severity_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    issues.sort(key=lambda x: severity_order.get(x["severity"], 9))
    return issues


# ══════════════════════════════════════════════════════════════════════════════
# UI
# ══════════════════════════════════════════════════════════════════════════════

# ── 사이드바 ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🔍 SEO Diagnostic Pro")
    st.caption("Screaming Frog 스타일 완전 자동 SEO 진단")
    st.divider()

    base_url = st.text_input(
        "분석할 URL",
        placeholder="https://example.com",
        help="분석할 웹사이트의 루트 URL을 입력하세요",
    )

    api_key = st.text_input(
        "PageSpeed API 키 (선택)",
        type="password",
        placeholder="Enter로 스킵 가능",
        help="Google PageSpeed Insights API 키. 없으면 비워두세요.",
    )

    mode = st.radio(
        "수집 모드",
        options=["크롤링 (링크 따라가며 수집)", "사이트맵 기반 (더 빠르고 정확)"],
        index=0,
    )

    max_pages = st.slider("최대 분석 페이지 수", 5, 500, 50, step=5)
    crawl_delay = st.slider("크롤링 딜레이 (초)", 0.1, 2.0, 0.5, step=0.1)

    st.divider()
    run_btn = st.button("🚀 분석 시작", use_container_width=True, type="primary")


# ── 메인 영역 ─────────────────────────────────────────────────────────────────
if not run_btn:
    st.markdown("""
    # 🔍 SEO Diagnostic Pro

    **Screaming Frog + Sitebulb + Ahrefs 수준의 완전 자동 SEO 진단 툴**

    ---

    ### 분석 항목
    | 항목 | 설명 |
    |------|------|
    | **Title / Meta Description** | 길이 검사 + 중복 탐지 |
    | **H1 태그** | 존재 여부 + 내용 |
    | **콘텐츠 단어 수** | 300단어 미만 = 얇은 콘텐츠 경고 |
    | **내부 링크** | Outgoing + Incoming 분석 |
    | **이미지 Alt** | 누락된 Alt 태그 탐지 |
    | **HTTP 상태 코드** | 4xx/5xx 에러 탐지 |
    | **페이지 로드 시간** | 3초 이상 = 경고 |
    | **PageSpeed** | Mobile 점수 (API 키 필요) |
    | **사이트 구조** | 폴더 트리 시각화 |

    ### 사용법
    1. 왼쪽 사이드바에 URL 입력
    2. 수집 모드 및 페이지 수 설정
    3. **🚀 분석 시작** 클릭!
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
is_sitemap_mode = "사이트맵" in mode

session = build_session()

# ── 크롤링 실행 ──────────────────────────────────────────────────────────────
st.header(f"🔍 {base_domain} 분석 중...")

progress_bar = st.progress(0)
status_text = st.empty()

pages = []

if is_sitemap_mode:
    status_text.text("사이트맵 탐색 중...")
    sitemaps = discover_sitemaps(base_url, session)
    if sitemaps:
        status_text.text(f"사이트맵 발견: {', '.join(sitemaps)}")
        all_urls = set()
        for sm in sitemaps:
            all_urls |= parse_sitemap(sm, session, max_pages, base_domain)
        all_urls = sorted(all_urls)[:max_pages]
        status_text.text(f"사이트맵에서 {len(all_urls)}개 URL 발견. 분석 시작...")

        for i, url in enumerate(all_urls, 1):
            status_text.text(f"분석 중... [{i}/{len(all_urls)}] {url}")
            progress_bar.progress(i / len(all_urls))
            page = analyze_page(url, session)
            pages.append(page)
            time.sleep(crawl_delay)
    else:
        st.warning("사이트맵을 찾지 못했습니다. 크롤링 모드로 전환합니다.")
        is_sitemap_mode = False

if not is_sitemap_mode and not pages:
    pages = crawl_site(base_url, session, max_pages, base_domain, crawl_delay, progress_bar, status_text)

progress_bar.progress(1.0)
status_text.text(f"분석 완료! 총 {len(pages)}개 페이지")

if not pages:
    st.error("분석할 페이지가 없습니다. URL을 확인해주세요.")
    st.stop()

# ── 들어오는 내부 링크 계산 ───────────────────────────────────────────────────
incoming_map = defaultdict(int)
for p in pages:
    for link in p["outgoing_internal_links"]:
        incoming_map[link] += 1

# ── PageSpeed 수집 ────────────────────────────────────────────────────────────
pagespeed_scores = {}
if api_key:
    ps_bar = st.progress(0)
    ps_text = st.empty()
    ok_pages = [p for p in pages if p["status_code"] == 200]
    for i, p in enumerate(ok_pages, 1):
        ps_text.text(f"PageSpeed 조회 중... [{i}/{len(ok_pages)}] {p['url']}")
        ps_bar.progress(i / len(ok_pages))
        score = get_pagespeed_score(p["url"], api_key)
        if score is not None:
            pagespeed_scores[p["url"]] = score
    ps_bar.empty()
    ps_text.empty()

# ── 진단 실행 ─────────────────────────────────────────────────────────────────
issues = run_diagnostics(pages, incoming_map, pagespeed_scores)

# ══════════════════════════════════════════════════════════════════════════════
# 결과 출력
# ══════════════════════════════════════════════════════════════════════════════

st.divider()

# ── 요약 대시보드 ─────────────────────────────────────────────────────────────
st.header("📊 요약 대시보드")

high = sum(1 for i in issues if i["severity"] == "HIGH")
med = sum(1 for i in issues if i["severity"] == "MEDIUM")
low = sum(1 for i in issues if i["severity"] == "LOW")

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("총 페이지", len(pages))
col2.metric("총 이슈", len(issues))
col3.metric("🔴 HIGH", high)
col4.metric("🟡 MEDIUM", med)
col5.metric("🟢 LOW", low)

avg_load = round(sum(p["load_time"] for p in pages) / len(pages), 2) if pages else 0
avg_words = round(sum(p["word_count"] for p in pages) / len(pages)) if pages else 0

col6, col7, col8, col9 = st.columns(4)
col6.metric("평균 로드 시간", f"{avg_load}s")
col7.metric("평균 단어 수", avg_words)
col8.metric("총 이미지", sum(p["total_images"] for p in pages))
col9.metric("Alt 누락 이미지", sum(p["images_missing_alt"] for p in pages))

# ── 탭 구성 ──────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🌳 사이트 구조",
    "🔗 내부 링크",
    "📋 페이지별 상세",
    "⚠️ 진단 리포트",
    "💾 JSON 다운로드",
])

# ── TAB 1: 사이트 구조 트리 ───────────────────────────────────────────────────
with tab1:
    st.subheader("사이트 구조 트리")
    tree_str = build_tree_string([p["url"] for p in pages], base_domain)
    st.code(tree_str, language=None)

# ── TAB 2: 내부 링크 ─────────────────────────────────────────────────────────
with tab2:
    st.subheader("페이지별 들어오는 내부 링크 (Incoming Links)")

    sorted_incoming = sorted(incoming_map.items(), key=lambda x: x[1], reverse=True)
    all_urls_set = {p["url"] for p in pages}
    orphans = all_urls_set - set(incoming_map.keys())

    link_data = []
    for url, count in sorted_incoming:
        status = "⚠️ 부족" if count < MIN_INCOMING_LINKS else "✅ 양호"
        link_data.append({"URL": url, "들어오는 링크 수": count, "상태": status})
    for url in sorted(orphans):
        link_data.append({"URL": url, "들어오는 링크 수": 0, "상태": "🚨 고아 페이지"})

    st.dataframe(link_data, use_container_width=True, hide_index=True)

# ── TAB 3: 페이지별 상세 ─────────────────────────────────────────────────────
with tab3:
    st.subheader("페이지별 상세 분석")

    for p in pages:
        url = p["url"]
        inc = incoming_map.get(url, 0)
        ps = pagespeed_scores.get(url, "-")
        status_icon = "✅" if p["status_code"] == 200 else "❌"

        with st.expander(f"{status_icon} {url} — {p['status_code']}"):
            c1, c2, c3 = st.columns(3)
            c1.metric("상태 코드", p["status_code"])
            c2.metric("로드 시간", f"{p['load_time']}s")
            c3.metric("단어 수", p["word_count"])

            c4, c5, c6 = st.columns(3)
            c4.metric("내부 링크 (Out)", len(p["outgoing_internal_links"]))
            c5.metric("내부 링크 (In)", inc)
            c6.metric("PageSpeed", ps)

            st.markdown(f"""
            | 항목 | 값 |
            |---|---|
            | **Title** ({p['title_length']}자) | {p['title'][:80] or '없음'} |
            | **Description** ({p['meta_description_length']}자) | {p['meta_description'][:80] or '없음'} |
            | **H1** | {'✅ ' + p['h1'][:60] if p['h1_exists'] else '❌ 없음'} |
            | **Canonical** | {p['canonical'] or '없음'} |
            | **이미지** | 전체 {p['total_images']}개, Alt 누락 {p['images_missing_alt']}개 |
            """)

# ── TAB 4: 진단 리포트 ───────────────────────────────────────────────────────
with tab4:
    st.subheader(f"자동 진단 리포트 — 총 {len(issues)}건")

    severity_filter = st.multiselect(
        "심각도 필터",
        options=["HIGH", "MEDIUM", "LOW"],
        default=["HIGH", "MEDIUM", "LOW"],
    )

    filtered = [i for i in issues if i["severity"] in severity_filter]

    for idx, issue in enumerate(filtered, 1):
        with st.expander(f"{issue['icon']} [{issue['severity']}] {issue['type']} — {issue['detail'][:60]}"):
            st.markdown(f"**문제:** {issue['detail']}")
            st.markdown(f"**권장 조치:** {issue['recommendation']}")
            st.markdown("**해당 페이지:**")
            for pg in issue["pages"][:10]:
                st.markdown(f"- `{pg}`")
            if len(issue["pages"]) > 10:
                st.caption(f"... 외 {len(issue['pages']) - 10}개 페이지")

    if not filtered:
        st.success("필터 조건에 해당하는 이슈가 없습니다! 🎉")

# ── TAB 5: JSON 다운로드 ─────────────────────────────────────────────────────
with tab5:
    st.subheader("JSON 리포트 다운로드")

    report = {
        "generated_at": datetime.now().isoformat(),
        "base_url": base_url,
        "total_pages": len(pages),
        "mode": "sitemap" if is_sitemap_mode else "crawl",
        "summary": {"high": high, "medium": med, "low": low, "total": len(issues)},
        "pages": [],
        "incoming_links": dict(sorted_incoming),
        "issues": issues,
    }
    for p in pages:
        page_data = dict(p)
        page_data["incoming_links"] = incoming_map.get(p["url"], 0)
        if p["url"] in pagespeed_scores:
            page_data["pagespeed_mobile"] = pagespeed_scores[p["url"]]
        report["pages"].append(page_data)

    json_str = json.dumps(report, ensure_ascii=False, indent=2)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")

    st.download_button(
        label="📥 JSON 리포트 다운로드",
        data=json_str,
        file_name=f"seo_report_{timestamp}.json",
        mime="application/json",
        use_container_width=True,
    )

    with st.expander("JSON 미리보기"):
        st.json(report)
