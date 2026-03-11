#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
weballin Scoring Engine v2.1
Proprietary diagnostic algorithms.
Copyright (c) 2024-2026 weballin. All rights reserved.
Unauthorized reproduction or distribution is prohibited.
"""
import base64 as _b
import zlib as _z
import json as _j
import hashlib as _h
from functools import lru_cache as _lc

# ── Internal configuration (compressed + encoded) ──
_C = "eNqdVm1v2zYQ/iuC9sEG5Eq25JfYWJd2Q9EBXZEAGwIjECTqbDGSSJWk7LhB/vuOlORXJW0+WCLvnufuueMdHxKBsqR4fMgmcxgnTPJkijie8LULRU9qJhN0r3GJNpEoqYyIooI/dGKWR0iiZI5C5Y8LwXLcIp6xEE+5n3TxsLPFOfMKwQum8Kkgjy6Y0P5PoL3JFzJXh2iEcMwW9Jb/SPZUKqZgjZVqYKzf5FQ/kxBf3rAYJkJmPFPCCVEkJinUE6l4FUYixwJPHiOx0d2yk8n5WWQl3ZKnYkPx0WKx9ecxf+WHaYxXLgO0u6CxFwnDMgGIWVsOVuuL8bvlh+GhvFvEetV3M7+2x8Mfl6NnZgUGsxXLb+ySXL3c+RLNKSlKaGU10pEiWIcSVToaeyFPUzQJJHKd1MjAUXuqMiFTR/p2DXFK/lBUJZCiUMxh/sEJamTZvKtqI8X5Fwx1s/8x7+T8NVTJV7xW5CnjIVN2ckrA+QKnOeMW/QdWFhz2PJcjhXnOBE9wppJYCBMoSxWbC3qxJ2dJo8dqMvfbNiZSsYUL/LzKuUe9B1kB5+XFdCTXHEhILGGpYzjkuqeTe2nHCZBVJlxT2iSoqJl4sqKqLhiuTdJStOFJjSjEzB88Jywu+cRDfX1xf5N4hs6s4wQwBj1OFi4ryZJNIhRPX4qhyrgR8F4pbsAIz5V0TIz9A5yl3d3lcvnq8m55UVL6BIVVKJNgk8ukyKiE/NcprjmS2u2JODPkxRnQM+plIShRdOxqfMVFaA/JUCGgWCPEV/Zw0Mb3VJRlPHgr3F0uMaZzNY9Sc1RoIjr8vLJMdPNO1UVFwYOWe4zPjOjMbH/YPuoNft7E/6g8leb9LxJ93OoOd3e73+Xr/f63V7vV5n0Bl0OpN+t9PrdTs9p3x1gH8rC39/hs/p5N7nLJ5O3UjELJwqpU5Glk4W5Jf3Cyn5/JqpGMhp6YcN1CXLD2J4K8qVW8K0gYHfSq2qjRcIEqKVNYVpkGM0C5MZnhBZQP9XPM0Sk/1HNcsUkG2FTsG5HFqHqJV/E8Y5gqKSqDqJrHg0l8NZI6P4oaOTwq9J3a+nCWoHJq3LBlrZqFbYIm13cPCzBa2P2sqvWtmqbWX3cOP1xfX30oEIHpJVUoTlMO9VNABMqSTk5Qibc+a3hOY1amEnI9WwqaRohb+K5FXqIFtW3CRV8nrAZqzjRHHIONuihWe2d2fdnSXqnKJ5XwP/5iN3Bd4dFznJJQtrLPH7B0gBjfslLN73x4N/hq3vZT2MoC1NqexNNGMBjQ3tDxmOv6cNYlBeyKRgGgc8K4LjWlY5pSnf1n99u+u8PVLdtRXSIQqCu9T/2kI"
_K = b"w3b@ll1n_$c0r3_v2"

def _d(s):
    """Decompress configuration."""
    try:
        return _j.loads(_z.decompress(_b.b64decode(s)))
    except Exception:
        return {}

@_lc(maxsize=1)
def _cfg():
    """Load scoring parameters."""
    c = _d(_C)
    if not c:
        # Fallback configuration
        c = _fallback_cfg()
    return c

def _fallback_cfg():
    return {
        "h": {"w": [30, 20, 10, 15, 15, 10], "t": [1.0, 3.0, 5.0, 0.7]},
        "e": {"ax": [25, 25, 25, 25]},
        "a": {"ax": [25, 20, 20, 15, 10, 10]},
        "c": {"w": [15, 10, 10, 10, 10, 10, 5, 10, 5, 5, 5, 5]},
    }

def _sig(data, salt=""):
    """Generate integrity signature."""
    raw = _j.dumps(data, sort_keys=True, ensure_ascii=False) + salt
    return _h.sha256(raw.encode()).hexdigest()[:12]


# ══════════════════════════════════════════════════════════════════════
# PUBLIC API — Health Score
# ══════════════════════════════════════════════════════════════════════

def compute_health_score(total_pages, high, med, low, avg_load, https_count, schema_count):
    """
    사이트 건강 점수 계산.
    Returns: (score: int, breakdown: dict, signature: str)
    """
    cfg = _fallback_cfg()
    w = cfg["h"]["w"]
    t = cfg["h"]["t"]
    bd = {}

    if total_pages <= 0:
        return 0, {}, _sig({"s": 0})

    # Category 1: Critical issues ratio
    r1 = min(high / total_pages, t[0])
    s1 = round((1 - r1) * w[0])
    bd["\uce58\uba85\uc801 \uc774\uc288"] = {
        "score": s1, "max": w[0],
        "detail": f"HIGH \uc774\uc288 {high}\uac74 / {total_pages} \ud398\uc774\uc9c0 (\ube44\uc728 {r1*100:.0f}%)"
    }

    # Category 2: Important issues ratio
    mpp = med / total_pages
    r2 = min(mpp / t[1], t[0])
    s2 = round((1 - r2) * w[1])
    bd["\uc911\uc694 \uc774\uc288"] = {
        "score": s2, "max": w[1],
        "detail": f"MEDIUM \uc774\uc288 {med}\uac74 (\ud398\uc774\uc9c0\ub2f9 \ud3c9\uade0 {mpp:.1f}\uac74)"
    }

    # Category 3: Minor issues ratio
    lpp = low / total_pages
    r3 = min(lpp / t[2], t[0])
    s3 = round((1 - r3) * w[2])
    bd["\uacbd\ubbf8\ud55c \uc774\uc288"] = {
        "score": s3, "max": w[2],
        "detail": f"LOW \uc774\uc288 {low}\uac74 (\ud398\uc774\uc9c0\ub2f9 \ud3c9\uade0 {lpp:.1f}\uac74)"
    }

    # Category 4: Page speed
    if avg_load <= 1.5:
        s4 = w[3]
    elif avg_load <= 3.0:
        s4 = round(w[3] * (1 - (avg_load - 1.5) / 3.0))
    elif avg_load <= 6.0:
        s4 = round((w[3] // 3) * (1 - (avg_load - 3.0) / 3.0))
    else:
        s4 = 0
    s4 = max(0, s4)
    bd["\ud398\uc774\uc9c0 \uc18d\ub3c4"] = {
        "score": s4, "max": w[3],
        "detail": f"\ud3c9\uade0 \ub85c\ub529 \uc2dc\uac04 {avg_load}\ucd08"
    }

    # Category 5: HTTPS
    hr = https_count / total_pages
    s5 = round(hr * w[4])
    bd["HTTPS \ubcf4\uc548"] = {
        "score": s5, "max": w[4],
        "detail": f"HTTPS \uc801\uc6a9 {https_count}/{total_pages} ({hr*100:.0f}%)"
    }

    # Category 6: Structured data
    sr = schema_count / total_pages
    s6 = round(min(sr / t[3], 1.0) * w[5])
    bd["\uad6c\uc870\ud654 \ub370\uc774\ud130"] = {
        "score": s6, "max": w[5],
        "detail": f"Schema \uc801\uc6a9 {schema_count}/{total_pages} ({sr*100:.0f}%)"
    }

    total = max(0, min(100, sum(v["score"] for v in bd.values())))
    return total, bd, _sig({"s": total, "p": total_pages})


# ══════════════════════════════════════════════════════════════════════
# PUBLIC API — AI Readiness Score
# ══════════════════════════════════════════════════════════════════════

def compute_ai_readiness(page_data):
    """
    페이지별 AI 준비도 점수 계산.
    Returns: dict with score, grade_kr, details
    """
    cfg = _fallback_cfg()
    ax = cfg["a"]["ax"]
    score = 0
    dims = {}

    eeat = page_data.get("_eeat") or {}
    if not isinstance(eeat, dict): eeat = {}
    schema = page_data.get("_schema") or {}
    if not isinstance(schema, dict): schema = {}
    tech = page_data.get("_tech") or {}
    if not isinstance(tech, dict): tech = {}

    wc = page_data.get("Words", 0)
    img_count = page_data.get("Images", 0)
    ext_links = page_data.get("Ext Links", 0)
    thr = page_data.get("Text/HTML %", 0)

    # D1: Citation readiness
    d1, d1i = 0, []
    if wc >= 1500:
        d1 += 10; d1i.append(f"\u2705 \ucf58\ud150\uce20 \uae38\uc774 \ucda9\ubd84 ({wc}\ub2e8\uc5b4)")
    elif wc >= 800:
        d1 += 6; d1i.append(f"\u26a0\ufe0f \ucf58\ud150\uce20 \ubcf4\ud1b5 ({wc}\ub2e8\uc5b4, 1500+ \uad8c\uc7a5)")
    elif wc >= 300:
        d1 += 3; d1i.append(f"\u274c \ucf58\ud150\uce20 \ubd80\uc871 ({wc}\ub2e8\uc5b4, 1500+ \uad8c\uc7a5)")
    else:
        d1i.append(f"\u274c \ucf58\ud150\uce20 \ub9e4\uc6b0 \uc9e7\uc74c ({wc}\ub2e8\uc5b4)")

    if img_count >= 3:
        d1 += 5; d1i.append(f"\u2705 \uc774\ubbf8\uc9c0 \ud48d\ubd80 ({img_count}\uac1c)")
    elif img_count >= 1:
        d1 += 2; d1i.append(f"\u26a0\ufe0f \uc774\ubbf8\uc9c0 \ubd80\uc871 ({img_count}\uac1c, 3\uac1c+ \uad8c\uc7a5)")
    else:
        d1i.append("\u274c \uc774\ubbf8\uc9c0 \uc5c6\uc74c")

    if ext_links >= 3:
        d1 += 5; d1i.append(f"\u2705 \uc678\ubd80 \uc778\uc6a9 \ub9c1\ud06c \ucda9\ubd84 ({ext_links}\uac1c)")
    elif ext_links >= 1:
        d1 += 3; d1i.append(f"\u26a0\ufe0f \uc678\ubd80 \uc778\uc6a9 \ubd80\uc871 ({ext_links}\uac1c, 3\uac1c+ \uad8c\uc7a5)")
    else:
        d1i.append("\u274c \uc678\ubd80 \uc778\uc6a9 \ub9c1\ud06c \uc5c6\uc74c \u2014 \uc2e0\ub8b0\uc131 \uc800\ud558")

    if thr >= 30:
        d1 += 5; d1i.append(f"\u2705 \ud14d\uc2a4\ud2b8 \ube44\uc728 \uc591\ud638 ({thr:.0f}%)")
    elif thr >= 15:
        d1 += 3; d1i.append(f"\u26a0\ufe0f \ud14d\uc2a4\ud2b8 \ube44\uc728 \ubcf4\ud1b5 ({thr:.0f}%, 30%+ \uad8c\uc7a5)")
    else:
        d1i.append(f"\u274c \ud14d\uc2a4\ud2b8 \ube44\uc728 \ub0ae\uc74c ({thr:.0f}%)")

    d1v = min(d1, ax[0])
    score += d1v
    dims["\uc778\uc6a9 \uc900\ube44\ub3c4"] = {"score": d1v, "max": ax[0], "items": d1i}

    # D2: Answer fitness
    d2, d2i = 0, []
    th = sum(page_data.get(f"H{i}s", 0) for i in range(2, 4))
    if th >= 5:
        d2 += 8; d2i.append(f"\u2705 \uc11c\ube0c\ud5e4\ub529 \uad6c\uc870 \ud48d\ubd80 (H2/H3 {th}\uac1c)")
    elif th >= 3:
        d2 += 5; d2i.append(f"\u26a0\ufe0f \uc11c\ube0c\ud5e4\ub529 \ubcf4\ud1b5 (H2/H3 {th}\uac1c, 5\uac1c+ \uad8c\uc7a5)")
    elif th >= 1:
        d2 += 3; d2i.append(f"\u274c \uc11c\ube0c\ud5e4\ub529 \ubd80\uc871 (H2/H3 {th}\uac1c)")
    else:
        d2i.append("\u274c H2/H3 \uc11c\ube0c\ud5e4\ub529 \uc5c6\uc74c \u2014 AI\uac00 \ub2f5\ubcc0 \uad6c\uc870\ub97c \ud30c\uc545\ud558\uae30 \uc5b4\ub824\uc6c0")

    if tech.get("heading_hierarchy_ok"):
        d2 += 6; d2i.append("\u2705 \ud5e4\ub529 \uacc4\uce35 \uad6c\uc870 \uc815\uc0c1")
    else:
        d2i.append("\u274c \ud5e4\ub529 \uacc4\uce35 \uad6c\uc870 \uc624\ub958 \u2014 H1\u2192H2\u2192H3 \uc21c\uc11c \uad8c\uc7a5")

    if wc >= 500:
        d2 += 6; d2i.append("\u2705 \ub2f5\ubcc0\uc5d0 \ucda9\ubd84\ud55c \ucf58\ud150\uce20 \ubd84\ub7c9")
    else:
        d2i.append("\u274c \ucf58\ud150\uce20\uac00 \ub108\ubb34 \uc9e7\uc544 AI \ub2f5\ubcc0 \uc18c\uc2a4\ub85c \ubd80\uc801\ud569")

    d2v = min(d2, ax[1])
    score += d2v
    dims["\ub2f5\ubcc0 \uc801\ud569\uc131"] = {"score": d2v, "max": ax[1], "items": d2i}

    # D3: Content authority
    d3, d3i = 0, []
    if eeat.get("has_author"):
        d3 += 7; d3i.append("\u2705 \uc800\uc790 \uc815\ubcf4 \uba85\uc2dc")
    else:
        d3i.append("\u274c \uc800\uc790 \uc815\ubcf4 \uc5c6\uc74c \u2014 \uc804\ubb38\uc131 \ud310\ub2e8 \ubd88\uac00")

    if eeat.get("has_published_date"):
        d3 += 5; d3i.append("\u2705 \ubc1c\ud589\uc77c \uba85\uc2dc")
    else:
        d3i.append("\u274c \ubc1c\ud589\uc77c \uc5c6\uc74c \u2014 \ucd5c\uc2e0\uc131 \ud310\ub2e8 \ubd88\uac00")

    if eeat.get("has_modified_date"):
        d3 += 4; d3i.append("\u2705 \uc218\uc815\uc77c \uba85\uc2dc (\ucd5c\uc2e0 \ucf58\ud150\uce20 \uc2e0\ud638)")
    else:
        d3i.append("\u26a0\ufe0f \uc218\uc815\uc77c \uc5c6\uc74c \u2014 \uc5c5\ub370\uc774\ud2b8 \uc5ec\ubd80 \uc54c \uc218 \uc5c6\uc74c")

    if ext_links >= 2:
        d3 += 4; d3i.append("\u2705 \uc678\ubd80 \ucd9c\ucc98 \uc778\uc6a9\uc73c\ub85c \uad8c\uc704 \uac15\ud654")
    else:
        d3i.append("\u274c \uc678\ubd80 \ucd9c\ucc98 \uc778\uc6a9 \ubd80\uc871")

    d3v = min(d3, ax[2])
    score += d3v
    dims["\ucf58\ud150\uce20 \uad8c\uc704"] = {"score": d3v, "max": ax[2], "items": d3i}

    # D4: Knowledge Graph
    d4, d4i = 0, []
    all_types = schema.get("all_types", [])
    if not isinstance(all_types, list): all_types = []

    if schema.get("has_schema"):
        d4 += 4; d4i.append(f"\u2705 \uad6c\uc870\ud654 \ub370\uc774\ud130 \uc874\uc7ac ({', '.join(all_types[:5])})")
    else:
        d4i.append("\u274c \uad6c\uc870\ud654 \ub370\uc774\ud130 \uc5c6\uc74c \u2014 Knowledge Graph \uc5f0\ub3d9 \ubd88\uac00")

    if "FAQPage" in all_types:
        d4 += 5; d4i.append("\u2705 FAQPage \uc2a4\ud0a4\ub9c8 (AI \uc778\uc6a9\ub960 +41%)")
    else:
        d4i.append("\u26a0\ufe0f FAQPage \uc2a4\ud0a4\ub9c8 \uc5c6\uc74c \u2014 FAQ \ucf58\ud150\uce20\uac00 \uc788\ub2e4\uba74 \ucd94\uac00 \uad8c\uc7a5")

    if any(t in all_types for t in ("Article", "BlogPosting", "NewsArticle")):
        d4 += 3; d4i.append("\u2705 Article \uc2a4\ud0a4\ub9c8")
    else:
        d4i.append("\u26a0\ufe0f Article \uc2a4\ud0a4\ub9c8 \uc5c6\uc74c")

    if "BreadcrumbList" in all_types:
        d4 += 3; d4i.append("\u2705 BreadcrumbList \uc2a4\ud0a4\ub9c8")

    d4v = min(d4, ax[3])
    score += d4v
    dims["\uc9c0\uc2dd \uadf8\ub798\ud504 \uc5f0\ub3d9"] = {"score": d4v, "max": ax[3], "items": d4i}

    # D5: Technical accessibility
    d5, d5i = 0, []
    if not tech.get("is_noindex"):
        d5 += 3; d5i.append("\u2705 \uc778\ub371\uc2f1 \ud5c8\uc6a9")
    else:
        d5i.append("\u274c noindex \uc124\uc815 \u2014 AI \uac80\uc0c9 \ub178\ucd9c \ubd88\uac00")

    if tech.get("heading_hierarchy_ok"):
        d5 += 3; d5i.append("\u2705 \ud5e4\ub529 \uacc4\uce35 \uad6c\uc870 \uc815\uc0c1")
    else:
        d5i.append("\u274c \ud5e4\ub529 \uacc4\uce35 \uc624\ub958")

    if tech.get("has_viewport"):
        d5 += 2; d5i.append("\u2705 \ubaa8\ubc14\uc77c \ubdf0\ud3ec\ud2b8 \uc124\uc815")
    else:
        d5i.append("\u26a0\ufe0f \ubdf0\ud3ec\ud2b8 \uba54\ud0c0\ud0dc\uadf8 \uc5c6\uc74c")

    if tech.get("lang"):
        d5 += 2; d5i.append(f"\u2705 \uc5b8\uc5b4 \uba85\uc2dc: {tech['lang']}")
    else:
        d5i.append("\u274c html lang \uc18d\uc131 \uc5c6\uc74c")

    d5v = min(d5, ax[4])
    score += d5v
    dims["\uae30\uc220\uc801 \uc811\uadfc\uc131"] = {"score": d5v, "max": ax[4], "items": d5i}

    # D6: Differentiation
    d6, d6i = 0, []
    if wc >= 2000:
        d6 += 4; d6i.append(f"\u2705 \uc2ec\uce35 \ucf58\ud150\uce20 ({wc}\ub2e8\uc5b4)")
    elif wc >= 1000:
        d6 += 2; d6i.append(f"\u26a0\ufe0f \ucf58\ud150\uce20 \uae38\uc774 \ubcf4\ud1b5 ({wc}\ub2e8\uc5b4, 2000+ \uad8c\uc7a5)")
    else:
        d6i.append("\u274c \ucf58\ud150\uce20 \uc9e7\uc74c \u2014 \uacbd\uc7c1 \ucf58\ud150\uce20 \ub300\ube44 \ucc28\ubcc4\ud654 \uc5b4\ub824\uc6c0")

    if img_count >= 3:
        d6 += 3; d6i.append(f"\u2705 \uc2dc\uac01 \uc790\ub8cc \ud48d\ubd80 ({img_count}\uac1c \uc774\ubbf8\uc9c0)")
    else:
        d6i.append("\u26a0\ufe0f \uc2dc\uac01 \uc790\ub8cc \ubd80\uc871 \u2014 \uc774\ubbf8\uc9c0/\uc778\ud3ec\uadf8\ub798\ud53d \ucd94\uac00 \uad8c\uc7a5")

    if page_data.get("Has Schema"):
        d6 += 3; d6i.append("\u2705 \uad6c\uc870\ud654 \ub370\uc774\ud130\ub85c \ucc28\ubcc4\ud654")
    else:
        d6i.append("\u274c Schema \uc5c6\uc74c")

    d6v = min(d6, ax[5])
    score += d6v
    dims["\ucc28\ubcc4\ud654"] = {"score": d6v, "max": ax[5], "items": d6i}

    total = min(score, 100)
    if total >= 70:
        gk = "AI-Ready"
    elif total >= 50:
        gk = "\uac1c\uc120 \ud544\uc694"
    else:
        gk = "\ucd5c\uc801\ud654 \ud544\uc694"

    return {"score": total, "grade_kr": gk, "details": dims, "_sig": _sig({"s": total})}


# ══════════════════════════════════════════════════════════════════════
# PUBLIC API — Content Optimization Score (lightweight)
# ══════════════════════════════════════════════════════════════════════

def compute_content_score(page_data):
    """
    저장된 크롤 데이터에서 콘텐츠 최적화 간이 점수 계산.
    Returns: (score, items_list)
    """
    from urllib.parse import urlparse

    s = 0
    items = []
    h1 = (page_data.get("H1") or "").strip()
    title = (page_data.get("Title") or "").strip()
    words = page_data.get("Words", 0)

    auto_kw = h1 if h1 else title
    kl = auto_kw.lower()

    # T1: Title keyword
    if kl and kl in title.lower():
        s += 15; items.append(("Title \ud0a4\uc6cc\ub4dc", True))
    else:
        items.append(("Title \ud0a4\uc6cc\ub4dc", False))

    # T2: H1
    if h1:
        s += 10; items.append(("H1 \ud0dc\uadf8", True))
    else:
        items.append(("H1 \ud0dc\uadf8", False))

    # T3: Description
    desc_len = page_data.get("Desc Len", 0)
    desc = page_data.get("Meta Desc", "")
    if desc and 70 <= desc_len <= 160:
        s += 10; items.append(("Description", True))
    elif desc:
        s += 5; items.append(("Description \uae38\uc774", False))
    else:
        items.append(("Description", False))

    # T4: Subheadings
    h2s = page_data.get("H2s", 0)
    if h2s >= 3:
        s += 10; items.append(("\uc11c\ube0c\ud5e4\ub529 \uad6c\uc870", True))
    elif h2s >= 1:
        s += 5; items.append(("\uc11c\ube0c\ud5e4\ub529 \ubd80\uc871", False))
    else:
        items.append(("\uc11c\ube0c\ud5e4\ub529 \uc5c6\uc74c", False))

    # T5: Length
    if words >= 2000: s += 10
    elif words >= 1000: s += 7
    elif words >= 500: s += 4
    items.append(("\ucf58\ud150\uce20 \uae38\uc774", words >= 1000))

    # T6: Images
    imgs = page_data.get("Images", 0)
    if imgs >= 3: s += 10
    elif imgs >= 1: s += 5
    items.append(("\uc774\ubbf8\uc9c0", imgs >= 1))

    # T7: Internal links
    il = len(page_data.get("_internal_links", []))
    if il >= 5: s += 5
    elif il >= 2: s += 3
    items.append(("\ub0b4\ubd80 \ub9c1\ud06c", il >= 3))

    # T8: External links
    el = page_data.get("Ext Links", 0)
    if el >= 2: s += 5
    items.append(("\uc678\ubd80 \uc778\uc6a9", el >= 1))

    # T9: Schema
    if page_data.get("Has Schema"):
        s += 5; items.append(("\uad6c\uc870\ud654 \ub370\uc774\ud130", True))
    else:
        items.append(("\uad6c\uc870\ud654 \ub370\uc774\ud130", False))

    # T10: HTTPS
    if page_data.get("HTTPS"): s += 5

    # T11: Title length
    tl = page_data.get("Title Len", 0)
    if 30 <= tl <= 60:
        s += 5; items.append(("Title \uae38\uc774", True))
    else:
        items.append(("Title \uae38\uc774", False))

    # T12: URL
    url = page_data.get("URL", "")
    path = urlparse(url).path if url else ""
    if len(path) <= 80: s += 5
    items.append(("URL \uad6c\uc870", len(path) <= 80))

    return min(s, 100), items, auto_kw[:50] if len(auto_kw) > 50 else auto_kw


def verify_score(score, signature):
    """Verify score integrity."""
    expected = _sig({"s": score})
    return expected == signature
