#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
weballin v5 — SaaS-style SEO Diagnostic Tool
프로젝트 관리 · 자동 모니터링 · 변화 추적 · 콘텐츠 & 테크니컬 인사이트
"""

import re, json, time, math, os
import pandas as pd
import streamlit as st
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urlparse, urljoin, urldefrag, parse_qs
from collections import defaultdict
from itertools import groupby

import database as db
import crawler
import google_api
from datetime import timedelta

# ── 설정 ─────────────────────────────────────────────────────────────────────
TITLE_MIN, TITLE_MAX = 30, 60
DESC_MIN, DESC_MAX = 120, 160
THIN_CONTENT_THRESHOLD = 300
MIN_INCOMING_LINKS = 3
PAGESPEED_THRESHOLD = 90
URL_MAX_LENGTH = 100

FIELD_NAMES_KR = {
    "title": "Title", "meta_description": "Meta Description",
    "h1": "H1", "word_count": "콘텐츠 길이",
    "status_code": "HTTP 상태", "schema_types": "Schema",
    "canonical_url": "Canonical URL", "is_https": "HTTPS",
}

st.set_page_config(
    page_title="weballin",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS (Semrush-inspired Modern Design) ─────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

/* ── Streamlit 기본 UI 숨기기 ── */
#MainMenu {visibility: hidden;}
header {visibility: hidden;}
footer {visibility: hidden;}
[data-testid="stToolbar"] {display: none !important;}
[data-testid="manage-app-button"] {display: none !important;}
[data-testid="stDecoration"] {display: none !important;}
.stDeployButton {display: none !important;}

/* ── 전역 타이포그래피 & 기본 ── */
html, body, [class*="css"] {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif !important;
}
.stApp { background: #0a0e1a; }
.stTabs [data-baseweb="tab-list"] {
    gap: 0;
    background: #111827;
    border-radius: 12px;
    padding: 4px;
    border: 1px solid #1e293b;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 8px;
    padding: 8px 16px;
    font-size: .82rem;
    font-weight: 500;
    color: #94a3b8;
    background: transparent;
    border: none;
    transition: all .15s;
}
.stTabs [aria-selected="true"] {
    background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%) !important;
    color: #fff !important;
    font-weight: 600;
    box-shadow: 0 2px 8px rgba(99,102,241,.3);
}
.stTabs [data-baseweb="tab"]:hover:not([aria-selected="true"]) {
    background: #1e293b;
    color: #e2e8f0;
}
.stTabs [data-baseweb="tab-border"] { display: none; }
.stTabs [data-baseweb="tab-highlight"] { display: none; }

/* ── Expander 스타일 ── */
.streamlit-expanderHeader {
    background: #111827 !important;
    border: 1px solid #1e293b !important;
    border-radius: 10px !important;
    font-weight: 600 !important;
    font-size: .88rem !important;
    color: #e2e8f0 !important;
}
details[open] > summary { border-radius: 10px 10px 0 0 !important; }
.streamlit-expanderContent {
    background: #0f172a !important;
    border: 1px solid #1e293b !important;
    border-top: none !important;
    border-radius: 0 0 10px 10px !important;
}

/* ── 버튼 스타일 ── */
.stButton > button[kind="primary"],
.stFormSubmitButton > button[kind="primary"] {
    background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%) !important;
    border: none !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
    transition: all .2s !important;
    box-shadow: 0 2px 8px rgba(99,102,241,.25) !important;
}
.stButton > button[kind="primary"]:hover,
.stFormSubmitButton > button[kind="primary"]:hover {
    box-shadow: 0 4px 16px rgba(99,102,241,.4) !important;
    transform: translateY(-1px);
}
.stButton > button[kind="secondary"] {
    background: #1e293b !important;
    border: 1px solid #334155 !important;
    border-radius: 8px !important;
    color: #e2e8f0 !important;
}

/* ── 사이드바 ── */
section[data-testid="stSidebar"] {
    background: #0f172a !important;
    border-right: 1px solid #1e293b !important;
}
section[data-testid="stSidebar"] .stButton > button {
    background: transparent !important;
    border: 1px solid transparent !important;
    border-radius: 8px !important;
    color: #94a3b8 !important;
    font-weight: 500 !important;
    text-align: left !important;
    transition: all .15s !important;
}
section[data-testid="stSidebar"] .stButton > button:hover {
    background: #1e293b !important;
    border-color: #334155 !important;
    color: #e2e8f0 !important;
}

/* ── 입력 필드 ── */
.stTextInput > div > div > input,
.stSelectbox > div > div,
.stTextArea > div > div > textarea {
    background: #111827 !important;
    border: 1px solid #1e293b !important;
    border-radius: 8px !important;
    color: #e2e8f0 !important;
}
.stTextInput > div > div > input:focus {
    border-color: #6366f1 !important;
    box-shadow: 0 0 0 2px rgba(99,102,241,.2) !important;
}

/* ── Metric 카드 ── */
[data-testid="stMetric"] {
    background: #111827;
    border: 1px solid #1e293b;
    border-radius: 10px;
    padding: 16px;
    transition: all .2s;
}
[data-testid="stMetric"]:hover {
    border-color: #6366f1;
    box-shadow: 0 2px 12px rgba(99,102,241,.1);
}
[data-testid="stMetricLabel"] { color: #94a3b8 !important; font-size: .78rem !important; font-weight: 500 !important; }
[data-testid="stMetricValue"] { color: #e2e8f0 !important; font-weight: 700 !important; }

/* ── DataFrame ── */
.stDataFrame { font-size: .83rem; border-radius: 10px; overflow: hidden; }

/* ── Progress bar ── */
.stProgress > div > div > div > div {
    background: linear-gradient(90deg, #6366f1, #8b5cf6) !important;
    border-radius: 4px;
}

/* ── 커스텀 카드 컴포넌트 ── */

/* 네비게이션 바 */
.nav-bar{background:linear-gradient(135deg,#111827 0%,#0f172a 100%);padding:14px 24px;border:1px solid #1e293b;display:flex;align-items:center;justify-content:space-between;margin-bottom:24px;border-radius:12px}
.nav-bar .brand{background:linear-gradient(135deg,#6366f1,#a855f7);-webkit-background-clip:text;-webkit-text-fill-color:transparent;font-weight:800;font-size:1.3rem;letter-spacing:-.02em}
.nav-bar .user-info{color:#94a3b8;font-size:.85rem;font-weight:500}

/* 요약 카드 (KPI) */
.summary-card{background:linear-gradient(135deg,#111827 0%,#0f172a 100%);border:1px solid #1e293b;border-radius:12px;padding:18px 14px;text-align:center;transition:all .25s;cursor:default;position:relative;overflow:hidden}
.summary-card::before{content:'';position:absolute;top:0;left:0;right:0;height:3px;background:linear-gradient(90deg,#6366f1,#8b5cf6);opacity:0;transition:opacity .25s}
.summary-card:hover{border-color:#6366f1;transform:translateY(-3px);box-shadow:0 8px 24px rgba(99,102,241,.12)}
.summary-card:hover::before{opacity:1}
.summary-card .num{font-size:1.9rem;font-weight:800;color:#e2e8f0;letter-spacing:-.02em}
.summary-card .label{font-size:.75rem;color:#64748b;margin-top:4px;font-weight:500;text-transform:uppercase;letter-spacing:.05em}
.summary-card .hint{font-size:.68rem;color:#475569;margin-top:6px;opacity:0;transition:opacity .25s}
.summary-card:hover .hint{opacity:1}
.summary-card.red .num{color:#ef4444}.summary-card.yellow .num{color:#f59e0b}.summary-card.green .num{color:#22c55e}

/* 이슈 카드 */
.issue-card{background:#111827;border:1px solid #1e293b;border-radius:10px;padding:12px 16px;margin-bottom:8px;transition:all .2s;cursor:default}
.issue-card:hover{border-color:#334155;background:#1e293b;box-shadow:0 2px 8px rgba(0,0,0,.2)}
.issue-card .issue-fix{display:none;color:#22c55e;font-size:.82rem;margin-top:8px;padding-top:8px;border-top:1px solid #1e293b;font-weight:500}
.issue-card:hover .issue-fix{display:block}
.issue-card .issue-header{margin-bottom:4px;font-weight:600;color:#e2e8f0}
.issue-card .issue-detail{color:#94a3b8;font-size:.85rem;line-height:1.5}
.issue-card .issue-url{color:#818cf8;font-size:.8rem;margin-top:4px;font-weight:500}
.issue-card.high{border-left:3px solid #ef4444}.issue-card.medium{border-left:3px solid #f59e0b}.issue-card.low{border-left:3px solid #22c55e}

/* 배지 */
.badge-high{background:rgba(239,68,68,.12);color:#ef4444;padding:3px 10px;border-radius:6px;font-size:.73rem;font-weight:600;letter-spacing:.02em}
.badge-medium{background:rgba(245,158,11,.12);color:#f59e0b;padding:3px 10px;border-radius:6px;font-size:.73rem;font-weight:600;letter-spacing:.02em}
.badge-low{background:rgba(34,197,94,.12);color:#22c55e;padding:3px 10px;border-radius:6px;font-size:.73rem;font-weight:600;letter-spacing:.02em}

/* 점수 원형 */
.score-circle{width:150px;height:150px;border-radius:50%;display:flex;align-items:center;justify-content:center;margin:20px auto;font-size:2.5rem;font-weight:800;position:relative}
.score-good{background:rgba(34,197,94,.08);border:4px solid #22c55e;color:#22c55e;box-shadow:0 0 30px rgba(34,197,94,.15)}
.score-medium{background:rgba(245,158,11,.08);border:4px solid #f59e0b;color:#f59e0b;box-shadow:0 0 30px rgba(245,158,11,.15)}
.score-bad{background:rgba(239,68,68,.08);border:4px solid #ef4444;color:#ef4444;box-shadow:0 0 30px rgba(239,68,68,.15)}

/* 히스토리 카드 */
.history-card{background:#111827;border:1px solid #1e293b;border-radius:8px;padding:10px 14px;margin-bottom:6px;font-size:.82rem}
.history-card .delta-up{color:#ef4444;font-weight:600}.history-card .delta-down{color:#22c55e;font-weight:600}

/* 랜딩 */
.landing-hero{background:linear-gradient(135deg,#0f172a 0%,#1e1b4b 50%,#312e81 100%);padding:60px 20px;border-radius:20px;text-align:center}
.landing-hero h1{color:#fff;font-size:2.5rem;margin-bottom:10px}
.landing-hero p{color:#94a3b8;font-size:1.1rem}

/* 인증 */
.auth-container{max-width:420px;margin:40px auto;padding:32px;background:#111827;border:1px solid #1e293b;border-radius:16px;box-shadow:0 4px 24px rgba(0,0,0,.3)}

/* 프로젝트 카드 */
.project-card{background:linear-gradient(135deg,#111827 0%,#0f172a 100%);border:1px solid #1e293b;border-radius:14px;padding:22px;margin-bottom:14px;transition:all .25s;cursor:pointer;position:relative;overflow:hidden}
.project-card::after{content:'';position:absolute;top:0;left:0;right:0;height:3px;background:linear-gradient(90deg,#6366f1,#a855f7);opacity:0;transition:opacity .25s}
.project-card:hover{border-color:#6366f1;transform:translateY(-3px);box-shadow:0 8px 32px rgba(99,102,241,.15)}
.project-card:hover::after{opacity:1}

/* 인사이트 카드 */
.insight-card{background:#111827;border:1px solid #1e293b;border-radius:10px;padding:16px;margin-bottom:10px;transition:all .2s}
.insight-card:hover{border-color:#334155;box-shadow:0 2px 8px rgba(0,0,0,.15)}
.insight-card.urgent{border-left:4px solid #ef4444}
.insight-card.improved{border-left:4px solid #22c55e}
.insight-card.resolved{border-left:4px solid #22c55e}
.insight-card.new-page{border-left:4px solid #818cf8}
.insight-card.new-issue{border-left:4px solid #f59e0b}
.insight-card.lost-page{border-left:4px solid #ef4444}
.insight-card .insight-title{font-weight:600;color:#e2e8f0;margin-bottom:4px;font-size:.92rem}
.insight-card .insight-detail{color:#94a3b8;font-size:.85rem;line-height:1.5}
.insight-card .insight-url{color:#818cf8;font-size:.8rem;margin-top:4px;font-weight:500}

/* 기능 카드 (랜딩) */
.feature-card{background:#111827;border:1px solid #1e293b;border-radius:14px;padding:28px;text-align:center;transition:all .25s}
.feature-card:hover{border-color:#6366f1;transform:translateY(-2px);box-shadow:0 4px 20px rgba(99,102,241,.1)}
.feature-card h3{color:#818cf8;font-size:1.1rem;margin-bottom:8px;font-weight:700}
.feature-card p{color:#94a3b8;font-size:.88rem;line-height:1.5}

/* 블러 티저 */
.blurred{filter:blur(4px);user-select:none;pointer-events:none}

/* 퀵스캔 메트릭 */
.metric-row{display:flex;gap:10px;justify-content:center;flex-wrap:wrap;margin:16px 0}
.metric-item{background:#111827;border:1px solid #1e293b;border-radius:10px;padding:14px 18px;text-align:center;min-width:120px;transition:all .2s}
.metric-item:hover{border-color:#334155;box-shadow:0 2px 8px rgba(0,0,0,.15)}
.metric-item .metric-icon{font-size:1.4rem;margin-bottom:6px}
.metric-item .metric-label{font-size:.73rem;color:#64748b;font-weight:500;text-transform:uppercase;letter-spacing:.04em}
.metric-item .metric-value{font-size:.88rem;font-weight:600;color:#e2e8f0}
.metric-item.pass .metric-icon{color:#22c55e}
.metric-item.fail .metric-icon{color:#ef4444}
.metric-item.warn .metric-icon{color:#f59e0b}

/* 티저 이슈 */
.teaser-issue{background:#111827;border:1px solid #1e293b;border-radius:8px;padding:12px 16px;margin-bottom:6px;display:flex;align-items:center;gap:10px}
.teaser-issue .issue-name{color:#e2e8f0;font-size:.88rem;flex:1;font-weight:500}
.teaser-issue .issue-fix-blur{color:#22c55e;font-size:.85rem;filter:blur(4px);user-select:none}

/* 스케줄 배지 */
.schedule-badge{display:inline-block;padding:3px 10px;border-radius:6px;font-size:.73rem;font-weight:600;letter-spacing:.02em}
.schedule-manual{background:#1e293b;color:#64748b}
.schedule-daily{background:rgba(99,102,241,.12);color:#818cf8}
.schedule-weekly{background:rgba(245,158,11,.12);color:#f59e0b}

/* Google Sign-In */
.google-btn{display:flex;align-items:center;justify-content:center;gap:12px;background:#ffffff;color:#3c4043;border:1px solid #dadce0;border-radius:10px;padding:14px 24px;font-size:1rem;font-weight:500;cursor:pointer;transition:all .2s;text-decoration:none;width:100%;margin:12px 0}
.google-btn:hover{background:#f7f8f8;box-shadow:0 2px 8px rgba(0,0,0,.15)}
.google-btn img{width:20px;height:20px}

/* 플랜 배지 */
.plan-badge{display:inline-block;padding:4px 12px;border-radius:8px;font-size:.73rem;font-weight:600}
.plan-free{background:#1e293b;color:#64748b}
.plan-business{background:rgba(99,102,241,.12);color:#818cf8}

/* 프로젝트 제한 바 */
.limit-bar{background:#1e293b;border-radius:6px;height:6px;overflow:hidden;margin:6px 0}
.limit-bar-fill{background:linear-gradient(90deg,#6366f1,#8b5cf6);height:100%;border-radius:6px;transition:width .3s}
.limit-bar-full .limit-bar-fill{background:linear-gradient(90deg,#ef4444,#f87171)}

/* 트래픽/SC */
.traffic-metric{background:#111827;border:1px solid #1e293b;border-radius:12px;padding:18px;text-align:center;transition:all .2s}
.traffic-metric:hover{border-color:#334155;box-shadow:0 2px 8px rgba(0,0,0,.15)}
.traffic-metric .big-num{font-size:2rem;font-weight:800;color:#e2e8f0;letter-spacing:-.02em}
.traffic-metric .label{font-size:.75rem;color:#64748b;margin-top:4px;font-weight:500;text-transform:uppercase;letter-spacing:.04em}
.traffic-metric .delta{font-size:.82rem;margin-top:6px;font-weight:600}
.traffic-metric .delta.up{color:#22c55e}
.traffic-metric .delta.down{color:#ef4444}

/* CWV 게이지 */
.cwv-card{background:#111827;border:1px solid #1e293b;border-radius:12px;padding:18px;text-align:center;min-width:100px;transition:all .2s}
.cwv-card:hover{border-color:#334155}
.cwv-card .cwv-value{font-size:1.5rem;font-weight:700;color:#e2e8f0}
.cwv-card .cwv-label{font-size:.73rem;color:#64748b;margin-top:4px;font-weight:500;text-transform:uppercase;letter-spacing:.03em}
.cwv-card .cwv-rating{font-size:.72rem;padding:3px 10px;border-radius:6px;margin-top:6px;display:inline-block;font-weight:600}
.cwv-good{color:#22c55e;background:rgba(34,197,94,.1)}
.cwv-needs-improvement{color:#f59e0b;background:rgba(245,158,11,.1)}
.cwv-poor{color:#ef4444;background:rgba(239,68,68,.1)}

/* 변경 히스토리 */
.change-card{background:#111827;border:1px solid #1e293b;border-radius:10px;padding:12px 16px;margin-bottom:8px;transition:all .15s}
.change-card:hover{border-color:#334155}
.change-card .change-field{color:#818cf8;font-weight:600;font-size:.85rem}
.change-card .change-old{color:#ef4444;text-decoration:line-through;font-size:.82rem}
.change-card .change-new{color:#22c55e;font-size:.82rem;font-weight:500}
.change-card .change-url{color:#64748b;font-size:.78rem;margin-top:4px}

/* SC 연결 */
.sc-connected{background:rgba(34,197,94,.06);border:1px solid rgba(34,197,94,.2);border-radius:10px;padding:14px;margin:10px 0}
.sc-disconnected{background:rgba(239,68,68,.06);border:1px solid rgba(239,68,68,.2);border-radius:10px;padding:14px;margin:10px 0}

/* 크롤링 상태 */
.crawl-status{background:#111827;border:1px solid #1e293b;border-radius:10px;padding:12px 18px;font-family:'SF Mono','JetBrains Mono','Consolas',monospace;font-size:.82rem;color:#818cf8;margin-bottom:8px}
.crawl-status .url{color:#94a3b8}.crawl-status .count{color:#22c55e;font-weight:700}.crawl-status .eta{color:#f59e0b;font-weight:600}

/* ── 글로벌 세부 조정 ── */
h1, h2, h3, h4 { color: #f1f5f9 !important; font-weight: 700 !important; letter-spacing: -.02em; }
.stCaption { color: #64748b !important; }
.stMarkdown a { color: #818cf8 !important; }
.stMarkdown a:hover { color: #a5b4fc !important; }
hr { border-color: #1e293b !important; }
.stAlert { border-radius: 10px !important; }
</style>
""", unsafe_allow_html=True)


# ── 세션 상태 초기화 ──────────────────────────────────────────────────────────
if "user" not in st.session_state:
    st.session_state.user = None
if "view" not in st.session_state:
    st.session_state.view = "landing"
if "current_project_id" not in st.session_state:
    st.session_state.current_project_id = None
if "card_filter" not in st.session_state:
    st.session_state.card_filter = None


# ── 네비게이션 헬퍼 ──────────────────────────────────────────────────────────
def navigate(view, **kwargs):
    st.session_state.view = view
    for k, v in kwargs.items():
        st.session_state[k] = v
    st.rerun()


def fmt_time(seconds):
    if seconds < 0:
        return "--:--"
    m, s = divmod(int(seconds), 60)
    return f"{m:02d}:{s:02d}"


# ── 사이드바 ──────────────────────────────────────────────────────────────────
def render_sidebar():
    with st.sidebar:
        st.markdown("### 🌐 weballin")
        st.caption("v5.0 — SaaS Edition")
        st.divider()

        if st.session_state.user:
            user = st.session_state.user
            st.markdown(f"**{user['name']}**님 환영합니다")
            st.caption(user["email"])
            st.divider()

            if st.button("📊 대시보드", use_container_width=True):
                navigate("dashboard")

            # 프로젝트 목록
            st.markdown("#### 프로젝트")
            projects = db.get_projects(user["id"])
            for proj in projects:
                label = f"📁 {proj['name']}"
                if st.button(label, key=f"sidebar_proj_{proj['id']}", use_container_width=True):
                    navigate("project_detail", current_project_id=proj["id"])

            if st.button("➕ 새 프로젝트 만들기", use_container_width=True):
                navigate("project_new")

            # SC connection status for selected project
            if st.session_state.current_project_id:
                try:
                    sc_conn = db.get_sc_connection(st.session_state.current_project_id)
                    if sc_conn:
                        st.divider()
                        st.markdown("#### 🔍 서치콘솔")
                        st.markdown(f'<div class="sc-connected">✅ 연결됨: {sc_conn.get("site_url", "")}</div>', unsafe_allow_html=True)
                        # Quick stats: last 7 days
                        try:
                            end_date = datetime.utcnow().strftime("%Y-%m-%d")
                            start_date = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
                            sc_data = db.get_sc_analytics(st.session_state.current_project_id, start_date, end_date)
                            if sc_data:
                                total_clicks_7d = sum(r.get("clicks", 0) for r in sc_data)
                                total_impressions_7d = sum(r.get("impressions", 0) for r in sc_data)
                                st.caption(f"최근 7일: 클릭 {total_clicks_7d:,} · 노출 {total_impressions_7d:,}")
                        except Exception:
                            pass
                except Exception:
                    pass

            st.divider()
            if st.button("🚪 로그아웃", use_container_width=True):
                st.session_state.user = None
                st.session_state.current_project_id = None
                navigate("landing")
        else:
            st.markdown("SEO 진단을 시작하세요!")
            st.caption("무료로 가입하고 시작하세요.")


# ══════════════════════════════════════════════════════════════════════════════
# VIEW: 랜딩 페이지
# ══════════════════════════════════════════════════════════════════════════════
def render_landing():
    # Top navigation with auth buttons
    tc1, tc2, tc3, tc4 = st.columns([4, 1.5, 1.5, 1])
    with tc1:
        st.markdown('<span style="color:#818cf8;font-weight:800;font-size:1.3rem;">🌐 weballin</span>', unsafe_allow_html=True)
    with tc3:
        if st.button("로그인", use_container_width=True, key="top_login"):
            navigate("login")
    with tc4:
        if st.button("무료 시작", use_container_width=True, type="primary", key="top_signup"):
            navigate("signup")

    # Hero section
    st.markdown("""
    <div style="background:linear-gradient(135deg,#0f172a 0%,#1e1b4b 40%,#312e81 80%,#6366f1 150%);padding:80px 20px;border-radius:20px;text-align:center;margin-bottom:30px;">
        <h1 style="color:#ffffff;font-size:3rem;font-weight:800;margin-bottom:12px;line-height:1.3;">
            당신의 사이트,<br>검색엔진은 어떻게 보고 있을까요?
        </h1>
        <p style="color:#c9d1d9;font-size:1.2rem;margin-bottom:8px;">
            SEO · 콘텐츠 · 테크니컬 · AI 최적화까지 한 번에 진단하고 개선하세요
        </p>
        <p style="color:#94a3b8;font-size:.95rem;">
            지금 바로 무료로 사이트를 분석해보세요 — 가입도 필요 없습니다
        </p>
    </div>
    """, unsafe_allow_html=True)

    # Quick scan
    st.markdown("")
    col_url, col_btn = st.columns([4, 1])
    with col_url:
        scan_url = st.text_input(
            "URL 입력",
            placeholder="https://yoursite.com",
            label_visibility="collapsed",
            key="landing_url",
        )
    with col_btn:
        scan_btn = st.button("무료 진단 시작", use_container_width=True, type="primary")

    if scan_btn and scan_url:
        if not scan_url.startswith(("http://", "https://")):
            scan_url = "https://" + scan_url

        with st.spinner("AI가 사이트를 분석하고 있습니다..."):
            result = crawler.quick_scan(scan_url)

        if result and not result.get("error", "").strip():
            # WAF 차단 안내
            if result.get("waf_blocked"):
                st.markdown(f"""
                <div style="background:rgba(245,158,11,.06);border:1px solid rgba(245,158,11,.25);border-radius:10px;padding:20px;margin:20px 0;text-align:center;">
                    <div style="font-size:2.5rem;margin-bottom:10px;">🛡️</div>
                    <h3 style="color:#f59e0b;margin-bottom:8px;">WAF(웹 방화벽)에 의해 접근이 제한되었습니다</h3>
                    <p style="color:#94a3b8;font-size:.9rem;">
                        <strong>{scan_url}</strong> 사이트는 봇 접근을 차단하고 있습니다.<br>
                        이런 사이트도 <strong>프로젝트를 만들어 전체 크롤링</strong>을 시도하면 일부 페이지를 수집할 수 있습니다.
                    </p>
                </div>
                """, unsafe_allow_html=True)

            score = result.get("score", 0)
            score_class = "score-good" if score >= 80 else ("score-medium" if score >= 50 else "score-bad")

            st.markdown(f"""
            <div style="text-align:center;margin:20px 0;">
                <div class="score-circle {score_class}">{score}</div>
                <p style="color:#94a3b8;font-size:.9rem;">SEO 종합 점수</p>
            </div>
            """, unsafe_allow_html=True)

            # Key metrics
            title_ok = result.get("title_len", 0) > 0
            desc_ok = result.get("desc_len", 0) > 0
            https_ok = result.get("is_https", False)
            schema_ok = result.get("has_schema", False)
            speed_ok = result.get("load_time", 99) < 3.0

            metric_items = [
                ("Title", title_ok, "✅" if title_ok else "❌"),
                ("Description", desc_ok, "✅" if desc_ok else "❌"),
                ("HTTPS", https_ok, "🔒" if https_ok else "🔓"),
                ("Schema", schema_ok, "📊" if schema_ok else "❌"),
                ("Speed", speed_ok, "⚡" if speed_ok else "🐢"),
            ]

            mc = st.columns(5)
            for col, (label, ok, icon) in zip(mc, metric_items):
                color = "#22c55e" if ok else "#ef4444"
                col.markdown(f"""
                <div class="summary-card" style="padding:12px;">
                    <div style="font-size:1.5rem;">{icon}</div>
                    <div class="label">{label}</div>
                    <div style="color:{color};font-size:.85rem;font-weight:600;">{"양호" if ok else "개선 필요"}</div>
                </div>
                """, unsafe_allow_html=True)

            # Issues preview
            issues_preview = result.get("issues_preview", [])[:5]
            if issues_preview:
                st.markdown("### 발견된 주요 이슈")
                for iss in issues_preview:
                    sev = iss.get("severity", "MEDIUM")
                    sev_cls = sev.lower()
                    msg = iss.get("message", "")
                    st.markdown(f"""
                    <div class="issue-card {sev_cls}" style="position:relative;">
                        <div class="issue-header">
                            <span class="badge-{sev_cls}">{sev}</span>
                            <span style="color:#e2e8f0;font-weight:600;margin-left:8px;">{msg}</span>
                        </div>
                        <div class="blurred" style="margin-top:8px;color:#22c55e;font-size:.85rem;">
                            해결 방법: 무료 가입 후 확인하세요
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

            st.markdown("")
            c1, c2, c3 = st.columns([1, 2, 1])
            with c2:
                if st.button("무료로 시작하고 상세 분석 받기", use_container_width=True, type="primary", key="cta_signup"):
                    navigate("signup")
        elif result and result.get("error"):
            st.error(f"분석 실패: {result['error']}")
        else:
            st.error("사이트에 접속할 수 없습니다. URL을 확인해주세요.")

    # Why us section
    st.markdown("---")
    st.markdown("""
    <div style="text-align:center;margin:30px 0 20px 0;">
        <h2 style="color:#e2e8f0;font-size:1.8rem;">왜 weballin인가요?</h2>
        <p style="color:#94a3b8;font-size:1rem;">전문가 수준의 SEO 분석을 누구나 쉽게</p>
    </div>
    """, unsafe_allow_html=True)

    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        st.markdown("""
        <div class="feature-card" style="min-height:180px;">
            <div style="font-size:2.5rem;margin-bottom:10px;">🔬</div>
            <h3>200+ 항목 심층 진단</h3>
            <p>Title, Description, Schema, E-E-A-T, Core Web Vitals, 보안까지 — 검색엔진이 보는 모든 것을 분석합니다</p>
        </div>
        """, unsafe_allow_html=True)
    with fc2:
        st.markdown("""
        <div class="feature-card" style="min-height:180px;">
            <div style="font-size:2.5rem;margin-bottom:10px;">📈</div>
            <h3>변화 추적 & 인사이트</h3>
            <p>크롤링마다 변화를 감지하고, Search Console · PageSpeed 데이터를 통합해 실행 가능한 인사이트를 제공합니다</p>
        </div>
        """, unsafe_allow_html=True)
    with fc3:
        st.markdown("""
        <div class="feature-card" style="min-height:180px;">
            <div style="font-size:2.5rem;margin-bottom:10px;">🤖</div>
            <h3>AI 시대 SEO 대응</h3>
            <p>구조화 데이터, 콘텐츠 품질, 기술적 최적화를 통해 AI 검색 시대에도 발견되는 사이트를 만드세요</p>
        </div>
        """, unsafe_allow_html=True)

    # How it works
    st.markdown("---")
    st.markdown("""
    <div style="text-align:center;margin:20px 0;">
        <h2 style="color:#e2e8f0;font-size:1.5rem;">3단계로 시작하세요</h2>
    </div>
    """, unsafe_allow_html=True)

    s1, s2, s3 = st.columns(3)
    with s1:
        st.markdown("""
        <div style="text-align:center;padding:20px;">
            <div style="background:rgba(99,102,241,.12);width:60px;height:60px;border-radius:50%;display:flex;align-items:center;justify-content:center;margin:0 auto 12px;font-size:1.5rem;font-weight:800;color:#818cf8;">1</div>
            <h4 style="color:#e2e8f0;">이메일로 가입</h4>
            <p style="color:#94a3b8;font-size:.85rem;">30초면 끝. 법인 이메일은 프로젝트 5개!</p>
        </div>
        """, unsafe_allow_html=True)
    with s2:
        st.markdown("""
        <div style="text-align:center;padding:20px;">
            <div style="background:rgba(34,197,94,.1);width:60px;height:60px;border-radius:50%;display:flex;align-items:center;justify-content:center;margin:0 auto 12px;font-size:1.5rem;font-weight:800;color:#22c55e;">2</div>
            <h4 style="color:#e2e8f0;">사이트 등록 & 크롤링</h4>
            <p style="color:#94a3b8;font-size:.85rem;">URL 입력하면 자동으로 200+ 항목을 진단합니다</p>
        </div>
        """, unsafe_allow_html=True)
    with s3:
        st.markdown("""
        <div style="text-align:center;padding:20px;">
            <div style="background:rgba(99,102,241,.12);width:60px;height:60px;border-radius:50%;display:flex;align-items:center;justify-content:center;margin:0 auto 12px;font-size:1.5rem;font-weight:800;color:#818cf8;">3</div>
            <h4 style="color:#e2e8f0;">인사이트 확인 & 개선</h4>
            <p style="color:#94a3b8;font-size:.85rem;">콘텐츠 & 테크니컬 인사이트로 즉시 행동하세요</p>
        </div>
        """, unsafe_allow_html=True)

    # Final CTA
    st.markdown("---")
    st.markdown("""
    <div style="text-align:center;padding:40px 20px;">
        <h2 style="color:#e2e8f0;font-size:1.8rem;margin-bottom:8px;">지금 시작하세요</h2>
        <p style="color:#94a3b8;font-size:1rem;margin-bottom:20px;">무료로 사이트를 진단하고, SEO 전문가처럼 개선하세요</p>
    </div>
    """, unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        if st.button("무료로 시작하기", use_container_width=True, type="primary", key="bottom_cta"):
            navigate("signup")


# ══════════════════════════════════════════════════════════════════════════════
# Google OAuth 처리
# ══════════════════════════════════════════════════════════════════════════════
def _handle_google_callback():
    """URL 파라미터에 Google OAuth 콜백이 있으면 처리합니다."""
    params = st.query_params
    google_email = params.get("google_email", "")
    google_name = params.get("google_name", "")
    google_picture = params.get("google_picture", "")
    if google_email:
        user = db.create_user_google(google_email, google_name or google_email.split("@")[0], google_picture)
        st.session_state.user = user
        st.query_params.clear()
        st.session_state.view = "dashboard"
        return True
    return False

def _render_google_auth_page(title, subtitle):
    """Google 로그인/가입 공통 UI"""
    st.markdown(f"""
    <div style="text-align:center;margin:40px 0 20px 0;">
        <h2 style="color:#818cf8;">{title}</h2>
        <p style="color:#94a3b8;">{subtitle}</p>
    </div>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 1.5, 1])
    with col2:
        st.markdown("""
        <div style="background:#0f172a;border:1px solid #1e293b;border-radius:12px;padding:40px 30px;text-align:center;">
            <p style="color:#94a3b8;margin-bottom:20px;font-size:.9rem;">
                이메일로 간편하게 시작하세요
            </p>
        </div>
        """, unsafe_allow_html=True)

        # Google OAuth 방식: Streamlit secrets에 client_id가 있으면 실제 OAuth, 없으면 데모 모드
        google_client_id = ""
        try:
            google_client_id = st.secrets.get("google", {}).get("client_id", "")
        except Exception:
            pass

        if google_client_id:
            # 실제 Google OAuth (Google Identity Services)
            redirect_uri = st.secrets.get("google", {}).get("redirect_uri", "http://localhost:8501")
            auth_url = (
                f"https://accounts.google.com/o/oauth2/v2/auth?"
                f"client_id={google_client_id}&"
                f"redirect_uri={redirect_uri}&"
                f"response_type=code&"
                f"scope=openid%20email%20profile&"
                f"access_type=offline&"
                f"prompt=consent"
            )
            st.markdown(f"""
            <a href="{auth_url}" class="google-btn" target="_self">
                <svg width="20" height="20" viewBox="0 0 48 48"><path fill="#EA4335" d="M24 9.5c3.54 0 6.71 1.22 9.21 3.6l6.85-6.85C35.9 2.38 30.47 0 24 0 14.62 0 6.51 5.38 2.56 13.22l7.98 6.19C12.43 13.72 17.74 9.5 24 9.5z"/><path fill="#4285F4" d="M46.98 24.55c0-1.57-.15-3.09-.38-4.55H24v9.02h12.94c-.58 2.96-2.26 5.48-4.78 7.18l7.73 6c4.51-4.18 7.09-10.36 7.09-17.65z"/><path fill="#FBBC05" d="M10.53 28.59c-.48-1.45-.76-2.99-.76-4.59s.27-3.14.76-4.59l-7.98-6.19C.92 16.46 0 20.12 0 24c0 3.88.92 7.54 2.56 10.78l7.97-6.19z"/><path fill="#34A853" d="M24 48c6.48 0 11.93-2.13 15.89-5.81l-7.73-6c-2.15 1.45-4.92 2.3-8.16 2.3-6.26 0-11.57-4.22-13.47-9.91l-7.98 6.19C6.51 42.62 14.62 48 24 48z"/></svg>
                Google 계정으로 계속하기
            </a>
            """, unsafe_allow_html=True)
        else:
            # 이메일 입력 및 인증
            with st.form("auth_form"):
                auth_email = st.text_input("이메일", placeholder="you@gmail.com 또는 you@company.com")
                auth_password = st.text_input("비밀번호 (법인 이메일만 해당)", type="password", placeholder="법인 이메일은 비밀번호가 필요합니다")

                submitted = st.form_submit_button(
                    "계속하기",
                    use_container_width=True,
                    type="primary",
                )
                if submitted:
                    if not auth_email or "@" not in auth_email:
                        st.error("올바른 이메일 주소를 입력해주세요.")
                    else:
                        is_corp = db.is_corporate_email(auth_email)
                        auto_name = auth_email.split("@")[0]

                        if is_corp:
                            if not auth_password:
                                st.error("법인 이메일은 비밀번호를 입력해주세요.")
                            else:
                                # 1. 비밀번호 로그인 시도
                                existing_user = db.verify_user(auth_email, auth_password)
                                if existing_user:
                                    st.session_state.user = existing_user
                                    st.success("로그인 성공!")
                                    time.sleep(0.5)
                                    navigate("dashboard")
                                else:
                                    # 2. 이메일이 DB에 존재하는지 확인
                                    conn = db.get_db()
                                    try:
                                        row = conn.execute("SELECT * FROM users WHERE email = ?", (auth_email,)).fetchone()
                                    finally:
                                        conn.close()

                                    if row:
                                        # 기존 사용자가 있음 — 비밀번호 업데이트 (이전에 Google로 가입한 경우)
                                        pw_hash, salt = db.hash_password(auth_password)
                                        conn = db.get_db()
                                        try:
                                            conn.execute(
                                                "UPDATE users SET password_hash = ?, salt = ? WHERE email = ?",
                                                (pw_hash, salt, auth_email)
                                            )
                                            conn.commit()
                                            updated_row = conn.execute("SELECT * FROM users WHERE email = ?", (auth_email,)).fetchone()
                                            st.session_state.user = dict(updated_row)
                                        finally:
                                            conn.close()
                                        st.success("비밀번호가 설정되었습니다. 로그인 완료!")
                                        time.sleep(0.5)
                                        navigate("dashboard")
                                    else:
                                        # 3. 완전 새 사용자
                                        try:
                                            user_id = db.create_user(auth_email, auth_password, auto_name)
                                            user = db.get_user(user_id)
                                            st.session_state.user = user
                                            st.success("가입 완료! 프로젝트 5개까지 생성 가능합니다.")
                                            time.sleep(0.5)
                                            navigate("dashboard")
                                        except ValueError:
                                            st.error("가입 중 오류가 발생했습니다. 다시 시도해주세요.")
                        else:
                            # 개인 이메일: 비밀번호 불필요
                            user = db.create_user_google(auth_email, auto_name)
                            st.session_state.user = user
                            st.info("프로젝트 1개 사용 가능합니다. 법인 이메일로 가입하면 5개까지!")
                            time.sleep(0.5)
                            navigate("dashboard")

            # 비밀번호 초기화
            with st.expander("비밀번호를 잊으셨나요?"):
                with st.form("reset_pw_form"):
                    reset_email = st.text_input("등록한 이메일", key="reset_email_input")
                    new_pw = st.text_input("새 비밀번호", type="password", key="reset_pw_input")
                    new_pw_confirm = st.text_input("새 비밀번호 확인", type="password", key="reset_pw_confirm")
                    reset_btn = st.form_submit_button("비밀번호 재설정")
                    if reset_btn:
                        if not reset_email or "@" not in reset_email:
                            st.error("이메일을 입력해주세요.")
                        elif not new_pw or len(new_pw) < 4:
                            st.error("비밀번호는 4자 이상 입력해주세요.")
                        elif new_pw != new_pw_confirm:
                            st.error("비밀번호가 일치하지 않습니다.")
                        else:
                            conn = db.get_db()
                            try:
                                row = conn.execute("SELECT * FROM users WHERE email = ?", (reset_email,)).fetchone()
                            finally:
                                conn.close()
                            if row:
                                pw_hash, salt = db.hash_password(new_pw)
                                conn = db.get_db()
                                try:
                                    conn.execute("UPDATE users SET password_hash = ?, salt = ? WHERE email = ?", (pw_hash, salt, reset_email))
                                    conn.commit()
                                finally:
                                    conn.close()
                                st.success("비밀번호가 재설정되었습니다. 위 폼에서 로그인하세요.")
                            else:
                                st.error("등록되지 않은 이메일입니다.")

        # 안내 메시지
        st.markdown("""
        <div style="background:#111827;border:1px solid #1e293b;border-radius:8px;padding:16px;margin:20px 0;text-align:center;">
            <p style="color:#818cf8;font-size:.9rem;font-weight:600;margin-bottom:8px;">
                🏢 법인 이메일 = 프로젝트 5개 · 개인 이메일 = 프로젝트 1개
            </p>
            <p style="color:#94a3b8;font-size:.82rem;margin:0;">
                법인 이메일은 비밀번호를 설정하여 안전하게 로그인합니다.<br>
                개인 이메일(Gmail, Naver 등)은 이메일만 입력하면 바로 시작됩니다.
            </p>
        </div>
        """, unsafe_allow_html=True)

    # 하단 네비게이션
    col1, col2, col3 = st.columns([1, 1.5, 1])
    with col2:
        if st.button("← 메인으로 돌아가기", use_container_width=True):
            navigate("landing")


# ══════════════════════════════════════════════════════════════════════════════
# VIEW: 로그인
# ══════════════════════════════════════════════════════════════════════════════
def render_login():
    _render_google_auth_page("🔍 로그인", "이메일로 로그인하세요")


# ══════════════════════════════════════════════════════════════════════════════
# VIEW: 회원가입
# ══════════════════════════════════════════════════════════════════════════════
def render_signup():
    _render_google_auth_page("🚀 무료 가입", "이메일로 30초 만에 시작하세요")


# ══════════════════════════════════════════════════════════════════════════════
# VIEW: 대시보드
# ══════════════════════════════════════════════════════════════════════════════
def render_dashboard():
    user = st.session_state.user
    st.markdown(f"""
    <div class="nav-bar">
        <span class="brand">🌐 weballin</span>
        <span class="user-info">{user['name']}님 환영합니다</span>
    </div>
    """, unsafe_allow_html=True)

    # 플랜 & 프로젝트 제한 정보
    can_create, current_count, max_proj = db.can_create_project(user["id"])
    is_corp = db.is_corporate_email(user.get("email", ""))
    plan_label = "Business" if is_corp else "Free"
    plan_cls = "plan-business" if is_corp else "plan-free"

    st.markdown(f"""
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;">
        <h2 style="margin:0;">📊 대시보드</h2>
        <div style="text-align:right;">
            <span class="plan-badge {plan_cls}">{plan_label}</span>
            <span style="color:#94a3b8;font-size:.82rem;margin-left:8px;">프로젝트 {current_count}/{max_proj}</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # 프로젝트 사용량 바
    fill_pct = min(current_count / max_proj * 100, 100) if max_proj > 0 else 100
    bar_cls = "limit-bar-full" if current_count >= max_proj else ""
    st.markdown(f"""
    <div class="limit-bar {bar_cls}"><div class="limit-bar-fill" style="width:{fill_pct}%;"></div></div>
    """, unsafe_allow_html=True)

    if not is_corp and current_count >= max_proj:
        st.markdown("""
        <div style="background:rgba(99,102,241,.06);border:1px solid rgba(99,102,241,.25);border-radius:8px;padding:12px;margin:12px 0;text-align:center;">
            <p style="color:#818cf8;font-size:.9rem;margin:0;">
                🏢 법인 이메일로 가입하면 프로젝트를 5개까지 만들 수 있습니다!
            </p>
        </div>
        """, unsafe_allow_html=True)

    projects = db.get_projects(user["id"])

    if not projects:
        st.info("아직 프로젝트가 없습니다. 첫 번째 프로젝트를 만들어보세요!")
        if st.button("➕ 새 프로젝트 만들기", type="primary"):
            navigate("project_new")
        return

    # Project overview cards
    for proj in projects:
        latest = db.get_latest_crawl(proj["id"])

        # Schedule badge
        sched = proj.get("schedule", "manual")
        sched_labels = {"manual": "수동", "daily": "매일", "weekly": "매주"}
        sched_cls = f"schedule-{sched}"
        sched_label = sched_labels.get(sched, sched)

        issue_summary = ""
        last_crawl_info = "크롤링 기록 없음"
        if latest:
            last_crawl_info = f"마지막 크롤링: {(latest.get('completed_at') or '')[:16]}"
            h = latest.get("high_issues", 0)
            m = latest.get("medium_issues", 0)
            lo = latest.get("low_issues", 0)
            issue_summary = f"🔴 {h} · 🟡 {m} · 🟢 {lo}"

        next_crawl = ""
        if sched == "daily":
            next_crawl = f"다음 크롤링: 매일 {proj.get('schedule_time', '09:00')}"
        elif sched == "weekly":
            next_crawl = f"다음 크롤링: 매주 {proj.get('schedule_time', '09:00')}"

        st.markdown(f"""
        <div class="project-card">
            <div style="display:flex;justify-content:space-between;align-items:center;">
                <div>
                    <strong style="color:#e2e8f0;font-size:1.1rem;">📁 {proj['name']}</strong>
                    <span class="schedule-badge {sched_cls}" style="margin-left:8px;">{sched_label}</span>
                </div>
                <span style="color:#94a3b8;font-size:.85rem;">{last_crawl_info}</span>
            </div>
            <div style="color:#818cf8;font-size:.85rem;margin-top:4px;">{proj['url']}</div>
            <div style="margin-top:8px;display:flex;gap:16px;align-items:center;">
                <span style="color:#94a3b8;font-size:.85rem;">{issue_summary}</span>
                <span style="color:#475569;font-size:.8rem;">{next_crawl}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

        if st.button(f"상세 보기 →", key=f"dash_proj_{proj['id']}", use_container_width=True):
            navigate("project_detail", current_project_id=proj["id"])

    st.markdown("")
    if can_create:
        if st.button("➕ 새 프로젝트 만들기", use_container_width=True):
            navigate("project_new")
    else:
        st.button(f"프로젝트 한도 도달 ({current_count}/{max_proj})", use_container_width=True, disabled=True)
        if not is_corp:
            st.caption("🏢 법인 이메일로 가입하면 5개까지 가능합니다")


# ══════════════════════════════════════════════════════════════════════════════
# VIEW: 프로젝트 생성
# ══════════════════════════════════════════════════════════════════════════════
def render_project_new():
    user = st.session_state.user

    st.markdown(f"""
    <div class="nav-bar">
        <span class="brand">🌐 weballin</span>
        <span class="user-info">새 프로젝트 만들기</span>
    </div>
    """, unsafe_allow_html=True)

    if st.button("← 대시보드로 돌아가기"):
        navigate("dashboard")

    # 프로젝트 제한 체크
    can_create, current_count, max_proj = db.can_create_project(user["id"])
    if not can_create:
        st.error(f"프로젝트 한도에 도달했습니다 ({current_count}/{max_proj})")
        if not db.is_corporate_email(user.get("email", "")):
            st.info("🏢 법인 이메일로 가입하면 프로젝트를 5개까지 만들 수 있습니다!")
        return

    st.markdown(f"## ➕ 새 프로젝트 만들기 ({current_count}/{max_proj})")

    with st.form("project_form"):
        proj_name = st.text_input("프로젝트 이름", placeholder="내 블로그 SEO")
        proj_url = st.text_input("사이트 URL", placeholder="https://example.com")

        submitted = st.form_submit_button("프로젝트 생성", use_container_width=True, type="primary")

        if submitted:
            if not proj_name or not proj_url:
                st.error("프로젝트 이름과 URL을 입력해주세요.")
            else:
                if not proj_url.startswith(("http://", "https://")):
                    proj_url = "https://" + proj_url
                proj_id = db.create_project(
                    user_id=user["id"],
                    name=proj_name,
                    url=proj_url,
                    crawl_mode="full",
                    crawl_path="",
                    max_pages=200,
                    crawl_delay=0.5,
                    schedule="manual",
                    schedule_time="09:00",
                )
                st.success("프로젝트가 생성되었습니다!")
                navigate("project_detail", current_project_id=proj_id)


# ══════════════════════════════════════════════════════════════════════════════
# VIEW: 프로젝트 상세
# ══════════════════════════════════════════════════════════════════════════════
def render_project_detail():
    user = st.session_state.user
    project_id = st.session_state.current_project_id
    project = db.get_project(project_id)

    if not project:
        st.error("프로젝트를 찾을 수 없습니다.")
        if st.button("대시보드로 돌아가기"):
            navigate("dashboard")
        return

    # Header
    st.markdown(f"""
    <div class="nav-bar">
        <span class="brand">📁 {project['name']}</span>
        <span class="user-info">{project['url']} · {project['crawl_mode']} · 최대 {project['max_pages']}p</span>
    </div>
    """, unsafe_allow_html=True)

    col_back, col_delete = st.columns([6, 1])
    with col_back:
        if st.button("← 대시보드"):
            navigate("dashboard")
    with col_delete:
        if st.button("🗑️ 삭제", type="secondary"):
            db.delete_project(project_id)
            navigate("dashboard")
            return

    # Tabs
    tab_insights, tab_aigeo, tab_results, tab_history, tab_sc, tab_speed, tab_changes, tab_settings = st.tabs([
        "💡 인사이트", "🤖 AI/GEO", "📊 결과 분석", "📈 히스토리",
        "🔍 서치콘솔", "⚡ 사이트 속도", "📝 변경 히스토리", "⚙️ 설정"
    ])

    # ── 인사이트 탭 ──
    with tab_insights:
        render_insights(project)

    # ── AI/GEO 탭 ──
    with tab_aigeo:
        render_ai_geo(project)

    # ── 결과 분석 탭 ──
    with tab_results:
        render_results_analysis(project)

    # ── 히스토리 탭 ──
    with tab_history:
        render_crawl_history(project)

    # ── 서치콘솔 탭 ──
    with tab_sc:
        render_search_console(project)

    # ── 사이트 속도 탭 ──
    with tab_speed:
        render_pagespeed(project)

    # ── 변경 히스토리 탭 ──
    with tab_changes:
        render_page_changes(project)

    # ── 설정 탭 (크롤링 실행 포함) ──
    with tab_settings:
        render_project_settings(project)


# ── 프로젝트 개요 ────────────────────────────────────────────────────────────
def render_project_overview(project):
    latest = db.get_latest_crawl(project["id"])

    if not latest:
        st.info("아직 크롤링 기록이 없습니다. '설정' 탭에서 첫 번째 크롤링을 시작하세요.")
        return

    total_pages = latest.get("total_pages", 0)
    high = latest.get("high_issues", 0)
    med = latest.get("medium_issues", 0)
    low = latest.get("low_issues", 0)
    total_issues = latest.get("total_issues", 0)

    # Load pages for additional metrics
    pages = []
    try:
        pages_raw = latest.get("pages_json", "[]")
        if pages_raw:
            pages = json.loads(pages_raw)
    except (json.JSONDecodeError, TypeError):
        pages = []

    avg_load = 0
    schema_count = 0
    https_count = 0
    if pages:
        avg_load = round(sum(p.get("Load (s)", 0) for p in pages) / len(pages), 2) if pages else 0
        schema_count = sum(1 for p in pages if p.get("Has Schema"))
        https_count = sum(1 for p in pages if p.get("HTTPS"))

    # ── 건강 점수 계산 (비율 기반, PageSpeed 방식) ──
    # 6개 카테고리 × 가중치 = 100점 만점
    # 각 카테고리는 0~1 비율로 계산 후 가중치를 곱함
    health_breakdown = {}

    if total_pages > 0:
        # 1. 치명적 이슈 비율 (30점) — HIGH 이슈가 있는 페이지 비율
        high_ratio = min(high / total_pages, 1.0)
        cat1_score = round((1 - high_ratio) * 30)
        health_breakdown["치명적 이슈"] = {"score": cat1_score, "max": 30,
            "detail": f"HIGH 이슈 {high}건 / {total_pages} 페이지 (비율 {high_ratio*100:.0f}%)"}

        # 2. 중요 이슈 비율 (20점) — MEDIUM 이슈 비율 (페이지당 평균)
        med_per_page = med / total_pages
        med_ratio = min(med_per_page / 3.0, 1.0)  # 페이지당 3건 이상이면 0점
        cat2_score = round((1 - med_ratio) * 20)
        health_breakdown["중요 이슈"] = {"score": cat2_score, "max": 20,
            "detail": f"MEDIUM 이슈 {med}건 (페이지당 평균 {med_per_page:.1f}건)"}

        # 3. 경미한 이슈 비율 (10점) — LOW 이슈 비율
        low_per_page = low / total_pages
        low_ratio = min(low_per_page / 5.0, 1.0)  # 페이지당 5건 이상이면 0점
        cat3_score = round((1 - low_ratio) * 10)
        health_breakdown["경미한 이슈"] = {"score": cat3_score, "max": 10,
            "detail": f"LOW 이슈 {low}건 (페이지당 평균 {low_per_page:.1f}건)"}

        # 4. 페이지 속도 (15점)
        if avg_load <= 1.5:
            cat4_score = 15
        elif avg_load <= 3.0:
            cat4_score = round(15 * (1 - (avg_load - 1.5) / 3.0))
        elif avg_load <= 6.0:
            cat4_score = round(5 * (1 - (avg_load - 3.0) / 3.0))
        else:
            cat4_score = 0
        cat4_score = max(0, cat4_score)
        health_breakdown["페이지 속도"] = {"score": cat4_score, "max": 15,
            "detail": f"평균 로딩 시간 {avg_load}초"}

        # 5. HTTPS 보안 (15점)
        https_ratio = https_count / total_pages
        cat5_score = round(https_ratio * 15)
        health_breakdown["HTTPS 보안"] = {"score": cat5_score, "max": 15,
            "detail": f"HTTPS 적용 {https_count}/{total_pages} ({https_ratio*100:.0f}%)"}

        # 6. 구조화 데이터 (10점)
        schema_ratio = schema_count / total_pages
        cat6_score = round(min(schema_ratio / 0.7, 1.0) * 10)  # 70% 이상이면 만점
        health_breakdown["구조화 데이터"] = {"score": cat6_score, "max": 10,
            "detail": f"Schema 적용 {schema_count}/{total_pages} ({schema_ratio*100:.0f}%)"}

        health_score = sum(v["score"] for v in health_breakdown.values())
    else:
        health_score = 0
    health_score = max(0, min(100, int(health_score)))

    if health_score >= 80:
        health_color = "#22c55e"
        health_cls = "score-good"
        health_label = "양호"
    elif health_score >= 50:
        health_color = "#f59e0b"
        health_cls = "score-medium"
        health_label = "개선 필요"
    else:
        health_color = "#ef4444"
        health_cls = "score-bad"
        health_label = "위험"

    # Health score visual
    st.markdown(f"""
    <div style="text-align:center;margin:20px 0;">
        <div class="score-circle {health_cls}" style="width:140px;height:140px;font-size:2.4rem;margin:0 auto;">{health_score}</div>
        <p style="color:{health_color};font-weight:600;font-size:1.1rem;margin-top:8px;">사이트 건강 점수: {health_label}</p>
    </div>
    """, unsafe_allow_html=True)

    # Health score breakdown (PageSpeed style)
    if health_breakdown:
        with st.expander("📊 점수 산출 근거", expanded=True):
            for cat_name, cat_data in health_breakdown.items():
                pct = (cat_data["score"] / cat_data["max"] * 100) if cat_data["max"] > 0 else 0
                bar_color = "#22c55e" if pct >= 80 else "#f59e0b" if pct >= 50 else "#ef4444"
                label_emoji = "🟢" if pct >= 80 else "🟡" if pct >= 50 else "🔴"
                st.markdown(f"{label_emoji} **{cat_name}** — {cat_data['score']}/{cat_data['max']}")
                st.progress(pct / 100)
                st.caption(cat_data["detail"])

    card_data = [
        ("총 페이지", total_pages, "", "전체 수집된 페이지 수"),
        ("총 이슈", total_issues, "", "발견된 모든 SEO 이슈"),
        ("🔴 HIGH", high, "red", "즉시 수정 필요"),
        ("🟡 MEDIUM", med, "yellow", "개선 권장"),
        ("🟢 LOW", low, "green", "참고 수준"),
        ("평균 로딩", f"{avg_load}s", "", "3초 이상은 이탈률 증가"),
        ("Schema", f"{schema_count}/{total_pages}", "", "구조화 데이터 적용"),
        ("HTTPS", f"{https_count}/{total_pages}", "", "HTTPS 적용 비율"),
    ]
    cols = st.columns(len(card_data))
    for col, (label, val, cls, hint) in zip(cols, card_data):
        col.markdown(f"""
        <div class="summary-card {cls}">
            <div class="num">{val}</div>
            <div class="label">{label}</div>
            <div class="hint">{hint}</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("")
    st.caption(f"마지막 크롤링: {(latest.get('completed_at') or 'N/A')}")


# ── 크롤링 실행 ──────────────────────────────────────────────────────────────
def render_crawl_execution(project):
    st.markdown(f"""
    - **사이트**: {project['url']}
    - **모드**: {project['crawl_mode']}
    - **최대 페이지**: {project['max_pages']}
    - **딜레이**: {project['crawl_delay']}초
    """)

    # 크롤 봇(User-Agent) 선택
    with st.expander("🤖 크롤 봇 & 렌더링 설정", expanded=False):
        bot_names = list(crawler.USER_AGENTS.keys())
        selected_bot = st.selectbox(
            "크롤 봇 (User-Agent)",
            bot_names,
            index=0,
            help="검색엔진 봇, AI 봇, 브라우저 등 다양한 User-Agent로 크롤링합니다. "
                 "사이트가 특정 봇을 차단하는지 테스트할 수 있습니다.",
        )
        st.caption(f"`{crawler.USER_AGENTS[selected_bot][:80]}...`" if len(crawler.USER_AGENTS[selected_bot]) > 80 else f"`{crawler.USER_AGENTS[selected_bot]}`")

        col_js1, col_js2 = st.columns([1, 3])
        with col_js1:
            js_rendering = st.toggle("JavaScript 렌더링", value=False, key="js_render_toggle")
        with col_js2:
            if js_rendering:
                st.info("🌐 Playwright 헤드리스 브라우저로 JavaScript를 실행한 후 페이지를 분석합니다. "
                        "SPA/React/Vue 등 JS 기반 사이트에 유용하지만 크롤링 속도가 느려집니다.")
            else:
                st.caption("JavaScript를 실행하지 않고 서버 응답 HTML만 분석합니다.")

        st.markdown("**봇 카테고리:**")
        cat_cols = st.columns(4)
        with cat_cols[0]:
            st.markdown("🔍 **검색엔진**\n- Googlebot\n- Googlebot (스마트폰)\n- Bingbot\n- Naver Bot\n- Yandex Bot")
        with cat_cols[1]:
            st.markdown("🤖 **AI 봇**\n- GPTBot (OpenAI)\n- ChatGPT-User\n- ClaudeBot (Anthropic)\n- Google-Extended")
        with cat_cols[2]:
            st.markdown("📱 **모바일**\n- Safari (iPhone)\n- Samsung Internet")
        with cat_cols[3]:
            st.markdown("💻 **데스크톱**\n- Chrome (기본)\n- Safari (Mac)\n- Firefox\n- Edge")

    if st.button("▶ 크롤링 시작", type="primary", key="start_crawl"):
        # Create crawl run in DB
        run_id = db.create_crawl_run(project["id"])

        # Progress UI
        stat_cols = st.columns(5)
        ph_crawled = stat_cols[0].empty()
        ph_queued = stat_cols[1].empty()
        ph_elapsed = stat_cols[2].empty()
        ph_eta = stat_cols[3].empty()
        ph_speed = stat_cols[4].empty()
        progress_bar = st.progress(0.0)
        status_line = st.empty()
        st.markdown("### 📋 수집 데이터 (실시간)")
        table_ph = st.empty()

        crawl_start = time.time()

        def progress_callback(cnt, total, cur_url):
            queue_sz = max(total - cnt, 0)
            pages_so_far = []

            elapsed = time.time() - crawl_start
            spd = cnt / elapsed if elapsed > 0 else 0
            eta = (total - cnt) / spd if spd > 0 and cnt < total else 0
            pct = min(cnt / total, 1.0) if total > 0 else 0

            progress_bar.progress(pct)
            ph_crawled.metric("Crawled", f"{cnt}/{total}")
            ph_queued.metric("Queue", queue_sz)
            ph_elapsed.metric("Elapsed", fmt_time(elapsed))
            ph_eta.metric("ETA", fmt_time(eta))
            ph_speed.metric("Speed", f"{spd:.1f} pg/s")

            short = cur_url if len(cur_url) < 80 else cur_url[:77] + "..."
            status_line.markdown(
                f'<div class="crawl-status"><span class="count">[{cnt}/{total}]</span> '
                f'<span class="url">{short}</span> '
                f'<span class="eta">ETA {fmt_time(eta)}</span> ({pct*100:.0f}%)</div>',
                unsafe_allow_html=True,
            )

            if pages_so_far:
                dc = ["URL", "Status", "Title", "Title Len", "Desc Len", "H1",
                      "Words", "Outlinks", "Schema Types", "HTTPS", "HTML KB", "Load (s)"]
                df = pd.DataFrame(pages_so_far)
                av = [c for c in dc if c in df.columns]
                d = df[av].copy()
                if "URL" in d.columns:
                    d["URL"] = d["URL"].apply(lambda x: urlparse(x).path or "/")
                table_ph.dataframe(d, use_container_width=True, hide_index=True, height=400)

        # Run crawl
        crawl_ua = crawler.USER_AGENTS.get(selected_bot)
        result = crawler.run_crawl(
            base_url=project["url"],
            mode=project.get("crawl_mode") or "full",
            max_pages=project.get("max_pages") or 200,
            delay=float(project.get("crawl_delay") or 0.5),
            path=project.get("crawl_path", ""),
            progress_callback=progress_callback,
            user_agent=crawl_ua,
            js_rendering=js_rendering,
        )

        elapsed_total = time.time() - crawl_start
        pages = result.get("pages", [])
        issues = result.get("issues", [])

        progress_bar.progress(1.0)
        status_line.markdown(
            f'<div class="crawl-status" style="border-color:#22c55e;">'
            f'✅ <span class="count">크롤링 완료!</span> {len(pages)}개 페이지 · '
            f'{fmt_time(elapsed_total)} 소요</div>',
            unsafe_allow_html=True,
        )

        if pages:
            # Count issues by severity
            high = sum(1 for i in issues if i.get("severity") == "HIGH")
            med = sum(1 for i in issues if i.get("severity") == "MEDIUM")
            low = sum(1 for i in issues if i.get("severity") == "LOW")

            # Clean pages for JSON storage (remove non-serializable items)
            def clean_page(p):
                return {k: v for k, v in p.items() if not k.startswith("_") or k in (
                    "_schema", "_eeat", "_tech", "_security", "_perf", "_content", "_internal_links"
                )}

            pages_json = json.dumps([clean_page(p) for p in pages], ensure_ascii=False, default=str)
            issues_json = json.dumps(issues, ensure_ascii=False, default=str)

            # Update crawl run
            db.update_crawl_run(
                run_id,
                status="completed",
                completed_at=datetime.utcnow().isoformat(),
                total_pages=len(pages),
                high_issues=high,
                medium_issues=med,
                low_issues=low,
                total_issues=len(issues),
                pages_json=pages_json,
                issues_json=issues_json,
            )

            # Update project last_crawl_at
            db.update_project(project["id"], last_crawl_at=datetime.utcnow().isoformat())

            # Generate insights
            prev_crawl = db.get_previous_crawl(project["id"])
            prev_run_id = prev_crawl["id"] if prev_crawl else None
            db.generate_insights(project["id"], run_id, prev_run_id)

            # Save page snapshots and detect changes
            try:
                db.save_page_snapshots(project["id"], run_id, pages)
                changes = db.detect_page_changes(project["id"], run_id)
                if changes:
                    st.info(f"📝 {len(changes)}건의 페이지 변경이 감지되었습니다. '변경 히스토리' 탭에서 확인하세요.")
            except Exception as e:
                st.warning(f"페이지 스냅샷 저장 중 오류: {e}")

            st.success(f"크롤링 완료! {len(pages)}개 페이지 분석, {len(issues)}개 이슈 발견")

            # robots.txt 경고 표시
            robots_info = result.get("robots_info", {})
            if robots_info and robots_info.get("warnings"):
                for warn in robots_info["warnings"]:
                    st.warning(f"🤖 {warn}")
                if robots_info.get("is_fully_blocked"):
                    st.markdown("""
                    <div style="background:rgba(239,68,68,.06);border:1px solid rgba(239,68,68,.25);border-radius:8px;padding:12px;margin:8px 0;">
                        <p style="color:#ef4444;font-weight:600;margin-bottom:4px;">⚠️ 강제 크롤링 안내</p>
                        <p style="color:#94a3b8;font-size:.85rem;margin:0;">
                            이 사이트는 robots.txt에서 크롤러 접근을 차단하고 있습니다.
                            SEO 진단 목적으로 강제 수집하였으며, 이는 검색엔진이 실제로 색인하는 내용과 다를 수 있습니다.
                            Google Search Console에서 실제 색인 상태를 확인하세요.
                        </p>
                    </div>
                    """, unsafe_allow_html=True)
                if robots_info.get("disallowed_paths"):
                    with st.expander(f"🤖 robots.txt 차단 경로 ({len(robots_info['disallowed_paths'])}개)"):
                        for p in robots_info["disallowed_paths"][:30]:
                            st.code(p, language=None)
        else:
            db.update_crawl_run(run_id, status="failed", completed_at=datetime.utcnow().isoformat())
            st.error("크롤링에 실패했습니다. URL을 확인해주세요.")


# ── 결과 분석 ────────────────────────────────────────────────────────────────
def render_results_analysis(project):
    latest = db.get_latest_crawl(project["id"])

    if not latest:
        st.info("아직 크롤링 결과가 없습니다. '설정' 탭에서 크롤링을 시작하세요.")
        return

    # Load data from JSON
    try:
        pages = json.loads(latest.get("pages_json", "[]") or "[]")
        issues = json.loads(latest.get("issues_json", "[]") or "[]")
    except (json.JSONDecodeError, TypeError):
        st.error("크롤링 데이터를 로드할 수 없습니다.")
        return

    if not pages:
        st.warning("저장된 페이지 데이터가 없습니다.")
        return

    # Issue counts
    high = sum(1 for i in issues if i.get("severity") == "HIGH")
    med = sum(1 for i in issues if i.get("severity") == "MEDIUM")
    low = sum(1 for i in issues if i.get("severity") == "LOW")

    # Summary cards
    avg_load = round(sum(p.get("Load (s)", 0) for p in pages) / len(pages), 2) if pages else 0
    schema_count = sum(1 for p in pages if p.get("Has Schema"))
    https_count = sum(1 for p in pages if p.get("HTTPS"))

    card_data = [
        ("총 페이지", len(pages), "", "전체 수집된 페이지"),
        ("총 이슈", len(issues), "", "발견된 모든 이슈"),
        ("🔴 HIGH", high, "red", "즉시 수정 필요"),
        ("🟡 MEDIUM", med, "yellow", "개선 권장"),
        ("🟢 LOW", low, "green", "참고 수준"),
        ("평균 로딩", f"{avg_load}s", "", ""),
        ("Schema", f"{schema_count}/{len(pages)}", "", ""),
        ("HTTPS", f"{https_count}/{len(pages)}", "", ""),
    ]
    cols = st.columns(len(card_data))
    for col, (label, val, cls, hint) in zip(cols, card_data):
        col.markdown(f"""
        <div class="summary-card {cls}">
            <div class="num">{val}</div>
            <div class="label">{label}</div>
            <div class="hint">{hint}</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("")

    # Clickable severity filter buttons
    fc1, fc2, fc3, fc4 = st.columns(4)
    with fc1:
        if st.button(f"🔴 HIGH ({high}건) 보기", use_container_width=True, key="res_high"):
            st.session_state.card_filter = "HIGH"
    with fc2:
        if st.button(f"🟡 MEDIUM ({med}건) 보기", use_container_width=True, key="res_med"):
            st.session_state.card_filter = "MEDIUM"
    with fc3:
        if st.button(f"🟢 LOW ({low}건) 보기", use_container_width=True, key="res_low"):
            st.session_state.card_filter = "LOW"
    with fc4:
        if st.button("전체 보기", use_container_width=True, key="res_all"):
            st.session_state.card_filter = None

    # Show filtered issues
    if st.session_state.card_filter:
        sev = st.session_state.card_filter
        fi = [i for i in issues if i.get("severity") == sev]
        icons = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}
        st.markdown(f"### {icons[sev]} {sev} 이슈 — {len(fi)}건")

        fi_sorted = sorted(fi, key=lambda x: x.get("type", ""))
        for itype, group in groupby(fi_sorted, key=lambda x: x.get("type", "")):
            gl = list(group)
            with st.expander(f"**{itype}** ({len(gl)}건)", expanded=True):
                for iss in gl[:30]:
                    p_list = iss.get("pages", [])
                    url_path = urlparse(p_list[0]).path if p_list else ""
                    sev_cls = sev.lower()
                    fix_text = iss.get("fix", "")
                    st.markdown(f"""
                    <div class="issue-card {sev_cls}">
                        <div class="issue-header"><span class="badge-{sev_cls}">{sev}</span> <strong>{itype}</strong></div>
                        <div class="issue-detail">{iss.get('detail','')}</div>
                        <div class="issue-url">{url_path}</div>
                        <div class="issue-fix">💡 {fix_text}</div>
                    </div>
                    """, unsafe_allow_html=True)
                if len(gl) > 30:
                    st.caption(f"... 외 {len(gl)-30}건")
        st.markdown("---")

    # Analysis tabs
    (tab_all, tab_td, tab_links, tab_schema, tab_eeat, tab_tech,
     tab_sec, tab_perf, tab_tree, tab_issues, tab_export) = st.tabs([
        "📋 All Pages", "🏷️ Title & Desc", "🔗 Links", "📊 Schema",
        "🛡️ E-E-A-T", "⚙️ Technical", "🔒 Security", "📈 Performance",
        "🌳 Structure", "⚠️ Issues", "💾 Export",
    ])

    # Compute incoming links map
    incoming_map = defaultdict(int)
    for p in pages:
        for link in p.get("_internal_links", []):
            incoming_map[link] += 1

    with tab_all:
        dc = ["URL", "Status", "Title", "Title Len", "Meta Desc", "Desc Len", "H1",
              "Canonical", "Words", "Outlinks", "Schema Types", "HTTPS", "Noindex",
              "HTML KB", "Ext Scripts", "Compression", "Load (s)", "Error"]
        df = pd.DataFrame(pages)
        av = [c for c in dc if c in df.columns]
        if av:
            st.dataframe(df[av], use_container_width=True, hide_index=True, height=500)
        else:
            st.info("데이터 없음")

    with tab_td:
        td = []
        for p in pages:
            tl = p.get("Title Len", 0)
            dl = p.get("Desc Len", 0)
            ts = "❌ 없음" if tl == 0 else (f"⚠️ {tl}자" if tl < TITLE_MIN or tl > TITLE_MAX else f"✅ {tl}자")
            ds = "❌ 없음" if dl == 0 else (f"⚠️ {dl}자" if dl < DESC_MIN or dl > DESC_MAX else f"✅ {dl}자")
            h1_text = p.get("H1", "")
            hs = f"✅ {h1_text[:30]}" if h1_text else "❌ 없음"
            cn = p.get("Canonical", "")
            cs = "❌ 없음" if not cn else ("✅ Self" if cn == p.get("URL") else f"↗️ {cn[:40]}")
            td.append({
                "URL": urlparse(p.get("URL", "")).path or "/",
                "Title 상태": ts,
                "Title": p.get("Title", "")[:60],
                "Desc 상태": ds,
                "Description": p.get("Meta Desc", "")[:60],
                "H1": hs,
                "Canonical": cs,
            })
        if td:
            st.dataframe(pd.DataFrame(td), use_container_width=True, hide_index=True, height=500)
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Title 적정", f"{sum(1 for p in pages if TITLE_MIN <= p.get('Title Len',0) <= TITLE_MAX)}/{len(pages)}")
            c2.metric("Desc 적정", f"{sum(1 for p in pages if DESC_MIN <= p.get('Desc Len',0) <= DESC_MAX)}/{len(pages)}")
            c3.metric("H1 있음", f"{sum(1 for p in pages if p.get('H1 Len',0) > 0)}/{len(pages)}")
            c4.metric("Canonical 있음", f"{sum(1 for p in pages if p.get('Canonical'))}/{len(pages)}")

    with tab_links:
        ld = []
        for p in pages:
            inc = incoming_map.get(p.get("URL", ""), 0)
            cq = p.get("_content", {})
            st_txt = "🚨 고아" if inc == 0 else ("⚠️ 부족" if inc < MIN_INCOMING_LINKS else "✅ 양호")
            ld.append({
                "URL": urlparse(p.get("URL", "")).path or "/",
                "Inlinks": inc,
                "Outlinks": p.get("Outlinks", 0),
                "External": cq.get("external_links_count", 0) if isinstance(cq, dict) else 0,
                "Nofollow": cq.get("nofollow_links_count", 0) if isinstance(cq, dict) else 0,
                "상태": st_txt,
            })
        if ld:
            st.dataframe(pd.DataFrame(ld).sort_values("Inlinks"), use_container_width=True, hide_index=True, height=400)

    with tab_schema:
        sd = []
        for p in pages:
            si = p.get("_schema", {})
            if not isinstance(si, dict):
                si = {}
            sd.append({
                "URL": urlparse(p.get("URL", "")).path or "/",
                "상태": "✅" if si.get("has_schema") else "❌",
                "Types": ", ".join(si.get("all_types", [])) or "-",
                "JSON-LD": len(si.get("json_ld", [])),
                "검증": ", ".join(si.get("validation_issues", [])) or "-",
            })
        if sd:
            st.dataframe(pd.DataFrame(sd), use_container_width=True, hide_index=True, height=400)
            sc_count = sum(1 for p in pages if p.get("Has Schema"))
            c1, c2 = st.columns(2)
            c1.metric("✅ Schema 있음", sc_count)
            c2.metric("❌ Schema 없음", len(pages) - sc_count)

    with tab_eeat:
        ed = []
        for p in pages:
            ee = p.get("_eeat", {})
            if not isinstance(ee, dict):
                ee = {}
            ed.append({
                "URL": urlparse(p.get("URL", "")).path or "/",
                "Author": ee.get("author_name", "")[:30] or "❌",
                "Published": "✅" if ee.get("has_published_date") else "❌",
                "About": "✅" if ee.get("has_about_link") else "❌",
                "Contact": "✅" if ee.get("has_contact_link") else "❌",
                "Privacy": "✅" if ee.get("has_privacy_link") else "❌",
                "Org Schema": "✅" if ee.get("has_org_schema") else "❌",
                "Breadcrumb": "✅" if ee.get("has_breadcrumb") else "❌",
                "Social": ", ".join(ee.get("social_links", [])) or "-",
            })
        if ed:
            st.dataframe(pd.DataFrame(ed), use_container_width=True, hide_index=True, height=400)

    with tab_tech:
        td2 = []
        for p in pages:
            tc = p.get("_tech", {})
            if not isinstance(tc, dict):
                tc = {}
            td2.append({
                "URL": urlparse(p.get("URL", "")).path or "/",
                "Robots": tc.get("meta_robots", "") or "-",
                "Noindex": "⚠️" if tc.get("is_noindex") else "✅",
                "Viewport": "✅" if tc.get("has_viewport") else "❌",
                "Lang": tc.get("lang", "") or "-",
                "OG Image": "✅" if tc.get("og_image") else "❌",
                "Twitter": tc.get("twitter_card", "") or "-",
                "Hreflang": len(tc.get("hreflang_tags", [])),
                "URL Len": tc.get("url_length", 0),
            })
        if td2:
            st.dataframe(pd.DataFrame(td2), use_container_width=True, hide_index=True, height=400)
            st.markdown("#### Heading Hierarchy")
            hd = []
            for p in pages:
                tc = p.get("_tech", {})
                if not isinstance(tc, dict):
                    tc = {}
                hs = tc.get("headings", {})
                if not isinstance(hs, dict):
                    hs = {}
                hd.append({
                    "URL": urlparse(p.get("URL", "")).path or "/",
                    "H1": hs.get("h1", 0),
                    "H2": hs.get("h2", 0),
                    "H3": hs.get("h3", 0),
                    "H4": hs.get("h4", 0),
                    "계층": "✅" if tc.get("heading_hierarchy_ok") else "❌",
                    "이슈": "; ".join(tc.get("heading_issues", [])) or "-",
                })
            if hd:
                st.dataframe(pd.DataFrame(hd), use_container_width=True, hide_index=True, height=300)

    with tab_sec:
        sd2 = []
        for p in pages:
            sc = p.get("_security", {})
            if not isinstance(sc, dict):
                sc = {}
            sd2.append({
                "URL": urlparse(p.get("URL", "")).path or "/",
                "HTTPS": "✅" if sc.get("is_https") else "❌",
                "HSTS": "✅" if sc.get("has_hsts") else "❌",
                "X-CTO": "✅" if sc.get("has_xcto") else "❌",
                "XFO": "✅" if sc.get("has_xfo") else "❌",
                "CSP": "✅" if sc.get("has_csp") else "❌",
                "Mixed": sc.get("mixed_content_count", 0),
            })
        if sd2:
            st.dataframe(pd.DataFrame(sd2), use_container_width=True, hide_index=True, height=400)

    with tab_perf:
        pd2 = []
        for p in pages:
            pf = p.get("_perf", {})
            if not isinstance(pf, dict):
                pf = {}
            pd2.append({
                "URL": urlparse(p.get("URL", "")).path or "/",
                "HTML KB": pf.get("html_size_kb", 0),
                "Scripts": pf.get("external_scripts", 0),
                "CSS": pf.get("external_stylesheets", 0),
                "Images": pf.get("image_count", 0),
                "No Lazy": pf.get("images_no_lazy", 0),
                "Compression": pf.get("compression_type", "") or "❌",
                "Load": p.get("Load (s)", 0),
            })
        if pd2:
            st.dataframe(pd.DataFrame(pd2), use_container_width=True, hide_index=True, height=400)

    with tab_tree:
        base_domain = urlparse(project["url"]).netloc
        urls = [p.get("URL", "") for p in pages if p.get("URL")]
        if urls:
            tree_str = _build_tree_string(urls, base_domain)
            st.code(tree_str, language=None)
        else:
            st.info("URL 데이터가 없습니다.")

    with tab_issues:
        st.subheader(f"Issues — {len(issues)}건")
        fc1, fc2 = st.columns([1, 3])
        with fc1:
            sev_f = st.multiselect("심각도", ["HIGH", "MEDIUM", "LOW"],
                                   default=["HIGH", "MEDIUM", "LOW"], key="iss_sev")
        with fc2:
            to = sorted(set(i.get("type", "") for i in issues))
            type_f = st.multiselect("유형", to, default=to, key="iss_type")
        filtered = [i for i in issues if i.get("severity") in sev_f and i.get("type") in type_f]

        for iss in filtered[:100]:
            sev_cls = iss.get("severity", "MEDIUM").lower()
            p_list = iss.get("pages", [])
            url_path = urlparse(p_list[0]).path if p_list else ""
            fix_text = iss.get("fix", "")
            st.markdown(f"""
            <div class="issue-card {sev_cls}">
                <div class="issue-header"><span class="badge-{sev_cls}">{iss.get('severity','')}</span> <strong>{iss.get('type','')}</strong></div>
                <div class="issue-detail">{iss.get('detail','')}</div>
                <div class="issue-url">{url_path}</div>
                <div class="issue-fix">💡 {fix_text}</div>
            </div>
            """, unsafe_allow_html=True)
        if len(filtered) > 100:
            st.caption(f"... 외 {len(filtered)-100}건")

        # Type summary
        st.markdown("#### 유형별 요약")
        ts2 = defaultdict(lambda: {"HIGH": 0, "MEDIUM": 0, "LOW": 0, "total": 0})
        for i in issues:
            ts2[i.get("type", "")][i.get("severity", "MEDIUM")] += 1
            ts2[i.get("type", "")]["total"] += 1
        sr = [{"유형": t, "🔴": c["HIGH"], "🟡": c["MEDIUM"], "🟢": c["LOW"], "합계": c["total"]}
              for t, c in sorted(ts2.items(), key=lambda x: -x[1]["total"])]
        if sr:
            st.dataframe(pd.DataFrame(sr), use_container_width=True, hide_index=True)

    with tab_export:
        def clean_export(p):
            c = {k: v for k, v in p.items() if not k.startswith("_")}
            si = p.get("_schema", {}) or {}
            c["Schema_Types"] = ",".join(si.get("all_types", []))
            ee = p.get("_eeat", {}) or {}
            c["EEAT_Author"] = ee.get("author_name", "")
            c["EEAT_Date"] = ee.get("has_published_date", False)
            tc = p.get("_tech", {}) or {}
            c["Tech_OG_Image"] = tc.get("og_image", "")
            c["Tech_Twitter"] = tc.get("twitter_card", "")
            sc = p.get("_security", {}) or {}
            c["Sec_HTTPS"] = sc.get("is_https", False)
            c["Sec_HSTS"] = sc.get("has_hsts", False)
            pf = p.get("_perf", {}) or {}
            c["Perf_HTML_KB"] = pf.get("html_size_kb", 0)
            c["Perf_Compression"] = pf.get("compression_type", "")
            return c

        report = {
            "generated_at": datetime.now().isoformat(),
            "base_url": project["url"],
            "total_pages": len(pages),
            "summary": {"high": high, "medium": med, "low": low, "total": len(issues)},
            "pages": [clean_export(p) for p in pages],
            "issues": issues,
        }
        ts = datetime.now().strftime("%Y%m%d_%H%M")
        c1, c2 = st.columns(2)
        with c1:
            st.download_button(
                "📥 JSON",
                json.dumps(report, ensure_ascii=False, indent=2, default=str),
                f"seo_{ts}.json",
                "application/json",
                use_container_width=True,
            )
        with c2:
            csv_data = pd.DataFrame([clean_export(p) for p in pages]).to_csv(index=False)
            st.download_button("📥 CSV", csv_data, f"seo_{ts}.csv", "text/csv", use_container_width=True)
        with st.expander("JSON 미리보기"):
            st.json(report)


# ── 크롤링 히스토리 ──────────────────────────────────────────────────────────
def render_crawl_history(project):
    runs = db.get_crawl_runs(project["id"], limit=20)

    if not runs:
        st.info("아직 크롤링 기록이 없습니다.")
        return

    st.markdown("### 📈 크롤링 히스토리")

    # History table
    history_data = []
    for run in runs:
        history_data.append({
            "ID": run["id"],
            "시작": (run.get("started_at") or "")[:16],
            "완료": (run.get("completed_at") or "")[:16],
            "상태": run.get("status", ""),
            "총 페이지": run.get("total_pages", 0),
            "🔴 HIGH": run.get("high_issues", 0),
            "🟡 MEDIUM": run.get("medium_issues", 0),
            "🟢 LOW": run.get("low_issues", 0),
            "총 이슈": run.get("total_issues", 0),
        })

    st.dataframe(pd.DataFrame(history_data), use_container_width=True, hide_index=True)

    # Comparison between runs
    if len(runs) >= 2:
        st.markdown("### 📊 최근 2회 비교")
        curr = runs[0]
        prev = runs[1]

        def delta_str(current_val, prev_val):
            diff = current_val - prev_val
            if diff > 0:
                return f'<span class="delta-up">▲{diff}</span>'
            elif diff < 0:
                return f'<span class="delta-down">▼{abs(diff)}</span>'
            return "→0"

        st.markdown(f"""
        <div class="history-card">
            📈 이전 대비 변화: 페이지 {delta_str(curr.get('total_pages',0), prev.get('total_pages',0))} ·
            🔴HIGH {delta_str(curr.get('high_issues',0), prev.get('high_issues',0))} ·
            🟡MEDIUM {delta_str(curr.get('medium_issues',0), prev.get('medium_issues',0))} ·
            🟢LOW {delta_str(curr.get('low_issues',0), prev.get('low_issues',0))}
        </div>
        """, unsafe_allow_html=True)


# ── 인사이트 ──────────────────────────────────────────────────────────────────
def render_insights(project):
    # ── 개요 (건강 점수 + 요약 카드) ──
    render_project_overview(project)

    latest = db.get_latest_crawl(project["id"])
    if not latest:
        st.info("인사이트를 생성하려면 먼저 크롤링을 실행하세요.")
        return

    # ── 변경 감지 알림 배너 ──
    try:
        changes = db.get_page_changes(project["id"], crawl_run_id=latest["id"])
        if changes:
            change_types = defaultdict(int)
            for c in changes:
                change_types[FIELD_NAMES_KR.get(c.get("field", c.get("field_name", "")), c.get("field", c.get("field_name", "")))] += 1
            change_summary = " · ".join([f"{k} {v}건" for k, v in sorted(change_types.items(), key=lambda x: -x[1])])
            st.markdown(f"""
            <div style="background:rgba(239,68,68,.08);border:1px solid #f85149;border-radius:8px;padding:14px 20px;margin-bottom:16px;">
                <div style="display:flex;align-items:center;gap:10px;">
                    <span style="font-size:1.4rem;">🔔</span>
                    <div>
                        <div style="color:#ef4444;font-weight:700;font-size:1rem;">페이지 변경 감지 — {len(changes)}건</div>
                        <div style="color:#e2e8f0;font-size:.85rem;margin-top:2px;">{change_summary}</div>
                        <div style="color:#94a3b8;font-size:.78rem;margin-top:4px;">이전 크롤링 대비 변경된 항목입니다. '변경 히스토리' 탭에서 상세 확인하세요.</div>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
    except Exception:
        pass

    st.divider()

    # ── 통합 인사이트 (Search Console + 크롤 데이터) ──
    try:
        sc_conn = db.get_sc_connection(project["id"])
    except Exception:
        sc_conn = None

    if sc_conn:
        st.markdown("### 🔗 통합 인사이트 (Search Console + 크롤 데이터)")

        try:
            end_str = datetime.utcnow().strftime("%Y-%m-%d")
            start_str = (datetime.utcnow() - timedelta(days=28)).strftime("%Y-%m-%d")
            sc_data = db.get_sc_analytics(project["id"], start_str, end_str)

            # Load current crawl issues
            try:
                current_pages = json.loads(latest.get("pages_json", "[]") or "[]")
                current_issues = json.loads(latest.get("issues_json", "[]") or "[]")
            except (json.JSONDecodeError, TypeError):
                current_pages = []
                current_issues = []

            if sc_data and current_pages:
                # Build SC page data map
                sc_page_map = {}
                for r in sc_data:
                    pg = r.get("page", "")
                    if pg not in sc_page_map:
                        sc_page_map[pg] = {"clicks": 0, "impressions": 0, "position_sum": 0, "count": 0}
                    sc_page_map[pg]["clicks"] += r.get("clicks", 0)
                    sc_page_map[pg]["impressions"] += r.get("impressions", 0)
                    sc_page_map[pg]["position_sum"] += r.get("position", 0)
                    sc_page_map[pg]["count"] += 1

                # Build issue page set
                issue_pages = set()
                for iss in current_issues:
                    for p in iss.get("pages", []):
                        issue_pages.add(p)

                crawl_urls = set(p.get("URL", "") for p in current_pages)

                # 1. 트래픽은 높지만 SEO 이슈가 있는 페이지
                high_traffic_issue_pages = []
                for pg, d in sorted(sc_page_map.items(), key=lambda x: -x[1]["clicks"])[:50]:
                    if pg in issue_pages:
                        high_traffic_issue_pages.append(pg)
                if high_traffic_issue_pages:
                    with st.expander(f"🚨 트래픽은 높지만 SEO 이슈가 있는 페이지 ({len(high_traffic_issue_pages)}건)"):
                        for pg in high_traffic_issue_pages[:10]:
                            clicks = sc_page_map[pg]["clicks"]
                            st.markdown(f"""
                            <div class="insight-card urgent">
                                <div class="insight-title">클릭 {clicks:,}회 — SEO 이슈 존재</div>
                                <div class="insight-url">{pg}</div>
                                <div class="insight-detail">트래픽이 높은 페이지에 이슈가 있습니다. 우선적으로 수정하세요.</div>
                            </div>
                            """, unsafe_allow_html=True)

                # 2. 순위 하락 중인 페이지
                try:
                    mid_point = (datetime.utcnow() - timedelta(days=14)).strftime("%Y-%m-%d")
                    first_half = db.get_sc_analytics(project["id"], start_str, mid_point)
                    second_half = db.get_sc_analytics(project["id"], mid_point, end_str)

                    first_pos = {}
                    for r in (first_half or []):
                        pg = r.get("page", "")
                        if pg not in first_pos:
                            first_pos[pg] = {"pos_sum": 0, "cnt": 0}
                        first_pos[pg]["pos_sum"] += r.get("position", 0)
                        first_pos[pg]["cnt"] += 1

                    declining = []
                    second_pos = {}
                    for r in (second_half or []):
                        pg = r.get("page", "")
                        if pg not in second_pos:
                            second_pos[pg] = {"pos_sum": 0, "cnt": 0}
                        second_pos[pg]["pos_sum"] += r.get("position", 0)
                        second_pos[pg]["cnt"] += 1

                    for pg in second_pos:
                        if pg in first_pos:
                            avg1 = first_pos[pg]["pos_sum"] / first_pos[pg]["cnt"]
                            avg2 = second_pos[pg]["pos_sum"] / second_pos[pg]["cnt"]
                            if avg2 > avg1 + 2:  # Position got worse by 2+
                                declining.append({"page": pg, "before": round(avg1, 1), "after": round(avg2, 1)})

                    if declining:
                        declining.sort(key=lambda x: x["after"] - x["before"], reverse=True)
                        with st.expander(f"📉 순위 하락 중인 페이지 ({len(declining)}건)"):
                            for d in declining[:10]:
                                st.markdown(f"""
                                <div class="insight-card urgent">
                                    <div class="insight-title">순위 {d['before']} → {d['after']} (▼{round(d['after']-d['before'],1)})</div>
                                    <div class="insight-url">{d['page']}</div>
                                    <div class="insight-detail">최근 2주간 순위가 하락하고 있습니다. 콘텐츠 업데이트를 검토하세요.</div>
                                </div>
                                """, unsafe_allow_html=True)
                except Exception:
                    pass

                # 3. 노출은 높지만 CTR이 낮은 페이지
                low_ctr_pages = []
                for pg, d in sc_page_map.items():
                    if d["impressions"] >= 100:
                        ctr = (d["clicks"] / d["impressions"] * 100) if d["impressions"] > 0 else 0
                        if ctr < 2:
                            low_ctr_pages.append({"page": pg, "impressions": d["impressions"], "ctr": round(ctr, 2)})
                if low_ctr_pages:
                    low_ctr_pages.sort(key=lambda x: -x["impressions"])
                    with st.expander(f"👁️ 노출은 높지만 CTR이 낮은 페이지 ({len(low_ctr_pages)}건)"):
                        for lc in low_ctr_pages[:10]:
                            st.markdown(f"""
                            <div class="insight-card new-issue">
                                <div class="insight-title">노출 {lc['impressions']:,}회 · CTR {lc['ctr']}%</div>
                                <div class="insight-url">{lc['page']}</div>
                                <div class="insight-detail">Title과 Meta Description을 개선하여 CTR을 높이세요.</div>
                            </div>
                            """, unsafe_allow_html=True)

                # 4. 인덱싱되지 않은 페이지
                sc_pages_set = set(sc_page_map.keys())
                not_indexed = [u for u in crawl_urls if u and u not in sc_pages_set]
                if not_indexed:
                    with st.expander(f"🔍 인덱싱되지 않은 페이지 ({len(not_indexed)}건)"):
                        for ni_url in not_indexed[:15]:
                            st.markdown(f"""
                            <div class="insight-card new-issue">
                                <div class="insight-title">Search Console에 데이터 없음</div>
                                <div class="insight-url">{ni_url}</div>
                                <div class="insight-detail">이 페이지는 크롤링에서 발견되었지만 Search Console에 트래픽 데이터가 없습니다. 인덱싱 상태를 확인하세요.</div>
                            </div>
                            """, unsafe_allow_html=True)

                # 5. PageSpeed 점수가 낮은 고트래픽 페이지
                try:
                    ps_results = db.get_pagespeed_data(project["id"])
                    if ps_results:
                        ps_score_map = {}
                        for ps in ps_results:
                            url = ps.get("url", "")
                            score = ps.get("score", 0)
                            ps_score_map[url] = int(score * 100) if score <= 1 else int(score)

                        slow_high_traffic = []
                        for pg, d in sorted(sc_page_map.items(), key=lambda x: -x[1]["clicks"])[:30]:
                            if pg in ps_score_map and ps_score_map[pg] < 50:
                                slow_high_traffic.append({"page": pg, "clicks": d["clicks"], "score": ps_score_map[pg]})

                        if slow_high_traffic:
                            with st.expander(f"🐢 PageSpeed 점수가 낮은 고트래픽 페이지 ({len(slow_high_traffic)}건)"):
                                for sl in slow_high_traffic:
                                    st.markdown(f"""
                                    <div class="insight-card urgent">
                                        <div class="insight-title">클릭 {sl['clicks']:,}회 · 속도 점수 {sl['score']}</div>
                                        <div class="insight-url">{sl['page']}</div>
                                        <div class="insight-detail">트래픽이 높은 페이지의 로딩 속도가 느립니다. 사용자 경험과 SEO에 부정적 영향을 줄 수 있습니다.</div>
                                    </div>
                                    """, unsafe_allow_html=True)
                except Exception:
                    pass

                # ── 6. 키워드 잠식(Cannibalization) 탐지 ──
                try:
                    query_page_map = {}
                    for r in sc_data:
                        q = r.get("query", "")
                        pg = r.get("page", "")
                        if q and pg:
                            query_page_map.setdefault(q, {})
                            if pg not in query_page_map[q]:
                                query_page_map[q][pg] = {"clicks": 0, "impressions": 0, "position_sum": 0, "count": 0}
                            query_page_map[q][pg]["clicks"] += r.get("clicks", 0)
                            query_page_map[q][pg]["impressions"] += r.get("impressions", 0)
                            query_page_map[q][pg]["position_sum"] += r.get("position", 0)
                            query_page_map[q][pg]["count"] += 1

                    cannibalized = []
                    for query, pages_map in query_page_map.items():
                        if len(pages_map) >= 2:
                            total_imp = sum(d["impressions"] for d in pages_map.values())
                            if total_imp >= 20:
                                sorted_pages = sorted(pages_map.items(), key=lambda x: -x[1]["clicks"])
                                cannibalized.append({
                                    "query": query,
                                    "pages": [{
                                        "url": pg,
                                        "clicks": d["clicks"],
                                        "impressions": d["impressions"],
                                        "avg_pos": round(d["position_sum"] / d["count"], 1) if d["count"] else 0
                                    } for pg, d in sorted_pages],
                                })
                    cannibalized.sort(key=lambda x: -sum(p["impressions"] for p in x["pages"]))

                    if cannibalized:
                        with st.expander(f"🔀 키워드 잠식 (Cannibalization) — {len(cannibalized)}개 쿼리", expanded=False):
                            st.markdown("""
                            <div style="background:#0f172a;border:1px solid #1e293b;border-radius:8px;padding:12px;margin-bottom:12px;font-size:.85rem;color:#94a3b8;">
                                💡 동일 키워드에 여러 페이지가 경쟁하면 순위가 분산됩니다. 하나의 대표 페이지를 정하고 나머지에서 301 리다이렉트 또는 canonical 태그를 설정하세요.
                            </div>
                            """, unsafe_allow_html=True)
                            for cann in cannibalized[:15]:
                                st.markdown(f"**🔑 \"{cann['query']}\"** — {len(cann['pages'])}개 페이지 경쟁")
                                for pg in cann["pages"]:
                                    pos_color = "#22c55e" if pg["avg_pos"] <= 10 else "#f59e0b" if pg["avg_pos"] <= 20 else "#ef4444"
                                    st.markdown(f"""
                                    <div style="background:#111827;border:1px solid #1e293b;border-radius:6px;padding:8px 12px;margin:4px 0;font-size:.82rem;">
                                        <span style="color:{pos_color};font-weight:700;">#{pg['avg_pos']}</span>
                                        <span style="color:#94a3b8;margin:0 8px;">클릭 {pg['clicks']:,} · 노출 {pg['impressions']:,}</span>
                                        <span style="color:#818cf8;">{pg['url'][:80]}</span>
                                    </div>
                                    """, unsafe_allow_html=True)
                                st.markdown("")
                except Exception:
                    pass

                # ── 7. Low-Hanging Fruit (쉬운 승리 키워드) ──
                try:
                    low_hanging = []
                    query_agg = {}
                    for r in sc_data:
                        q = r.get("query", "")
                        if q:
                            if q not in query_agg:
                                query_agg[q] = {"clicks": 0, "impressions": 0, "position_sum": 0, "count": 0, "page": ""}
                            query_agg[q]["clicks"] += r.get("clicks", 0)
                            query_agg[q]["impressions"] += r.get("impressions", 0)
                            query_agg[q]["position_sum"] += r.get("position", 0)
                            query_agg[q]["count"] += 1
                            if not query_agg[q]["page"]:
                                query_agg[q]["page"] = r.get("page", "")

                    for q, d in query_agg.items():
                        avg_pos = d["position_sum"] / d["count"] if d["count"] else 0
                        ctr = (d["clicks"] / d["impressions"] * 100) if d["impressions"] > 0 else 0
                        # 순위 11~20위 + 노출 50회 이상
                        if 11 <= avg_pos <= 25 and d["impressions"] >= 50:
                            low_hanging.append({
                                "query": q, "avg_pos": round(avg_pos, 1),
                                "clicks": d["clicks"], "impressions": d["impressions"],
                                "ctr": round(ctr, 2), "page": d["page"],
                            })
                        # 또는 노출 높은데 CTR 낮은 (순위 1~10위)
                        elif avg_pos <= 10 and d["impressions"] >= 100 and ctr < 3:
                            low_hanging.append({
                                "query": q, "avg_pos": round(avg_pos, 1),
                                "clicks": d["clicks"], "impressions": d["impressions"],
                                "ctr": round(ctr, 2), "page": d["page"],
                                "type": "low_ctr",
                            })

                    low_hanging.sort(key=lambda x: -x["impressions"])

                    if low_hanging:
                        with st.expander(f"🎯 Low-Hanging Fruit — {len(low_hanging)}개 기회 키워드", expanded=False):
                            st.markdown("""
                            <div style="background:#0f172a;border:1px solid #1e293b;border-radius:8px;padding:12px;margin-bottom:12px;font-size:.85rem;color:#94a3b8;">
                                💡 조금만 최적화하면 순위와 클릭을 크게 올릴 수 있는 키워드입니다. Title/Description 개선, 콘텐츠 보강으로 빠른 성과를 얻으세요.
                            </div>
                            """, unsafe_allow_html=True)
                            for lh in low_hanging[:20]:
                                is_low_ctr = lh.get("type") == "low_ctr"
                                badge = '<span style="background:rgba(245,158,11,.1);color:#f59e0b;padding:2px 6px;border-radius:4px;font-size:.72rem;">CTR 개선</span>' if is_low_ctr else '<span style="background:rgba(99,102,241,.12);color:#818cf8;padding:2px 6px;border-radius:4px;font-size:.72rem;">순위 상승 가능</span>'
                                st.markdown(f"""
                                <div class="insight-card new-issue">
                                    <div class="insight-title">{badge} {lh['query']}</div>
                                    <div class="insight-detail">순위 #{lh['avg_pos']} · 노출 {lh['impressions']:,} · 클릭 {lh['clicks']:,} · CTR {lh['ctr']}%</div>
                                    <div class="insight-url">{lh['page'][:80] if lh['page'] else ''}</div>
                                </div>
                                """, unsafe_allow_html=True)
                except Exception:
                    pass

            else:
                st.info("Search Console 데이터를 동기화한 후 통합 인사이트를 확인하세요.")
        except Exception as e:
            st.warning(f"통합 인사이트 생성 중 오류: {e}")

        st.divider()

    insights = db.get_insights(project["id"], crawl_run_id=latest["id"])

    if not insights:
        st.info("아직 인사이트가 없습니다. 두 번째 크롤링부터 비교 인사이트가 생성됩니다.")

        # Still show current issues as urgent insights from the crawl data
        try:
            current_issues = json.loads(latest.get("issues_json", "[]") or "[]")
        except (json.JSONDecodeError, TypeError):
            current_issues = []

        if current_issues:
            st.markdown("### 현재 크롤링 기반 주요 이슈")
            high_issues = [i for i in current_issues if i.get("severity") == "HIGH"]
            if high_issues:
                for iss in high_issues[:10]:
                    p_list = iss.get("pages", [])
                    url_path = urlparse(p_list[0]).path if p_list else ""
                    st.markdown(f"""
                    <div class="insight-card urgent">
                        <div class="insight-title">🚨 {iss.get('type', '')}: {iss.get('detail', '')}</div>
                        <div class="insight-url">{url_path}</div>
                    </div>
                    """, unsafe_allow_html=True)
        return

    # Filter out excluded URLs
    try:
        excluded_urls = db.get_excluded_urls(project["id"])
        excluded_patterns = [ex.get("url_pattern", "") for ex in excluded_urls if ex.get("url_pattern")]
    except Exception:
        excluded_patterns = []

    def _is_excluded(url):
        for pat in excluded_patterns:
            if pat and (pat == url or pat in url or url.startswith(pat)):
                return True
        return False

    if excluded_patterns:
        insights = [i for i in insights if not _is_excluded(i.get("url", ""))]

    # Classify insights
    content_urgent = [i for i in insights if i["category"] == "content" and i["insight_type"] == "urgent"]
    content_improved = [i for i in insights if i["category"] == "content" and i["insight_type"] in ("improved", "resolved")]
    content_new_pages = [i for i in insights if i["category"] == "content" and i["insight_type"] == "new_page"]
    content_new_issues = [i for i in insights if i["category"] == "content" and i["insight_type"] == "new_issue"]

    tech_urgent = [i for i in insights if i["category"] == "technical" and i["insight_type"] == "urgent"]
    tech_improved = [i for i in insights if i["category"] == "technical" and i["insight_type"] in ("improved", "resolved")]
    tech_new_issues = [i for i in insights if i["category"] == "technical" and i["insight_type"] == "new_issue"]
    tech_new_pages = [i for i in insights if i["category"] == "technical" and i["insight_type"] == "new_page"]
    tech_lost_pages = [i for i in insights if i["category"] == "technical" and i["insight_type"] == "lost_page"]

    st.markdown(f"### 💡 인사이트 대시보드")
    st.caption(f"크롤링 #{latest['id']} 기준 · {(latest.get('completed_at') or '')[:16]}")

    # 요약 카드
    sum_cols = st.columns(4)
    sum_cols[0].metric("🚨 콘텐츠 긴급", f"{len(content_urgent)}건")
    sum_cols[1].metric("🚨 테크니컬 긴급", f"{len(tech_urgent)}건")
    sum_cols[2].metric("✅ 개선된 항목", f"{len(content_improved) + len(tech_improved)}건")
    sum_cols[3].metric("🆕 새 이슈", f"{len(content_new_issues) + len(tech_new_issues)}건")

    col_content, col_tech = st.columns(2)

    def _render_insight_list(items, max_items=15):
        for ins in items[:max_items]:
            severity = ins.get("severity", "MEDIUM").lower()
            st.markdown(f"""
            <div class="insight-card {ins.get('insight_type', '')}">
                <div class="insight-title"><span class="badge-{severity}">{ins.get('severity','')}</span> {ins['title']}</div>
                <div class="insight-detail">{ins.get('detail', '')}</div>
                <div class="insight-url">{ins.get('url', '')}</div>
            </div>
            """, unsafe_allow_html=True)

    # ── Left: 콘텐츠 사이드 ──
    with col_content:
        st.markdown("## 📝 콘텐츠 사이드")

        with st.expander(f"🚨 즉시 해야 할 것 ({len(content_urgent)}건)", expanded=False):
            if content_urgent:
                _render_insight_list(content_urgent)
            else:
                st.success("긴급한 콘텐츠 이슈가 없습니다!")

        with st.expander(f"✅ 좋아진 점 ({len(content_improved)}건)"):
            if content_improved:
                _render_insight_list(content_improved)
            else:
                st.caption("이전 대비 개선된 콘텐츠가 없습니다.")

        all_new_pages = content_new_pages + [i for i in tech_new_pages]
        with st.expander(f"🆕 새로 발견된 페이지 ({len(all_new_pages)}건)"):
            if all_new_pages:
                for ins in all_new_pages[:20]:
                    st.markdown(f"""
                    <div class="insight-card new-page">
                        <div class="insight-title">{ins['title']}</div>
                        <div class="insight-url">{ins.get('url', '')}</div>
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.caption("새로 발견된 페이지가 없습니다.")

        # ── Search Console 연동 미리보기 ──
        try:
            sc_conn = db.get_sc_connection(project["id"])
        except Exception:
            sc_conn = None

        if sc_conn:
            with st.expander("🔍 Search Console 데이터", expanded=False):
                try:
                    end_str = datetime.utcnow().strftime("%Y-%m-%d")
                    start_str = (datetime.utcnow() - timedelta(days=28)).strftime("%Y-%m-%d")
                    sc_data = db.get_sc_analytics(project["id"], start_str, end_str)
                    if sc_data:
                        total_clicks = sum(r.get("clicks", 0) for r in sc_data)
                        total_impressions = sum(r.get("impressions", 0) for r in sc_data)
                        avg_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
                        positions = [r.get("position", 0) for r in sc_data if r.get("position")]
                        avg_position = round(sum(positions) / len(positions), 1) if positions else 0

                        mc = st.columns(4)
                        mc[0].metric("클릭수", f"{total_clicks:,}")
                        mc[1].metric("노출수", f"{total_impressions:,}")
                        mc[2].metric("평균 CTR", f"{avg_ctr:.1f}%")
                        mc[3].metric("평균 순위", f"{avg_position}")

                        # 상위 키워드
                        kw_map = {}
                        for r in sc_data:
                            q = r.get("query", "")
                            if q:
                                if q not in kw_map:
                                    kw_map[q] = {"clicks": 0, "impressions": 0}
                                kw_map[q]["clicks"] += r.get("clicks", 0)
                                kw_map[q]["impressions"] += r.get("impressions", 0)
                        top_kws = sorted(kw_map.items(), key=lambda x: -x[1]["clicks"])[:5]
                        if top_kws:
                            st.markdown("**상위 키워드**")
                            for q, d in top_kws:
                                st.markdown(f"- **{q}** — 클릭 {d['clicks']:,} · 노출 {d['impressions']:,}")
                    else:
                        st.info("Search Console 데이터를 동기화하면 여기에 실제 데이터가 표시됩니다.")
                except Exception:
                    st.info("Search Console 데이터를 불러오는 중 오류가 발생했습니다.")
        else:
            with st.expander("🔍 Search Console 미리보기 (연동 시 제공)", expanded=False):
                st.markdown("""
                <div style="opacity:0.6;">
                <p style="color:#e2e8f0;font-weight:600;margin-bottom:12px;">Search Console을 연동하면 다음 데이터를 확인할 수 있습니다:</p>
                </div>
                """, unsafe_allow_html=True)
                demo_cols = st.columns(4)
                demo_cols[0].metric("클릭수", "1,247", help="예시 데이터")
                demo_cols[1].metric("노출수", "45,832", help="예시 데이터")
                demo_cols[2].metric("평균 CTR", "2.7%", help="예시 데이터")
                demo_cols[3].metric("평균 순위", "18.3", help="예시 데이터")
                st.markdown("""
                <div style="opacity:0.6;">
                <p style="font-size:0.85rem;margin-top:8px;"><strong>상위 키워드 (예시)</strong></p>
                <ul style="font-size:0.82rem;color:#94a3b8;">
                    <li><strong>SEO 최적화 방법</strong> — 클릭 312 · 노출 8,420</li>
                    <li><strong>메타 태그 작성법</strong> — 클릭 198 · 노출 5,130</li>
                    <li><strong>사이트맵 제출</strong> — 클릭 145 · 노출 3,890</li>
                    <li><strong>구조화 데이터 적용</strong> — 클릭 89 · 노출 2,760</li>
                    <li><strong>페이지 속도 개선</strong> — 클릭 76 · 노출 2,100</li>
                </ul>
                <p style="font-size:0.82rem;color:#f59e0b;margin-top:12px;">💡 '서치콘솔' 탭에서 Google Search Console을 연동하세요.</p>
                </div>
                """, unsafe_allow_html=True)

    # ── Right: 테크니컬 사이드 ──
    with col_tech:
        st.markdown("## ⚙️ 테크니컬 사이드")

        with st.expander(f"🚨 즉시 해야 할 것 ({len(tech_urgent)}건)", expanded=False):
            if tech_urgent:
                _render_insight_list(tech_urgent)
            else:
                st.success("긴급한 테크니컬 이슈가 없습니다!")

        with st.expander(f"✅ 좋아진 점 ({len(tech_improved)}건)"):
            if tech_improved:
                _render_insight_list(tech_improved)
            else:
                st.caption("이전 대비 개선된 테크니컬 항목이 없습니다.")

        with st.expander(f"⚠️ 새로 발견된 이슈 ({len(tech_new_issues)}건)"):
            if tech_new_issues:
                _render_insight_list(tech_new_issues)
            else:
                st.caption("새로 발견된 이슈가 없습니다.")

        if tech_lost_pages:
            with st.expander(f"🚫 사라진 페이지 ({len(tech_lost_pages)}건)"):
                _render_insight_list(tech_lost_pages)

        # ── PageSpeed 미리보기 ──
        try:
            ps_results = db.get_pagespeed_data(project["id"])
        except Exception:
            ps_results = None

        if ps_results:
            with st.expander("⚡ PageSpeed 페이지별 성능", expanded=False):
                for ps in ps_results[:10]:
                    url = ps.get("url", "")
                    score = ps.get("score", 0)
                    score_pct = int(score * 100) if score <= 1 else int(score)
                    lcp = ps.get("lcp", 0)
                    cls_val = ps.get("cls", 0)
                    fid = ps.get("fid", ps.get("inp", 0))
                    s_color = "#22c55e" if score_pct >= 90 else "#f59e0b" if score_pct >= 50 else "#ef4444"
                    st.markdown(f"""
                    <div style="background:#0f172a;border:1px solid #1e293b;border-radius:8px;padding:12px;margin-bottom:8px;">
                        <div style="display:flex;justify-content:space-between;align-items:center;">
                            <span style="color:#e2e8f0;font-size:0.85rem;max-width:60%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="{url}">{url}</span>
                            <span style="color:{s_color};font-weight:700;font-size:1.1rem;">{score_pct}</span>
                        </div>
                        <div style="display:flex;gap:16px;margin-top:6px;font-size:0.78rem;color:#94a3b8;">
                            <span>LCP: {lcp:.1f}s</span>
                            <span>CLS: {cls_val:.3f}</span>
                            <span>FID/INP: {fid}ms</span>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                if len(ps_results) > 10:
                    st.caption(f"외 {len(ps_results) - 10}개 페이지 — '사이트 속도' 탭에서 전체 확인")
        else:
            with st.expander("⚡ PageSpeed 미리보기 (측정 시 제공)", expanded=False):
                example_pages = [
                    {"url": f"{project['url']}/", "score": 78, "lcp": "2.1s", "cls": "0.05", "fid": "120ms"},
                    {"url": f"{project['url']}/about", "score": 85, "lcp": "1.8s", "cls": "0.02", "fid": "90ms"},
                    {"url": f"{project['url']}/blog", "score": 62, "lcp": "3.2s", "cls": "0.15", "fid": "210ms"},
                    {"url": f"{project['url']}/contact", "score": 91, "lcp": "1.4s", "cls": "0.01", "fid": "65ms"},
                    {"url": f"{project['url']}/products", "score": 45, "lcp": "4.5s", "cls": "0.25", "fid": "380ms"},
                ]
                st.markdown('<div style="opacity:0.6;">', unsafe_allow_html=True)
                for pg in example_pages:
                    s_color = "#22c55e" if pg["score"] >= 90 else "#f59e0b" if pg["score"] >= 50 else "#ef4444"
                    st.markdown(f"""
                    <div style="background:#0f172a;border:1px solid #1e293b;border-radius:8px;padding:12px;margin-bottom:8px;">
                        <div style="display:flex;justify-content:space-between;align-items:center;">
                            <span style="color:#e2e8f0;font-size:0.85rem;">{pg["url"]}</span>
                            <span style="color:{s_color};font-weight:700;font-size:1.1rem;">{pg["score"]}</span>
                        </div>
                        <div style="display:flex;gap:16px;margin-top:6px;font-size:0.78rem;color:#94a3b8;">
                            <span>LCP: {pg["lcp"]}</span>
                            <span>CLS: {pg["cls"]}</span>
                            <span>FID: {pg["fid"]}</span>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                st.markdown('</div>', unsafe_allow_html=True)
                st.markdown("""
                <p style="font-size:0.82rem;color:#f59e0b;margin-top:8px;">💡 '사이트 속도' 탭에서 PageSpeed 측정을 실행하세요.</p>
                """, unsafe_allow_html=True)


# ── AI/GEO — AI 검색 최적화 ─────────────────────────────────────────────────
def render_ai_geo(project):
    st.markdown("### 🤖 AI 검색 최적화 (AI/GEO)")
    st.caption("AI 검색엔진(ChatGPT, Perplexity, Gemini, Claude)에서 인용되고 노출될 준비가 되어 있는지 진단합니다.")

    project_url = project.get("url", "").rstrip("/")
    project_id = project["id"]

    # ═══════════════════════════════════════════════════════════════════════
    # 섹션 1: AI 봇 접근성 매트릭스
    # ═══════════════════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("#### 🛡️ AI 봇 접근성 매트릭스")
    st.caption("robots.txt에서 각 AI 봇의 접근 허용/차단 상태를 분석합니다.")

    if st.button("🔍 AI 봇 접근성 분석", key="ai_bot_scan"):
        with st.spinner("robots.txt 분석 중..."):
            bot_result = crawler.analyze_ai_bot_access(project_url)

        if bot_result.get("robots_exists"):
            bots = bot_result["bots"]

            # 요약
            allowed = sum(1 for b in bots.values() if b["status"] == "허용")
            blocked = sum(1 for b in bots.values() if b["status"] == "차단")
            sc1, sc2, sc3 = st.columns(3)
            sc1.metric("총 AI 봇", f"{len(bots)}개")
            sc2.metric("허용", f"{allowed}개", delta=None)
            sc3.metric("차단", f"{blocked}개", delta=None)

            # 티어별 분류
            for tier, tier_label, tier_desc in [
                ("학습", "🎓 학습용 (Training)", "AI 모델 학습 데이터 수집 — 차단해도 검색 노출에 영향 없음"),
                ("검색", "🔍 검색용 (Search/Citation)", "AI 검색 결과에 노출 — 차단하면 AI 검색에서 제외됨"),
                ("사용자", "👤 사용자용 (User-Initiated)", "사용자가 대화 중 링크 조회 — 차단해도 큰 영향 없음"),
            ]:
                tier_bots = {k: v for k, v in bots.items() if v["tier"] == tier}
                if not tier_bots:
                    continue

                with st.expander(f"{tier_label} ({len(tier_bots)}개)", expanded=True):
                    st.caption(tier_desc)
                    for bot_name, info in tier_bots.items():
                        status = info["status"]
                        if status == "차단":
                            icon = "🔴"
                            color = "#ef4444"
                        elif status == "허용":
                            icon = "🟢"
                            color = "#22c55e"
                        else:
                            icon = "⚪"
                            color = "#8b949e"

                        st.markdown(f"""
                        <div style="display:flex;align-items:center;gap:12px;padding:8px 12px;background:#111827;border:1px solid #1e293b;border-radius:6px;margin-bottom:4px;">
                            <span style="font-size:1.1rem;">{icon}</span>
                            <div style="flex:1;">
                                <span style="color:#e2e8f0;font-weight:600;">{bot_name}</span>
                                <span style="color:#94a3b8;font-size:.82rem;margin-left:8px;">{info['company']}</span>
                            </div>
                            <span style="color:{color};font-weight:600;font-size:.85rem;">{status}</span>
                        </div>
                        """, unsafe_allow_html=True)

            # 권장사항
            search_blocked = [k for k, v in bots.items() if v["tier"] == "검색" and v["status"] == "차단"]
            if search_blocked:
                st.warning(f"⚠️ 검색용 AI 봇 {len(search_blocked)}개가 차단되어 있습니다: {', '.join(search_blocked)}. AI 검색(ChatGPT, Perplexity 등)에서 노출되지 않을 수 있습니다.")
            else:
                st.success("✅ 모든 검색용 AI 봇이 접근을 허용하고 있습니다. AI 검색 노출에 문제 없습니다.")
        else:
            st.info("robots.txt 파일이 없거나 접근할 수 없습니다. 기본적으로 모든 AI 봇의 접근이 허용됩니다.")

    # ═══════════════════════════════════════════════════════════════════════
    # 섹션 2: llms.txt 체크
    # ═══════════════════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("#### 📄 llms.txt 체크")
    st.caption("llms.txt는 AI 모델이 사이트 구조를 이해하도록 돕는 새로운 표준입니다.")

    if st.button("🔍 llms.txt 확인", key="llms_txt_check"):
        with st.spinner("llms.txt 확인 중..."):
            llms_result = crawler.check_llms_txt(project_url)

        if llms_result["exists"]:
            st.success(f"✅ llms.txt 발견! 점수: {llms_result['score']}/100")

            checks = [
                ("H1 사이트명", llms_result["has_h1"]),
                ("Blockquote 설명", llms_result["has_blockquote"]),
                ("섹션 (## 카테고리)", llms_result["has_sections"]),
                ("콘텐츠 링크", llms_result["has_links"]),
            ]
            for name, passed in checks:
                icon = "✅" if passed else "❌"
                st.markdown(f"- {icon} {name}")

            if llms_result["issues"]:
                st.markdown("**개선 사항:**")
                for issue in llms_result["issues"]:
                    st.markdown(f"- ⚠️ {issue}")

            with st.expander("llms.txt 내용 보기"):
                st.code(llms_result["content"][:3000], language="markdown")
        else:
            st.warning("❌ llms.txt 파일이 없습니다.")
            st.markdown("""
            <div style="background:#0f172a;border:1px solid #1e293b;border-radius:8px;padding:16px;margin-top:8px;">
                <p style="color:#e2e8f0;font-weight:600;margin-bottom:8px;">llms.txt 만들기 가이드</p>
                <p style="color:#94a3b8;font-size:.85rem;margin-bottom:12px;">사이트 루트에 <code>/llms.txt</code> 파일을 만들면 AI가 사이트를 더 잘 이해합니다.</p>
                <pre style="background:#111827;border:1px solid #1e293b;border-radius:6px;padding:12px;font-size:.82rem;color:#e2e8f0;overflow-x:auto;"># 사이트 이름

> 사이트에 대한 간단한 설명

## 주요 콘텐츠
- [페이지 제목](URL): 설명
- [페이지 제목](URL): 설명

## 카테고리
- [카테고리명](URL): 설명</pre>
                <p style="color:#f59e0b;font-size:.82rem;margin-top:8px;">💡 <a href="https://llmstxt.org/" target="_blank" style="color:#818cf8;">llms.txt 공식 사양</a> 참고</p>
            </div>
            """, unsafe_allow_html=True)

    # ═══════════════════════════════════════════════════════════════════════
    # 섹션 3: E-E-A-T 종합 스코어 + AI 준비도 (크롤 데이터 기반)
    # ═══════════════════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("#### 📊 페이지별 E-E-A-T 점수 & AI 준비도")

    latest = db.get_latest_crawl(project_id)
    pages = []
    if not latest:
        st.info("크롤링 데이터가 없습니다. '설정' 탭에서 먼저 크롤링을 실행하세요.")
    else:
        try:
            pages_raw = latest.get("pages_json", "[]")
            pages = json.loads(pages_raw) if pages_raw else []
        except (json.JSONDecodeError, TypeError):
            pages = []

        if not pages:
            st.info("크롤링된 페이지 데이터가 없습니다.")
        else:
            # 사이트 전체 E-E-A-T + AI 준비도 평균 계산
            eeat_scores = []
            ai_scores = []

            for p in pages:
                try:
                    if p.get("Status") != 200:
                        continue
                    eeat_info = p.get("_eeat") or {}
                    if not isinstance(eeat_info, dict):
                        eeat_info = {}
                    schema_info = p.get("_schema") or {}
                    if not isinstance(schema_info, dict):
                        schema_info = {}
                    security_info = p.get("_security") or {}
                    if not isinstance(security_info, dict):
                        security_info = {}
                    content_info = p.get("_content") or {}
                    if not isinstance(content_info, dict):
                        content_info = {}
                    tech_info = p.get("_tech") or {}
                    if not isinstance(tech_info, dict):
                        tech_info = {}

                    eeat_result = crawler.calculate_eeat_score(eeat_info, schema_info, security_info, content_info)
                    eeat_scores.append({"url": p.get("URL", ""), **eeat_result})

                    ai_est = _estimate_ai_readiness(p)
                    ai_scores.append({"url": p.get("URL", ""), **ai_est})
                except Exception:
                    continue

            if eeat_scores:
                avg_eeat = round(sum(e["score"] for e in eeat_scores) / len(eeat_scores))
                avg_ai = round(sum(a["score"] for a in ai_scores) / len(ai_scores))

                # 종합 점수 카드
                sc1, sc2 = st.columns(2)
                with sc1:
                    eeat_color = "#22c55e" if avg_eeat >= 60 else "#f59e0b" if avg_eeat >= 40 else "#ef4444"
                    st.markdown(f"""
                    <div style="text-align:center;background:#0f172a;border:1px solid #1e293b;border-radius:12px;padding:24px;">
                        <div style="font-size:.85rem;color:#94a3b8;margin-bottom:8px;">사이트 E-E-A-T 점수</div>
                        <div class="score-circle {'score-good' if avg_eeat >= 60 else 'score-medium' if avg_eeat >= 40 else 'score-bad'}" style="width:100px;height:100px;font-size:1.8rem;margin:0 auto;">{avg_eeat}</div>
                        <div style="color:{eeat_color};font-weight:600;margin-top:8px;">{'우수' if avg_eeat >= 60 else '보통' if avg_eeat >= 40 else '개선 필요'}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with sc2:
                    ai_color = "#22c55e" if avg_ai >= 70 else "#f59e0b" if avg_ai >= 50 else "#ef4444"
                    st.markdown(f"""
                    <div style="text-align:center;background:#0f172a;border:1px solid #1e293b;border-radius:12px;padding:24px;">
                        <div style="font-size:.85rem;color:#94a3b8;margin-bottom:8px;">AI 검색 준비도</div>
                        <div class="score-circle {'score-good' if avg_ai >= 70 else 'score-medium' if avg_ai >= 50 else 'score-bad'}" style="width:100px;height:100px;font-size:1.8rem;margin:0 auto;">{avg_ai}</div>
                        <div style="color:{ai_color};font-weight:600;margin-top:8px;">{'AI-Ready' if avg_ai >= 70 else '개선 필요' if avg_ai >= 50 else '최적화 필요'}</div>
                    </div>
                    """, unsafe_allow_html=True)

                # 페이지별 상세 테이블
                st.markdown("")
                with st.expander(f"📋 페이지별 E-E-A-T 상세 ({len(eeat_scores)}개 페이지)", expanded=False):
                    # Sort by score ascending (worst first)
                    eeat_sorted = sorted(eeat_scores, key=lambda x: x["score"])
                    for es in eeat_sorted[:30]:
                        grade_color = es.get("color", "#8b949e")
                        url_short = urlparse(es["url"]).path or "/"
                        st.markdown(f"""
                        <div style="display:flex;align-items:center;gap:12px;padding:8px 12px;background:#111827;border:1px solid #1e293b;border-radius:6px;margin-bottom:4px;">
                            <span style="color:{grade_color};font-weight:800;font-size:1.1rem;min-width:30px;">{es['grade']}</span>
                            <span style="color:{grade_color};font-weight:700;min-width:40px;">{es['score']}</span>
                            <span style="color:#818cf8;font-size:.85rem;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="{es['url']}">{url_short}</span>
                        </div>
                        """, unsafe_allow_html=True)

                    # 가장 점수 낮은 페이지의 상세 진단
                    if eeat_sorted:
                        worst = eeat_sorted[0]
                        st.markdown(f"**가장 개선이 필요한 페이지:** `{urlparse(worst['url']).path}`")
                        for dim_name, dim_data in worst.get("details", {}).items():
                            st.markdown(f"**{dim_name}** ({dim_data['score']}/25)")
                            for item in dim_data.get("items", []):
                                st.markdown(f"  - {item}")

                with st.expander(f"🤖 페이지별 AI 준비도 상세 ({len(ai_scores)}개 페이지)", expanded=False):
                    ai_sorted = sorted(ai_scores, key=lambda x: x["score"])
                    for ais in ai_sorted[:30]:
                        url_short = urlparse(ais["url"]).path or "/"
                        sc_val = ais["score"]
                        sc_color = "#22c55e" if sc_val >= 70 else "#f59e0b" if sc_val >= 50 else "#ef4444"
                        st.markdown(f"""
                        <div style="display:flex;align-items:center;gap:12px;padding:8px 12px;background:#111827;border:1px solid #1e293b;border-radius:6px;margin-bottom:4px;">
                            <span style="color:{sc_color};font-weight:700;min-width:40px;">{sc_val}</span>
                            <span style="color:#818cf8;font-size:.85rem;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="{ais['url']}">{url_short}</span>
                            <span style="color:#94a3b8;font-size:.78rem;">{ais.get('grade_kr', '')}</span>
                        </div>
                        """, unsafe_allow_html=True)

                    # 가장 점수 낮은 페이지의 상세 진단
                    if ai_sorted:
                        worst_ai = ai_sorted[0]
                        st.markdown(f"**가장 개선이 필요한 페이지:** `{urlparse(worst_ai['url']).path}`")
                        ai_dims = worst_ai.get("details", {})
                        if ai_dims:
                            for dim_name, dim_data in ai_dims.items():
                                dim_score = dim_data.get("score", 0)
                                dim_max = dim_data.get("max", 1)
                                dim_pct = (dim_score / dim_max * 100) if dim_max > 0 else 0
                                dim_color = "#22c55e" if dim_pct >= 70 else "#f59e0b" if dim_pct >= 50 else "#ef4444"
                                st.markdown(f"""
                                <div style="margin-bottom:6px;">
                                    <div style="display:flex;justify-content:space-between;align-items:center;">
                                        <span style="color:#e2e8f0;font-weight:600;font-size:.85rem;">{dim_name}</span>
                                        <span style="color:{dim_color};font-weight:700;font-size:.85rem;">{dim_score}/{dim_max}</span>
                                    </div>
                                    <div style="background:#1e293b;border-radius:3px;height:4px;margin:3px 0;">
                                        <div style="background:{dim_color};height:100%;width:{dim_pct}%;border-radius:3px;"></div>
                                    </div>
                                </div>
                                """, unsafe_allow_html=True)
                                for item in dim_data.get("items", []):
                                    st.markdown(f"  - {item}")

    # ═══════════════════════════════════════════════════════════════════════
    # 섹션 4: 콘텐츠 최적화 분석 (키워드 기반 — 자동 감지)
    # ═══════════════════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("#### 📝 콘텐츠 최적화 분석")
    st.caption("각 페이지의 핵심 키워드를 자동 감지하고, 키워드 최적화 수준을 분석합니다. 키워드는 수정 가능합니다.")

    if pages:
        # 페이지별 자동 키워드 추출 및 간이 점수 계산
        content_pages = []
        for p in pages:
            if p.get("Status") != 200:
                continue
            url = p.get("URL", "")
            h1 = (p.get("H1") or "").strip()
            title = (p.get("Title") or "").strip()
            words = p.get("Words", 0)
            if words < 50:
                continue  # 너무 짧은 페이지 제외

            # 자동 키워드: H1 우선, 없으면 Title
            auto_keyword = h1 if h1 else title
            # 키워드가 너무 길면 앞부분만
            if len(auto_keyword) > 50:
                auto_keyword = auto_keyword[:50]

            # 간이 콘텐츠 점수 (저장된 데이터 기반)
            co_score = 0
            co_items = []

            # Title에 H1 키워드 포함 여부
            kw_lower = auto_keyword.lower()
            if kw_lower and kw_lower in title.lower():
                co_score += 15
                co_items.append(("Title 키워드", True))
            else:
                co_items.append(("Title 키워드", False))

            # H1 존재
            if h1:
                co_score += 10
                co_items.append(("H1 태그", True))
            else:
                co_items.append(("H1 태그", False))

            # Meta Description 존재 및 길이
            desc = p.get("Meta Desc", "")
            desc_len = p.get("Desc Len", 0)
            if desc and 70 <= desc_len <= 160:
                co_score += 10
                co_items.append(("Description", True))
            elif desc:
                co_score += 5
                co_items.append(("Description 길이", False))
            else:
                co_items.append(("Description", False))

            # 서브헤딩 구조
            h2s = p.get("H2s", 0)
            h3s = p.get("H3s", 0)
            if h2s >= 3:
                co_score += 10
                co_items.append(("서브헤딩 구조", True))
            elif h2s >= 1:
                co_score += 5
                co_items.append(("서브헤딩 부족", False))
            else:
                co_items.append(("서브헤딩 없음", False))

            # 콘텐츠 길이
            if words >= 2000:
                co_score += 10
            elif words >= 1000:
                co_score += 7
            elif words >= 500:
                co_score += 4
            co_items.append(("콘텐츠 길이", words >= 1000))

            # 이미지
            imgs = p.get("Images", 0)
            if imgs >= 3:
                co_score += 10
            elif imgs >= 1:
                co_score += 5
            co_items.append(("이미지", imgs >= 1))

            # 내부 링크
            int_links = len(p.get("_internal_links", []))
            if int_links >= 5:
                co_score += 5
            elif int_links >= 2:
                co_score += 3
            co_items.append(("내부 링크", int_links >= 3))

            # 외부 링크
            ext_links = p.get("Ext Links", 0)
            if ext_links >= 2:
                co_score += 5
            co_items.append(("외부 인용", ext_links >= 1))

            # Schema
            if p.get("Has Schema"):
                co_score += 5
                co_items.append(("구조화 데이터", True))
            else:
                co_items.append(("구조화 데이터", False))

            # HTTPS
            if p.get("HTTPS"):
                co_score += 5

            # Title 길이
            title_len = p.get("Title Len", 0)
            if 30 <= title_len <= 60:
                co_score += 5
                co_items.append(("Title 길이", True))
            else:
                co_items.append(("Title 길이", False))

            # URL 길이 합리적
            path = urlparse(url).path
            if len(path) <= 80:
                co_score += 5
            co_items.append(("URL 구조", len(path) <= 80))

            co_score = min(co_score, 100)

            content_pages.append({
                "url": url,
                "keyword": auto_keyword,
                "score": co_score,
                "words": words,
                "items": co_items,
                "h1": h1,
                "title": title,
            })

        # 점수순 정렬
        content_pages.sort(key=lambda x: x["score"])

        if content_pages:
            # 요약 통계
            avg_co = round(sum(cp["score"] for cp in content_pages) / len(content_pages))
            good_co = sum(1 for cp in content_pages if cp["score"] >= 70)
            bad_co = sum(1 for cp in content_pages if cp["score"] < 40)

            co_stat_cols = st.columns(4)
            co_stat_cols[0].metric("평균 점수", f"{avg_co}/100")
            co_stat_cols[1].metric("우수 (70+)", f"{good_co}개")
            co_stat_cols[2].metric("개선 필요 (<40)", f"{bad_co}개")
            co_stat_cols[3].metric("분석 페이지", f"{len(content_pages)}개")

            # 페이지별 테이블
            st.markdown("**페이지별 콘텐츠 최적화 점수**")
            for cp in content_pages[:50]:
                sc = cp["score"]
                sc_color = "#22c55e" if sc >= 70 else "#f59e0b" if sc >= 40 else "#ef4444"
                url_short = urlparse(cp["url"]).path or "/"
                passed = sum(1 for _, ok in cp["items"] if ok)
                total_checks = len(cp["items"])
                item_icons = "".join("✅" if ok else "❌" for _, ok in cp["items"])

                st.markdown(f"""
                <div style="background:#111827;border:1px solid #1e293b;border-radius:6px;padding:10px 14px;margin-bottom:4px;">
                    <div style="display:flex;align-items:center;gap:12px;">
                        <span style="color:{sc_color};font-weight:800;font-size:1.1rem;min-width:36px;">{sc}</span>
                        <div style="flex:1;overflow:hidden;">
                            <div style="color:#818cf8;font-size:.85rem;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="{cp['url']}">{url_short}</div>
                            <div style="color:#94a3b8;font-size:.75rem;margin-top:2px;">키워드: {cp['keyword'][:40]} · {cp['words']}단어 · {passed}/{total_checks} 항목 통과</div>
                        </div>
                        <span style="font-size:.7rem;letter-spacing:-1px;">{item_icons}</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)

            # 개별 페이지 상세 분석 (수동)
            st.markdown("---")
            st.markdown("**개별 페이지 상세 분석**")
            st.caption("특정 페이지의 키워드를 수정하고 상세 분석을 실행할 수 있습니다.")

            page_urls = [cp["url"] for cp in content_pages]
            page_keywords = {cp["url"]: cp["keyword"] for cp in content_pages}

            with st.form("content_opt_detail_form", clear_on_submit=False):
                co_cols = st.columns([3, 2])
                with co_cols[0]:
                    co_url = st.selectbox("분석할 페이지", page_urls, format_func=lambda u: urlparse(u).path or "/", key="co_url_select")
                with co_cols[1]:
                    default_kw = page_keywords.get(co_url, "")
                    co_keyword = st.text_input("타겟 키워드 (수정 가능)", value=default_kw, key="co_keyword_input")
                co_submit = st.form_submit_button("🔍 상세 분석 시작", use_container_width=True, type="primary")

            if co_submit and co_url and co_keyword:
                with st.spinner(f"'{co_keyword}' 키워드로 페이지 분석 중..."):
                    co_result = crawler.analyze_content_optimization(co_url.strip(), co_keyword.strip())

                if co_result:
                    score = co_result.get("score", 0)
                    if score >= 80:
                        score_color, score_label = "#22c55e", "우수"
                    elif score >= 60:
                        score_color, score_label = "#818cf8", "양호"
                    elif score >= 40:
                        score_color, score_label = "#f59e0b", "보통"
                    else:
                        score_color, score_label = "#ef4444", "개선 필요"

                    st.markdown(f"""
                    <div style="text-align:center;margin:16px 0;">
                        <div class="score-circle {'score-good' if score >= 60 else 'score-medium' if score >= 40 else 'score-bad'}" style="width:120px;height:120px;font-size:2rem;margin:0 auto;">{score}</div>
                        <p style="color:{score_color};font-weight:600;font-size:1.1rem;margin-top:8px;">콘텐츠 최적화 점수: {score_label}</p>
                        <p style="color:#94a3b8;font-size:.82rem;">키워드: "{co_keyword}" · {co_result.get('word_count', 0)}단어 · 밀도 {co_result.get('keyword_density', 0)}%</p>
                    </div>
                    """, unsafe_allow_html=True)

                    checks = co_result.get("checks", [])
                    if checks:
                        st.markdown("**진단 항목**")
                        for check in checks:
                            icon = "✅" if check["pass"] else "❌"
                            bg = "rgba(34,197,94,.08)" if check["pass"] else "rgba(239,68,68,.08)"
                            border = "rgba(34,197,94,.25)" if check["pass"] else "rgba(239,68,68,.25)"
                            st.markdown(f"""
                            <div style="background:{bg};border:1px solid {border};border-radius:6px;padding:8px 14px;margin-bottom:4px;display:flex;align-items:center;gap:10px;">
                                <span style="font-size:1rem;">{icon}</span>
                                <div>
                                    <span style="color:#e2e8f0;font-weight:600;font-size:.9rem;">{check['name']}</span>
                                    <span style="color:#94a3b8;font-size:.82rem;margin-left:8px;">{check['detail']}</span>
                                </div>
                            </div>
                            """, unsafe_allow_html=True)

                    recs = co_result.get("recommendations", [])
                    if recs:
                        st.markdown("**개선 권장사항**")
                        for i, rec in enumerate(recs, 1):
                            st.markdown(f"""
                            <div style="background:#0f172a;border:1px solid #1e293b;border-left:3px solid #d29922;border-radius:6px;padding:10px 14px;margin-bottom:4px;">
                                <span style="color:#f59e0b;font-weight:700;">{i}.</span>
                                <span style="color:#e2e8f0;font-size:.9rem;margin-left:6px;">{rec}</span>
                            </div>
                            """, unsafe_allow_html=True)

                    met_cols = st.columns(5)
                    met_cols[0].metric("단어 수", f"{co_result.get('word_count', 0):,}")
                    met_cols[1].metric("키워드 빈도", f"{co_result.get('keyword_count', 0)}회")
                    met_cols[2].metric("헤딩 수", f"{co_result.get('heading_count', 0)}개")
                    met_cols[3].metric("이미지", f"{co_result.get('image_count', 0)}개")
                    met_cols[4].metric("내부 링크", f"{co_result.get('internal_links', 0)}개")
    else:
        st.info("크롤링 데이터가 필요합니다. 설정 탭에서 크롤링을 실행하세요.")


def _estimate_ai_readiness(page_data):
    """
    크롤 데이터에서 AI 준비도를 추정합니다.
    (실제 soup 없이 저장된 데이터만으로 계산)
    각 차원별 점수 + 상세 항목(✅/❌)을 반환합니다.
    """
    score = 0
    dim_details = {}  # {차원명: {"score": n, "max": m, "items": [...]}}

    eeat = page_data.get("_eeat") or {}
    if not isinstance(eeat, dict): eeat = {}
    schema = page_data.get("_schema") or {}
    if not isinstance(schema, dict): schema = {}
    tech = page_data.get("_tech") or {}
    if not isinstance(tech, dict): tech = {}
    content = page_data.get("_content") or {}
    if not isinstance(content, dict): content = {}

    # ── 1. 인용 준비도 (25점) ──
    cr = 0
    cr_items = []
    wc = page_data.get("Words", 0)
    if wc >= 1500:
        cr += 10
        cr_items.append(f"✅ 콘텐츠 길이 충분 ({wc}단어)")
    elif wc >= 800:
        cr += 6
        cr_items.append(f"⚠️ 콘텐츠 보통 ({wc}단어, 1500+ 권장)")
    elif wc >= 300:
        cr += 3
        cr_items.append(f"❌ 콘텐츠 부족 ({wc}단어, 1500+ 권장)")
    else:
        cr_items.append(f"❌ 콘텐츠 매우 짧음 ({wc}단어)")

    img_count = page_data.get("Images", 0)
    if img_count >= 3:
        cr += 5
        cr_items.append(f"✅ 이미지 풍부 ({img_count}개)")
    elif img_count >= 1:
        cr += 2
        cr_items.append(f"⚠️ 이미지 부족 ({img_count}개, 3개+ 권장)")
    else:
        cr_items.append("❌ 이미지 없음")

    ext_links = page_data.get("Ext Links", 0)
    if ext_links >= 3:
        cr += 5
        cr_items.append(f"✅ 외부 인용 링크 충분 ({ext_links}개)")
    elif ext_links >= 1:
        cr += 3
        cr_items.append(f"⚠️ 외부 인용 부족 ({ext_links}개, 3개+ 권장)")
    else:
        cr_items.append("❌ 외부 인용 링크 없음 — 신뢰성 저하")

    thr = page_data.get("Text/HTML %", 0)
    if thr >= 30:
        cr += 5
        cr_items.append(f"✅ 텍스트 비율 양호 ({thr:.0f}%)")
    elif thr >= 15:
        cr += 3
        cr_items.append(f"⚠️ 텍스트 비율 보통 ({thr:.0f}%, 30%+ 권장)")
    else:
        cr_items.append(f"❌ 텍스트 비율 낮음 ({thr:.0f}%)")
    cr_val = min(cr, 25)
    score += cr_val
    dim_details["인용 준비도"] = {"score": cr_val, "max": 25, "items": cr_items}

    # ── 2. 답변 적합성 (20점) ──
    aa = 0
    aa_items = []
    total_headings = sum(page_data.get(f"H{i}s", 0) for i in range(2, 4))
    if total_headings >= 5:
        aa += 8
        aa_items.append(f"✅ 서브헤딩 구조 풍부 (H2/H3 {total_headings}개)")
    elif total_headings >= 3:
        aa += 5
        aa_items.append(f"⚠️ 서브헤딩 보통 (H2/H3 {total_headings}개, 5개+ 권장)")
    elif total_headings >= 1:
        aa += 3
        aa_items.append(f"❌ 서브헤딩 부족 (H2/H3 {total_headings}개)")
    else:
        aa_items.append("❌ H2/H3 서브헤딩 없음 — AI가 답변 구조를 파악하기 어려움")

    if tech.get("heading_hierarchy_ok"):
        aa += 6
        aa_items.append("✅ 헤딩 계층 구조 정상")
    else:
        aa_items.append("❌ 헤딩 계층 구조 오류 — H1→H2→H3 순서 권장")

    if wc >= 500:
        aa += 6
        aa_items.append("✅ 답변에 충분한 콘텐츠 분량")
    else:
        aa_items.append("❌ 콘텐츠가 너무 짧아 AI 답변 소스로 부적합")
    aa_val = min(aa, 20)
    score += aa_val
    dim_details["답변 적합성"] = {"score": aa_val, "max": 20, "items": aa_items}

    # ── 3. 콘텐츠 권위 (20점) ──
    ca = 0
    ca_items = []
    if eeat.get("has_author"):
        ca += 7
        ca_items.append("✅ 저자 정보 명시")
    else:
        ca_items.append("❌ 저자 정보 없음 — 전문성 판단 불가")

    if eeat.get("has_published_date"):
        ca += 5
        ca_items.append("✅ 발행일 명시")
    else:
        ca_items.append("❌ 발행일 없음 — 최신성 판단 불가")

    if eeat.get("has_modified_date"):
        ca += 4
        ca_items.append("✅ 수정일 명시 (최신 콘텐츠 신호)")
    else:
        ca_items.append("⚠️ 수정일 없음 — 업데이트 여부 알 수 없음")

    if ext_links >= 2:
        ca += 4
        ca_items.append("✅ 외부 출처 인용으로 권위 강화")
    else:
        ca_items.append("❌ 외부 출처 인용 부족")
    ca_val = min(ca, 20)
    score += ca_val
    dim_details["콘텐츠 권위"] = {"score": ca_val, "max": 20, "items": ca_items}

    # ── 4. 지식 그래프 연동 (15점) ──
    kg = 0
    kg_items = []
    all_types = schema.get("all_types", [])
    if not isinstance(all_types, list): all_types = []

    if schema.get("has_schema"):
        kg += 4
        kg_items.append(f"✅ 구조화 데이터 존재 ({', '.join(all_types[:5])})")
    else:
        kg_items.append("❌ 구조화 데이터 없음 — Knowledge Graph 연동 불가")

    if "FAQPage" in all_types:
        kg += 5
        kg_items.append("✅ FAQPage 스키마 (AI 인용률 +41%)")
    else:
        kg_items.append("⚠️ FAQPage 스키마 없음 — FAQ 콘텐츠가 있다면 추가 권장")

    if any(t in all_types for t in ("Article", "BlogPosting", "NewsArticle")):
        kg += 3
        kg_items.append("✅ Article 스키마")
    else:
        kg_items.append("⚠️ Article 스키마 없음")

    if "BreadcrumbList" in all_types:
        kg += 3
        kg_items.append("✅ BreadcrumbList 스키마")
    kg_val = min(kg, 15)
    score += kg_val
    dim_details["지식 그래프 연동"] = {"score": kg_val, "max": 15, "items": kg_items}

    # ── 5. 기술적 접근성 (10점) ──
    ta = 0
    ta_items = []
    if not tech.get("is_noindex"):
        ta += 3
        ta_items.append("✅ 인덱싱 허용")
    else:
        ta_items.append("❌ noindex 설정 — AI 검색 노출 불가")

    if tech.get("heading_hierarchy_ok"):
        ta += 3
        ta_items.append("✅ 헤딩 계층 구조 정상")
    else:
        ta_items.append("❌ 헤딩 계층 오류")

    if tech.get("has_viewport"):
        ta += 2
        ta_items.append("✅ 모바일 뷰포트 설정")
    else:
        ta_items.append("⚠️ 뷰포트 메타태그 없음")

    if tech.get("lang"):
        ta += 2
        ta_items.append(f"✅ 언어 명시: {tech['lang']}")
    else:
        ta_items.append("❌ html lang 속성 없음")
    ta_val = min(ta, 10)
    score += ta_val
    dim_details["기술적 접근성"] = {"score": ta_val, "max": 10, "items": ta_items}

    # ── 6. 차별화 (10점) ──
    di = 0
    di_items = []
    if wc >= 2000:
        di += 4
        di_items.append(f"✅ 심층 콘텐츠 ({wc}단어)")
    elif wc >= 1000:
        di += 2
        di_items.append(f"⚠️ 콘텐츠 길이 보통 ({wc}단어, 2000+ 권장)")
    else:
        di_items.append(f"❌ 콘텐츠 짧음 — 경쟁 콘텐츠 대비 차별화 어려움")

    if img_count >= 3:
        di += 3
        di_items.append(f"✅ 시각 자료 풍부 ({img_count}개 이미지)")
    else:
        di_items.append("⚠️ 시각 자료 부족 — 이미지/인포그래픽 추가 권장")

    if page_data.get("Has Schema"):
        di += 3
        di_items.append("✅ 구조화 데이터로 차별화")
    else:
        di_items.append("❌ Schema 없음")
    di_val = min(di, 10)
    score += di_val
    dim_details["차별화"] = {"score": di_val, "max": 10, "items": di_items}

    total = min(score, 100)
    if total >= 70:
        grade_kr = "AI-Ready"
    elif total >= 50:
        grade_kr = "개선 필요"
    else:
        grade_kr = "최적화 필요"

    return {"score": total, "grade_kr": grade_kr, "details": dim_details}


# ── 프로젝트 설정 ──────────────────────────────────────────────────────────
def render_project_settings(project):
    project_id = project["id"]

    # ── 크롤링 실행 섹션 ──
    st.markdown("### 🚀 크롤링 실행")
    render_crawl_execution(project)

    st.markdown("---")
    st.markdown("### ⚙️ 프로젝트 설정")
    st.caption("크롤링 모드, 속도, 스케줄 등 프로젝트 설정을 변경할 수 있습니다.")

    with st.form("project_settings_form"):
        st.markdown("#### 🌐 기본 정보")
        new_name = st.text_input("프로젝트 이름", value=project["name"])
        new_url = st.text_input("사이트 URL", value=project["url"])

        st.markdown("---")
        st.markdown("#### 🕷️ 크롤링 설정")

        mode_options = ["full", "sitemap", "path", "mixed"]
        mode_labels = {
            "full": "full — 전체 크롤링 (사이트 내 모든 링크를 따라가며 수집)",
            "sitemap": "sitemap — 사이트맵 기반 (sitemap.xml에 등록된 URL만 수집)",
            "path": "path — 특정 경로 (지정한 경로 하위만 수집)",
            "mixed": "mixed — 혼합 모드 (사이트맵 + 링크 크롤링 병행)",
        }
        current_mode_idx = mode_options.index(project["crawl_mode"]) if project["crawl_mode"] in mode_options else 0
        new_mode = st.selectbox(
            "크롤링 모드",
            mode_options,
            index=current_mode_idx,
            format_func=lambda x: mode_labels.get(x, x),
        )

        new_path = st.text_input(
            "크롤링 경로 (path 모드에서만 사용)",
            value=project.get("crawl_path", ""),
            placeholder="/blog/",
            help="path 모드 선택 시 이 경로 하위의 페이지만 크롤링합니다.",
        )

        new_max_pages = st.slider(
            "최대 페이지 수",
            min_value=10, max_value=5000,
            value=project.get("max_pages") or 200,
            step=10,
            help="한 번의 크롤링에서 수집할 최대 페이지 수입니다.",
        )

        st.markdown("---")
        st.markdown("#### ⏱️ 크롤링 속도 설정")

        st.markdown("""
        <div style="background:#0f172a;border:1px solid #1e293b;border-radius:8px;padding:16px;margin-bottom:16px;">
            <p style="color:#e2e8f0;font-weight:600;margin-bottom:8px;">💡 크롤링 속도 가이드</p>
            <table style="width:100%;color:#94a3b8;font-size:.85rem;border-collapse:collapse;">
                <tr style="border-bottom:1px solid #1e293b;">
                    <td style="padding:6px 8px;font-weight:600;color:#22c55e;">0.5초 이상 (권장)</td>
                    <td style="padding:6px 8px;">서버에 부담 없이 안전하게 크롤링</td>
                </tr>
                <tr style="border-bottom:1px solid #1e293b;">
                    <td style="padding:6px 8px;font-weight:600;color:#f59e0b;">0.3~0.5초</td>
                    <td style="padding:6px 8px;">소규모 사이트에서는 괜찮으나, 대규모 사이트에서는 주의 필요</td>
                </tr>
                <tr>
                    <td style="padding:6px 8px;font-weight:600;color:#ef4444;">0.3초 미만</td>
                    <td style="padding:6px 8px;">서버에 과부하를 줄 수 있으며, 방화벽에 의해 차단될 수 있음</td>
                </tr>
            </table>
        </div>
        """, unsafe_allow_html=True)

        new_delay = st.slider(
            "크롤링 딜레이 (초)",
            min_value=0.3, max_value=5.0,
            value=max(float(project.get("crawl_delay") or 0.5), 0.3),
            step=0.1,
            help="각 페이지 요청 사이의 대기 시간입니다. 최소 0.3초로 제한됩니다.",
        )

        # Speed warning
        if new_delay < 0.5:
            st.warning(
                "⚠️ **크롤링 속도 경고**: 딜레이가 0.5초 미만이면 대상 서버에 과도한 부하를 줄 수 있습니다. "
                "서버가 이를 공격으로 인식하여 IP를 차단하거나, 웹사이트 성능에 악영향을 줄 수 있습니다. "
                "특히 공유 호스팅이나 소규모 서버를 사용하는 사이트에서는 **0.5초 이상을 권장**합니다."
            )

        st.markdown("---")
        st.markdown("#### 🤖 robots.txt 설정")
        respect_robots = st.radio(
            "robots.txt 준수 여부",
            ["respect", "ignore"],
            index=0 if project.get("respect_robots", "respect") == "respect" else 1,
            format_func=lambda x: {
                "respect": "✅ 준수 — robots.txt에서 차단된 경로는 크롤링하지 않음 (권장)",
                "ignore": "⚠️ 무시 — robots.txt와 관계없이 모든 경로를 강제 크롤링",
            }.get(x, x),
            help="robots.txt를 무시하면 차단된 경로도 수집합니다. SEO 진단 목적에서는 유용하지만, "
                 "대상 사이트의 정책을 존중하는 것이 권장됩니다.",
        )
        if respect_robots == "ignore":
            st.warning("robots.txt를 무시하고 크롤링합니다. 일부 사이트에서 IP 차단이 발생할 수 있습니다.")

        st.markdown("---")
        st.markdown("#### 📅 자동 수집 스케줄")
        st.caption("모든 시간은 한국 표준시(KST, UTC+9) 기준입니다.")

        schedule_options = ["manual", "daily", "biweekly", "weekly"]
        schedule_labels = {
            "manual": "수동 (직접 크롤링 실행)",
            "daily": "매일 (매일 지정한 시간에 자동 크롤링)",
            "biweekly": "주 2회 (선택한 요일에 자동 크롤링)",
            "weekly": "주 1회 (선택한 요일에 자동 크롤링)",
        }
        current_sched = project.get("schedule") or "manual"
        current_schedule_idx = schedule_options.index(current_sched) if current_sched in schedule_options else 0
        new_schedule = st.selectbox(
            "스케줄",
            schedule_options,
            index=current_schedule_idx,
            format_func=lambda x: schedule_labels.get(x, x),
        )

        # 시간/요일 파싱
        saved_schedule_time = project.get("schedule_time") or "09:00"
        saved_day = ""
        saved_day2 = ""
        saved_time_str = saved_schedule_time
        if " " in saved_schedule_time:
            parts = saved_schedule_time.split(" ")
            if len(parts) >= 3:
                saved_day = parts[0]
                saved_day2 = parts[1]
                saved_time_str = parts[2]
            elif len(parts) == 2:
                saved_day = parts[0]
                saved_time_str = parts[1]

        new_schedule_time = saved_schedule_time
        days = ["월요일", "화요일", "수요일", "목요일", "금요일", "토요일", "일요일"]

        if new_schedule != "manual":
            try:
                default_time = datetime.strptime(saved_time_str, "%H:%M").time()
            except Exception:
                default_time = datetime.strptime("09:00", "%H:%M").time()
            time_val = st.time_input("수집 시간 (KST)", value=default_time, key=f"settings_time_{project_id}")
            time_str = time_val.strftime("%H:%M")

            if new_schedule == "daily":
                st.caption(f"매일 {time_str} (KST)에 자동으로 크롤링됩니다.")
                new_schedule_time = time_str
            elif new_schedule == "weekly":
                default_day_idx = days.index(saved_day) if saved_day in days else 0
                schedule_day = st.selectbox("수집 요일", days, index=default_day_idx, key=f"settings_day_{project_id}")
                new_schedule_time = f"{schedule_day} {time_str}"
                st.caption(f"매주 {schedule_day} {time_str} (KST)에 자동으로 크롤링됩니다.")
            elif new_schedule == "biweekly":
                bw_cols = st.columns(2)
                with bw_cols[0]:
                    default_day1_idx = days.index(saved_day) if saved_day in days else 1  # 화요일
                    bw_day1 = st.selectbox("첫 번째 요일", days, index=default_day1_idx, key=f"settings_bwday1_{project_id}")
                with bw_cols[1]:
                    default_day2_idx = days.index(saved_day2) if saved_day2 in days else 4  # 금요일
                    bw_day2 = st.selectbox("두 번째 요일", days, index=default_day2_idx, key=f"settings_bwday2_{project_id}")
                new_schedule_time = f"{bw_day1} {bw_day2} {time_str}"
                st.caption(f"매주 {bw_day1}, {bw_day2} {time_str} (KST)에 자동으로 크롤링됩니다.")

        st.markdown("")
        submitted = st.form_submit_button("💾 설정 저장", use_container_width=True, type="primary")

        if submitted:
            if not new_name or not new_url:
                st.error("프로젝트 이름과 URL은 필수입니다.")
            else:
                if not new_url.startswith(("http://", "https://")):
                    new_url = "https://" + new_url

                db.update_project(
                    project_id,
                    name=new_name,
                    url=new_url,
                    crawl_mode=new_mode,
                    crawl_path=new_path,
                    max_pages=new_max_pages,
                    crawl_delay=max(new_delay, 0.3),
                    schedule=new_schedule,
                    schedule_time=new_schedule_time,
                    respect_robots=respect_robots,
                )
                st.success("설정이 저장되었습니다!")
                st.rerun()

    # ── 제외 URL 관리 ──
    st.markdown("---")
    st.markdown("#### 🚫 제외 URL 관리")
    st.caption("전략적으로 특정 SEO 요소가 없는 페이지나, 이슈 리포트에서 제외하고 싶은 URL을 등록하세요. 등록된 URL은 인사이트와 이슈 리포트에서 제외됩니다.")

    # Add new exclusion
    with st.form("add_exclude_url", clear_on_submit=True):
        ex_cols = st.columns([3, 2, 1])
        with ex_cols[0]:
            new_exclude_url = st.text_input("제외할 URL 또는 경로", placeholder="https://example.com/landing 또는 /admin/", key=f"exclude_url_{project_id}")
        with ex_cols[1]:
            new_exclude_reason = st.text_input("사유 (선택)", placeholder="랜딩 페이지 — 의도적으로 간결하게 유지", key=f"exclude_reason_{project_id}")
        with ex_cols[2]:
            st.markdown("<br>", unsafe_allow_html=True)
            add_btn = st.form_submit_button("➕ 추가")
        if add_btn and new_exclude_url:
            db.add_excluded_url(project_id, new_exclude_url.strip(), new_exclude_reason.strip())
            st.success(f"'{new_exclude_url}' 제외 목록에 추가됨")
            st.rerun()

    # Show existing exclusions
    try:
        excluded_list = db.get_excluded_urls(project_id)
    except Exception:
        excluded_list = []

    if excluded_list:
        st.markdown(f"**등록된 제외 URL ({len(excluded_list)}건)**")
        for ex in excluded_list:
            ec1, ec2, ec3 = st.columns([3, 2, 1])
            with ec1:
                st.code(ex.get("url_pattern", ""), language=None)
            with ec2:
                st.caption(ex.get("reason", "") or "—")
            with ec3:
                if st.button("삭제", key=f"del_exclude_{ex['id']}"):
                    db.delete_excluded_url(ex["id"])
                    st.rerun()
    else:
        st.caption("등록된 제외 URL이 없습니다.")

    # Current settings summary
    st.markdown("---")
    st.markdown("#### 📋 현재 설정 요약")

    delay_val = float(project.get("crawl_delay") or 0.5)
    if delay_val < 0.5:
        delay_color = "#ef4444"
        delay_status = "빠름 (주의)"
    elif delay_val < 1.0:
        delay_color = "#f59e0b"
        delay_status = "보통"
    else:
        delay_color = "#22c55e"
        delay_status = "안전"

    schedule_val = project.get("schedule") or "manual"
    schedule_display = {"manual": "수동", "biweekly": "주 2회 (화, 금)", "weekly": "주 1회"}.get(schedule_val, schedule_val)
    schedule_time_display = project.get("schedule_time") or ""
    if schedule_time_display:
        schedule_display += f" · {schedule_time_display}"
    schedule_cls = {"manual": "schedule-manual", "biweekly": "schedule-daily", "weekly": "schedule-weekly"}.get(schedule_val, "schedule-manual")

    st.markdown(f"""
    <div style="background:#0f172a;border:1px solid #1e293b;border-radius:8px;padding:16px;">
        <table style="width:100%;color:#e2e8f0;font-size:.9rem;border-collapse:collapse;">
            <tr style="border-bottom:1px solid #1e293b;">
                <td style="padding:8px;color:#94a3b8;width:140px;">사이트 URL</td>
                <td style="padding:8px;color:#818cf8;">{project['url']}</td>
            </tr>
            <tr style="border-bottom:1px solid #1e293b;">
                <td style="padding:8px;color:#94a3b8;">크롤링 모드</td>
                <td style="padding:8px;">{project['crawl_mode']}</td>
            </tr>
            <tr style="border-bottom:1px solid #1e293b;">
                <td style="padding:8px;color:#94a3b8;">최대 페이지</td>
                <td style="padding:8px;">{project['max_pages']}</td>
            </tr>
            <tr style="border-bottom:1px solid #1e293b;">
                <td style="padding:8px;color:#94a3b8;">크롤링 딜레이</td>
                <td style="padding:8px;"><span style="color:{delay_color};font-weight:600;">{delay_val}초</span> <span style="font-size:.8rem;color:#94a3b8;">({delay_status})</span></td>
            </tr>
            <tr style="border-bottom:1px solid #1e293b;">
                <td style="padding:8px;color:#94a3b8;">스케줄</td>
                <td style="padding:8px;"><span class="{schedule_cls} schedule-badge">{schedule_display}</span></td>
            </tr>
            <tr>
                <td style="padding:8px;color:#94a3b8;">마지막 크롤링</td>
                <td style="padding:8px;">{project.get('last_crawl_at', '없음')}</td>
            </tr>
        </table>
    </div>
    """, unsafe_allow_html=True)


# ── 서치콘솔 탭 ──────────────────────────────────────────────────────────────
def render_search_console(project):
    project_id = project["id"]

    st.markdown("### 🔍 Google Search Console 연동")

    # 가이드 섹션
    with st.expander("📖 Search Console 연동 가이드 (클릭하여 펼치기)", expanded=False):
        st.markdown("""
        #### Search Console을 연동하면 얻을 수 있는 정보
        - **트래픽 데이터**: 각 페이지별 클릭수, 노출수, CTR, 평균 순위
        - **검색 키워드**: 사이트에 유입되는 검색어와 순위 변화
        - **인덱싱 이슈**: Google이 감지한 크롤링/인덱싱 문제와 해결 방법
        - **통합 인사이트**: 크롤링 데이터 + 트래픽 데이터를 교차 분석하여 더 깊은 인사이트 제공

        ---

        #### 연동 방법 (단계별 안내)

        **Step 1: Google Cloud Console에서 서비스 계정 만들기**
        1. [Google Cloud Console](https://console.cloud.google.com/)에 접속
        2. 새 프로젝트를 만들거나 기존 프로젝트 선택
        3. 좌측 메뉴 → **API 및 서비스** → **사용자 인증 정보**
        4. **+ 사용자 인증 정보 만들기** → **서비스 계정** 선택
        5. 서비스 계정 이름 입력 후 생성
        6. 생성된 서비스 계정 클릭 → **키** 탭 → **키 추가** → **새 키 만들기** → **JSON** 선택
        7. JSON 파일이 다운로드됩니다 (이 파일을 아래에 업로드)

        **Step 2: Search Console API 활성화**
        1. Google Cloud Console → **API 및 서비스** → **라이브러리**
        2. "Google Search Console API" 검색 → **사용** 클릭

        **Step 3: Search Console에 서비스 계정 추가**
        1. [Google Search Console](https://search.google.com/search-console)에 접속
        2. 해당 사이트 선택 → **설정** → **사용자 및 권한**
        3. **사용자 추가** 클릭
        4. 서비스 계정 이메일 (JSON 파일 내 `client_email` 값) 입력
        5. 권한: **전체** 또는 **제한** 선택 후 추가

        **Step 4: 아래에서 연결**
        1. 다운로드한 JSON 파일을 아래 업로드
        2. 사이트 URL 입력 (예: `sc-domain:example.com` 또는 `https://example.com/`)
        3. "Search Console 연결" 버튼 클릭

        ---

        #### 사이트 URL 형식
        | 형식 | 예시 | 설명 |
        |------|------|------|
        | 도메인 속성 | `sc-domain:example.com` | 전체 도메인 (www 포함, http/https 모두) |
        | URL 접두어 | `https://example.com/` | 특정 프로토콜+서브도메인만 |

        > 💡 **도메인 속성**(`sc-domain:`)을 사용하면 모든 하위 도메인과 프로토콜의 데이터를 한 번에 볼 수 있어 권장됩니다.
        """)

    # Section A: 연결 설정
    st.markdown("#### 연결 설정")
    try:
        sc_conn = db.get_sc_connection(project_id)
    except Exception:
        sc_conn = None

    if not sc_conn:
        st.markdown('<div class="sc-disconnected">❌ Search Console이 연결되지 않았습니다.</div>', unsafe_allow_html=True)

        st.markdown("**서비스 계정 JSON 파일 업로드**")
        uploaded_json = st.file_uploader("서비스 계정 JSON", type=["json"], key=f"sc_json_{project_id}")
        site_url_input = st.text_input(
            "사이트 URL (예: sc-domain:example.com 또는 https://example.com/)",
            key=f"sc_site_url_{project_id}",
        )

        if st.button("🔗 Search Console 연결", key=f"sc_connect_{project_id}", type="primary"):
            if not uploaded_json or not site_url_input:
                st.error("JSON 파일과 사이트 URL을 모두 입력해주세요.")
            else:
                try:
                    json_content = uploaded_json.read().decode("utf-8")
                    # Validate JSON
                    json.loads(json_content)
                    db.save_sc_connection(project_id, json_content, site_url_input)
                    st.success("Search Console이 연결되었습니다!")
                    st.rerun()
                except json.JSONDecodeError:
                    st.error("유효한 JSON 파일이 아닙니다.")
                except Exception as e:
                    st.error(f"연결 실패: {e}")
        return

    # Connected state
    st.markdown(f"""
    <div class="sc-connected">
        ✅ Search Console 연결됨<br>
        <span style="color:#94a3b8;font-size:.85rem;">사이트: {sc_conn.get('site_url', '')} · 마지막 동기화: {sc_conn.get('last_sync', '없음')}</span>
    </div>
    """, unsafe_allow_html=True)

    if st.button("🔌 연결 해제", key=f"sc_disconnect_{project_id}"):
        try:
            db.delete_sc_connection(project_id)
            st.success("Search Console 연결이 해제되었습니다.")
            st.rerun()
        except Exception as e:
            st.error(f"연결 해제 실패: {e}")

    st.divider()

    # Section B: 트래픽 데이터
    st.markdown("#### 트래픽 데이터")

    col_start, col_end = st.columns(2)
    with col_start:
        default_start = datetime.utcnow() - timedelta(days=28)
        start_date = st.date_input("시작일", value=default_start, key=f"sc_start_{project_id}")
    with col_end:
        end_date = st.date_input("종료일", value=datetime.utcnow(), key=f"sc_end_{project_id}")

    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    if st.button("🔄 데이터 동기화", key=f"sc_sync_{project_id}", type="primary"):
        with st.spinner("Search Console 데이터를 가져오는 중..."):
            try:
                creds_json = sc_conn.get("credentials_json", "")
                site_url = sc_conn.get("site_url", "")
                sc_client = google_api.SearchConsoleClient(creds_json, site_url)

                # Fetch and save data
                sc_data = sc_client.fetch_analytics(start_str, end_str)
                if sc_data:
                    db.save_sc_analytics(project_id, sc_data)
                    db.update_sc_last_sync(project_id)
                    st.success(f"{len(sc_data)}건의 데이터를 동기화했습니다.")
                    st.rerun()
                else:
                    st.warning("가져올 데이터가 없습니다.")
            except Exception as e:
                st.error(f"동기화 실패: {e}")

    # Load saved SC data
    try:
        sc_data = db.get_sc_analytics(project_id, start_str, end_str)
    except Exception:
        sc_data = []

    if not sc_data:
        st.info("아직 동기화된 데이터가 없습니다. '데이터 동기화' 버튼을 눌러주세요.")
        return

    # Summary metrics
    total_clicks = sum(r.get("clicks", 0) for r in sc_data)
    total_impressions = sum(r.get("impressions", 0) for r in sc_data)
    avg_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
    avg_position = sum(r.get("position", 0) for r in sc_data) / len(sc_data) if sc_data else 0

    # Delta calculation (compare with previous period)
    period_days = (end_date - start_date).days
    prev_start = (start_date - timedelta(days=period_days)).strftime("%Y-%m-%d")
    prev_end = (start_date - timedelta(days=1)).strftime("%Y-%m-%d")
    try:
        prev_data = db.get_sc_analytics(project_id, prev_start, prev_end)
    except Exception:
        prev_data = []

    prev_clicks = sum(r.get("clicks", 0) for r in prev_data) if prev_data else 0
    prev_impressions = sum(r.get("impressions", 0) for r in prev_data) if prev_data else 0
    prev_ctr = (prev_clicks / prev_impressions * 100) if prev_impressions > 0 else 0
    prev_position = sum(r.get("position", 0) for r in prev_data) / len(prev_data) if prev_data else 0

    def _delta_html(current, previous, reverse=False):
        diff = current - previous
        if abs(diff) < 0.01:
            return ""
        if reverse:
            cls = "down" if diff > 0 else "up"
        else:
            cls = "up" if diff > 0 else "down"
        sign = "+" if diff > 0 else ""
        if isinstance(diff, float):
            return f'<div class="delta {cls}">{sign}{diff:.1f}</div>'
        return f'<div class="delta {cls}">{sign}{diff:,}</div>'

    mc1, mc2, mc3, mc4 = st.columns(4)
    with mc1:
        st.markdown(f"""
        <div class="traffic-metric">
            <div class="big-num">{total_clicks:,}</div>
            <div class="label">총 클릭</div>
            {_delta_html(total_clicks, prev_clicks)}
        </div>
        """, unsafe_allow_html=True)
    with mc2:
        st.markdown(f"""
        <div class="traffic-metric">
            <div class="big-num">{total_impressions:,}</div>
            <div class="label">총 노출</div>
            {_delta_html(total_impressions, prev_impressions)}
        </div>
        """, unsafe_allow_html=True)
    with mc3:
        st.markdown(f"""
        <div class="traffic-metric">
            <div class="big-num">{avg_ctr:.1f}%</div>
            <div class="label">평균 CTR</div>
            {_delta_html(avg_ctr, prev_ctr)}
        </div>
        """, unsafe_allow_html=True)
    with mc4:
        st.markdown(f"""
        <div class="traffic-metric">
            <div class="big-num">{avg_position:.1f}</div>
            <div class="label">평균 순위</div>
            {_delta_html(avg_position, prev_position, reverse=True)}
        </div>
        """, unsafe_allow_html=True)

    st.markdown("")

    # Daily trend chart
    st.markdown("#### 일별 트렌드")
    try:
        daily_data = {}
        for r in sc_data:
            d = r.get("date", "")
            if d not in daily_data:
                daily_data[d] = {"클릭": 0, "노출": 0}
            daily_data[d]["클릭"] += r.get("clicks", 0)
            daily_data[d]["노출"] += r.get("impressions", 0)

        if daily_data:
            df_daily = pd.DataFrame([
                {"날짜": d, "클릭": v["클릭"], "노출": v["노출"]}
                for d, v in sorted(daily_data.items())
            ])
            df_daily = df_daily.set_index("날짜")
            st.area_chart(df_daily)
    except Exception as e:
        st.warning(f"차트 생성 실패: {e}")

    # Top 10 키워드
    st.markdown("#### 🔑 Top 10 키워드")
    try:
        keyword_data = {}
        for r in sc_data:
            q = r.get("query", "")
            if not q:
                continue
            if q not in keyword_data:
                keyword_data[q] = {"clicks": 0, "impressions": 0, "position_sum": 0, "count": 0}
            keyword_data[q]["clicks"] += r.get("clicks", 0)
            keyword_data[q]["impressions"] += r.get("impressions", 0)
            keyword_data[q]["position_sum"] += r.get("position", 0)
            keyword_data[q]["count"] += 1

        if keyword_data:
            kw_rows = []
            for q, d in keyword_data.items():
                ctr = (d["clicks"] / d["impressions"] * 100) if d["impressions"] > 0 else 0
                pos = d["position_sum"] / d["count"] if d["count"] > 0 else 0
                kw_rows.append({
                    "키워드": q, "클릭": d["clicks"], "노출": d["impressions"],
                    "CTR(%)": round(ctr, 1), "평균 순위": round(pos, 1),
                })
            kw_df = pd.DataFrame(sorted(kw_rows, key=lambda x: -x["클릭"])[:10])
            st.dataframe(kw_df, use_container_width=True, hide_index=True)
    except Exception as e:
        st.warning(f"키워드 데이터 로드 실패: {e}")

    # Top 10 페이지
    st.markdown("#### 📄 Top 10 페이지")
    try:
        page_data = {}
        for r in sc_data:
            pg = r.get("page", "")
            if not pg:
                continue
            if pg not in page_data:
                page_data[pg] = {"clicks": 0, "impressions": 0, "position_sum": 0, "count": 0}
            page_data[pg]["clicks"] += r.get("clicks", 0)
            page_data[pg]["impressions"] += r.get("impressions", 0)
            page_data[pg]["position_sum"] += r.get("position", 0)
            page_data[pg]["count"] += 1

        if page_data:
            pg_rows = []
            for pg, d in page_data.items():
                ctr = (d["clicks"] / d["impressions"] * 100) if d["impressions"] > 0 else 0
                pos = d["position_sum"] / d["count"] if d["count"] > 0 else 0
                pg_rows.append({
                    "페이지": pg, "클릭": d["clicks"], "노출": d["impressions"],
                    "CTR(%)": round(ctr, 1), "평균 순위": round(pos, 1),
                })
            pg_df = pd.DataFrame(sorted(pg_rows, key=lambda x: -x["클릭"])[:10])
            st.dataframe(pg_df, use_container_width=True, hide_index=True)
    except Exception as e:
        st.warning(f"페이지 데이터 로드 실패: {e}")

    # 기기별/국가별 breakdown
    st.markdown("#### 📱 기기별 분석")
    try:
        device_data = {}
        for r in sc_data:
            dev = r.get("device", "알 수 없음")
            if dev not in device_data:
                device_data[dev] = {"clicks": 0, "impressions": 0}
            device_data[dev]["clicks"] += r.get("clicks", 0)
            device_data[dev]["impressions"] += r.get("impressions", 0)

        if device_data:
            dev_rows = [{"기기": k, "클릭": v["clicks"], "노출": v["impressions"]} for k, v in device_data.items()]
            st.dataframe(pd.DataFrame(dev_rows), use_container_width=True, hide_index=True)
    except Exception:
        pass

    st.markdown("#### 🌍 국가별 분석")
    try:
        country_data = {}
        for r in sc_data:
            country = r.get("country", "알 수 없음")
            if country not in country_data:
                country_data[country] = {"clicks": 0, "impressions": 0}
            country_data[country]["clicks"] += r.get("clicks", 0)
            country_data[country]["impressions"] += r.get("impressions", 0)

        if country_data:
            co_rows = sorted(
                [{"국가": k, "클릭": v["clicks"], "노출": v["impressions"]} for k, v in country_data.items()],
                key=lambda x: -x["클릭"],
            )[:20]
            st.dataframe(pd.DataFrame(co_rows), use_container_width=True, hide_index=True)
    except Exception:
        pass

    # Section C: 인덱싱 이슈
    st.divider()
    st.markdown("#### 🚨 인덱싱 이슈")
    try:
        creds_json = sc_conn.get("credentials_json", "")
        site_url = sc_conn.get("site_url", "")
        sc_client = google_api.SearchConsoleClient(creds_json, site_url)
        indexing_issues = sc_client.get_indexing_issues()

        if indexing_issues:
            for issue in indexing_issues:
                severity = issue.get("severity", "MEDIUM")
                sev_cls = severity.lower()
                issue_type = issue.get("type", "알 수 없는 이슈")
                affected_url = issue.get("url", "")
                first_detected = issue.get("first_detected", "")
                last_detected = issue.get("last_detected", "")

                # Get explanation from google_api
                try:
                    explanation = google_api.explain_sc_issue(issue_type)
                except Exception:
                    explanation = {"description": issue_type, "fix_steps": []}

                st.markdown(f"""
                <div class="issue-card {sev_cls}">
                    <div class="issue-header">
                        <span class="badge-{sev_cls}">{severity}</span>
                        <strong style="margin-left:8px;">{issue_type}</strong>
                    </div>
                    <div class="issue-url">{affected_url}</div>
                    <div class="issue-detail">{explanation.get('description', '')}</div>
                    <div style="color:#475569;font-size:.75rem;margin-top:4px;">
                        처음 감지: {first_detected} · 최근 감지: {last_detected}
                    </div>
                </div>
                """, unsafe_allow_html=True)

                fix_steps = explanation.get("fix_steps", [])
                if fix_steps:
                    with st.expander(f"💡 해결 방법 — {issue_type}"):
                        for step in fix_steps:
                            st.markdown(f"- {step}")
        else:
            st.success("인덱싱 이슈가 없습니다!")
    except Exception as e:
        st.info(f"인덱싱 이슈를 가져올 수 없습니다. 데이터 동기화 후 다시 시도해주세요. ({e})")


# ── 사이트 속도 탭 ──────────────────────────────────────────────────────────
def render_pagespeed(project):
    project_id = project["id"]

    st.markdown("### ⚡ PageSpeed Insights 분석")

    # 가이드 섹션
    with st.expander("📖 PageSpeed Insights 사용 가이드 (클릭하여 펼치기)", expanded=False):
        st.markdown("""
        #### PageSpeed Insights란?
        Google의 **Lighthouse** 엔진을 사용하여 웹페이지의 성능을 측정하는 도구입니다.
        실제 사용자 경험에 영향을 미치는 **Core Web Vitals** 지표를 포함한 종합적인 성능 분석을 제공합니다.

        ---

        #### 측정되는 핵심 지표 (Core Web Vitals)

        | 지표 | 의미 | 양호 | 개선 필요 | 나쁨 |
        |------|------|------|-----------|------|
        | **LCP** (Largest Contentful Paint) | 페이지에서 가장 큰 콘텐츠가 표시되는 시간 | ≤ 2.5초 | ≤ 4.0초 | > 4.0초 |
        | **FID/INP** (Interaction to Next Paint) | 사용자 입력에 대한 응답 시간 | ≤ 200ms | ≤ 500ms | > 500ms |
        | **CLS** (Cumulative Layout Shift) | 페이지 로딩 중 레이아웃 이동 정도 | ≤ 0.1 | ≤ 0.25 | > 0.25 |
        | **TTFB** (Time to First Byte) | 서버 응답 시간 | ≤ 800ms | ≤ 1800ms | > 1800ms |

        ---

        #### API Key 설정 (선택사항)
        - API Key **없이도** 분석이 가능하지만, 요청 횟수에 제한이 있을 수 있습니다
        - 더 많은 분석이 필요하다면 아래 방법으로 API Key를 발급받으세요:

        1. [Google Cloud Console](https://console.cloud.google.com/)에 접속
        2. 좌측 메뉴 → **API 및 서비스** → **라이브러리**
        3. "PageSpeed Insights API" 검색 → **사용** 클릭
        4. **API 및 서비스** → **사용자 인증 정보** → **+ 사용자 인증 정보 만들기** → **API 키**
        5. 생성된 API 키를 아래에 입력

        > 💡 무료 API Key로 **하루 25,000건**까지 분석할 수 있습니다.

        ---

        #### 사용 방법
        1. (선택) API Key 입력
        2. 크롤링 데이터가 있으면 자동으로 수집된 URL 목록이 표시됩니다
        3. 분석할 페이지 수를 선택하세요 (한 번에 최대 20개)
        4. "PageSpeed 분석 실행" 버튼 클릭
        5. 각 URL별 성능 점수, Core Web Vitals, 개선 기회가 표시됩니다

        > ⏱️ 각 URL당 약 15~30초 소요됩니다. 페이지 수에 비례하여 시간이 걸립니다.
        """)

    # API key input
    api_key = st.text_input(
        "PageSpeed API Key (선택사항 — 없어도 분석 가능)",
        type="password",
        key=f"ps_api_key_{project_id}",
    )

    # URL selection
    latest = None
    try:
        latest = db.get_latest_crawl(project_id)
    except Exception:
        pass

    urls_to_analyze = []
    if latest:
        try:
            pages = json.loads(latest.get("pages_json", "[]") or "[]")
            all_urls = [p.get("URL", "") for p in pages if p.get("URL")]
            num_pages = st.slider("분석할 페이지 수", 1, min(len(all_urls), 100), min(5, len(all_urls)), key=f"ps_num_{project_id}",
                                  help="한 URL당 약 15~30초 소요됩니다. 많은 페이지를 분석하면 시간이 오래 걸릴 수 있습니다.")
            urls_to_analyze = all_urls[:num_pages]
        except Exception:
            pass

    if not urls_to_analyze:
        url_manual = st.text_input("분석할 URL", value=project["url"], key=f"ps_url_{project_id}")
        urls_to_analyze = [url_manual] if url_manual else []

    if st.button("🚀 PageSpeed 분석 실행", key=f"ps_run_{project_id}", type="primary"):
        if not urls_to_analyze:
            st.error("분석할 URL을 입력해주세요.")
        else:
            progress = st.progress(0.0)
            results = []
            ps_client = google_api.PageSpeedClient(api_key=api_key or "")
            for idx, url in enumerate(urls_to_analyze):
                with st.spinner(f"분석 중: {url}"):
                    try:
                        ps_result = ps_client.get_full_report(url)
                        if ps_result and not ps_result.get("error"):
                            ps_result["url"] = url
                            ps_result["analyzed_at"] = datetime.utcnow().isoformat()
                            results.append(ps_result)
                            # Save to DB
                            try:
                                cwv = ps_result.get("core_web_vitals", {})
                                db_data = {
                                    "score": ps_result.get("score", 0),
                                    "lcp": cwv.get("lcp", 0.0),
                                    "fid": cwv.get("fid", 0.0),
                                    "cls": cwv.get("cls", 0.0),
                                    "ttfb": cwv.get("ttfb", 0.0),
                                    "si": cwv.get("si", 0.0),
                                    "tbt": cwv.get("tbt", 0.0),
                                    "opportunities_json": json.dumps(ps_result.get("opportunities", []), ensure_ascii=False),
                                    "diagnostics_json": json.dumps(ps_result.get("diagnostics", []), ensure_ascii=False),
                                    "strategy": "mobile",
                                }
                                db.save_pagespeed_data(project_id, 0, url, db_data)
                            except Exception:
                                pass
                        elif ps_result and ps_result.get("error"):
                            st.warning(f"분석 실패 ({url}): {ps_result['error']}")
                    except Exception as e:
                        st.warning(f"분석 실패 ({url}): {e}")
                progress.progress((idx + 1) / len(urls_to_analyze))

            if results:
                st.success(f"{len(results)}개 URL 분석 완료!")
                st.rerun()

    # Display saved results
    st.divider()
    st.markdown("#### 📊 분석 결과")

    try:
        ps_results = db.get_pagespeed_data(project_id)
    except Exception:
        ps_results = []

    if not ps_results:
        st.info("아직 분석 결과가 없습니다. 'PageSpeed 분석 실행' 버튼을 눌러주세요.")
        return

    for ps in ps_results:
        url = ps.get("url", "")
        score = ps.get("score", 0)
        score_pct = int(score * 100) if score <= 1 else int(score)

        score_cls = "score-good" if score_pct >= 90 else ("score-medium" if score_pct >= 50 else "score-bad")

        st.markdown(f"##### {url}")

        # Performance score
        st.markdown(f"""
        <div style="text-align:center;margin:12px 0;">
            <div class="score-circle {score_cls}" style="width:100px;height:100px;font-size:1.8rem;margin:0 auto;">{score_pct}</div>
            <p style="color:#94a3b8;font-size:.82rem;">퍼포먼스 점수</p>
        </div>
        """, unsafe_allow_html=True)

        # Core Web Vitals from flat DB fields
        lcp_val = ps.get("lcp", 0.0)
        fid_val = ps.get("fid", 0.0)
        cls_val = ps.get("cls", 0.0)
        ttfb_val = ps.get("ttfb", 0.0)

        cwv_cols = st.columns(4)
        cwv_metrics = [
            ("LCP", f"{lcp_val:.2f}s", google_api.rate_lcp(lcp_val)),
            ("FID/INP", f"{fid_val:.0f}ms", google_api.rate_fid(fid_val)),
            ("CLS", f"{cls_val:.3f}", google_api.rate_cls(cls_val)),
            ("TTFB", f"{ttfb_val:.0f}ms", google_api.rate_ttfb(ttfb_val)),
        ]
        for col, (label, value, rating) in zip(cwv_cols, cwv_metrics):
            rating_cls = "cwv-good" if rating == "good" else ("cwv-needs-improvement" if rating == "needs-improvement" else "cwv-poor")
            rating_kr = "양호" if rating == "good" else ("개선 필요" if rating == "needs-improvement" else "나쁨")
            col.markdown(f"""
            <div class="cwv-card">
                <div class="cwv-value {rating_cls}">{value}</div>
                <div class="cwv-label">{label}</div>
                <div class="cwv-rating {rating_cls}">{rating_kr}</div>
            </div>
            """, unsafe_allow_html=True)

        # Opportunities from JSON field
        opps_raw = ps.get("opportunities_json", "[]")
        try:
            opportunities = json.loads(opps_raw) if isinstance(opps_raw, str) else opps_raw
        except Exception:
            opportunities = []
        if opportunities:
            st.markdown("##### 🎯 개선 기회")
            for opp in opportunities:
                opp_title = opp.get("title", "")
                savings = opp.get("estimated_savings", "")
                priority = opp.get("priority", "")

                try:
                    opp_explain = google_api.explain_pagespeed_opportunity(opp.get("id", ""))
                except Exception:
                    opp_explain = {"description": opp_title, "fix_steps": []}

                priority_badge = ""
                if priority:
                    p_cls = "badge-high" if priority == "high" else ("badge-medium" if priority == "medium" else "badge-low")
                    priority_badge = f'<span class="{p_cls}">{priority.upper()}</span>'

                with st.expander(f"{priority_badge} {opp_title} — 예상 절감: {savings}"):
                    st.markdown(opp_explain.get("description", ""))
                    fix_steps = opp_explain.get("fix_steps", [])
                    if fix_steps:
                        for step in fix_steps:
                            st.markdown(f"- {step}")

        # Diagnostics
        diagnostics = ps.get("diagnostics", [])
        if diagnostics:
            with st.expander("🔬 진단 정보"):
                for diag in diagnostics:
                    st.markdown(f"- **{diag.get('title', '')}**: {diag.get('description', '')}")

        st.divider()

    # PageSpeed history chart
    st.markdown("#### 📈 PageSpeed 점수 히스토리")
    try:
        ps_history = db.get_pagespeed_history(project_id)
        if ps_history:
            history_data = {}
            for h in ps_history:
                url = h.get("url", "")
                date = h.get("analyzed_at", "")[:10]
                score = h.get("performance_score", 0)
                score_pct = int(score * 100) if score <= 1 else int(score)
                if url not in history_data:
                    history_data[url] = []
                history_data[url].append({"날짜": date, "점수": score_pct})

            for url, data in history_data.items():
                st.markdown(f"**{url}**")
                df_h = pd.DataFrame(data).set_index("날짜")
                st.line_chart(df_h)
        else:
            st.info("히스토리 데이터가 아직 없습니다.")
    except Exception:
        st.info("히스토리 데이터가 아직 없습니다.")


# ── 변경 히스토리 탭 ────────────────────────────────────────────────────────
def render_page_changes(project):
    project_id = project["id"]

    st.markdown("### 📝 페이지 변경 히스토리")
    st.caption("크롤링 간 페이지의 Title, Description, H1, 콘텐츠 등의 변경을 추적합니다.")

    try:
        changes = db.get_page_changes(project_id)
    except Exception:
        changes = []

    if not changes:
        st.info("아직 감지된 변경사항이 없습니다. 두 번째 크롤링 이후부터 변경사항이 추적됩니다.")
        return

    # Summary cards
    title_changes = [c for c in changes if c.get("field") == "title"]
    desc_changes = [c for c in changes if c.get("field") == "meta_description"]
    h1_changes = [c for c in changes if c.get("field") == "h1"]
    content_changes = [c for c in changes if c.get("field") == "word_count"]
    other_changes = [c for c in changes if c.get("field") not in ("title", "meta_description", "h1", "word_count")]

    sc1, sc2, sc3, sc4, sc5 = st.columns(5)
    with sc1:
        st.markdown(f"""
        <div class="summary-card">
            <div class="num">{len(changes)}</div>
            <div class="label">총 변경</div>
        </div>
        """, unsafe_allow_html=True)
    with sc2:
        st.markdown(f"""
        <div class="summary-card">
            <div class="num">{len(title_changes)}</div>
            <div class="label">Title 변경</div>
        </div>
        """, unsafe_allow_html=True)
    with sc3:
        st.markdown(f"""
        <div class="summary-card">
            <div class="num">{len(desc_changes)}</div>
            <div class="label">Description 변경</div>
        </div>
        """, unsafe_allow_html=True)
    with sc4:
        st.markdown(f"""
        <div class="summary-card">
            <div class="num">{len(h1_changes)}</div>
            <div class="label">H1 변경</div>
        </div>
        """, unsafe_allow_html=True)
    with sc5:
        st.markdown(f"""
        <div class="summary-card">
            <div class="num">{len(content_changes)}</div>
            <div class="label">콘텐츠 변경</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("")

    # Filters
    fc1, fc2 = st.columns(2)
    with fc1:
        all_urls = sorted(set(c.get("url", "") for c in changes))
        url_filter = st.multiselect("URL 필터", all_urls, key=f"change_url_{project_id}")
    with fc2:
        all_fields = sorted(set(c.get("field", "") for c in changes))
        field_labels = [FIELD_NAMES_KR.get(f, f) for f in all_fields]
        field_filter = st.multiselect("필드 필터", all_fields, format_func=lambda x: FIELD_NAMES_KR.get(x, x), key=f"change_field_{project_id}")

    # Apply filters
    filtered = changes
    if url_filter:
        filtered = [c for c in filtered if c.get("url", "") in url_filter]
    if field_filter:
        filtered = [c for c in filtered if c.get("field", "") in field_filter]

    # Sort by date (newest first)
    filtered = sorted(filtered, key=lambda x: x.get("detected_at", ""), reverse=True)

    # Change timeline
    st.markdown(f"#### 변경 타임라인 ({len(filtered)}건)")
    for change in filtered[:100]:
        url = change.get("url", "")
        field = change.get("field", "")
        field_kr = FIELD_NAMES_KR.get(field, field)
        old_val = change.get("old_value", "")
        new_val = change.get("new_value", "")
        detected = (change.get("detected_at") or "")[:16]

        # Truncate long values
        old_display = (old_val[:80] + "...") if len(str(old_val)) > 80 else old_val
        new_display = (new_val[:80] + "...") if len(str(new_val)) > 80 else new_val

        st.markdown(f"""
        <div class="change-card">
            <div style="display:flex;justify-content:space-between;align-items:center;">
                <span class="change-field">{field_kr}</span>
                <span style="color:#475569;font-size:.72rem;">{detected}</span>
            </div>
            <div class="change-old">이전: {old_display}</div>
            <div class="change-new">변경: {new_display}</div>
            <div class="change-url">{url}</div>
        </div>
        """, unsafe_allow_html=True)

    if len(filtered) > 100:
        st.caption(f"... 외 {len(filtered) - 100}건")


# ── 유틸리티: 사이트 트리 ────────────────────────────────────────────────────
def _build_tree_string(urls, base_domain):
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
# 라우터
# ══════════════════════════════════════════════════════════════════════════════
def main():
    # Google OAuth 콜백 처리
    _handle_google_callback()

    render_sidebar()

    view = st.session_state.view

    # If user is logged in and on landing, redirect to dashboard
    if st.session_state.user and view == "landing":
        view = "dashboard"
        st.session_state.view = "dashboard"

    # If user is not logged in and on protected views, redirect to landing
    protected_views = {"dashboard", "project_new", "project_detail"}
    if not st.session_state.user and view in protected_views:
        view = "landing"
        st.session_state.view = "landing"

    if view == "landing":
        render_landing()
    elif view == "login":
        render_login()
    elif view == "signup":
        render_signup()
    elif view == "dashboard":
        render_dashboard()
    elif view == "project_new":
        render_project_new()
    elif view == "project_detail":
        render_project_detail()
    else:
        render_landing()


if __name__ == "__main__":
    main()
