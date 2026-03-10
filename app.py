#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SEO Diagnostic Pro v4 — Full-featured SEO Diagnostic Tool
5가지 크롤 모드 · 클릭형 대시보드 · 호버 개선 가이드 · 히스토리 추적
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

# ── 설정 ─────────────────────────────────────────────────────────────────────
USER_AGENT = "SEODiagnosticPro/4.0 (+https://seodiagnosticpro.dev/bot)"
REQUEST_TIMEOUT = 15
TITLE_MIN, TITLE_MAX = 30, 60
DESC_MIN, DESC_MAX = 120, 160
THIN_CONTENT_THRESHOLD = 300
MIN_INCOMING_LINKS = 3
PAGESPEED_THRESHOLD = 90
URL_MAX_LENGTH = 100
PAGE_SIZE_WARN = 3 * 1024 * 1024
MAX_EXTERNAL_SCRIPTS = 15
HISTORY_DIR = os.path.expanduser("~/.seo-diagnostic-pro/history")
os.makedirs(HISTORY_DIR, exist_ok=True)

st.set_page_config(page_title="SEO Diagnostic Pro", page_icon="🔍", layout="wide", initial_sidebar_state="expanded")

# ── CSS ──────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
.sf-header{background:linear-gradient(135deg,#1a1a2e 0%,#16213e 50%,#0f3460 100%);padding:12px 20px;border-radius:8px;margin-bottom:16px;display:flex;align-items:center;justify-content:space-between}
.sf-header h1{color:#e94560;font-size:1.4rem;margin:0;font-weight:700}
.sf-header span{color:#a3a3a3;font-size:.85rem}
.crawl-status{background:#0d1117;border:1px solid #30363d;border-radius:6px;padding:10px 16px;font-family:'SF Mono','Consolas',monospace;font-size:.82rem;color:#58a6ff;margin-bottom:8px}
.crawl-status .url{color:#8b949e}.crawl-status .count{color:#3fb950;font-weight:bold}.crawl-status .eta{color:#f0883e}
.summary-card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:14px;text-align:center;transition:all .2s;cursor:default}
.summary-card:hover{border-color:#58a6ff;transform:translateY(-2px)}
.summary-card .num{font-size:1.8rem;font-weight:800;color:#58a6ff}
.summary-card .label{font-size:.78rem;color:#8b949e;margin-top:2px}
.summary-card .hint{font-size:.65rem;color:#484f58;margin-top:4px;opacity:0;transition:opacity .2s}
.summary-card:hover .hint{opacity:1}
.summary-card.red .num{color:#f85149}.summary-card.yellow .num{color:#d29922}.summary-card.green .num{color:#3fb950}
.issue-card{background:#161b22;border:1px solid #30363d;border-radius:6px;padding:10px 14px;margin-bottom:6px;transition:all .2s;cursor:default}
.issue-card:hover{border-color:#58a6ff;background:#1c2333}
.issue-card .issue-fix{display:none;color:#3fb950;font-size:.82rem;margin-top:6px;padding-top:6px;border-top:1px solid #30363d}
.issue-card:hover .issue-fix{display:block}
.issue-card .issue-header{margin-bottom:4px}.issue-card .issue-detail{color:#8b949e;font-size:.85rem}
.issue-card .issue-url{color:#58a6ff;font-size:.8rem;margin-top:2px}
.issue-card.high{border-left:3px solid #f85149}.issue-card.medium{border-left:3px solid #d29922}.issue-card.low{border-left:3px solid #3fb950}
.badge-high{background:#f8514922;color:#f85149;padding:2px 8px;border-radius:4px;font-size:.75rem;font-weight:600}
.badge-medium{background:#d2992222;color:#d29922;padding:2px 8px;border-radius:4px;font-size:.75rem;font-weight:600}
.badge-low{background:#3fb95022;color:#3fb950;padding:2px 8px;border-radius:4px;font-size:.75rem;font-weight:600}
.stDataFrame{font-size:.85rem}
.history-card{background:#0d1117;border:1px solid #30363d;border-radius:6px;padding:8px 12px;margin-bottom:4px;font-size:.8rem}
.history-card .delta-up{color:#f85149}.history-card .delta-down{color:#3fb950}
</style>
""", unsafe_allow_html=True)


# ── 유틸리티 ──────────────────────────────────────────────────────────────────
def normalize_url(url):
    url, _ = urldefrag(url)
    parsed = urlparse(url)
    path = parsed.path.rstrip("/") or "/"
    return f"{parsed.scheme}://{parsed.netloc}{path}" + (f"?{parsed.query}" if parsed.query else "")

def is_same_domain(url, base_domain):
    try: return urlparse(url).netloc == base_domain
    except: return False

def build_session():
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8", "Accept-Encoding": "gzip, deflate, br"})
    return s

def fmt_time(seconds):
    if seconds < 0: return "--:--"
    m, s = divmod(int(seconds), 60)
    return f"{m:02d}:{s:02d}"

# ── History ──────────────────────────────────────────────────────────────────
def save_history(domain, data):
    fn = f"{domain}_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
    with open(os.path.join(HISTORY_DIR, fn), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)

def load_history(domain):
    files = []
    for fn in os.listdir(HISTORY_DIR):
        if fn.startswith(domain.replace(".", "_") + "_") and fn.endswith(".json"):
            try:
                with open(os.path.join(HISTORY_DIR, fn), "r", encoding="utf-8") as f:
                    d = json.load(f)
                d["_fn"] = fn
                files.append(d)
            except: pass
    files.sort(key=lambda x: x.get("generated_at", ""), reverse=True)
    return files

# ── robots.txt / sitemap ─────────────────────────────────────────────────────
def discover_sitemaps(base_url, session):
    sitemaps = []
    try:
        r = session.get(urljoin(base_url, "/robots.txt"), timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            for line in r.text.splitlines():
                if line.strip().lower().startswith("sitemap:"):
                    sitemaps.append(line.split(":", 1)[1].strip())
    except: pass
    if not sitemaps:
        for path in ["/sitemap.xml", "/sitemap_index.xml"]:
            try:
                r = session.head(urljoin(base_url, path), timeout=REQUEST_TIMEOUT)
                if r.status_code == 200:
                    sitemaps.append(urljoin(base_url, path)); break
            except: pass
    return sitemaps

def parse_sitemap(sitemap_url, session, max_pages, base_domain):
    urls = set()
    def _parse(url, depth=0):
        if depth > 3 or len(urls) >= max_pages: return
        try:
            r = session.get(url, timeout=REQUEST_TIMEOUT)
            soup = BeautifulSoup(r.content, "html.parser")
            for sm in soup.find_all("sitemap"):
                loc = sm.find("loc")
                if loc: _parse(loc.text.strip(), depth+1)
            for u in soup.find_all("url"):
                if len(urls) >= max_pages: return
                loc = u.find("loc")
                if loc:
                    n = normalize_url(loc.text.strip())
                    if is_same_domain(n, base_domain): urls.add(n)
        except: pass
    _parse(sitemap_url)
    return urls

# ── Schema Detection ─────────────────────────────────────────────────────────
COMMON_SCHEMA_REQUIRED = {
    "Article": ["headline","author","datePublished"], "Product": ["name","image"],
    "LocalBusiness": ["name","address"], "Organization": ["name","url"],
    "BreadcrumbList": ["itemListElement"], "FAQPage": ["mainEntity"],
    "WebSite": ["name","url"], "Person": ["name"],
}

def detect_schema(soup, url):
    info = {"json_ld":[],"json_ld_types":[],"microdata_types":[],"rdfa_types":[],"all_types":[],"has_schema":False,"validation_issues":[]}
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            items = data if isinstance(data, list) else [data]
            expanded = []
            for item in items:
                if isinstance(item, dict) and "@graph" in item: expanded.extend(item["@graph"])
                else: expanded.append(item)
            for item in expanded:
                if not isinstance(item, dict): continue
                st_type = item.get("@type", "Unknown")
                types = st_type if isinstance(st_type, list) else [st_type]
                info["json_ld_types"].extend(types)
                info["json_ld"].append(item)
                for t in types:
                    if t in COMMON_SCHEMA_REQUIRED:
                        for prop in COMMON_SCHEMA_REQUIRED[t]:
                            if prop not in item: info["validation_issues"].append(f"{t}: '{prop}' 누락")
        except: pass
    for el in soup.find_all(attrs={"itemtype": True}):
        itype = el.get("itemtype","").rstrip("/").split("/")[-1]
        if itype: info["microdata_types"].append(itype)
    for el in soup.find_all(attrs={"typeof": True}):
        for t in el.get("typeof","").split():
            info["rdfa_types"].append(t.split(":")[-1] if ":" in t else t)
    info["all_types"] = list(set(info["json_ld_types"]+info["microdata_types"]+info["rdfa_types"]))
    info["has_schema"] = len(info["all_types"]) > 0
    return info

# ── E-E-A-T Detection ────────────────────────────────────────────────────────
SOCIAL_PATTERNS = {"facebook":re.compile(r"facebook\.com/",re.I),"twitter":re.compile(r"(twitter|x)\.com/",re.I),"linkedin":re.compile(r"linkedin\.com/",re.I),"youtube":re.compile(r"youtube\.com/",re.I),"instagram":re.compile(r"instagram\.com/",re.I)}

def detect_eeat(soup, schema_info, url):
    e = {"has_author":False,"author_name":"","has_published_date":False,"has_modified_date":False,"has_about_link":False,"has_contact_link":False,"has_privacy_link":False,"has_terms_link":False,"has_org_schema":False,"social_links":[],"has_breadcrumb":False,"has_reviews_schema":False}
    am = soup.find("meta", attrs={"name":re.compile(r"^author$",re.I)})
    if am and am.get("content"): e["has_author"]=True; e["author_name"]=am["content"].strip()
    for item in schema_info.get("json_ld",[]):
        if isinstance(item,dict):
            if item.get("@type")=="Person": e["has_author"]=True; e["author_name"]=item.get("name",e["author_name"])
            if "author" in item:
                e["has_author"]=True
                a=item["author"]
                if isinstance(a,dict): e["author_name"]=a.get("name",e["author_name"])
                elif isinstance(a,str): e["author_name"]=a
            if item.get("datePublished"): e["has_published_date"]=True
            if item.get("dateModified"): e["has_modified_date"]=True
    if not e["has_author"]:
        bl = soup.find(class_=re.compile(r"(author|byline|writer)",re.I))
        if bl: e["has_author"]=True; e["author_name"]=bl.get_text(strip=True)[:80]
        al = soup.find("a",rel="author")
        if al: e["has_author"]=True; e["author_name"]=al.get_text(strip=True)[:80]
    if soup.find("meta",attrs={"property":re.compile(r"article:published_time",re.I)}): e["has_published_date"]=True
    if soup.find("meta",attrs={"property":re.compile(r"article:modified_time",re.I)}): e["has_modified_date"]=True
    if not e["has_published_date"]:
        for tt in soup.find_all("time"):
            if tt.get("datetime"): e["has_published_date"]=True; break
    for a in soup.find_all("a",href=True):
        h=a["href"].lower(); t=a.get_text(strip=True).lower(); c=h+" "+t
        if any(k in c for k in ["about","about-us","회사소개","소개"]): e["has_about_link"]=True
        if any(k in c for k in ["contact","contact-us","문의","연락처"]): e["has_contact_link"]=True
        if any(k in c for k in ["privacy","privacy-policy","개인정보","개인정보처리방침"]): e["has_privacy_link"]=True
        if any(k in c for k in ["terms","terms-of-service","tos","이용약관"]): e["has_terms_link"]=True
        for plat,pat in SOCIAL_PATTERNS.items():
            if pat.search(a.get("href","")) and plat not in e["social_links"]: e["social_links"].append(plat)
    for t in schema_info.get("all_types",[]):
        if t in ("Organization","Corporation","LocalBusiness"): e["has_org_schema"]=True; break
    if "BreadcrumbList" in schema_info.get("all_types",[]): e["has_breadcrumb"]=True
    elif soup.find(attrs={"class":re.compile(r"breadcrumb",re.I)}): e["has_breadcrumb"]=True
    elif soup.find("nav",attrs={"aria-label":re.compile(r"breadcrumb",re.I)}): e["has_breadcrumb"]=True
    for t in schema_info.get("all_types",[]):
        if t in ("Review","AggregateRating"): e["has_reviews_schema"]=True; break
    return e

# ── Technical SEO ────────────────────────────────────────────────────────────
def detect_technical_seo(soup, response, url):
    t = {"meta_robots":"","x_robots_tag":"","is_noindex":False,"is_nofollow":False,"hreflang_tags":[],"hreflang_self_ref":False,"og_title":"","og_description":"","og_image":"","og_url":"","og_type":"","twitter_card":"","twitter_title":"","twitter_description":"","twitter_image":"","has_viewport":False,"viewport_content":"","charset":"","lang":"","content_type":"","is_redirect":False,"redirect_url":"","url_length":len(url),"has_query_params":bool(urlparse(url).query),"headings":{"h1":0,"h2":0,"h3":0,"h4":0,"h5":0,"h6":0},"heading_hierarchy_ok":True,"heading_issues":[],"has_iframes":False,"iframe_count":0,"has_noscript":False,"canonical":"","rel_next":"","rel_prev":""}
    rm = soup.find("meta",attrs={"name":re.compile(r"^robots$",re.I)})
    if rm and rm.get("content"):
        t["meta_robots"]=rm["content"].strip(); cl=t["meta_robots"].lower()
        t["is_noindex"]="noindex" in cl; t["is_nofollow"]="nofollow" in cl
    xr=response.headers.get("X-Robots-Tag",""); t["x_robots_tag"]=xr
    if "noindex" in xr.lower(): t["is_noindex"]=True
    for link in soup.find_all("link",rel="alternate"):
        hl=link.get("hreflang")
        if hl:
            href=link.get("href",""); t["hreflang_tags"].append({"lang":hl,"href":href})
            if normalize_url(href)==normalize_url(url): t["hreflang_self_ref"]=True
    for prop,key in {"og:title":"og_title","og:description":"og_description","og:image":"og_image","og:url":"og_url","og:type":"og_type"}.items():
        tag=soup.find("meta",attrs={"property":prop})
        if tag and tag.get("content"): t[key]=tag["content"].strip()
    for name,key in {"twitter:card":"twitter_card","twitter:title":"twitter_title","twitter:description":"twitter_description","twitter:image":"twitter_image"}.items():
        tag=soup.find("meta",attrs={"name":name}) or soup.find("meta",attrs={"property":name})
        if tag and tag.get("content"): t[key]=tag["content"].strip()
    vp=soup.find("meta",attrs={"name":re.compile(r"^viewport$",re.I)})
    if vp and vp.get("content"): t["has_viewport"]=True; t["viewport_content"]=vp["content"].strip()
    cm=soup.find("meta",charset=True)
    if cm: t["charset"]=cm["charset"]
    else:
        ct=soup.find("meta",attrs={"http-equiv":re.compile(r"content-type",re.I)})
        if ct and ct.get("content"):
            m=re.search(r"charset=([^\s;]+)",ct["content"],re.I)
            if m: t["charset"]=m.group(1)
    html_tag=soup.find("html")
    if html_tag: t["lang"]=html_tag.get("lang","")
    t["content_type"]=response.headers.get("Content-Type","")
    if response.history: t["is_redirect"]=True; t["redirect_url"]=response.url
    for lv in range(1,7): t["headings"][f"h{lv}"]=len(soup.find_all(f"h{lv}"))
    if t["headings"]["h1"]==0: t["heading_hierarchy_ok"]=False; t["heading_issues"].append("H1 없음")
    elif t["headings"]["h1"]>1: t["heading_issues"].append(f"H1 {t['headings']['h1']}개")
    found=[i for i in range(1,7) if t["headings"][f"h{i}"]>0]
    for i in range(len(found)-1):
        if found[i+1]-found[i]>1: t["heading_hierarchy_ok"]=False; t["heading_issues"].append(f"H{found[i]}→H{found[i+1]} 건너뜀")
    t["has_iframes"]=len(soup.find_all("iframe"))>0; t["iframe_count"]=len(soup.find_all("iframe"))
    t["has_noscript"]=soup.find("noscript") is not None
    cn=soup.find("link",attrs={"rel":"canonical"})
    if cn and cn.get("href"): t["canonical"]=cn["href"].strip()
    rn=soup.find("link",attrs={"rel":"next"})
    if rn and rn.get("href"): t["rel_next"]=rn["href"]
    rp=soup.find("link",attrs={"rel":"prev"})
    if rp and rp.get("href"): t["rel_prev"]=rp["href"]
    return t

# ── Security ─────────────────────────────────────────────────────────────────
def detect_security(response, soup, url):
    s = {"is_https":urlparse(url).scheme=="https","has_hsts":False,"hsts_value":"","has_xcto":False,"has_xfo":False,"has_csp":False,"csp_value":"","has_referrer_policy":False,"has_permissions_policy":False,"mixed_content":[],"mixed_content_count":0}
    h=response.headers
    if h.get("Strict-Transport-Security"): s["has_hsts"]=True; s["hsts_value"]=h["Strict-Transport-Security"]
    if h.get("X-Content-Type-Options"): s["has_xcto"]=True
    if h.get("X-Frame-Options"): s["has_xfo"]=True
    csp=h.get("Content-Security-Policy","")
    if csp: s["has_csp"]=True; s["csp_value"]=csp[:200]
    if h.get("Referrer-Policy"): s["has_referrer_policy"]=True
    if h.get("Permissions-Policy"): s["has_permissions_policy"]=True
    if s["is_https"]:
        for tag,attr in [("img","src"),("script","src"),("link","href"),("iframe","src"),("video","src"),("audio","src"),("source","src"),("embed","src"),("object","data")]:
            for el in soup.find_all(tag):
                v=el.get(attr,"")
                if v.startswith("http://"): s["mixed_content"].append(f"<{tag}> {v[:100]}")
        s["mixed_content_count"]=len(s["mixed_content"])
    return s

# ── Performance ──────────────────────────────────────────────────────────────
def detect_performance(soup, response):
    p = {"html_size_bytes":len(response.content),"html_size_kb":round(len(response.content)/1024,1),"external_scripts":len(soup.find_all("script",src=True)),"external_stylesheets":len(soup.find_all("link",rel="stylesheet")),"inline_css_count":0,"inline_js_count":0,"image_count":0,"images_no_lazy":0,"has_compression":False,"compression_type":""}
    p["inline_css_count"]=len(soup.find_all("style"))
    p["inline_js_count"]=len([s for s in soup.find_all("script") if not s.get("src")])
    imgs=soup.find_all("img"); p["image_count"]=len(imgs)
    p["images_no_lazy"]=sum(1 for img in imgs if img.get("loading","").lower()!="lazy")
    enc=response.headers.get("Content-Encoding","")
    if enc: p["has_compression"]=True; p["compression_type"]=enc
    return p

# ── Content Quality ──────────────────────────────────────────────────────────
def detect_content_quality(soup, url, base_domain):
    c={"external_links":[],"external_links_count":0,"nofollow_links_count":0,"total_links_count":0,"nofollow_ratio":0.0,"text_to_html_ratio":0.0,"word_count":0}
    body=soup.find("body"); html_text=str(soup); body_text=body.get_text(separator=" ",strip=True) if body else ""
    c["word_count"]=len(body_text.split())
    if len(html_text)>0: c["text_to_html_ratio"]=round(len(body_text)/len(html_text)*100,1)
    all_a=soup.find_all("a",href=True); c["total_links_count"]=len(all_a)
    nf=0; ext=[]
    for a in all_a:
        rel=a.get("rel",[]); rel=rel.split() if isinstance(rel,str) else rel
        if "nofollow" in [r.lower() for r in rel]: nf+=1
        full=urljoin(url,a["href"].strip()); p=urlparse(full)
        if p.scheme in ("http","https") and p.netloc and p.netloc!=base_domain: ext.append(full)
    c["external_links"]=ext[:50]; c["external_links_count"]=len(ext); c["nofollow_links_count"]=nf
    if c["total_links_count"]>0: c["nofollow_ratio"]=round(nf/c["total_links_count"]*100,1)
    return c

# ── Page Analysis ────────────────────────────────────────────────────────────
def analyze_page(url, session, crawl_depth=0):
    r_dict = {"URL":url,"Status":None,"Title":"","Title Len":0,"Meta Desc":"","Desc Len":0,"H1":"","H1 Len":0,"Canonical":"","Words":0,"Images":0,"Alt Missing":0,"Outlinks":0,"Load (s)":0.0,"Error":"","Crawl Depth":crawl_depth,"_internal_links":[],"_schema":{},"Schema Types":"","Has Schema":False,"_eeat":{},"Author":"","Has Date":False,"_tech":{},"Meta Robots":"","Noindex":False,"Viewport":False,"Lang":"","OG Image":"","Twitter Card":"","Hreflang":0,"_security":{},"HTTPS":False,"HSTS":False,"Mixed Content":0,"_perf":{},"HTML KB":0,"Ext Scripts":0,"Ext CSS":0,"Img No Lazy":0,"Compression":"","_content":{},"Ext Links":0,"Text/HTML %":0.0,"H1s":0,"H2s":0,"H3s":0,"H4s":0,"H5s":0,"H6s":0}
    try:
        start=time.time(); r=session.get(url,timeout=REQUEST_TIMEOUT)
        r_dict["Load (s)"]=round(time.time()-start,2); r_dict["Status"]=r.status_code
        if r.status_code!=200 and not (300<=r.status_code<400): return r_dict
        soup=BeautifulSoup(r.content,"html.parser")
        tag=soup.find("title")
        if tag and tag.string: r_dict["Title"]=tag.string.strip(); r_dict["Title Len"]=len(r_dict["Title"])
        meta=soup.find("meta",attrs={"name":re.compile(r"^description$",re.I)})
        if meta and meta.get("content"): r_dict["Meta Desc"]=meta["content"].strip(); r_dict["Desc Len"]=len(r_dict["Meta Desc"])
        h1=soup.find("h1")
        if h1: r_dict["H1"]=h1.get_text(strip=True); r_dict["H1 Len"]=len(r_dict["H1"])
        cn=soup.find("link",attrs={"rel":"canonical"})
        if cn and cn.get("href"): r_dict["Canonical"]=cn["href"].strip()
        body=soup.find("body")
        if body: r_dict["Words"]=len(body.get_text(separator=" ",strip=True).split())
        imgs=soup.find_all("img"); r_dict["Images"]=len(imgs)
        r_dict["Alt Missing"]=sum(1 for img in imgs if not img.get("alt","").strip())
        bd=urlparse(url).netloc; il=set()
        for a in soup.find_all("a",href=True):
            full=urljoin(url,a["href"].strip()); n=normalize_url(full)
            if is_same_domain(n,bd): il.add(n)
        r_dict["_internal_links"]=sorted(il); r_dict["Outlinks"]=len(il)
        si=detect_schema(soup,url); r_dict["_schema"]=si; r_dict["Schema Types"]=", ".join(si["all_types"]); r_dict["Has Schema"]=si["has_schema"]
        ee=detect_eeat(soup,si,url); r_dict["_eeat"]=ee; r_dict["Author"]=ee["author_name"][:40]; r_dict["Has Date"]=ee["has_published_date"] or ee["has_modified_date"]
        tc=detect_technical_seo(soup,r,url); r_dict["_tech"]=tc; r_dict["Meta Robots"]=tc["meta_robots"]; r_dict["Noindex"]=tc["is_noindex"]; r_dict["Viewport"]=tc["has_viewport"]; r_dict["Lang"]=tc["lang"]; r_dict["OG Image"]="Y" if tc["og_image"] else "N"; r_dict["Twitter Card"]=tc["twitter_card"] or ""; r_dict["Hreflang"]=len(tc["hreflang_tags"])
        for i in range(1,7): r_dict[f"H{i}s"]=tc["headings"][f"h{i}"]
        sc=detect_security(r,soup,url); r_dict["_security"]=sc; r_dict["HTTPS"]=sc["is_https"]; r_dict["HSTS"]=sc["has_hsts"]; r_dict["Mixed Content"]=sc["mixed_content_count"]
        pf=detect_performance(soup,r); r_dict["_perf"]=pf; r_dict["HTML KB"]=pf["html_size_kb"]; r_dict["Ext Scripts"]=pf["external_scripts"]; r_dict["Ext CSS"]=pf["external_stylesheets"]; r_dict["Img No Lazy"]=pf["images_no_lazy"]; r_dict["Compression"]=pf["compression_type"] or "None"
        cq=detect_content_quality(soup,url,bd); r_dict["_content"]=cq; r_dict["Ext Links"]=cq["external_links_count"]; r_dict["Text/HTML %"]=cq["text_to_html_ratio"]
    except Exception as e: r_dict["Error"]=str(e)[:120]
    return r_dict

# ── PageSpeed ────────────────────────────────────────────────────────────────
def get_pagespeed_score(url, api_key):
    if not api_key: return None
    try:
        r=requests.get("https://www.googleapis.com/pagespeedonline/v5/runPagespeed",params={"url":url,"strategy":"mobile","key":api_key},timeout=60)
        return int(r.json()["lighthouseResult"]["categories"]["performance"]["score"]*100)
    except: return None

# ── Site Tree ────────────────────────────────────────────────────────────────
def build_tree_string(urls, base_domain):
    tree={}
    for url in sorted(urls):
        parts=[p for p in urlparse(url).path.split("/") if p] or ["(root)"]
        node=tree
        for part in parts: node=node.setdefault(part,{})
    lines=[f"{base_domain}/"]
    def _r(node,prefix=""):
        items=sorted(node.keys())
        for i,name in enumerate(items):
            last=i==len(items)-1; lines.append(f"{prefix}{'└── ' if last else '├── '}{name}")
            _r(node[name],prefix+("    " if last else "│   "))
    _r(tree); return "\n".join(lines)

# ── Diagnostics ──────────────────────────────────────────────────────────────
def run_diagnostics(pages, incoming_map, pagespeed_scores):
    issues=[]
    title_map=defaultdict(list)
    for p in pages:
        if p["Title"]: title_map[p["Title"]].append(p["URL"])
    for title,urls in title_map.items():
        if len(urls)>1: issues.append({"type":"중복 Title","severity":"HIGH","detail":f'"{title[:40]}..." → {len(urls)}개 중복',"pages":urls,"fix":"각 페이지마다 고유한 Title을 작성하세요."})
    hp=[p for p in pages if urlparse(p["URL"]).path in ("/","")]
    for h in hp:
        st_types=h.get("_schema",{}).get("all_types",[])
        if not any(t in ("Organization","Corporation","LocalBusiness") for t in st_types):
            issues.append({"type":"Organization 스키마 없음","severity":"MEDIUM","detail":"홈페이지에 Organization 스키마 없음","pages":[h["URL"]],"fix":"홈페이지에 Organization 스키마를 추가하세요."})
    for p in pages:
        url=p["URL"]; tl=p["Title Len"]; dl=p["Desc Len"]; ok=p["Status"]==200
        if tl==0: issues.append({"type":"Title 없음","severity":"HIGH","detail":"Title 태그 없음","pages":[url],"fix":"고유하고 설명적인 Title 태그를 추가하세요."})
        elif tl<TITLE_MIN or tl>TITLE_MAX: issues.append({"type":"Title 길이","severity":"MEDIUM","detail":f"Title {tl}자 → {TITLE_MIN}~{TITLE_MAX}자 권장","pages":[url],"fix":f"현재 {tl}자. {TITLE_MIN}~{TITLE_MAX}자로 조절하세요."})
        if dl==0: issues.append({"type":"Description 없음","severity":"MEDIUM","detail":"Meta Description 없음","pages":[url],"fix":"핵심 키워드와 CTA를 포함한 설명을 추가하세요."})
        elif dl<DESC_MIN or dl>DESC_MAX: issues.append({"type":"Description 길이","severity":"MEDIUM","detail":f"Description {dl}자 → {DESC_MIN}~{DESC_MAX}자 권장","pages":[url],"fix":f"현재 {dl}자. {DESC_MIN}~{DESC_MAX}자로 조절하세요."})
        if p["H1 Len"]==0: issues.append({"type":"H1 없음","severity":"HIGH","detail":"H1 태그 없음","pages":[url],"fix":"페이지당 하나의 H1 태그 + 핵심 키워드 포함."})
        if p["Words"]<THIN_CONTENT_THRESHOLD and ok: issues.append({"type":"Thin Content","severity":"MEDIUM","detail":f"단어 수 {p['Words']}개 (최소 {THIN_CONTENT_THRESHOLD} 권장)","pages":[url],"fix":"FAQ, 관련 정보, 사용자 후기 등으로 콘텐츠 보강."})
        if p["Alt Missing"]>0: issues.append({"type":"Alt 누락","severity":"LOW","detail":f"Alt 없는 이미지 {p['Alt Missing']}/{p['Images']}개","pages":[url],"fix":"모든 이미지에 설명적인 alt 텍스트를 추가하세요."})
        if p["Status"] and p["Status"]>=400: issues.append({"type":f"HTTP {p['Status']}","severity":"HIGH","detail":f"HTTP {p['Status']} 에러","pages":[url],"fix":"깨진 링크 수정 또는 301 리다이렉트 설정."})
        inc=incoming_map.get(url,0)
        if inc<MIN_INCOMING_LINKS and ok: issues.append({"type":"Inlinks 부족","severity":"MEDIUM","detail":f"들어오는 링크 {inc}개 (최소 {MIN_INCOMING_LINKS} 권장)","pages":[url],"fix":"내부 링크 추가 (실크 로드 전략 권장)."})
        sc=pagespeed_scores.get(url)
        if sc is not None and sc<PAGESPEED_THRESHOLD: issues.append({"type":"PageSpeed 낮음","severity":"HIGH" if sc<50 else "MEDIUM","detail":f"Mobile 점수: {sc}/100","pages":[url],"fix":"이미지 최적화, Lazy Loading, 렌더링 차단 리소스 제거."})
        if p["Load (s)"]>3.0: issues.append({"type":"느린 로딩","severity":"MEDIUM","detail":f"로드 시간 {p['Load (s)']}초","pages":[url],"fix":"서버 응답 시간, CDN, 캐싱 설정 점검."})
        schema=p.get("_schema",{})
        if ok and not schema.get("has_schema"): issues.append({"type":"구조화 데이터 없음","severity":"MEDIUM","detail":"JSON-LD/Microdata/RDFa 없음","pages":[url],"fix":"구조화된 데이터를 추가하세요 (JSON-LD 권장)"})
        if schema.get("has_schema") and ok:
            if "BreadcrumbList" not in schema.get("all_types",[]): issues.append({"type":"BreadcrumbList 없음","severity":"LOW","detail":"BreadcrumbList 스키마 없음","pages":[url],"fix":"BreadcrumbList 스키마로 사이트 구조를 표현하세요"})
            for vi in schema.get("validation_issues",[]): issues.append({"type":"스키마 속성 누락","severity":"LOW","detail":vi,"pages":[url],"fix":f"스키마 필수 속성을 추가하세요: {vi}"})
        eeat=p.get("_eeat",{})
        if ok:
            if not eeat.get("has_author"): issues.append({"type":"저자 정보 없음","severity":"LOW","detail":"author 메타/스키마/바이라인 없음","pages":[url],"fix":"E-E-A-T: 저자 정보를 추가하세요"})
            if not eeat.get("has_published_date") and not eeat.get("has_modified_date"): issues.append({"type":"날짜 정보 없음","severity":"LOW","detail":"게시일/수정일 없음","pages":[url],"fix":"E-E-A-T: 게시일/수정일을 표시하세요"})
            if not eeat.get("has_about_link") and not eeat.get("has_contact_link"): issues.append({"type":"About/Contact 없음","severity":"LOW","detail":"About/Contact 링크 없음","pages":[url],"fix":"E-E-A-T: About/Contact 페이지 링크를 추가하세요"})
            if not eeat.get("has_privacy_link"): issues.append({"type":"개인정보처리방침 없음","severity":"LOW","detail":"Privacy Policy 링크 없음","pages":[url],"fix":"E-E-A-T: 개인정보 처리방침 링크를 추가하세요"})
        tech=p.get("_tech",{})
        if ok:
            if tech.get("is_noindex"): issues.append({"type":"Noindex 설정","severity":"HIGH","detail":f"meta robots: {tech.get('meta_robots','')}","pages":[url],"fix":"이 페이지가 noindex 설정되어 있습니다"})
            if not tech.get("og_image"): issues.append({"type":"OG Image 없음","severity":"MEDIUM","detail":"og:image 없음","pages":[url],"fix":"SNS 공유를 위해 Open Graph 이미지를 설정하세요"})
            if not tech.get("has_viewport"): issues.append({"type":"Viewport 없음","severity":"HIGH","detail":"viewport 메타 없음","pages":[url],"fix":"모바일 호환성을 위해 viewport 메타 태그를 추가하세요"})
            if not tech.get("twitter_card"): issues.append({"type":"Twitter Card 없음","severity":"LOW","detail":"twitter:card 없음","pages":[url],"fix":"Twitter Card 메타 태그를 추가하세요"})
            if tech.get("url_length",0)>URL_MAX_LENGTH: issues.append({"type":"URL 길이 초과","severity":"MEDIUM","detail":f"URL {tech['url_length']}자","pages":[url],"fix":"URL 길이를 100자 이내로 줄이세요"})
            if not tech.get("heading_hierarchy_ok"):
                for hi in tech.get("heading_issues",[]): issues.append({"type":"Heading 계층 문제","severity":"MEDIUM","detail":hi,"pages":[url],"fix":"H1→H2→H3 순서로 계층 구조를 지키세요"})
        sec=p.get("_security",{})
        if ok:
            if not sec.get("is_https"): issues.append({"type":"HTTPS 미사용","severity":"HIGH","detail":"HTTP 사용 중","pages":[url],"fix":"HTTPS로 전환하세요"})
            elif not sec.get("has_hsts"): issues.append({"type":"HSTS 없음","severity":"MEDIUM","detail":"HSTS 헤더 없음","pages":[url],"fix":"HSTS 헤더를 추가하세요"})
            if sec.get("mixed_content_count",0)>0: issues.append({"type":"Mixed Content","severity":"HIGH","detail":f"HTTP 리소스 {sec['mixed_content_count']}개","pages":[url],"fix":"HTTPS 페이지에서 HTTP 리소스를 로드하고 있습니다"})
        perf=p.get("_perf",{})
        if ok:
            if perf.get("html_size_bytes",0)>PAGE_SIZE_WARN: issues.append({"type":"페이지 용량 과다","severity":"HIGH","detail":f"HTML {round(perf['html_size_bytes']/(1024*1024),1)}MB","pages":[url],"fix":"페이지 용량이 큽니다. 최적화하세요"})
            if perf.get("images_no_lazy",0)>3: issues.append({"type":"Lazy Loading 없음","severity":"LOW","detail":f"lazy loading 없는 이미지 {perf['images_no_lazy']}개","pages":[url],"fix":"이미지에 loading='lazy' 속성을 추가하세요"})
            if perf.get("external_scripts",0)>MAX_EXTERNAL_SCRIPTS: issues.append({"type":"외부 스크립트 과다","severity":"MEDIUM","detail":f"외부 스크립트 {perf['external_scripts']}개","pages":[url],"fix":"외부 스크립트가 많습니다. 번들링을 고려하세요"})
            if not perf.get("has_compression"): issues.append({"type":"압축 없음","severity":"MEDIUM","detail":"Gzip/Brotli 미적용","pages":[url],"fix":"Gzip/Brotli 압축을 활성화하세요"})
    issues.sort(key=lambda x: {"HIGH":0,"MEDIUM":1,"LOW":2}.get(x["severity"],9))
    return issues


# ══════════════════════════════════════════════════════════════════════════════
# UI
# ══════════════════════════════════════════════════════════════════════════════

st.markdown('<div class="sf-header"><h1>🔍 SEO Diagnostic Pro</h1><span>Comprehensive SEO Diagnostic · v4.0</span></div>', unsafe_allow_html=True)

# ── 상단 입력바 (5가지 모드) ──────────────────────────────────────────────────
with st.container():
    col_url, col_mode, col_max, col_delay, col_btn = st.columns([3, 2, 1, 1, 1])
    with col_url:
        base_url = st.text_input("URL", placeholder="https://example.com", label_visibility="collapsed")
    with col_mode:
        mode = st.selectbox("모드", [
            "🔍 전체 크롤링",
            "🗺️ 사이트맵 기반",
            "📂 특정 경로 크롤링",
            "📋 URL 목록 입력",
            "🔄 사이트맵 + 크롤링 혼합",
        ], label_visibility="collapsed", help="크롤링 모드를 선택하세요")
    with col_max:
        max_pages = st.number_input("Max", min_value=5, max_value=10000, value=200, step=10, label_visibility="collapsed", help="최대 수집 페이지 수")
    with col_delay:
        crawl_delay = st.number_input("Delay", min_value=0.1, max_value=3.0, value=0.3, step=0.1, label_visibility="collapsed", help="크롤링 딜레이(초)")
    with col_btn:
        run_btn = st.button("▶ Start", use_container_width=True, type="primary")

# 모드별 추가 입력
path_filter = ""
url_list_text = ""
if "특정 경로" in mode:
    path_filter = st.text_input("📂 크롤링 경로 (예: /blog/)", placeholder="/blog/", help="이 경로 하위의 URL만 수집합니다")
if "URL 목록" in mode:
    url_list_text = st.text_area("📋 URL 목록 (한 줄에 하나씩)", height=120, placeholder="https://example.com/page1\nhttps://example.com/page2")

# ── 사이드바 ─────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ⚙️ 설정")
    api_key = st.text_input("PageSpeed API 키 (선택)", placeholder="비워두면 스킵", help="Google PageSpeed Insights API 키")
    st.divider()

    st.markdown("### 📖 크롤링 모드 설명")
    st.caption("🔍 **전체 크롤링** — 루트에서 모든 링크를 따라감")
    st.caption("🗺️ **사이트맵** — sitemap.xml 기반 수집")
    st.caption("📂 **특정 경로** — /blog/ 등 특정 섹션만 집중 크롤링")
    st.caption("📋 **URL 목록** — 직접 URL을 붙여넣기")
    st.caption("🔄 **혼합** — 사이트맵 + 크롤링 (가장 철저)")
    st.divider()

    st.markdown("### 📌 진단 기준")
    st.caption(f"Title: {TITLE_MIN}~{TITLE_MAX}자 · Desc: {DESC_MIN}~{DESC_MAX}자")
    st.caption(f"Thin Content: {THIN_CONTENT_THRESHOLD}단어 미만")
    st.caption(f"내부 링크 최소: {MIN_INCOMING_LINKS}개 · PageSpeed: {PAGESPEED_THRESHOLD}점")
    st.divider()

    # 히스토리
    st.markdown("### 📈 히스토리")
    if base_url:
        domain_key = urlparse(base_url if base_url.startswith("http") else "https://"+base_url).netloc.replace(".","_")
        history = load_history(domain_key)
        if history:
            st.caption(f"이전 분석 {len(history)}건")
            for h in history[:5]:
                ts = h.get("generated_at","")[:16]
                s = h.get("summary",{})
                st.markdown(f'<div class="history-card">{ts}<br>📄{s.get("total_pages",0)}p · 🔴{s.get("high",0)} 🟡{s.get("medium",0)} 🟢{s.get("low",0)}</div>', unsafe_allow_html=True)
        else:
            st.caption("이전 분석 기록 없음")

# ── 대기 화면 ─────────────────────────────────────────────────────────────────
if not run_btn:
    st.markdown("---")
    c1,c2,c3,c4 = st.columns(4)
    c1.markdown("#### 📊 실시간 크롤링\n수집과 동시에 테이블 업데이트")
    c2.markdown("#### 🏷️ Schema & E-E-A-T\n구조화 데이터, 신뢰 신호 분석")
    c3.markdown("#### ⚙️ Technical & Security\nMeta Robots, OG, HTTPS, HSTS")
    c4.markdown("#### 📈 Performance\n페이지 크기, 스크립트, 압축")
    st.stop()

# ── 입력 검증 ────────────────────────────────────────────────────────────────
if not base_url and "URL 목록" not in mode:
    st.error("URL을 입력해주세요."); st.stop()

if not base_url.startswith(("http://","https://")): base_url = "https://"+base_url
base_url = base_url.rstrip("/")
base_domain = urlparse(base_url).netloc
session = build_session()

# ══════════════════════════════════════════════════════════════════════════════
# 크롤링
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("---")
stat_cols = st.columns(5)
ph_crawled=stat_cols[0].empty(); ph_queued=stat_cols[1].empty()
ph_elapsed=stat_cols[2].empty(); ph_eta=stat_cols[3].empty(); ph_speed=stat_cols[4].empty()
progress_bar = st.progress(0.0)
status_line = st.empty()
st.markdown("### 📋 수집 데이터 (실시간)")
table_ph = st.empty()

pages=[]; crawl_start=time.time(); depth_map={}

def update_live(pages_so_far, cur_url, total, queue_sz):
    cnt=len(pages_so_far); elapsed=time.time()-crawl_start
    spd=cnt/elapsed if elapsed>0 else 0; eta=(total-cnt)/spd if spd>0 and cnt<total else 0
    pct=min(cnt/total,1.0) if total>0 else 0; progress_bar.progress(pct)
    ph_crawled.metric("Crawled",f"{cnt}/{total}"); ph_queued.metric("Queue",queue_sz)
    ph_elapsed.metric("Elapsed",fmt_time(elapsed)); ph_eta.metric("ETA",fmt_time(eta)); ph_speed.metric("Speed",f"{spd:.1f} pg/s")
    short=cur_url if len(cur_url)<80 else cur_url[:77]+"..."
    status_line.markdown(f'<div class="crawl-status"><span class="count">[{cnt}/{total}]</span> <span class="url">{short}</span> <span class="eta">ETA {fmt_time(eta)}</span> ({pct*100:.0f}%)</div>',unsafe_allow_html=True)
    if pages_so_far:
        dc=["URL","Status","Title","Title Len","Desc Len","H1","Words","Outlinks","Schema Types","HTTPS","HTML KB","Load (s)"]
        df=pd.DataFrame(pages_so_far); av=[c for c in dc if c in df.columns]; d=df[av].copy()
        d["URL"]=d["URL"].apply(lambda x: urlparse(x).path or "/")
        table_ph.dataframe(d,use_container_width=True,hide_index=True,height=400)

def crawl_bfs(start_url, max_p, delay, path_prefix=""):
    visited=set(); queue=[normalize_url(start_url)]; depth_map[normalize_url(start_url)]=0; results=[]
    while queue and len(visited)<max_p:
        url=queue.pop(0)
        if url in visited: continue
        if path_prefix and not urlparse(url).path.startswith(path_prefix): continue
        visited.add(url); cd=depth_map.get(url,0)
        page=analyze_page(url,session,cd); results.append(page)
        for link in page["_internal_links"]:
            if link not in visited:
                if not path_prefix or urlparse(link).path.startswith(path_prefix):
                    queue.append(link)
                    if link not in depth_map: depth_map[link]=cd+1
        update_live(results,url,max_p,len(queue)); time.sleep(delay)
    return results

def collect_sitemap_pages(max_p, delay):
    sitemaps=discover_sitemaps(base_url,session); results=[]
    if not sitemaps: return results
    all_urls=set()
    for sm in sitemaps: all_urls|=parse_sitemap(sm,session,max_p,base_domain)
    all_urls=sorted(all_urls)[:max_p]; total=len(all_urls)
    status_line.markdown(f'<div class="crawl-status">사이트맵에서 <span class="count">{total}</span>개 URL 발견</div>',unsafe_allow_html=True)
    for i,url in enumerate(all_urls):
        page=analyze_page(url,session,0); results.append(page)
        update_live(results,url,total,total-i-1); time.sleep(delay)
    return results

# 모드별 실행
if "전체 크롤링" in mode:
    pages = crawl_bfs(base_url, max_pages, crawl_delay)
elif "사이트맵 기반" in mode:
    pages = collect_sitemap_pages(max_pages, crawl_delay)
    if not pages:
        st.warning("사이트맵을 찾지 못했습니다. 크롤링으로 전환합니다.")
        pages = crawl_bfs(base_url, max_pages, crawl_delay)
elif "특정 경로" in mode:
    pf = path_filter.strip() or "/"
    if not pf.startswith("/"): pf = "/"+pf
    start = base_url + pf.rstrip("/")
    pages = crawl_bfs(start, max_pages, crawl_delay, path_prefix=pf)
elif "URL 목록" in mode:
    urls = [u.strip() for u in url_list_text.strip().splitlines() if u.strip()]
    total = min(len(urls), max_pages)
    for i, url in enumerate(urls[:max_pages]):
        if not url.startswith("http"): url = "https://"+url
        page = analyze_page(url, session, 0); pages.append(page)
        update_live(pages, url, total, total-i-1); time.sleep(crawl_delay)
elif "혼합" in mode:
    # 1) 사이트맵 먼저
    pages = collect_sitemap_pages(max_pages, crawl_delay)
    sitemap_urls = {p["URL"] for p in pages}
    # 2) 크롤링으로 추가 발견
    remaining = max_pages - len(pages)
    if remaining > 0:
        status_line.markdown('<div class="crawl-status">추가 크롤링 시작...</div>',unsafe_allow_html=True)
        visited = set(sitemap_urls)
        queue = []
        for p in pages:
            for link in p["_internal_links"]:
                if link not in visited: queue.append(link)
        while queue and len(pages) < max_pages:
            url = queue.pop(0)
            if url in visited: continue
            visited.add(url)
            page = analyze_page(url, session, 1); pages.append(page)
            for link in page["_internal_links"]:
                if link not in visited: queue.append(link)
            update_live(pages, url, max_pages, len(queue)); time.sleep(crawl_delay)

# 완료
elapsed_total=time.time()-crawl_start; progress_bar.progress(1.0)
status_line.markdown(f'<div class="crawl-status" style="border-color:#3fb950;">✅ <span class="count">크롤링 완료!</span> {len(pages)}개 페이지 · {fmt_time(elapsed_total)} 소요</div>',unsafe_allow_html=True)

if not pages: st.error("분석할 페이지가 없습니다."); st.stop()

# 내부 링크 계산
incoming_map=defaultdict(int)
for p in pages:
    for link in p["_internal_links"]: incoming_map[link]+=1
for p in pages: p["Inlinks"]=incoming_map.get(p["URL"],0)

# PageSpeed
pagespeed_scores={}
if api_key:
    st.markdown("### ⚡ PageSpeed 수집 중...")
    ps_bar=st.progress(0); ps_st=st.empty()
    ok_p=[p for p in pages if p["Status"]==200]
    for i,p in enumerate(ok_p,1):
        ps_st.text(f"[{i}/{len(ok_p)}] {p['URL']}"); ps_bar.progress(i/len(ok_p))
        sc=get_pagespeed_score(p["URL"],api_key)
        if sc is not None: pagespeed_scores[p["URL"]]=sc; p["PageSpeed"]=sc
    ps_bar.empty(); ps_st.empty()

# 진단
issues = run_diagnostics(pages, incoming_map, pagespeed_scores)

# 히스토리 저장
domain_key = base_domain.replace(".","_")
high=sum(1 for i in issues if i["severity"]=="HIGH")
med=sum(1 for i in issues if i["severity"]=="MEDIUM")
low=sum(1 for i in issues if i["severity"]=="LOW")
save_history(domain_key, {
    "generated_at":datetime.now().isoformat(),"base_url":base_url,"total_pages":len(pages),
    "crawl_time":round(elapsed_total,1),"mode":mode,
    "summary":{"high":high,"medium":med,"low":low,"total":len(issues),"pages":len(pages)},
    "issue_types":{t:sum(1 for i in issues if i["type"]==t) for t in set(i["type"] for i in issues)},
})

# 히스토리 비교
prev_history = load_history(domain_key)
history_delta = None
if len(prev_history) > 1:
    prev = prev_history[1]  # [0] is current
    ps = prev.get("summary",{})
    history_delta = {
        "high": high - ps.get("high",0),
        "medium": med - ps.get("medium",0),
        "low": low - ps.get("low",0),
        "pages": len(pages) - ps.get("pages",0),
    }

# ══════════════════════════════════════════════════════════════════════════════
# 결과 대시보드
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("---")

avg_load=round(sum(p["Load (s)"] for p in pages)/len(pages),2)
schema_count=sum(1 for p in pages if p.get("Has Schema"))
https_count=sum(1 for p in pages if p.get("HTTPS"))

# 요약 카드 (hover시 설명 표시)
card_data = [
    ("총 페이지",len(pages),"","전체 수집된 페이지 수"),
    ("총 이슈",len(issues),"","발견된 모든 SEO 이슈"),
    ("🔴 HIGH",high,"red","즉시 수정 필요한 심각한 이슈"),
    ("🟡 MEDIUM",med,"yellow","개선 권장 이슈"),
    ("🟢 LOW",low,"green","참고 수준의 이슈"),
    ("평균 로딩",f"{avg_load}s","","3초 이상은 이탈률 증가"),
    ("Schema",f"{schema_count}/{len(pages)}","","구조화 데이터 적용 비율"),
    ("HTTPS",f"{https_count}/{len(pages)}","","HTTPS 적용 비율"),
]
cols = st.columns(len(card_data))
for col,(label,val,cls,hint) in zip(cols,card_data):
    col.markdown(f'<div class="summary-card {cls}"><div class="num">{val}</div><div class="label">{label}</div><div class="hint">{hint}</div></div>',unsafe_allow_html=True)

# 히스토리 비교
if history_delta:
    def delta_str(v):
        if v > 0: return f'<span class="delta-up">▲{v}</span>'
        elif v < 0: return f'<span class="delta-down">▼{abs(v)}</span>'
        return "→0"
    st.markdown(f'<div class="history-card">📈 지난 분석 대비: 페이지 {delta_str(history_delta["pages"])} · 🔴HIGH {delta_str(history_delta["high"])} · 🟡MEDIUM {delta_str(history_delta["medium"])} · 🟢LOW {delta_str(history_delta["low"])}</div>',unsafe_allow_html=True)

st.markdown("")

# ── 클릭형 이슈 필터 버튼 ────────────────────────────────────────────────────
if "card_filter" not in st.session_state: st.session_state.card_filter = None

fc1,fc2,fc3,fc4 = st.columns(4)
with fc1:
    if st.button(f"🔴 HIGH ({high}건) 보기",use_container_width=True):
        st.session_state.card_filter="HIGH"
with fc2:
    if st.button(f"🟡 MEDIUM ({med}건) 보기",use_container_width=True):
        st.session_state.card_filter="MEDIUM"
with fc3:
    if st.button(f"🟢 LOW ({low}건) 보기",use_container_width=True):
        st.session_state.card_filter="LOW"
with fc4:
    if st.button("전체 보기",use_container_width=True):
        st.session_state.card_filter=None

# 필터된 이슈 + URL 즉시 표시 (카드 클릭시)
if st.session_state.card_filter:
    sev = st.session_state.card_filter
    fi = [i for i in issues if i["severity"]==sev]
    icons = {"HIGH":"🔴","MEDIUM":"🟡","LOW":"🟢"}
    st.markdown(f"### {icons[sev]} {sev} 이슈 — {len(fi)}건")

    # 유형별 그룹
    fi_sorted = sorted(fi, key=lambda x: x["type"])
    for itype, group in groupby(fi_sorted, key=lambda x: x["type"]):
        gl = list(group)
        with st.expander(f"**{itype}** ({len(gl)}건)", expanded=True):
            for iss in gl[:30]:
                url_path = urlparse(iss["pages"][0]).path if iss["pages"] else ""
                st.markdown(f'''<div class="issue-card {sev.lower()}"><div class="issue-header"><span class="badge-{sev.lower()}">{sev}</span> <strong>{itype}</strong></div><div class="issue-detail">{iss["detail"]}</div><div class="issue-url">{url_path}</div><div class="issue-fix">💡 {iss["fix"]}</div></div>''',unsafe_allow_html=True)
            if len(gl)>30: st.caption(f"... 외 {len(gl)-30}건")

    st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════════
# 탭
# ══════════════════════════════════════════════════════════════════════════════
tab_all,tab_td,tab_links,tab_schema,tab_eeat,tab_tech,tab_sec,tab_perf,tab_tree,tab_issues,tab_export = st.tabs(["📋 All Pages","🏷️ Title & Desc","🔗 Links","📊 Schema","🛡️ E-E-A-T","⚙️ Technical","🔒 Security","📈 Performance","🌳 Structure","⚠️ Issues","💾 Export"])

with tab_all:
    dc=["URL","Status","Title","Title Len","Meta Desc","Desc Len","H1","Canonical","Words","Inlinks","Outlinks","Schema Types","HTTPS","Noindex","HTML KB","Ext Scripts","Compression","Load (s)","Error"]
    if pagespeed_scores:
        for p in pages:
            if "PageSpeed" not in p: p["PageSpeed"]="-"
        dc.append("PageSpeed")
    df=pd.DataFrame(pages); av=[c for c in dc if c in df.columns]
    st.dataframe(df[av],use_container_width=True,hide_index=True,height=500)

with tab_td:
    td=[]
    for p in pages:
        tl=p["Title Len"]; dl=p["Desc Len"]
        ts="❌ 없음" if tl==0 else (f"⚠️ {tl}자" if tl<TITLE_MIN or tl>TITLE_MAX else f"✅ {tl}자")
        ds="❌ 없음" if dl==0 else (f"⚠️ {dl}자" if dl<DESC_MIN or dl>DESC_MAX else f"✅ {dl}자")
        hs=f"✅ {p['H1'][:30]}" if p["H1"] else "❌ 없음"
        cn=p["Canonical"]; cs="❌ 없음" if not cn else ("✅ Self" if cn==p["URL"] else f"↗️ {cn[:40]}")
        td.append({"URL":urlparse(p["URL"]).path or "/","Title 상태":ts,"Title":p["Title"][:60],"Desc 상태":ds,"Description":p["Meta Desc"][:60],"H1":hs,"Canonical":cs})
    st.dataframe(pd.DataFrame(td),use_container_width=True,hide_index=True,height=500)
    c1,c2,c3,c4=st.columns(4)
    c1.metric("Title 적정",f"{sum(1 for p in pages if TITLE_MIN<=p['Title Len']<=TITLE_MAX)}/{len(pages)}")
    c2.metric("Desc 적정",f"{sum(1 for p in pages if DESC_MIN<=p['Desc Len']<=DESC_MAX)}/{len(pages)}")
    c3.metric("H1 있음",f"{sum(1 for p in pages if p['H1 Len']>0)}/{len(pages)}")
    c4.metric("Canonical 있음",f"{sum(1 for p in pages if p['Canonical'])}/{len(pages)}")

with tab_links:
    ld=[]
    for p in pages:
        inc=incoming_map.get(p["URL"],0); cq=p.get("_content",{})
        st_txt="🚨 고아" if inc==0 else ("⚠️ 부족" if inc<MIN_INCOMING_LINKS else "✅ 양호")
        ld.append({"URL":urlparse(p["URL"]).path or "/","Inlinks":inc,"Outlinks":p["Outlinks"],"External":cq.get("external_links_count",0),"Nofollow":cq.get("nofollow_links_count",0),"상태":st_txt})
    st.dataframe(pd.DataFrame(ld).sort_values("Inlinks"),use_container_width=True,hide_index=True,height=400)

with tab_schema:
    sd=[]
    for p in pages:
        si=p.get("_schema",{})
        sd.append({"URL":urlparse(p["URL"]).path or "/","상태":"✅" if si.get("has_schema") else "❌","Types":", ".join(si.get("all_types",[])) or "-","JSON-LD":len(si.get("json_ld",[])),"검증":", ".join(si.get("validation_issues",[])) or "-"})
    st.dataframe(pd.DataFrame(sd),use_container_width=True,hide_index=True,height=400)
    c1,c2=st.columns(2); c1.metric("✅ Schema 있음",schema_count); c2.metric("❌ Schema 없음",len(pages)-schema_count)

with tab_eeat:
    ed=[]
    for p in pages:
        ee=p.get("_eeat",{})
        ed.append({"URL":urlparse(p["URL"]).path or "/","Author":ee.get("author_name","")[:30] or "❌","Published":"✅" if ee.get("has_published_date") else "❌","About":"✅" if ee.get("has_about_link") else "❌","Contact":"✅" if ee.get("has_contact_link") else "❌","Privacy":"✅" if ee.get("has_privacy_link") else "❌","Org Schema":"✅" if ee.get("has_org_schema") else "❌","Breadcrumb":"✅" if ee.get("has_breadcrumb") else "❌","Social":", ".join(ee.get("social_links",[])) or "-"})
    st.dataframe(pd.DataFrame(ed),use_container_width=True,hide_index=True,height=400)

with tab_tech:
    td2=[]
    for p in pages:
        tc=p.get("_tech",{})
        td2.append({"URL":urlparse(p["URL"]).path or "/","Robots":tc.get("meta_robots","") or "-","Noindex":"⚠️" if tc.get("is_noindex") else "✅","Viewport":"✅" if tc.get("has_viewport") else "❌","Lang":tc.get("lang","") or "-","OG Image":"✅" if tc.get("og_image") else "❌","Twitter":tc.get("twitter_card","") or "-","Hreflang":len(tc.get("hreflang_tags",[])),"URL Len":tc.get("url_length",0)})
    st.dataframe(pd.DataFrame(td2),use_container_width=True,hide_index=True,height=400)
    st.markdown("#### Heading Hierarchy")
    hd=[]
    for p in pages:
        tc=p.get("_tech",{}); hs=tc.get("headings",{})
        hd.append({"URL":urlparse(p["URL"]).path or "/","H1":hs.get("h1",0),"H2":hs.get("h2",0),"H3":hs.get("h3",0),"H4":hs.get("h4",0),"계층":"✅" if tc.get("heading_hierarchy_ok") else "❌","이슈":"; ".join(tc.get("heading_issues",[])) or "-"})
    st.dataframe(pd.DataFrame(hd),use_container_width=True,hide_index=True,height=300)

with tab_sec:
    sd2=[]
    for p in pages:
        sc=p.get("_security",{})
        sd2.append({"URL":urlparse(p["URL"]).path or "/","HTTPS":"✅" if sc.get("is_https") else "❌","HSTS":"✅" if sc.get("has_hsts") else "❌","X-CTO":"✅" if sc.get("has_xcto") else "❌","XFO":"✅" if sc.get("has_xfo") else "❌","CSP":"✅" if sc.get("has_csp") else "❌","Mixed":sc.get("mixed_content_count",0)})
    st.dataframe(pd.DataFrame(sd2),use_container_width=True,hide_index=True,height=400)

with tab_perf:
    pd2=[]
    for p in pages:
        pf=p.get("_perf",{})
        pd2.append({"URL":urlparse(p["URL"]).path or "/","HTML KB":pf.get("html_size_kb",0),"Scripts":pf.get("external_scripts",0),"CSS":pf.get("external_stylesheets",0),"Images":pf.get("image_count",0),"No Lazy":pf.get("images_no_lazy",0),"Compression":pf.get("compression_type","") or "❌","Load":p.get("Load (s)",0)})
    st.dataframe(pd.DataFrame(pd2),use_container_width=True,hide_index=True,height=400)

with tab_tree:
    st.code(build_tree_string([p["URL"] for p in pages],base_domain),language=None)

with tab_issues:
    st.subheader(f"Issues — {len(issues)}건")
    fc1,fc2=st.columns([1,3])
    default_sev = [st.session_state.card_filter] if st.session_state.card_filter else ["HIGH","MEDIUM","LOW"]
    with fc1: sev_f=st.multiselect("심각도",["HIGH","MEDIUM","LOW"],default=default_sev)
    with fc2:
        to=sorted(set(i["type"] for i in issues)); type_f=st.multiselect("유형",to,default=to)
    filtered=[i for i in issues if i["severity"] in sev_f and i["type"] in type_f]

    # 이슈를 호버형 카드로 표시
    for iss in filtered[:100]:
        sev_cls=iss["severity"].lower()
        url_path=urlparse(iss["pages"][0]).path if iss["pages"] else ""
        st.markdown(f'''<div class="issue-card {sev_cls}"><div class="issue-header"><span class="badge-{sev_cls}">{iss["severity"]}</span> <strong>{iss["type"]}</strong></div><div class="issue-detail">{iss["detail"]}</div><div class="issue-url">{url_path}</div><div class="issue-fix">💡 {iss["fix"]}</div></div>''',unsafe_allow_html=True)
    if len(filtered)>100: st.caption(f"... 외 {len(filtered)-100}건 (필터로 범위를 좁혀보세요)")

    # 유형별 요약
    st.markdown("#### 유형별 요약")
    ts2=defaultdict(lambda:{"HIGH":0,"MEDIUM":0,"LOW":0,"total":0})
    for i in issues: ts2[i["type"]][i["severity"]]+=1; ts2[i["type"]]["total"]+=1
    sr=[{"유형":t,"🔴":c["HIGH"],"🟡":c["MEDIUM"],"🟢":c["LOW"],"합계":c["total"]} for t,c in sorted(ts2.items(),key=lambda x:-x[1]["total"])]
    if sr: st.dataframe(pd.DataFrame(sr),use_container_width=True,hide_index=True)

with tab_export:
    def clean_export(p):
        c={k:v for k,v in p.items() if not k.startswith("_")}
        si=p.get("_schema",{}); c["Schema_Types"]=",".join(si.get("all_types",[]))
        ee=p.get("_eeat",{}); c["EEAT_Author"]=ee.get("author_name",""); c["EEAT_Date"]=ee.get("has_published_date",False)
        tc=p.get("_tech",{}); c["Tech_OG_Image"]=tc.get("og_image",""); c["Tech_Twitter"]=tc.get("twitter_card","")
        sc=p.get("_security",{}); c["Sec_HTTPS"]=sc.get("is_https",False); c["Sec_HSTS"]=sc.get("has_hsts",False)
        pf=p.get("_perf",{}); c["Perf_HTML_KB"]=pf.get("html_size_kb",0); c["Perf_Compression"]=pf.get("compression_type","")
        return c

    report={"generated_at":datetime.now().isoformat(),"base_url":base_url,"total_pages":len(pages),"crawl_time":round(elapsed_total,1),"mode":mode,"summary":{"high":high,"medium":med,"low":low,"total":len(issues)},"pages":[clean_export(p) for p in pages],"issues":issues}
    ts=datetime.now().strftime("%Y%m%d_%H%M")
    c1,c2=st.columns(2)
    with c1: st.download_button("📥 JSON",json.dumps(report,ensure_ascii=False,indent=2,default=str),f"seo_{ts}.json","application/json",use_container_width=True)
    with c2: st.download_button("📥 CSV",pd.DataFrame([clean_export(p) for p in pages]).to_csv(index=False),f"seo_{ts}.csv","text/csv",use_container_width=True)
    with st.expander("JSON 미리보기"): st.json(report)
