"""
SEO Diagnostic Pro - PostgreSQL(Supabase) 데이터베이스 레이어
사용자, 프로젝트, 크롤링 실행, 페이지 데이터, 인사이트를 관리합니다.
"""
from __future__ import annotations

import psycopg2
import psycopg2.extras
import hashlib
import secrets
import json
import os
from datetime import datetime

# ─────────────────────────────────────────────
# DB 연결 설정
# ─────────────────────────────────────────────

def _get_db_params() -> dict:
    """Streamlit secrets 또는 환경변수에서 DB 접속 정보를 가져옵니다."""
    try:
        import streamlit as st
        return {
            "host": st.secrets["DB_HOST"],
            "port": int(st.secrets.get("DB_PORT", 5432)),
            "dbname": st.secrets.get("DB_NAME", "neondb"),
            "user": st.secrets["DB_USER"],
            "password": st.secrets["DB_PASSWORD"],
            "sslmode": "require",
        }
    except Exception:
        pass
    host = os.environ.get("DB_HOST", "")
    if host:
        return {
            "host": host,
            "port": int(os.environ.get("DB_PORT", 5432)),
            "dbname": os.environ.get("DB_NAME", "neondb"),
            "user": os.environ.get("DB_USER", ""),
            "password": os.environ.get("DB_PASSWORD", ""),
            "sslmode": "require",
        }
    raise RuntimeError("DB 접속 정보가 설정되지 않았습니다.")


def get_db():
    """데이터베이스 연결을 반환합니다. Neon 서버측 풀러가 커넥션을 관리합니다."""
    return psycopg2.connect(**_get_db_params(), connect_timeout=10)


def _dict_row(cursor):
    """커서에서 dict 형태로 한 행을 가져옵니다."""
    row = cursor.fetchone()
    if row is None:
        return None
    return dict(row)


def _dict_rows(cursor):
    """커서에서 dict 리스트로 모든 행을 가져옵니다."""
    return [dict(r) for r in cursor.fetchall()]


def _cursor(conn):
    """RealDictCursor를 반환합니다."""
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


def init_db():
    """테이블이 없으면 자동으로 생성합니다."""
    conn = get_db()
    cur = _cursor(conn)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL DEFAULT '',
            salt TEXT NOT NULL DEFAULT '',
            name TEXT NOT NULL,
            profile_image TEXT DEFAULT '',
            auth_provider TEXT DEFAULT 'google',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            plan TEXT DEFAULT 'free',
            max_projects INTEGER DEFAULT 1
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS projects (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id),
            name TEXT NOT NULL,
            url TEXT NOT NULL,
            crawl_mode TEXT DEFAULT 'full',
            crawl_path TEXT DEFAULT '',
            max_pages INTEGER DEFAULT 200,
            crawl_delay REAL DEFAULT 0.3,
            schedule TEXT DEFAULT 'manual',
            schedule_time TEXT DEFAULT '09:00',
            respect_robots TEXT DEFAULT 'respect',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_crawl_at TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS crawl_runs (
            id SERIAL PRIMARY KEY,
            project_id INTEGER NOT NULL REFERENCES projects(id),
            started_at TEXT,
            completed_at TEXT,
            total_pages INTEGER DEFAULT 0,
            high_issues INTEGER DEFAULT 0,
            medium_issues INTEGER DEFAULT 0,
            low_issues INTEGER DEFAULT 0,
            total_issues INTEGER DEFAULT 0,
            status TEXT DEFAULT 'pending',
            pages_json TEXT,
            issues_json TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS insights (
            id SERIAL PRIMARY KEY,
            crawl_run_id INTEGER NOT NULL REFERENCES crawl_runs(id),
            project_id INTEGER NOT NULL,
            category TEXT NOT NULL,
            insight_type TEXT NOT NULL,
            title TEXT NOT NULL,
            detail TEXT,
            url TEXT,
            severity TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sc_connections (
            id SERIAL PRIMARY KEY,
            project_id INTEGER NOT NULL UNIQUE REFERENCES projects(id),
            credentials_json TEXT NOT NULL,
            site_url TEXT NOT NULL,
            connected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_sync_at TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sc_analytics (
            id SERIAL PRIMARY KEY,
            project_id INTEGER NOT NULL REFERENCES projects(id),
            date TEXT NOT NULL,
            url TEXT,
            query TEXT,
            clicks INTEGER DEFAULT 0,
            impressions INTEGER DEFAULT 0,
            ctr REAL DEFAULT 0.0,
            position REAL DEFAULT 0.0,
            device TEXT DEFAULT '',
            country TEXT DEFAULT '',
            synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sc_issues (
            id SERIAL PRIMARY KEY,
            project_id INTEGER NOT NULL REFERENCES projects(id),
            url TEXT NOT NULL,
            issue_type TEXT NOT NULL,
            severity TEXT DEFAULT 'MEDIUM',
            detail TEXT,
            first_detected TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_detected TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            resolved_at TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS pagespeed_data (
            id SERIAL PRIMARY KEY,
            project_id INTEGER NOT NULL REFERENCES projects(id),
            crawl_run_id INTEGER,
            url TEXT NOT NULL,
            strategy TEXT DEFAULT 'mobile',
            score INTEGER DEFAULT 0,
            lcp REAL DEFAULT 0.0,
            fid REAL DEFAULT 0.0,
            cls REAL DEFAULT 0.0,
            ttfb REAL DEFAULT 0.0,
            si REAL DEFAULT 0.0,
            tbt REAL DEFAULT 0.0,
            opportunities_json TEXT,
            diagnostics_json TEXT,
            measured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS page_snapshots (
            id SERIAL PRIMARY KEY,
            project_id INTEGER NOT NULL REFERENCES projects(id),
            crawl_run_id INTEGER NOT NULL REFERENCES crawl_runs(id),
            url TEXT NOT NULL,
            title TEXT DEFAULT '',
            meta_description TEXT DEFAULT '',
            h1 TEXT DEFAULT '',
            word_count INTEGER DEFAULT 0,
            status_code INTEGER DEFAULT 0,
            schema_types TEXT DEFAULT '',
            has_canonical INTEGER DEFAULT 0,
            canonical_url TEXT DEFAULT '',
            is_https INTEGER DEFAULT 0,
            load_time REAL DEFAULT 0.0,
            snapshot_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS page_changes (
            id SERIAL PRIMARY KEY,
            project_id INTEGER NOT NULL REFERENCES projects(id),
            url TEXT NOT NULL,
            field_name TEXT NOT NULL,
            old_value TEXT DEFAULT '',
            new_value TEXT DEFAULT '',
            crawl_run_id INTEGER NOT NULL REFERENCES crawl_runs(id),
            detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS excluded_urls (
            id SERIAL PRIMARY KEY,
            project_id INTEGER NOT NULL REFERENCES projects(id),
            url_pattern TEXT NOT NULL,
            reason TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS email_verifications (
            id SERIAL PRIMARY KEY,
            email TEXT NOT NULL,
            code TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP NOT NULL,
            verified BOOLEAN DEFAULT FALSE
        )
    """)

    # 인덱스
    cur.execute("CREATE INDEX IF NOT EXISTS idx_email_verifications_email ON email_verifications(email, code)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sc_analytics_project_date ON sc_analytics(project_id, date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sc_analytics_url ON sc_analytics(project_id, url)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pagespeed_project_url ON pagespeed_data(project_id, url)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_project_url ON page_snapshots(project_id, url)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_changes_project_url ON page_changes(project_id, url)")

    conn.commit()
    cur.close()
    conn.close()


# ─────────────────────────────────────────────
# 비밀번호 해싱 유틸리티
# ─────────────────────────────────────────────

def hash_password(password: str, salt: str = None) -> tuple[str, str]:
    """비밀번호를 SHA-256 + salt로 해싱합니다. (hash, salt) 튜플 반환."""
    if salt is None:
        salt = secrets.token_hex(32)
    pw_hash = hashlib.sha256((password + salt).encode("utf-8")).hexdigest()
    return pw_hash, salt


# ─────────────────────────────────────────────
# 사용자 인증 (Auth)
# ─────────────────────────────────────────────

# 법인 이메일이 아닌 무료 이메일 도메인 목록
FREE_EMAIL_DOMAINS = {
    "gmail.com", "googlemail.com", "yahoo.com", "yahoo.co.kr",
    "hotmail.com", "outlook.com", "live.com", "msn.com",
    "naver.com", "hanmail.net", "daum.net", "kakao.com",
    "nate.com", "lycos.co.kr", "empal.com", "dreamwiz.com",
    "icloud.com", "me.com", "mac.com", "aol.com",
    "protonmail.com", "proton.me", "zoho.com",
    "yandex.com", "mail.com", "gmx.com",
}

def save_verification_code(email: str, code: str, minutes: int = 5):
    """이메일 인증 코드를 저장합니다. 기존 미인증 코드는 삭제합니다."""
    conn = get_db()
    try:
        cur = _cursor(conn)
        cur.execute("DELETE FROM email_verifications WHERE email = %s AND verified = FALSE", (email,))
        cur.execute(
            """INSERT INTO email_verifications (email, code, expires_at)
               VALUES (%s, %s, CURRENT_TIMESTAMP + INTERVAL '%s minutes')""",
            (email, code, minutes),
        )
        conn.commit()
    finally:
        conn.close()


def verify_email_code(email: str, code: str) -> bool:
    """이메일 인증 코드를 검증합니다. 5분 이내 유효."""
    conn = get_db()
    try:
        cur = _cursor(conn)
        cur.execute(
            """SELECT id FROM email_verifications
               WHERE email = %s AND code = %s AND verified = FALSE
               AND expires_at > CURRENT_TIMESTAMP""",
            (email, code),
        )
        row = cur.fetchone()
        if row:
            cur.execute("UPDATE email_verifications SET verified = TRUE WHERE id = %s", (row["id"],))
            conn.commit()
            return True
        return False
    finally:
        conn.close()


def is_corporate_email(email: str) -> bool:
    """법인(회사) 이메일인지 확인합니다."""
    domain = email.split("@")[-1].lower()
    return domain not in FREE_EMAIL_DOMAINS

def get_max_projects_for_email(email: str) -> int:
    """이메일 유형에 따른 최대 프로젝트 수를 반환합니다."""
    return 5 if is_corporate_email(email) else 1

def create_user(email: str, password: str, name: str) -> int:
    """새 사용자를 생성합니다. 이미 존재하는 이메일이면 ValueError를 발생시킵니다."""
    conn = get_db()
    try:
        cur = _cursor(conn)
        pw_hash, salt = hash_password(password)
        max_proj = get_max_projects_for_email(email)
        cur.execute(
            "INSERT INTO users (email, password_hash, salt, name, max_projects) VALUES (%s, %s, %s, %s, %s) RETURNING id",
            (email, pw_hash, salt, name, max_proj),
        )
        row = cur.fetchone()
        conn.commit()
        return row["id"]
    except psycopg2.IntegrityError:
        conn.rollback()
        raise ValueError(f"이미 등록된 이메일입니다: {email}")
    finally:
        conn.close()

def create_user_google(email: str, name: str, profile_image: str = "") -> dict:
    """Google OAuth로 사용자를 생성하거나, 이미 존재하면 기존 사용자를 반환합니다."""
    conn = get_db()
    try:
        cur = _cursor(conn)
        cur.execute("SELECT * FROM users WHERE email = %s", (email,))
        row = cur.fetchone()
        if row:
            return dict(row)
        max_proj = get_max_projects_for_email(email)
        plan = "business" if is_corporate_email(email) else "free"
        cur.execute(
            """INSERT INTO users (email, name, profile_image, auth_provider, plan, max_projects)
               VALUES (%s, %s, %s, 'google', %s, %s) RETURNING id""",
            (email, name, profile_image, plan, max_proj),
        )
        new_row = cur.fetchone()
        conn.commit()
        user_id = new_row["id"]
        cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
        return dict(cur.fetchone())
    finally:
        conn.close()

def get_project_count(user_id: int) -> int:
    """사용자의 현재 프로젝트 수를 반환합니다."""
    conn = get_db()
    try:
        cur = _cursor(conn)
        cur.execute("SELECT COUNT(*) as cnt FROM projects WHERE user_id = %s", (user_id,))
        row = cur.fetchone()
        return row["cnt"]
    finally:
        conn.close()

def can_create_project(user_id: int) -> tuple[bool, int, int]:
    """프로젝트 생성 가능 여부를 반환합니다. (가능여부, 현재수, 최대수)"""
    user = get_user(user_id)
    if not user:
        return False, 0, 0
    current = get_project_count(user_id)
    max_proj = user.get("max_projects", 1)
    return current < max_proj, current, max_proj


def verify_user(email: str, password: str) -> dict | None:
    """이메일과 비밀번호로 사용자를 인증합니다. 성공 시 dict, 실패 시 None 반환."""
    conn = get_db()
    try:
        cur = _cursor(conn)
        cur.execute("SELECT * FROM users WHERE email = %s", (email,))
        row = cur.fetchone()
        if row is None:
            return None
        pw_hash, _ = hash_password(password, row["salt"])
        if pw_hash == row["password_hash"]:
            return dict(row)
        return None
    finally:
        conn.close()


def get_user(user_id: int) -> dict | None:
    """사용자 ID로 사용자 정보를 조회합니다."""
    conn = get_db()
    try:
        cur = _cursor(conn)
        cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


# ─────────────────────────────────────────────
# 프로젝트 관리
# ─────────────────────────────────────────────

def create_project(
    user_id: int,
    name: str,
    url: str,
    crawl_mode: str = "full",
    crawl_path: str = "",
    max_pages: int = 200,
    crawl_delay: float = 0.3,
    schedule: str = "manual",
    schedule_time: str = "09:00",
) -> int:
    """새 프로젝트를 생성하고 project_id를 반환합니다."""
    conn = get_db()
    try:
        cur = _cursor(conn)
        cur.execute(
            """INSERT INTO projects
               (user_id, name, url, crawl_mode, crawl_path, max_pages,
                crawl_delay, schedule, schedule_time)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
            (user_id, name, url, crawl_mode, crawl_path, max_pages,
             crawl_delay, schedule, schedule_time),
        )
        row = cur.fetchone()
        conn.commit()
        return row["id"]
    finally:
        conn.close()


def get_projects(user_id: int) -> list[dict]:
    """특정 사용자의 모든 프로젝트를 반환합니다."""
    conn = get_db()
    try:
        cur = _cursor(conn)
        cur.execute(
            "SELECT * FROM projects WHERE user_id = %s ORDER BY created_at DESC",
            (user_id,),
        )
        return _dict_rows(cur)
    finally:
        conn.close()


def get_project(project_id: int) -> dict | None:
    """프로젝트 ID로 프로젝트 정보를 조회합니다."""
    conn = get_db()
    try:
        cur = _cursor(conn)
        cur.execute("SELECT * FROM projects WHERE id = %s", (project_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def update_project(project_id: int, **kwargs):
    """프로젝트 정보를 업데이트합니다. 허용된 필드만 수정합니다."""
    allowed = {
        "name", "url", "crawl_mode", "crawl_path", "max_pages",
        "crawl_delay", "schedule", "schedule_time", "last_crawl_at",
    }
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return

    set_clause = ", ".join(f"{k} = %s" for k in updates)
    values = list(updates.values()) + [project_id]

    conn = get_db()
    try:
        cur = _cursor(conn)
        cur.execute(f"UPDATE projects SET {set_clause} WHERE id = %s", values)
        conn.commit()
    finally:
        conn.close()


def delete_project(project_id: int):
    """프로젝트와 관련된 모든 데이터를 삭제합니다."""
    conn = get_db()
    try:
        cur = _cursor(conn)
        cur.execute("DELETE FROM insights WHERE project_id = %s", (project_id,))
        cur.execute("DELETE FROM page_changes WHERE project_id = %s", (project_id,))
        cur.execute("DELETE FROM page_snapshots WHERE project_id = %s", (project_id,))
        cur.execute("DELETE FROM pagespeed_data WHERE project_id = %s", (project_id,))
        cur.execute("DELETE FROM sc_issues WHERE project_id = %s", (project_id,))
        cur.execute("DELETE FROM sc_analytics WHERE project_id = %s", (project_id,))
        cur.execute("DELETE FROM sc_connections WHERE project_id = %s", (project_id,))
        cur.execute("DELETE FROM excluded_urls WHERE project_id = %s", (project_id,))
        cur.execute("DELETE FROM crawl_runs WHERE project_id = %s", (project_id,))
        cur.execute("DELETE FROM projects WHERE id = %s", (project_id,))
        conn.commit()
    finally:
        conn.close()



def add_excluded_url(project_id: int, url_pattern: str, reason: str = "") -> int:
    """제외 URL 패턴을 추가합니다."""
    conn = get_db()
    try:
        cur = _cursor(conn)
        cur.execute(
            "INSERT INTO excluded_urls (project_id, url_pattern, reason) VALUES (%s, %s, %s) RETURNING id",
            (project_id, url_pattern, reason),
        )
        row = cur.fetchone()
        conn.commit()
        return row["id"]
    finally:
        conn.close()


def get_excluded_urls(project_id: int) -> list[dict]:
    """프로젝트의 제외 URL 목록을 반환합니다."""
    conn = get_db()
    try:
        cur = _cursor(conn)
        cur.execute(
            "SELECT * FROM excluded_urls WHERE project_id = %s ORDER BY created_at DESC",
            (project_id,),
        )
        return _dict_rows(cur)
    finally:
        conn.close()


def delete_excluded_url(excluded_id: int):
    """제외 URL을 삭제합니다."""
    conn = get_db()
    try:
        cur = _cursor(conn)
        cur.execute("DELETE FROM excluded_urls WHERE id = %s", (excluded_id,))
        conn.commit()
    finally:
        conn.close()


def is_url_excluded(project_id: int, url: str) -> bool:
    """URL이 제외 목록에 해당하는지 확인합니다."""
    excluded = get_excluded_urls(project_id)
    for ex in excluded:
        pattern = ex.get("url_pattern", "")
        if not pattern:
            continue
        if pattern == url or pattern in url or url.startswith(pattern):
            return True
    return False


# ─────────────────────────────────────────────
# 크롤링 실행 관리
# ─────────────────────────────────────────────

def create_crawl_run(project_id: int) -> int:
    """새 크롤링 실행을 생성합니다. run_id를 반환합니다."""
    conn = get_db()
    try:
        cur = _cursor(conn)
        now = datetime.utcnow().isoformat()
        cur.execute(
            "INSERT INTO crawl_runs (project_id, started_at, status) VALUES (%s, %s, 'running') RETURNING id",
            (project_id, now),
        )
        row = cur.fetchone()
        conn.commit()
        return row["id"]
    finally:
        conn.close()


def update_crawl_run(run_id: int, **kwargs):
    """크롤링 실행 상태를 업데이트합니다."""
    allowed = {
        "status", "completed_at", "total_pages",
        "high_issues", "medium_issues", "low_issues", "total_issues",
        "pages_json", "issues_json",
    }
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return

    set_clause = ", ".join(f"{k} = %s" for k in updates)
    values = list(updates.values()) + [run_id]

    conn = get_db()
    try:
        cur = _cursor(conn)
        cur.execute(f"UPDATE crawl_runs SET {set_clause} WHERE id = %s", values)
        conn.commit()
    finally:
        conn.close()


def get_crawl_runs(project_id: int, limit: int = 20) -> list[dict]:
    """특정 프로젝트의 크롤링 실행 목록을 최신순으로 반환합니다."""
    conn = get_db()
    try:
        cur = _cursor(conn)
        cur.execute(
            "SELECT * FROM crawl_runs WHERE project_id = %s ORDER BY id DESC LIMIT %s",
            (project_id, limit),
        )
        return _dict_rows(cur)
    finally:
        conn.close()


def get_latest_crawl(project_id: int) -> dict | None:
    """가장 최근 완료된 크롤링 실행을 반환합니다."""
    conn = get_db()
    try:
        cur = _cursor(conn)
        cur.execute(
            """SELECT * FROM crawl_runs
               WHERE project_id = %s AND status = 'completed'
               ORDER BY id DESC LIMIT 1""",
            (project_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_previous_crawl(project_id: int) -> dict | None:
    """두 번째로 최근 완료된 크롤링 실행을 반환합니다 (비교 분석용)."""
    conn = get_db()
    try:
        cur = _cursor(conn)
        cur.execute(
            """SELECT * FROM crawl_runs
               WHERE project_id = %s AND status = 'completed'
               ORDER BY id DESC LIMIT 1 OFFSET 1""",
            (project_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


# ─────────────────────────────────────────────
# 인사이트 관리
# ─────────────────────────────────────────────

def save_insights(crawl_run_id: int, project_id: int, insights: list[dict]):
    """인사이트 목록을 DB에 저장합니다."""
    conn = get_db()
    try:
        cur = _cursor(conn)
        for ins in insights:
            cur.execute(
                """INSERT INTO insights
                   (crawl_run_id, project_id, category, insight_type, title, detail, url, severity)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    crawl_run_id,
                    project_id,
                    ins.get("category", ""),
                    ins.get("insight_type", ""),
                    ins.get("title", ""),
                    ins.get("detail", ""),
                    ins.get("url", ""),
                    ins.get("severity", ""),
                ),
            )
        conn.commit()
    finally:
        conn.close()


def get_insights(project_id: int, crawl_run_id: int = None) -> list[dict]:
    """프로젝트의 인사이트를 조회합니다. crawl_run_id를 지정하면 해당 실행의 인사이트만 반환합니다."""
    conn = get_db()
    try:
        cur = _cursor(conn)
        if crawl_run_id is not None:
            cur.execute(
                """SELECT * FROM insights
                   WHERE project_id = %s AND crawl_run_id = %s
                   ORDER BY created_at DESC""",
                (project_id, crawl_run_id),
            )
        else:
            cur.execute(
                """SELECT * FROM insights
                   WHERE project_id = %s
                   ORDER BY created_at DESC""",
                (project_id,),
            )
        return _dict_rows(cur)
    finally:
        conn.close()


# ─────────────────────────────────────────────
# 인사이트 생성 (두 크롤링 비교 분석)
# ─────────────────────────────────────────────

def _parse_json_field(raw: str | None) -> list | dict:
    """JSON 문자열을 파싱합니다. 실패하면 빈 리스트를 반환합니다."""
    if not raw:
        return []
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return []


def _classify_issue_category(issue: dict) -> str:
    """이슈를 content 또는 technical 카테고리로 분류합니다."""
    content_keywords = [
        "title", "description", "meta description", "thin", "content",
        "schema", "structured data",
        "e-e-a-t", "eeat", "word", "keyword",
        "heading text", "h1", "h2", "heading structure", "heading hierarchy",
        "og:", "twitter:", "alt text",
        "duplicate title", "duplicate description", "duplicate content",
        "missing title", "missing description", "missing h1",
        "title too", "description too",
    ]
    technical_keywords = [
        "status code", "http status", "4xx", "5xx", "404", "500", "503",
        "performance", "speed", "load time", "response time",
        "security", "ssl", "https", "hsts", "mixed content",
        "canonical", "redirect", "robot", "sitemap",
        "server", "dns", "cdn",
        "crawl", "index", "render",
    ]

    text = json.dumps(issue, ensure_ascii=False).lower()

    content_score = sum(1 for kw in content_keywords if kw in text)
    tech_score = sum(1 for kw in technical_keywords if kw in text)

    if content_score > 0 and content_score >= tech_score:
        return "content"
    elif tech_score > 0:
        return "technical"
    issue_type = (issue.get("type", "") or "").lower()
    issue_title = (issue.get("title", "") or "").lower()
    combined = issue_type + " " + issue_title
    if any(kw in combined for kw in ["title", "desc", "h1", "heading", "content", "schema", "eeat", "alt", "og:", "meta"]):
        return "content"
    return "technical"


def generate_insights(
    project_id: int,
    current_run_id: int,
    previous_run_id: int = None,
) -> list[dict]:
    """
    현재 크롤링과 이전 크롤링을 비교하여 인사이트를 생성합니다.

    insight_type 종류:
      - urgent: 즉시 조치가 필요한 HIGH 심각도 이슈
      - new_issue: 이전 크롤링 이후 새로 발생한 이슈
      - resolved: 이전 크롤링 이후 해결된 이슈
      - improved: 개선된 페이지 (예: 타이틀 추가, 스키마 추가)
      - new_page: 새로 발견된 페이지
      - lost_page: 사라진 페이지
    """
    conn = get_db()
    try:
        cur = _cursor(conn)
        cur.execute("SELECT * FROM crawl_runs WHERE id = %s", (current_run_id,))
        current_row = cur.fetchone()
        if not current_row:
            return []

        current_pages = _parse_json_field(current_row["pages_json"])
        current_issues = _parse_json_field(current_row["issues_json"])

        prev_pages = []
        prev_issues = []
        if previous_run_id:
            cur.execute("SELECT * FROM crawl_runs WHERE id = %s", (previous_run_id,))
            prev_row = cur.fetchone()
            if prev_row:
                prev_pages = _parse_json_field(prev_row["pages_json"])
                prev_issues = _parse_json_field(prev_row["issues_json"])
    finally:
        conn.close()

    insights = []

    # ── 1. urgent: HIGH 심각도 이슈 즉시 보고 ──
    for issue in current_issues:
        severity = issue.get("severity", "").upper()
        if severity == "HIGH":
            category = _classify_issue_category(issue)
            insights.append({
                "category": category,
                "insight_type": "urgent",
                "title": issue.get("title", "심각한 이슈 발견"),
                "detail": issue.get("detail", issue.get("message", "")),
                "url": issue.get("url", ""),
                "severity": "HIGH",
            })

    def _page_urls(pages) -> set:
        if isinstance(pages, list):
            return {p.get("url", "") for p in pages if isinstance(p, dict) and p.get("url")}
        return set()

    def _issue_key(issue: dict) -> str:
        return f"{issue.get('url', '')}||{issue.get('title', '')}||{issue.get('type', '')}"

    current_page_urls = _page_urls(current_pages)
    prev_page_urls = _page_urls(prev_pages)

    current_issue_keys = {_issue_key(i) for i in current_issues}
    prev_issue_keys = {_issue_key(i) for i in prev_issues}

    prev_issue_map = {_issue_key(i): i for i in prev_issues}
    current_issue_map = {_issue_key(i): i for i in current_issues}

    # ── 2. new_issue: 새로 발생한 이슈 ──
    new_issue_keys = current_issue_keys - prev_issue_keys
    for key in new_issue_keys:
        issue = current_issue_map.get(key, {})
        severity = issue.get("severity", "").upper()
        if severity == "HIGH":
            continue
        category = _classify_issue_category(issue)
        insights.append({
            "category": category,
            "insight_type": "new_issue",
            "title": f"새 이슈: {issue.get('title', '알 수 없는 이슈')}",
            "detail": issue.get("detail", issue.get("message", "")),
            "url": issue.get("url", ""),
            "severity": severity or "MEDIUM",
        })

    # ── 3. resolved: 해결된 이슈 ──
    resolved_keys = prev_issue_keys - current_issue_keys
    for key in resolved_keys:
        issue = prev_issue_map.get(key, {})
        category = _classify_issue_category(issue)
        insights.append({
            "category": category,
            "insight_type": "resolved",
            "title": f"해결됨: {issue.get('title', '이슈')}",
            "detail": f"이전 크롤링에서 발견된 이슈가 해결되었습니다.",
            "url": issue.get("url", ""),
            "severity": "LOW",
        })

    # ── 4. improved: 개선된 페이지 감지 ──
    if isinstance(current_pages, list) and isinstance(prev_pages, list):
        prev_page_map = {p.get("url", ""): p for p in prev_pages if isinstance(p, dict)}

        for page in current_pages:
            if not isinstance(page, dict):
                continue
            page_url = page.get("url", "")
            if page_url not in prev_page_map:
                continue

            prev_page = prev_page_map[page_url]

            if not prev_page.get("title") and page.get("title"):
                insights.append({
                    "category": "content",
                    "insight_type": "improved",
                    "title": "타이틀 태그 추가됨",
                    "detail": f"타이틀이 추가되었습니다: \"{page['title']}\"",
                    "url": page_url,
                    "severity": "LOW",
                })

            if not prev_page.get("description") and page.get("description"):
                insights.append({
                    "category": "content",
                    "insight_type": "improved",
                    "title": "메타 설명 추가됨",
                    "detail": f"메타 설명이 추가되었습니다.",
                    "url": page_url,
                    "severity": "LOW",
                })

            prev_schema = prev_page.get("schema") or prev_page.get("structured_data")
            curr_schema = page.get("schema") or page.get("structured_data")
            if not prev_schema and curr_schema:
                insights.append({
                    "category": "content",
                    "insight_type": "improved",
                    "title": "구조화 데이터(Schema) 추가됨",
                    "detail": "페이지에 구조화 데이터가 추가되었습니다.",
                    "url": page_url,
                    "severity": "LOW",
                })

            prev_secure = prev_page.get("is_https", False)
            curr_secure = page.get("is_https", False)
            if not prev_secure and curr_secure:
                insights.append({
                    "category": "technical",
                    "insight_type": "improved",
                    "title": "HTTPS 전환 완료",
                    "detail": "페이지가 HTTPS로 전환되었습니다.",
                    "url": page_url,
                    "severity": "LOW",
                })

    # ── 5. new_page: 새로 발견된 페이지 ──
    new_pages = current_page_urls - prev_page_urls
    if new_pages:
        for page_url in list(new_pages)[:50]:
            insights.append({
                "category": "technical",
                "insight_type": "new_page",
                "title": "새 페이지 발견",
                "detail": f"이전 크롤링에 없었던 새 페이지가 발견되었습니다.",
                "url": page_url,
                "severity": "LOW",
            })

    # ── 6. lost_page: 사라진 페이지 ──
    lost_pages = prev_page_urls - current_page_urls
    if lost_pages:
        for page_url in list(lost_pages)[:50]:
            insights.append({
                "category": "technical",
                "insight_type": "lost_page",
                "title": "페이지 누락 감지",
                "detail": "이전 크롤링에서 존재했던 페이지가 더 이상 발견되지 않습니다.",
                "url": page_url,
                "severity": "MEDIUM",
            })

    if insights:
        save_insights(current_run_id, project_id, insights)

    return insights


# ─────────────────────────────────────────────
# Search Console 연결 관리
# ─────────────────────────────────────────────

def save_sc_connection(project_id: int, credentials_json: str, site_url: str):
    """SC 연결 정보 저장 (upsert)"""
    conn = get_db()
    try:
        cur = _cursor(conn)
        cur.execute("SELECT id FROM sc_connections WHERE project_id = %s", (project_id,))
        existing = cur.fetchone()
        if existing:
            cur.execute(
                """UPDATE sc_connections
                   SET credentials_json = %s, site_url = %s, connected_at = CURRENT_TIMESTAMP
                   WHERE project_id = %s""",
                (credentials_json, site_url, project_id),
            )
        else:
            cur.execute(
                """INSERT INTO sc_connections (project_id, credentials_json, site_url)
                   VALUES (%s, %s, %s)""",
                (project_id, credentials_json, site_url),
            )
        conn.commit()
    finally:
        conn.close()


def get_sc_connection(project_id: int) -> dict | None:
    """SC 연결 정보 조회"""
    conn = get_db()
    try:
        cur = _cursor(conn)
        cur.execute("SELECT * FROM sc_connections WHERE project_id = %s", (project_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def delete_sc_connection(project_id: int):
    """SC 연결 해제"""
    conn = get_db()
    try:
        cur = _cursor(conn)
        cur.execute("DELETE FROM sc_connections WHERE project_id = %s", (project_id,))
        conn.commit()
    finally:
        conn.close()


def update_sc_last_sync(project_id: int):
    """마지막 동기화 시간 업데이트"""
    conn = get_db()
    try:
        cur = _cursor(conn)
        cur.execute(
            "UPDATE sc_connections SET last_sync_at = CURRENT_TIMESTAMP WHERE project_id = %s",
            (project_id,),
        )
        conn.commit()
    finally:
        conn.close()


# ─────────────────────────────────────────────
# Search Console 데이터 관리
# ─────────────────────────────────────────────

def save_sc_analytics(project_id: int, data: list[dict]):
    """SC 분석 데이터 벌크 저장."""
    conn = get_db()
    try:
        cur = _cursor(conn)
        for row in data:
            cur.execute(
                """INSERT INTO sc_analytics
                   (project_id, date, url, query, clicks, impressions, ctr, position, device, country)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    project_id,
                    row.get("date", ""),
                    row.get("url", ""),
                    row.get("query", ""),
                    row.get("clicks", 0),
                    row.get("impressions", 0),
                    row.get("ctr", 0.0),
                    row.get("position", 0.0),
                    row.get("device", ""),
                    row.get("country", ""),
                ),
            )
        conn.commit()
    finally:
        conn.close()


def get_sc_analytics(project_id: int, start_date: str = None, end_date: str = None, url: str = None) -> list[dict]:
    """SC 분석 데이터 조회."""
    conn = get_db()
    try:
        cur = _cursor(conn)
        query = "SELECT * FROM sc_analytics WHERE project_id = %s"
        params: list = [project_id]

        if start_date:
            query += " AND date >= %s"
            params.append(start_date)
        if end_date:
            query += " AND date <= %s"
            params.append(end_date)
        if url:
            query += " AND url = %s"
            params.append(url)

        query += " ORDER BY date DESC"
        cur.execute(query, params)
        return _dict_rows(cur)
    finally:
        conn.close()


def get_sc_top_pages(project_id: int, start_date: str = None, end_date: str = None, limit: int = 50) -> list[dict]:
    """클릭 기준 상위 페이지."""
    conn = get_db()
    try:
        cur = _cursor(conn)
        query = """
            SELECT url,
                   SUM(clicks) as total_clicks,
                   SUM(impressions) as total_impressions,
                   CASE WHEN SUM(impressions) > 0 THEN CAST(SUM(clicks) AS REAL) / SUM(impressions) ELSE 0 END as avg_ctr,
                   AVG(position) as avg_position
            FROM sc_analytics
            WHERE project_id = %s
        """
        params: list = [project_id]

        if start_date:
            query += " AND date >= %s"
            params.append(start_date)
        if end_date:
            query += " AND date <= %s"
            params.append(end_date)

        query += " GROUP BY url ORDER BY total_clicks DESC LIMIT %s"
        params.append(limit)

        cur.execute(query, params)
        return _dict_rows(cur)
    finally:
        conn.close()


def get_sc_top_queries(project_id: int, start_date: str = None, end_date: str = None, limit: int = 50) -> list[dict]:
    """클릭 기준 상위 쿼리."""
    conn = get_db()
    try:
        cur = _cursor(conn)
        query = """
            SELECT query,
                   SUM(clicks) as total_clicks,
                   SUM(impressions) as total_impressions,
                   CASE WHEN SUM(impressions) > 0 THEN CAST(SUM(clicks) AS REAL) / SUM(impressions) ELSE 0 END as avg_ctr,
                   AVG(position) as avg_position
            FROM sc_analytics
            WHERE project_id = %s AND query != ''
        """
        params: list = [project_id]

        if start_date:
            query += " AND date >= %s"
            params.append(start_date)
        if end_date:
            query += " AND date <= %s"
            params.append(end_date)

        query += " GROUP BY query ORDER BY total_clicks DESC LIMIT %s"
        params.append(limit)

        cur.execute(query, params)
        return _dict_rows(cur)
    finally:
        conn.close()


def get_sc_daily_trend(project_id: int, days: int = 28) -> list[dict]:
    """일별 트렌드."""
    conn = get_db()
    try:
        cur = _cursor(conn)
        cur.execute(
            """SELECT date,
                      SUM(clicks) as total_clicks,
                      SUM(impressions) as total_impressions,
                      CASE WHEN SUM(impressions) > 0 THEN CAST(SUM(clicks) AS REAL) / SUM(impressions) ELSE 0 END as avg_ctr,
                      AVG(position) as avg_position
               FROM sc_analytics
               WHERE project_id = %s
               GROUP BY date
               ORDER BY date DESC
               LIMIT %s""",
            (project_id, days),
        )
        return _dict_rows(cur)
    finally:
        conn.close()


def save_sc_issues(project_id: int, issues: list[dict]):
    """SC 인덱싱 이슈 저장."""
    conn = get_db()
    try:
        cur = _cursor(conn)
        for issue in issues:
            url = issue.get("url", "")
            issue_type = issue.get("issue_type", "")
            cur.execute(
                """SELECT id FROM sc_issues
                   WHERE project_id = %s AND url = %s AND issue_type = %s AND resolved_at IS NULL""",
                (project_id, url, issue_type),
            )
            existing = cur.fetchone()
            if existing:
                cur.execute(
                    "UPDATE sc_issues SET last_detected = CURRENT_TIMESTAMP, severity = %s, detail = %s WHERE id = %s",
                    (issue.get("severity", "MEDIUM"), issue.get("detail", ""), existing["id"]),
                )
            else:
                cur.execute(
                    """INSERT INTO sc_issues (project_id, url, issue_type, severity, detail)
                       VALUES (%s, %s, %s, %s, %s)""",
                    (project_id, url, issue_type, issue.get("severity", "MEDIUM"), issue.get("detail", "")),
                )
        conn.commit()
    finally:
        conn.close()


def get_sc_issues(project_id: int, resolved: bool = False) -> list[dict]:
    """활성 또는 해결된 SC 이슈 조회"""
    conn = get_db()
    try:
        cur = _cursor(conn)
        if resolved:
            cur.execute(
                "SELECT * FROM sc_issues WHERE project_id = %s AND resolved_at IS NOT NULL ORDER BY resolved_at DESC",
                (project_id,),
            )
        else:
            cur.execute(
                "SELECT * FROM sc_issues WHERE project_id = %s AND resolved_at IS NULL ORDER BY last_detected DESC",
                (project_id,),
            )
        return _dict_rows(cur)
    finally:
        conn.close()


# ─────────────────────────────────────────────
# PageSpeed 데이터 관리
# ─────────────────────────────────────────────

def save_pagespeed_data(project_id: int, crawl_run_id: int, url: str, data: dict):
    """PageSpeed 데이터 저장."""
    conn = get_db()
    try:
        cur = _cursor(conn)
        cur.execute(
            """INSERT INTO pagespeed_data
               (project_id, crawl_run_id, url, strategy, score, lcp, fid, cls, ttfb, si, tbt, opportunities_json, diagnostics_json)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                project_id,
                crawl_run_id,
                url,
                data.get("strategy", "mobile"),
                data.get("score", 0),
                data.get("lcp", 0.0),
                data.get("fid", 0.0),
                data.get("cls", 0.0),
                data.get("ttfb", 0.0),
                data.get("si", 0.0),
                data.get("tbt", 0.0),
                data.get("opportunities_json", ""),
                data.get("diagnostics_json", ""),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_pagespeed_data(project_id: int, url: str = None, crawl_run_id: int = None) -> list[dict]:
    """PageSpeed 데이터 조회"""
    conn = get_db()
    try:
        cur = _cursor(conn)
        query = "SELECT * FROM pagespeed_data WHERE project_id = %s"
        params: list = [project_id]

        if url:
            query += " AND url = %s"
            params.append(url)
        if crawl_run_id:
            query += " AND crawl_run_id = %s"
            params.append(crawl_run_id)

        query += " ORDER BY measured_at DESC"
        cur.execute(query, params)
        return _dict_rows(cur)
    finally:
        conn.close()


def get_pagespeed_history(project_id: int, url: str) -> list[dict]:
    """특정 URL의 PageSpeed 히스토리 (시간순)"""
    conn = get_db()
    try:
        cur = _cursor(conn)
        cur.execute(
            "SELECT * FROM pagespeed_data WHERE project_id = %s AND url = %s ORDER BY measured_at ASC",
            (project_id, url),
        )
        return _dict_rows(cur)
    finally:
        conn.close()


# ─────────────────────────────────────────────
# 페이지 스냅샷 및 변경 감지
# ─────────────────────────────────────────────

def save_page_snapshots(project_id: int, crawl_run_id: int, pages: list[dict]):
    """크롤링된 페이지들의 스냅샷 저장."""
    conn = get_db()
    try:
        cur = _cursor(conn)
        for page in pages:
            url = page.get("URL") or page.get("url", "")
            title = page.get("Title") or page.get("title", "")
            meta_desc = page.get("Meta Desc") or page.get("meta_description", "")
            h1 = page.get("H1") or page.get("h1", "")
            word_count = page.get("Words") or page.get("word_count", 0)
            status_code = page.get("Status") or page.get("status_code", 0)
            schema_types = page.get("Schema Types") or page.get("schema_types", "")
            canonical_url = page.get("Canonical") or page.get("canonical_url", "")
            has_canonical = 1 if canonical_url else 0
            is_https = page.get("HTTPS") or page.get("is_https", 0)
            if isinstance(is_https, bool):
                is_https = 1 if is_https else 0
            load_time = page.get("Load (s)") or page.get("load_time", 0.0)

            cur.execute(
                """INSERT INTO page_snapshots
                   (project_id, crawl_run_id, url, title, meta_description, h1,
                    word_count, status_code, schema_types, has_canonical, canonical_url,
                    is_https, load_time)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    project_id, crawl_run_id, url, title, meta_desc, h1,
                    word_count, status_code, schema_types, has_canonical, canonical_url,
                    is_https, load_time,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def get_page_snapshots(project_id: int, url: str) -> list[dict]:
    """특정 URL의 스냅샷 히스토리 (시간순)"""
    conn = get_db()
    try:
        cur = _cursor(conn)
        cur.execute(
            "SELECT * FROM page_snapshots WHERE project_id = %s AND url = %s ORDER BY snapshot_at ASC",
            (project_id, url),
        )
        return _dict_rows(cur)
    finally:
        conn.close()


def get_latest_snapshot(project_id: int, url: str) -> dict | None:
    """특정 URL의 최신 스냅샷"""
    conn = get_db()
    try:
        cur = _cursor(conn)
        cur.execute(
            "SELECT * FROM page_snapshots WHERE project_id = %s AND url = %s ORDER BY snapshot_at DESC LIMIT 1",
            (project_id, url),
        )
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def detect_page_changes(project_id: int, crawl_run_id: int) -> list[dict]:
    """현재 크롤 결과와 이전 스냅샷을 비교하여 변경 사항 탐지."""
    conn = get_db()
    try:
        cur = _cursor(conn)
        cur.execute(
            "SELECT * FROM page_snapshots WHERE project_id = %s AND crawl_run_id = %s",
            (project_id, crawl_run_id),
        )
        current_snapshots = cur.fetchall()

        compare_fields = [
            "title", "meta_description", "h1", "word_count",
            "status_code", "schema_types", "canonical_url", "is_https",
        ]

        changes = []
        now = datetime.utcnow().isoformat()

        for snap in current_snapshots:
            snap = dict(snap)
            url = snap["url"]

            cur.execute(
                """SELECT * FROM page_snapshots
                   WHERE project_id = %s AND url = %s AND crawl_run_id < %s
                   ORDER BY snapshot_at DESC LIMIT 1""",
                (project_id, url, crawl_run_id),
            )
            prev = cur.fetchone()

            if not prev:
                continue

            prev = dict(prev)

            for field in compare_fields:
                old_val = str(prev.get(field, ""))
                new_val = str(snap.get(field, ""))
                if old_val != new_val:
                    cur.execute(
                        """INSERT INTO page_changes
                           (project_id, url, field_name, old_value, new_value, crawl_run_id, detected_at)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                        (project_id, url, field, old_val, new_val, crawl_run_id, now),
                    )
                    changes.append({
                        "url": url,
                        "field_name": field,
                        "old_value": old_val,
                        "new_value": new_val,
                        "detected_at": now,
                    })

        conn.commit()
        return changes
    finally:
        conn.close()


def get_page_changes(project_id: int, crawl_run_id: int = None, url: str = None, limit: int = 100) -> list[dict]:
    """페이지 변경 기록 조회"""
    conn = get_db()
    try:
        cur = _cursor(conn)
        query = "SELECT * FROM page_changes WHERE project_id = %s"
        params: list = [project_id]

        if crawl_run_id:
            query += " AND crawl_run_id = %s"
            params.append(crawl_run_id)
        if url:
            query += " AND url = %s"
            params.append(url)

        query += " ORDER BY detected_at DESC LIMIT %s"
        params.append(limit)

        cur.execute(query, params)
        return _dict_rows(cur)
    finally:
        conn.close()


def get_page_change_summary(project_id: int, crawl_run_id: int) -> dict:
    """변경 요약"""
    conn = get_db()
    try:
        cur = _cursor(conn)
        cur.execute(
            "SELECT field_name, COUNT(*) as cnt FROM page_changes WHERE project_id = %s AND crawl_run_id = %s GROUP BY field_name",
            (project_id, crawl_run_id),
        )
        rows = cur.fetchall()
        field_counts = {r["field_name"]: r["cnt"] for r in rows}

        cur.execute(
            "SELECT COUNT(DISTINCT url) as cnt FROM page_changes WHERE project_id = %s AND crawl_run_id = %s",
            (project_id, crawl_run_id),
        )
        pages_changed_row = cur.fetchone()

        total = sum(field_counts.values())

        return {
            "total_changes": total,
            "title_changes": field_counts.get("title", 0),
            "description_changes": field_counts.get("meta_description", 0),
            "h1_changes": field_counts.get("h1", 0),
            "content_changes": field_counts.get("word_count", 0),
            "schema_changes": field_counts.get("schema_types", 0),
            "status_changes": field_counts.get("status_code", 0),
            "pages_changed": pages_changed_row["cnt"] if pages_changed_row else 0,
        }
    finally:
        conn.close()


# 테이블은 이미 생성됨 (Neon DB에서 직접 실행 완료)
# init_db()는 최초 배포 시에만 수동 호출
