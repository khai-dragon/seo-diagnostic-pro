"""
Google API 통합 모듈 - SEO 진단 도구
Google Search Console API 및 PageSpeed Insights API 연동

Dependencies:
    - google-api-python-client
    - google-auth
    - requests
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Optional

import requests
import urllib3

urllib3.disable_warnings()

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Search Console 오류 유형별 한국어 설명 + 수정 방법
# ---------------------------------------------------------------------------

SEARCH_CONSOLE_ISSUES = {
    "Server error (5xx)": {
        "description": "서버에서 5xx 에러를 반환했습니다.",
        "impact": "HIGH",
        "category": "technical",
        "fix_steps": [
            "서버 로그를 확인하여 에러 원인을 파악하세요",
            "서버 리소스(CPU, 메모리)가 충분한지 확인하세요",
            "웹 서버 설정(Nginx/Apache)을 검토하세요",
            "데이터베이스 연결 상태를 확인하세요",
        ],
    },
    "Redirect error": {
        "description": "리다이렉트 체인이 너무 길거나 순환 리다이렉트가 발생했습니다.",
        "impact": "HIGH",
        "category": "technical",
        "fix_steps": [
            "리다이렉트 체인을 3단계 이하로 줄이세요",
            "순환 리다이렉트(A→B→A)가 없는지 확인하세요",
            "가능하면 최종 URL로 직접 리다이렉트하세요",
        ],
    },
    "Submitted URL blocked by robots.txt": {
        "description": "robots.txt에 의해 크롤링이 차단되었습니다.",
        "impact": "HIGH",
        "category": "technical",
        "fix_steps": [
            "robots.txt 파일에서 해당 URL의 Disallow 규칙을 제거하세요",
            "인덱싱이 필요한 페이지는 robots.txt에서 허용해야 합니다",
            "사이트맵에 포함된 URL이 robots.txt로 차단되면 안됩니다",
        ],
    },
    "Submitted URL marked 'noindex'": {
        "description": "페이지에 noindex 메타 태그가 설정되어 있습니다.",
        "impact": "HIGH",
        "category": "technical",
        "fix_steps": [
            "meta robots 태그에서 noindex를 제거하세요",
            "X-Robots-Tag 헤더도 확인하세요",
            "인덱싱이 필요한 페이지인지 다시 검토하세요",
        ],
    },
    "Submitted URL seems to be a Soft 404": {
        "description": "페이지가 200 상태코드를 반환하지만 내용이 404 페이지와 유사합니다.",
        "impact": "MEDIUM",
        "category": "content",
        "fix_steps": [
            "페이지에 실질적인 콘텐츠를 추가하세요",
            "존재하지 않는 페이지는 실제 404 상태코드를 반환하세요",
            "빈 페이지나 placeholder 페이지를 제거하세요",
        ],
    },
    "Submitted URL returns unauthorized request (401)": {
        "description": "인증이 필요한 페이지입니다.",
        "impact": "MEDIUM",
        "category": "technical",
        "fix_steps": [
            "로그인 없이 접근 가능하도록 설정하세요",
            "Googlebot이 접근할 수 없는 페이지는 사이트맵에서 제거하세요",
        ],
    },
    "Submitted URL not found (404)": {
        "description": "페이지를 찾을 수 없습니다 (404).",
        "impact": "HIGH",
        "category": "technical",
        "fix_steps": [
            "올바른 URL로 301 리다이렉트를 설정하세요",
            "사이트맵에서 해당 URL을 제거하세요",
            "내부 링크에서 이 URL을 참조하는 곳을 수정하세요",
        ],
    },
    "Crawled - currently not indexed": {
        "description": "Google이 크롤링했지만 인덱싱하지 않기로 결정했습니다.",
        "impact": "MEDIUM",
        "category": "content",
        "fix_steps": [
            "콘텐츠의 품질과 고유성을 높이세요",
            "내부 링크를 추가하여 페이지 중요도를 높이세요",
            "중복 콘텐츠가 아닌지 확인하세요",
            "canonical URL이 올바르게 설정되었는지 확인하세요",
        ],
    },
    "Discovered - currently not indexed": {
        "description": "Google이 URL을 발견했지만 아직 크롤링하지 않았습니다.",
        "impact": "LOW",
        "category": "technical",
        "fix_steps": [
            "사이트맵을 제출하고 인덱싱 요청을 하세요",
            "사이트의 크롤링 예산을 확인하세요",
            "내부 링크 구조를 개선하세요",
            "시간이 지나면 자동으로 크롤링될 수 있습니다",
        ],
    },
    "Duplicate without user-selected canonical": {
        "description": "Google이 이 페이지를 중복으로 판단하고 다른 URL을 canonical로 선택했습니다.",
        "impact": "MEDIUM",
        "category": "content",
        "fix_steps": [
            "canonical 태그를 명시적으로 설정하세요",
            "중복 콘텐츠를 제거하거나 차별화하세요",
            "URL 파라미터로 인한 중복이면 canonical을 파라미터 없는 URL로 설정하세요",
        ],
    },
    "Duplicate, Google chose different canonical than user": {
        "description": "사용자가 설정한 canonical과 Google이 선택한 canonical이 다릅니다.",
        "impact": "MEDIUM",
        "category": "technical",
        "fix_steps": [
            "canonical URL이 정확한지 확인하세요",
            "원본 페이지와 중복 페이지의 콘텐츠가 실질적으로 같은지 확인하세요",
            "301 리다이렉트를 사용하여 정규 URL을 명확하게 하세요",
        ],
    },
    "Blocked due to access forbidden (403)": {
        "description": "서버가 접근을 거부했습니다 (403).",
        "impact": "HIGH",
        "category": "technical",
        "fix_steps": [
            "서버 방화벽이나 접근 제어 설정을 확인하세요",
            "Googlebot의 IP가 차단되지 않았는지 확인하세요",
            ".htaccess나 서버 설정을 검토하세요",
        ],
    },
    "Page with redirect": {
        "description": "이 URL은 다른 페이지로 리다이렉트됩니다.",
        "impact": "LOW",
        "category": "technical",
        "fix_steps": [
            "의도된 리다이렉트인 경우 정상입니다",
            "내부 링크를 최종 URL로 업데이트하세요",
            "사이트맵에서 리다이렉트 URL을 제거하세요",
        ],
    },
    "Alternate page with proper canonical tag": {
        "description": "canonical 태그로 다른 페이지를 가리키는 대체 페이지입니다.",
        "impact": "LOW",
        "category": "content",
        "fix_steps": [
            "이것은 일반적으로 정상적인 상태입니다",
            "canonical이 올바른 원본 페이지를 가리키는지 확인하세요",
        ],
    },
}


# ---------------------------------------------------------------------------
# PageSpeed 기회(opportunity) 한국어 설명
# ---------------------------------------------------------------------------

PAGESPEED_OPPORTUNITIES = {
    "render-blocking-resources": {
        "title": "렌더링 차단 리소스 제거",
        "fix": "CSS와 JavaScript를 비동기로 로드하거나 인라인으로 변경하세요. <script defer> 또는 <link rel='preload'>를 사용하세요.",
    },
    "unused-css-rules": {
        "title": "사용하지 않는 CSS 제거",
        "fix": "PurgeCSS 등의 도구로 미사용 CSS를 제거하세요. 페이지별로 필요한 CSS만 로드하세요.",
    },
    "unused-javascript": {
        "title": "사용하지 않는 JavaScript 제거",
        "fix": "코드 스플리팅과 트리 셰이킹을 적용하세요. 불필요한 라이브러리를 제거하세요.",
    },
    "uses-responsive-images": {
        "title": "적절한 크기의 이미지 사용",
        "fix": "srcset 속성으로 다양한 화면 크기에 맞는 이미지를 제공하세요. CDN의 이미지 리사이징을 활용하세요.",
    },
    "offscreen-images": {
        "title": "화면 밖 이미지 지연 로딩",
        "fix": "loading='lazy' 속성을 추가하세요. Intersection Observer API를 활용하세요.",
    },
    "unminified-css": {
        "title": "CSS 압축",
        "fix": "CSS 파일을 minify하세요. cssnano, clean-css 등을 빌드 프로세스에 추가하세요.",
    },
    "unminified-javascript": {
        "title": "JavaScript 압축",
        "fix": "JavaScript 파일을 minify하세요. Terser, UglifyJS를 빌드 프로세스에 추가하세요.",
    },
    "uses-text-compression": {
        "title": "텍스트 압축 활성화",
        "fix": "서버에서 Gzip 또는 Brotli 압축을 활성화하세요.",
    },
    "uses-optimized-images": {
        "title": "이미지 최적화",
        "fix": "이미지를 WebP/AVIF 형식으로 변환하세요. 이미지 압축 도구(TinyPNG, ImageOptim)를 사용하세요.",
    },
    "modern-image-formats": {
        "title": "차세대 이미지 형식 사용",
        "fix": "WebP 또는 AVIF 형식으로 이미지를 제공하세요. <picture> 태그로 폴백을 제공하세요.",
    },
    "uses-long-cache-ttl": {
        "title": "정적 자산에 캐시 정책 적용",
        "fix": "정적 파일에 적절한 Cache-Control 헤더를 설정하세요. 최소 1년의 max-age를 권장합니다.",
    },
    "efficient-animated-content": {
        "title": "애니메이션 콘텐츠 최적화",
        "fix": "GIF 대신 비디오 형식(MP4, WebM)을 사용하세요. CSS 애니메이션을 활용하세요.",
    },
    "duplicated-javascript": {
        "title": "중복 JavaScript 모듈 제거",
        "fix": "번들러 설정을 확인하여 중복 모듈을 제거하세요.",
    },
    "legacy-javascript": {
        "title": "레거시 JavaScript 제거",
        "fix": "최신 브라우저에는 폴리필이 필요 없습니다. module/nomodule 패턴을 사용하세요.",
    },
    "total-byte-weight": {
        "title": "총 페이지 용량 줄이기",
        "fix": "불필요한 리소스를 제거하고, 압축과 캐싱을 활용하세요. 목표: 1.6MB 이하.",
    },
    "dom-size": {
        "title": "DOM 크기 줄이기",
        "fix": "불필요한 HTML 요소를 제거하세요. 가상 스크롤링이나 지연 렌더링을 고려하세요. 목표: 1,500개 노드 이하.",
    },
    "server-response-time": {
        "title": "서버 응답 시간 단축 (TTFB)",
        "fix": "서버 사이드 캐싱, CDN, 데이터베이스 최적화를 적용하세요. 목표: 200ms 이하.",
    },
    "font-display": {
        "title": "웹폰트 로딩 최적화",
        "fix": "font-display: swap 또는 optional을 사용하세요. 폰트를 preload하세요.",
    },
    "third-party-summary": {
        "title": "서드파티 코드 영향 줄이기",
        "fix": "불필요한 서드파티 스크립트를 제거하세요. 필수적인 것만 비동기로 로드하세요.",
    },
    "mainthread-work-breakdown": {
        "title": "메인 스레드 작업 최소화",
        "fix": "JavaScript 실행 시간을 줄이세요. 코드 스플리팅과 Web Worker를 활용하세요.",
    },
    "bootup-time": {
        "title": "JavaScript 실행 시간 단축",
        "fix": "코드를 분할하고 불필요한 스크립트를 제거하세요. 트리 셰이킹을 적용하세요.",
    },
    "critical-request-chains": {
        "title": "중요 요청 체인 최적화",
        "fix": "중요 리소스를 preload하세요. 렌더링 차단 요청을 최소화하세요.",
    },
    "redirects": {
        "title": "리다이렉트 줄이기",
        "fix": "불필요한 리다이렉트를 제거하세요. 직접 최종 URL로 연결하세요.",
    },
}


# ---------------------------------------------------------------------------
# CWV 등급 판정 헬퍼
# ---------------------------------------------------------------------------

def rate_lcp(value_seconds: float) -> str:
    """LCP 등급 판정: good < 2.5s, poor > 4.0s"""
    if value_seconds <= 2.5:
        return "good"
    elif value_seconds <= 4.0:
        return "needs-improvement"
    return "poor"


def rate_fid(value_ms: float) -> str:
    """FID/INP 등급: good < 100ms, poor > 300ms"""
    if value_ms <= 100:
        return "good"
    elif value_ms <= 300:
        return "needs-improvement"
    return "poor"


def rate_cls(value: float) -> str:
    """CLS 등급: good < 0.1, poor > 0.25"""
    if value <= 0.1:
        return "good"
    elif value <= 0.25:
        return "needs-improvement"
    return "poor"


def rate_ttfb(value_ms: float) -> str:
    """TTFB 등급: good < 800ms, poor > 1800ms"""
    if value_ms <= 800:
        return "good"
    elif value_ms <= 1800:
        return "needs-improvement"
    return "poor"


# 등급별 색상 및 한국어 라벨
CWV_RATING_COLORS = {
    "good": "#3fb950",
    "needs-improvement": "#d29922",
    "poor": "#f85149",
}

CWV_RATING_LABELS = {
    "good": "양호",
    "needs-improvement": "개선 필요",
    "poor": "나쁨",
}


# ---------------------------------------------------------------------------
# 이슈 설명 헬퍼 함수
# ---------------------------------------------------------------------------

def explain_sc_issue(issue_type: str) -> dict:
    """Search Console 이슈 유형에 대한 한국어 설명 반환"""
    default = {
        "description": issue_type,
        "impact": "MEDIUM",
        "category": "technical",
        "fix_steps": ["Google Search Console 문서를 참고하세요."],
    }
    return SEARCH_CONSOLE_ISSUES.get(issue_type, default)


def explain_pagespeed_opportunity(audit_id: str) -> dict:
    """PageSpeed 기회 항목에 대한 한국어 설명 반환"""
    default = {"title": audit_id, "fix": "해당 항목을 검토하고 최적화하세요."}
    return PAGESPEED_OPPORTUNITIES.get(audit_id, default)


# ---------------------------------------------------------------------------
# SearchConsoleClient
# ---------------------------------------------------------------------------

class SearchConsoleClient:
    """Google Search Console API 클라이언트"""

    SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]

    def __init__(self, credentials_json: str):
        """
        Service Account JSON 문자열로 초기화.

        Args:
            credentials_json: Service Account JSON 키 문자열
        """
        self.service = None
        self._credentials = None

        try:
            from google.oauth2 import service_account
            from googleapiclient.discovery import build

            info = json.loads(credentials_json)
            self._credentials = service_account.Credentials.from_service_account_info(
                info, scopes=self.SCOPES
            )
            self.service = build(
                "searchconsole", "v1", credentials=self._credentials
            )
            logger.info("Search Console API 서비스 초기화 완료")
        except Exception as e:
            logger.error(f"Search Console API 초기화 실패: {e}")
            self.service = None

    # ------------------------------------------------------------------
    # 사이트 관리
    # ------------------------------------------------------------------

    def verify_site_access(self, site_url: str) -> bool:
        """사이트 접근 권한 확인"""
        try:
            sites = self.list_sites()
            return any(s.get("siteUrl") == site_url for s in sites)
        except Exception as e:
            logger.error(f"사이트 접근 권한 확인 실패: {e}")
            return False

    def list_sites(self) -> list[dict]:
        """접근 가능한 모든 사이트 목록 반환"""
        if not self.service:
            logger.warning("Search Console 서비스가 초기화되지 않았습니다.")
            return []
        try:
            response = self.service.sites().list().execute()
            sites = response.get("siteEntry", [])
            return [
                {
                    "siteUrl": s.get("siteUrl", ""),
                    "permissionLevel": s.get("permissionLevel", ""),
                }
                for s in sites
            ]
        except Exception as e:
            logger.error(f"사이트 목록 조회 실패: {e}")
            return []

    # ------------------------------------------------------------------
    # Search Analytics
    # ------------------------------------------------------------------

    def get_search_analytics(
        self,
        site_url: str,
        start_date: str,
        end_date: str,
        dimensions: Optional[list] = None,
        row_limit: int = 5000,
        dimension_filter_groups: Optional[list] = None,
    ) -> list[dict]:
        """
        Search Analytics 데이터를 가져옵니다.

        Args:
            site_url: SC에 등록된 사이트 URL (예: "sc-domain:example.com")
            start_date: "YYYY-MM-DD"
            end_date: "YYYY-MM-DD"
            dimensions: ['query', 'page', 'device', 'country', 'date'] 등
            row_limit: 최대 행 수 (API 최대 25000)
            dimension_filter_groups: 필터 그룹

        Returns:
            [{"keys": [...], "clicks": int, "impressions": int,
              "ctr": float, "position": float}, ...]
        """
        if not self.service:
            logger.warning("Search Console 서비스가 초기화되지 않았습니다.")
            return []

        try:
            body: dict = {
                "startDate": start_date,
                "endDate": end_date,
                "rowLimit": min(row_limit, 25000),
            }
            if dimensions:
                body["dimensions"] = dimensions
            if dimension_filter_groups:
                body["dimensionFilterGroups"] = dimension_filter_groups

            response = (
                self.service.searchanalytics()
                .query(siteUrl=site_url, body=body)
                .execute()
            )
            rows = response.get("rows", [])
            return [
                {
                    "keys": row.get("keys", []),
                    "clicks": row.get("clicks", 0),
                    "impressions": row.get("impressions", 0),
                    "ctr": round(row.get("ctr", 0.0), 4),
                    "position": round(row.get("position", 0.0), 1),
                }
                for row in rows
            ]
        except Exception as e:
            logger.error(f"Search Analytics 조회 실패: {e}")
            return []

    def get_page_analytics(
        self, site_url: str, page_url: str, start_date: str, end_date: str
    ) -> dict:
        """
        특정 페이지의 트래픽 데이터를 가져옵니다.

        Returns:
            {
                "total_clicks": int,
                "total_impressions": int,
                "avg_ctr": float,
                "avg_position": float,
                "top_queries": [...],
                "daily": [...]
            }
        """
        result = {
            "total_clicks": 0,
            "total_impressions": 0,
            "avg_ctr": 0.0,
            "avg_position": 0.0,
            "top_queries": [],
            "daily": [],
        }

        # 페이지 필터 설정
        page_filter = [
            {
                "filters": [
                    {
                        "dimension": "page",
                        "operator": "equals",
                        "expression": page_url,
                    }
                ]
            }
        ]

        try:
            # 전체 요약 데이터
            summary_rows = self.get_search_analytics(
                site_url,
                start_date,
                end_date,
                dimensions=[],
                dimension_filter_groups=page_filter,
            )
            if summary_rows:
                row = summary_rows[0]
                result["total_clicks"] = row.get("clicks", 0)
                result["total_impressions"] = row.get("impressions", 0)
                result["avg_ctr"] = row.get("ctr", 0.0)
                result["avg_position"] = row.get("position", 0.0)

            # 상위 쿼리
            query_rows = self.get_search_analytics(
                site_url,
                start_date,
                end_date,
                dimensions=["query"],
                row_limit=50,
                dimension_filter_groups=page_filter,
            )
            result["top_queries"] = [
                {
                    "query": row["keys"][0] if row.get("keys") else "",
                    "clicks": row.get("clicks", 0),
                    "impressions": row.get("impressions", 0),
                    "ctr": row.get("ctr", 0.0),
                    "position": row.get("position", 0.0),
                }
                for row in query_rows
            ]

            # 일별 데이터
            daily_rows = self.get_search_analytics(
                site_url,
                start_date,
                end_date,
                dimensions=["date"],
                dimension_filter_groups=page_filter,
            )
            result["daily"] = [
                {
                    "date": row["keys"][0] if row.get("keys") else "",
                    "clicks": row.get("clicks", 0),
                    "impressions": row.get("impressions", 0),
                }
                for row in daily_rows
            ]

        except Exception as e:
            logger.error(f"페이지 분석 조회 실패 ({page_url}): {e}")

        return result

    def get_top_pages(
        self, site_url: str, start_date: str, end_date: str, limit: int = 100
    ) -> list[dict]:
        """
        클릭 수 기준 상위 페이지 목록.

        Returns:
            [{"page": str, "clicks": int, "impressions": int,
              "ctr": float, "position": float}, ...]
        """
        rows = self.get_search_analytics(
            site_url, start_date, end_date, dimensions=["page"], row_limit=limit
        )
        return [
            {
                "page": row["keys"][0] if row.get("keys") else "",
                "clicks": row.get("clicks", 0),
                "impressions": row.get("impressions", 0),
                "ctr": row.get("ctr", 0.0),
                "position": row.get("position", 0.0),
            }
            for row in rows
        ]

    def get_top_queries(
        self, site_url: str, start_date: str, end_date: str, limit: int = 100
    ) -> list[dict]:
        """
        클릭 수 기준 상위 키워드 목록.

        Returns:
            [{"query": str, "clicks": int, "impressions": int,
              "ctr": float, "position": float}, ...]
        """
        rows = self.get_search_analytics(
            site_url, start_date, end_date, dimensions=["query"], row_limit=limit
        )
        return [
            {
                "query": row["keys"][0] if row.get("keys") else "",
                "clicks": row.get("clicks", 0),
                "impressions": row.get("impressions", 0),
                "ctr": row.get("ctr", 0.0),
                "position": row.get("position", 0.0),
            }
            for row in rows
        ]

    def get_performance_by_device(
        self, site_url: str, start_date: str, end_date: str
    ) -> list[dict]:
        """기기별 성과 데이터"""
        rows = self.get_search_analytics(
            site_url, start_date, end_date, dimensions=["device"]
        )
        return [
            {
                "device": row["keys"][0] if row.get("keys") else "",
                "clicks": row.get("clicks", 0),
                "impressions": row.get("impressions", 0),
                "ctr": row.get("ctr", 0.0),
                "position": row.get("position", 0.0),
            }
            for row in rows
        ]

    def get_performance_by_country(
        self, site_url: str, start_date: str, end_date: str
    ) -> list[dict]:
        """국가별 성과 데이터"""
        rows = self.get_search_analytics(
            site_url, start_date, end_date, dimensions=["country"]
        )
        return [
            {
                "country": row["keys"][0] if row.get("keys") else "",
                "clicks": row.get("clicks", 0),
                "impressions": row.get("impressions", 0),
                "ctr": row.get("ctr", 0.0),
                "position": row.get("position", 0.0),
            }
            for row in rows
        ]

    def get_daily_trend(
        self, site_url: str, start_date: str, end_date: str
    ) -> list[dict]:
        """
        일별 트렌드 데이터.

        Returns:
            [{"date": str, "clicks": int, "impressions": int,
              "ctr": float, "position": float}, ...]
        """
        rows = self.get_search_analytics(
            site_url, start_date, end_date, dimensions=["date"]
        )
        return [
            {
                "date": row["keys"][0] if row.get("keys") else "",
                "clicks": row.get("clicks", 0),
                "impressions": row.get("impressions", 0),
                "ctr": row.get("ctr", 0.0),
                "position": row.get("position", 0.0),
            }
            for row in rows
        ]

    # ------------------------------------------------------------------
    # 사이트맵
    # ------------------------------------------------------------------

    def get_sitemaps(self, site_url: str) -> list[dict]:
        """등록된 사이트맵 목록"""
        if not self.service:
            return []
        try:
            response = self.service.sitemaps().list(siteUrl=site_url).execute()
            sitemaps = response.get("sitemap", [])
            return [
                {
                    "path": sm.get("path", ""),
                    "lastSubmitted": sm.get("lastSubmitted", ""),
                    "isPending": sm.get("isPending", False),
                    "isSitemapsIndex": sm.get("isSitemapsIndex", False),
                    "type": sm.get("type", ""),
                    "lastDownloaded": sm.get("lastDownloaded", ""),
                    "warnings": sm.get("warnings", 0),
                    "errors": sm.get("errors", 0),
                }
                for sm in sitemaps
            ]
        except Exception as e:
            logger.error(f"사이트맵 목록 조회 실패: {e}")
            return []

    # ------------------------------------------------------------------
    # URL Inspection
    # ------------------------------------------------------------------

    def inspect_url(self, site_url: str, page_url: str) -> dict:
        """
        URL Inspection API로 인덱싱 상태 확인.

        Note: 이 API는 엄격한 속도 제한이 있습니다. 신중하게 사용하세요.

        Returns:
            {
                "indexing_state": str,
                "crawl_state": str,
                "robots_txt_state": str,
                "last_crawl_time": str,
                "page_fetch_state": str,
                "referring_urls": list,
                "issues": list,
            }
        """
        default_result = {
            "indexing_state": "UNKNOWN",
            "crawl_state": "UNKNOWN",
            "robots_txt_state": "UNKNOWN",
            "last_crawl_time": "",
            "page_fetch_state": "UNKNOWN",
            "referring_urls": [],
            "issues": [],
        }

        if not self.service:
            default_result["issues"] = ["Search Console 서비스가 초기화되지 않았습니다."]
            return default_result

        try:
            request_body = {
                "inspectionUrl": page_url,
                "siteUrl": site_url,
            }
            response = (
                self.service.urlInspection()
                .index()
                .inspect(body=request_body)
                .execute()
            )

            inspection = response.get("inspectionResult", {})
            index_status = inspection.get("indexStatusResult", {})
            crawl_result = inspection.get("crawlResult", {}) if "crawlResult" in inspection else {}

            # 인덱싱 상태 파싱
            result = {
                "indexing_state": index_status.get("coverageState", "UNKNOWN"),
                "crawl_state": index_status.get("crawledAs", "UNKNOWN"),
                "robots_txt_state": index_status.get("robotsTxtState", "UNKNOWN"),
                "last_crawl_time": index_status.get("lastCrawlTime", ""),
                "page_fetch_state": index_status.get("pageFetchState", "UNKNOWN"),
                "referring_urls": index_status.get("referringUrls", []),
                "issues": [],
            }

            # 이슈 수집
            indexing_state = index_status.get("coverageState", "")
            if indexing_state and indexing_state not in (
                "Submitted and indexed",
                "Indexed, not submitted in sitemap",
            ):
                result["issues"].append(indexing_state)

            return result

        except Exception as e:
            error_msg = str(e)
            logger.error(f"URL Inspection 실패 ({page_url}): {error_msg}")

            # 속도 제한 에러 처리
            if "429" in error_msg or "rateLimitExceeded" in error_msg:
                default_result["issues"] = [
                    "API 속도 제한에 도달했습니다. 잠시 후 다시 시도하세요."
                ]
            else:
                default_result["issues"] = [f"URL 검사 실패: {error_msg}"]

            return default_result


# ---------------------------------------------------------------------------
# PageSpeedClient
# ---------------------------------------------------------------------------

class PageSpeedClient:
    """Google PageSpeed Insights API 클라이언트"""

    API_URL = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"

    def __init__(self, api_key: str = ""):
        self.api_key = api_key

    def _build_params(self, url: str, strategy: str = "mobile") -> dict:
        """API 요청 파라미터 생성"""
        params = {
            "url": url,
            "strategy": strategy,
            "category": "PERFORMANCE",
        }
        if self.api_key:
            params["key"] = self.api_key
        return params

    def _safe_numeric(self, value, default=0.0) -> float:
        """안전하게 숫자 변환"""
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def get_full_report(self, url: str, strategy: str = "mobile") -> dict:
        """
        Full Lighthouse 리포트.

        Args:
            url: 분석할 URL
            strategy: "mobile" or "desktop"

        Returns:
            성능 점수, Core Web Vitals, 개선 기회, 진단 결과를 포함한 dict.
            API 실패 시 {"error": str, "score": 0, ...} 반환.
        """
        error_result = {
            "error": "",
            "score": 0,
            "core_web_vitals": {
                "lcp": 0.0, "lcp_rating": "poor",
                "fid": 0.0, "fid_rating": "poor",
                "cls": 0.0, "cls_rating": "poor",
                "ttfb": 0.0, "ttfb_rating": "poor",
                "si": 0.0,
                "tbt": 0.0,
            },
            "opportunities": [],
            "diagnostics": [],
            "passed_audits": 0,
            "total_audits": 0,
        }

        try:
            params = self._build_params(url, strategy)
            resp = requests.get(
                self.API_URL, params=params, timeout=60, verify=False
            )

            if resp.status_code != 200:
                error_result["error"] = (
                    f"PageSpeed API HTTP {resp.status_code}: {resp.text[:200]}"
                )
                logger.error(error_result["error"])
                return error_result

            data = resp.json()

        except requests.exceptions.Timeout:
            error_result["error"] = "PageSpeed API 요청 시간이 초과되었습니다."
            logger.error(error_result["error"])
            return error_result
        except requests.exceptions.RequestException as e:
            error_result["error"] = f"PageSpeed API 요청 실패: {e}"
            logger.error(error_result["error"])
            return error_result
        except json.JSONDecodeError:
            error_result["error"] = "PageSpeed API 응답을 파싱할 수 없습니다."
            logger.error(error_result["error"])
            return error_result

        # Lighthouse 결과 파싱
        try:
            lighthouse = data.get("lighthouseResult", {})
            categories = lighthouse.get("categories", {})
            perf = categories.get("performance", {})
            audits = lighthouse.get("audits", {})

            # 성능 점수 (0-100)
            score = int((perf.get("score", 0) or 0) * 100)

            # Core Web Vitals 추출
            lcp_audit = audits.get("largest-contentful-paint", {})
            lcp_val = self._safe_numeric(lcp_audit.get("numericValue", 0)) / 1000  # ms → s

            fid_audit = audits.get("max-potential-fid", {})
            # INP가 있으면 INP 사용, 없으면 FID 사용
            inp_audit = audits.get("interaction-to-next-paint", {})
            if inp_audit.get("numericValue") is not None:
                fid_val = self._safe_numeric(inp_audit.get("numericValue", 0))
            else:
                fid_val = self._safe_numeric(fid_audit.get("numericValue", 0))

            cls_audit = audits.get("cumulative-layout-shift", {})
            cls_val = self._safe_numeric(cls_audit.get("numericValue", 0))

            ttfb_audit = audits.get("server-response-time", {})
            ttfb_val = self._safe_numeric(ttfb_audit.get("numericValue", 0))

            si_audit = audits.get("speed-index", {})
            si_val = self._safe_numeric(si_audit.get("numericValue", 0)) / 1000  # ms → s

            tbt_audit = audits.get("total-blocking-time", {})
            tbt_val = self._safe_numeric(tbt_audit.get("numericValue", 0))

            core_web_vitals = {
                "lcp": round(lcp_val, 2),
                "lcp_rating": rate_lcp(lcp_val),
                "fid": round(fid_val, 1),
                "fid_rating": rate_fid(fid_val),
                "cls": round(cls_val, 3),
                "cls_rating": rate_cls(cls_val),
                "ttfb": round(ttfb_val, 1),
                "ttfb_rating": rate_ttfb(ttfb_val),
                "si": round(si_val, 2),
                "tbt": round(tbt_val, 1),
            }

            # 개선 기회(Opportunities) 추출
            opportunities = []
            for audit_ref in perf.get("auditRefs", []):
                audit_id = audit_ref.get("id", "")
                audit = audits.get(audit_id, {})
                audit_score = audit.get("score")

                # score가 None이거나 1 미만인 항목 중 개선 기회인 것
                if audit_ref.get("group") == "load-opportunities" and (
                    audit_score is not None and audit_score < 1
                ):
                    details = audit.get("details", {})
                    savings_ms = self._safe_numeric(
                        details.get("overallSavingsMs", 0)
                    )
                    savings_bytes = int(
                        self._safe_numeric(details.get("overallSavingsBytes", 0))
                    )

                    opportunities.append(
                        {
                            "id": audit_id,
                            "title": audit.get("title", audit_id),
                            "description": audit.get("description", ""),
                            "score": round(self._safe_numeric(audit_score), 2),
                            "savings_ms": round(savings_ms, 1),
                            "savings_bytes": savings_bytes,
                            "details": details,
                        }
                    )

            # 절감 시간 기준 내림차순 정렬
            opportunities.sort(key=lambda x: x["savings_ms"], reverse=True)

            # 진단(Diagnostics) 추출
            diagnostics = []
            for audit_ref in perf.get("auditRefs", []):
                audit_id = audit_ref.get("id", "")
                audit = audits.get(audit_id, {})
                audit_score = audit.get("score")

                if audit_ref.get("group") == "diagnostics" and (
                    audit_score is not None and audit_score < 1
                ):
                    diagnostics.append(
                        {
                            "id": audit_id,
                            "title": audit.get("title", audit_id),
                            "description": audit.get("description", ""),
                            "score": round(self._safe_numeric(audit_score), 2),
                            "details": audit.get("details", {}),
                        }
                    )

            # 통과/전체 감사 수
            passed = sum(
                1
                for a in audits.values()
                if a.get("score") is not None and a.get("score") >= 0.9
            )
            total = sum(1 for a in audits.values() if a.get("score") is not None)

            return {
                "score": score,
                "core_web_vitals": core_web_vitals,
                "opportunities": opportunities,
                "diagnostics": diagnostics,
                "passed_audits": passed,
                "total_audits": total,
            }

        except Exception as e:
            error_result["error"] = f"Lighthouse 결과 파싱 실패: {e}"
            logger.error(error_result["error"])
            return error_result

    def get_core_web_vitals(self, url: str) -> dict:
        """CWV만 빠르게 가져오기"""
        report = self.get_full_report(url, strategy="mobile")
        if report.get("error"):
            return {
                "error": report["error"],
                "lcp": 0.0, "lcp_rating": "poor",
                "fid": 0.0, "fid_rating": "poor",
                "cls": 0.0, "cls_rating": "poor",
                "ttfb": 0.0, "ttfb_rating": "poor",
            }
        cwv = report.get("core_web_vitals", {})
        return {
            "lcp": cwv.get("lcp", 0.0),
            "lcp_rating": cwv.get("lcp_rating", "poor"),
            "fid": cwv.get("fid", 0.0),
            "fid_rating": cwv.get("fid_rating", "poor"),
            "cls": cwv.get("cls", 0.0),
            "cls_rating": cwv.get("cls_rating", "poor"),
            "ttfb": cwv.get("ttfb", 0.0),
            "ttfb_rating": cwv.get("ttfb_rating", "poor"),
        }
