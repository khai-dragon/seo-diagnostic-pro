# 🔍 SEO Diagnostic Pro

**Screaming Frog 스타일 완전 자동 SEO 진단 웹 툴**

[![Open in Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://share.streamlit.io)

## 기능

- **사이트 크롤링** — 링크를 따라가며 전체 사이트 수집
- **사이트맵 기반 수집** — sitemap.xml 기반으로 빠르게 수집
- **Title / Meta Description** — 길이 검사 + 중복 탐지
- **H1 태그** — 존재 여부 + 내용 분석
- **콘텐츠 단어 수** — 300단어 미만 얇은 콘텐츠 경고
- **내부 링크 분석** — Outgoing + Incoming 링크
- **이미지 Alt 텍스트** — 누락 탐지
- **HTTP 상태 코드** — 4xx/5xx 에러 탐지
- **PageSpeed Insights** — Mobile 점수 (API 키 필요)
- **사이트 구조 트리** — 폴더 구조 시각화
- **JSON 리포트** — 다운로드 가능

## 실행 방법

```bash
pip install -r requirements.txt
streamlit run app.py
```

## 배포

Streamlit Community Cloud에서 무료 배포 가능:
1. GitHub에 push
2. [share.streamlit.io](https://share.streamlit.io) 접속
3. 이 레포 선택 → Deploy

## 기술 스택

- Python 3.9+
- Streamlit
- Requests + BeautifulSoup4
