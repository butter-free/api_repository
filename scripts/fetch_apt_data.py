"""
국토교통부 아파트 매매 실거래가 수집 스크립트
매월 1일 GitHub Actions에서 실행 → data/apt_deals.json 생성
"""

import requests
import json
import os
import time
from datetime import datetime, timedelta

API_KEY = os.environ["MOLIT_API_KEY"]
BASE_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"

# 전달 기준 (yyyyMM)
now = datetime.now()
last_month = (now.replace(day=1) - timedelta(days=1))
DEAL_YMD = last_month.strftime("%Y%m")

SEOUL_DISTRICTS = [
    ("11110", "종로구"),   ("11140", "중구"),     ("11170", "용산구"),
    ("11200", "성동구"),   ("11215", "광진구"),   ("11230", "동대문구"),
    ("11260", "중랑구"),   ("11290", "성북구"),   ("11305", "강북구"),
    ("11320", "도봉구"),   ("11350", "노원구"),   ("11380", "은평구"),
    ("11410", "서대문구"), ("11440", "마포구"),   ("11470", "양천구"),
    ("11500", "강서구"),   ("11530", "구로구"),   ("11545", "금천구"),
    ("11560", "영등포구"), ("11590", "동작구"),   ("11620", "관악구"),
    ("11650", "서초구"),   ("11680", "강남구"),   ("11710", "송파구"),
    ("11740", "강동구"),
]

GYEONGGI_DISTRICTS = [
    ("41290", "과천시"),        ("41135", "성남 분당구"),   ("41465", "용인 수지구"),
    ("41450", "하남시"),        ("41463", "용인 기흥구"),   ("41173", "안양 동안구"),
    ("41117", "수원 영통구"),   ("41210", "광명시"),        ("41285", "고양 일산동구"),
    ("41287", "고양 일산서구"), ("41430", "의왕시"),        ("41310", "구리시"),
    ("41360", "남양주시"),      ("41190", "부천시"),        ("41150", "의정부시"),
    ("41570", "김포시"),        ("41590", "화성시"),        ("41410", "군포시"),
    ("41480", "파주시"),        ("41271", "안산 상록구"),
]

OTHER_DISTRICTS = [
    # 인천
    ("28170", "인천 미추홀구", "인천"), ("28185", "인천 연수구",   "인천"),
    ("28200", "인천 남동구",   "인천"), ("28237", "인천 부평구",   "인천"),
    ("28245", "인천 계양구",   "인천"), ("28260", "인천 서구",     "인천"),
    # 부산
    ("26215", "부산진구",      "부산"), ("26230", "부산 동래구",   "부산"),
    ("26260", "부산 남구",     "부산"), ("26305", "부산 해운대구", "부산"),
    ("26350", "부산 연제구",   "부산"), ("26360", "부산 수영구",   "부산"),
    # 대구
    ("27230", "대구 북구",     "대구"), ("27260", "대구 수성구",   "대구"),
    ("27290", "대구 달서구",   "대구"),
    # 광주
    ("29155", "광주 남구",     "광주"), ("29170", "광주 북구",     "광주"),
    ("29200", "광주 광산구",   "광주"),
    # 대전
    ("30170", "대전 서구",     "대전"), ("30200", "대전 유성구",   "대전"),
    ("30230", "대전 대덕구",   "대전"),
    # 울산
    ("31110", "울산 중구",     "울산"), ("31140", "울산 남구",     "울산"),
    ("31710", "울산 울주군",   "울산"),
    # 세종
    ("36110", "세종시",        "세종"),
    # 강원
    ("51110", "춘천시",        "강원"), ("51130", "원주시",        "강원"),
    ("51150", "강릉시",        "강원"),
    # 충북
    ("43111", "청주 상당구",   "충북"), ("43113", "청주 흥덕구",   "충북"),
    ("43130", "충주시",        "충북"),
    # 충남
    ("44130", "천안 동남구",   "충남"), ("44131", "천안 서북구",   "충남"),
    ("44200", "아산시",        "충남"), ("44210", "서산시",        "충남"),
    # 전북
    ("52111", "전주 완산구",   "전북"), ("52113", "전주 덕진구",   "전북"),
    ("52130", "군산시",        "전북"), ("52140", "익산시",        "전북"),
    # 전남
    ("46110", "목포시",        "전남"), ("46130", "여수시",        "전남"),
    ("46150", "순천시",        "전남"), ("46230", "광양시",        "전남"),
    # 경북
    ("47111", "포항 남구",     "경북"), ("47190", "구미시",        "경북"),
    ("47280", "경산시",        "경북"),
    # 경남
    ("48121", "창원 의창구",   "경남"), ("48270", "김해시",        "경남"),
    ("48310", "거제시",        "경남"), ("48330", "양산시",        "경남"),
    # 제주
    ("50110", "제주시",        "제주"), ("50130", "서귀포시",      "제주"),
]


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
}

def fetch_district(code: str, name: str, region: str) -> list:
    # serviceKey는 URL인코딩 없이 직접 삽입 (requests params= 사용 시 이중 인코딩 방지)
    url = (
        f"{BASE_URL}"
        f"?serviceKey={API_KEY}"
        f"&LAWD_CD={code}"
        f"&DEAL_YMD={DEAL_YMD}"
        f"&numOfRows=1000"
        f"&pageNo=1"
        f"&_type=json"
    )
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        print(f"  [{name}] HTTP {resp.status_code} / {len(resp.content)} bytes")
        if resp.status_code != 200 or not resp.content:
            print(f"  [{name}] raw: {resp.text[:200]}")
            return []
        data = resp.json()

        body = data["response"]["body"]
        items_wrapper = body.get("items")

        # 결과 없을 때 "items": "" 처리
        if not items_wrapper or not isinstance(items_wrapper, dict):
            print(f"  [{name}] 거래 없음")
            return []

        item_list = items_wrapper.get("item", [])
        if isinstance(item_list, dict):
            item_list = [item_list]

        deals = []
        for item in item_list:
            try:
                amount_str = item.get("dealAmount", "0").replace(",", "").strip()
                amount = int(amount_str) * 10_000
                if amount <= 0:
                    continue
                deals.append({
                    "districtName": name,
                    "regionName": region,
                    "dong": item.get("umdNm", "").strip(),
                    "aptName": item.get("aptNm", "").strip(),
                    "dealAmount": amount,
                })
            except (ValueError, AttributeError):
                continue

        deals.sort(key=lambda x: x["dealAmount"], reverse=True)
        top50 = deals[:50]
        print(f"  [{name}] {len(top50)}건")
        return top50

    except Exception as e:
        print(f"  [{name}] 오류: {e}")
        return []


def fetch_all(districts: list, region: str) -> list:
    results = []
    for code, name in districts:
        results += fetch_district(code, name, region)
        time.sleep(1)  # Rate Limiting 방지
    return results


def fetch_all_other(districts: list) -> list:
    """OTHER_DISTRICTS는 (code, name, region) 3-튜플"""
    results = []
    for code, name, region in districts:
        results += fetch_district(code, name, region)
        time.sleep(1)
    return results


print(f"=== 아파트 매매 데이터 수집 ({DEAL_YMD}) ===")

print("\n[서울]")
seoul = fetch_all(SEOUL_DISTRICTS, "서울")

print("\n[경기]")
gyeonggi = fetch_all(GYEONGGI_DISTRICTS, "경기")

print("\n[기타 시도]")
others = fetch_all_other(OTHER_DISTRICTS)

output = {
    "updatedAt": DEAL_YMD,
    "seoul": seoul,
    "gyeonggi": gyeonggi,
    "others": others,
}

os.makedirs("apt-deals-data", exist_ok=True)
with open("apt-deals-data/apt_deals.json", "w", encoding="utf-8") as f:
    json.dump(output, f, ensure_ascii=False, indent=2)

print(f"\n✅ 완료: 서울 {len(seoul)}건 / 경기 {len(gyeonggi)}건 / 기타 {len(others)}건")
print(f"   저장: apt-deals-data/apt_deals.json")
