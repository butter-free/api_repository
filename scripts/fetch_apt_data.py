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


print(f"=== 아파트 매매 데이터 수집 ({DEAL_YMD}) ===")

print("\n[서울]")
seoul = fetch_all(SEOUL_DISTRICTS, "서울")

print("\n[경기]")
gyeonggi = fetch_all(GYEONGGI_DISTRICTS, "경기")

output = {
    "updatedAt": DEAL_YMD,
    "seoul": seoul,
    "gyeonggi": gyeonggi,
}

os.makedirs("data", exist_ok=True)
with open("data/apt_deals.json", "w", encoding="utf-8") as f:
    json.dump(output, f, ensure_ascii=False, indent=2)

print(f"\n✅ 완료: 서울 {len(seoul)}건 / 경기 {len(gyeonggi)}건")
print(f"   저장: data/apt_deals.json")
