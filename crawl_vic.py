import requests
import json
import time

def crawl_vic_history(output_file="VIC_history.json"):
    all_data = []
    page = 1
    page_size = 50  # tăng page_size nếu muốn

    while True:
        url = f"https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol=VIC&StartDate=&EndDate=&PageIndex={page}&PageSize={page_size}"
        r = requests.get(url)
        r.encoding = "utf-8"

        try:
            data = r.json()
        except Exception as e:
            print(f"Lỗi đọc JSON ở trang {page}: {e}")
            break

        records = data.get("Data", {}).get("Data", [])
        if not records:
            break

        for rec in records:
            all_data.append({
                "Ngay": rec.get("Ngay"),
                "GiaDongCua": rec.get("GiaDongCua"),
                "GiaDieuChinh": rec.get("GiaDieuChinh"),
                "ThayDoi": rec.get("ThayDoi"),
                "KhoiLuongKhopLenh": rec.get("KhoiLuongKhopLenh"),
                "GiaTriKhopLenh": rec.get("GiaTriKhopLenh"),
                "KLThoaThuan": rec.get("KLThoaThuan"),
                "GtThoaThuan": rec.get("GtThoaThuan"),
                "GiaMoCua": rec.get("GiaMoCua"),
                "GiaCaoNhat": rec.get("GiaCaoNhat"),
                "GiaThapNhat": rec.get("GiaThapNhat"),
            })

        print(f"Đã lấy xong trang {page}, tổng số bản ghi: {len(all_data)}")
        page += 1
        time.sleep(0.5)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=4)

    print(f"Hoàn thành! Đã lưu {len(all_data)} phiên giao dịch vào {output_file}")


if __name__ == "__main__":
    crawl_vic_history()
