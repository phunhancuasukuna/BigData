import requests
import json
import time

def crawl_hpg_history(output_file="HPG_history.json"):
    all_data = []
    page = 1
    page_size = 50  # số bản ghi mỗi lần gọi API

    while True:
        url = f"https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol=HPG&StartDate=&EndDate=&PageIndex={page}&PageSize={page_size}"
        r = requests.get(url)
        r.encoding = "utf-8"

        try:
            data = r.json()
        except Exception as e:
            print(f"Lỗi đọc JSON ở trang {page}: {e}")
            break

        # ⚡ Fix ở đây: Data ngoài + Data trong
        records = data.get("Data", {}).get("Data", [])

        if not records:  # hết dữ liệu thì dừng
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
        time.sleep(0.5)  # tránh bị chặn request

    # lưu ra file JSON
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=4)

    print(f"Hoàn thành! Đã lưu {len(all_data)} phiên giao dịch vào {output_file}")


if __name__ == "__main__":
    crawl_hpg_history()
