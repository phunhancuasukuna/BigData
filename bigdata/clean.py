import json
import re
from datetime import datetime
import pandas as pd

# Hàm kiểm tra ngày hợp lệ
def is_valid_date(date_str):
    try:
        datetime.strptime(date_str, '%d/%m/%Y')
        return True
    except ValueError:
        return False

# Hàm tách giá trị thay đổi và % thay đổi
def parse_change(change_str):
    match = re.match(r'([+-]?\d+(?:\.\d+)?)\(([+-]?\d+(?:\.\d+)?) %\)', change_str)
    if match:
        return float(match.group(1)), float(match.group(2))
    return 0.0, 0.0

# Đọc dữ liệu JSON gốc
with open(r"C:\Users\javiz\OneDrive\Documents\bigdata\HPG_history.json", "r", encoding="utf-8") as f:
    data = json.load(f)

cleaned_data = []
seen_dates = set()

for i, record in enumerate(data):
    date = record['Ngay']

    # Bỏ qua ngày trùng lặp
    if date in seen_dates:
        print(f"Bỏ qua bản ghi trùng lặp: {date}")
        continue
    seen_dates.add(date)

    # Kiểm tra ngày hợp lệ
    if not is_valid_date(date):
        print(f"Bỏ qua bản ghi có ngày không hợp lệ: {date}")
        continue

    # Chuyển ngày sang format YYYY-MM-DD
    date_obj = datetime.strptime(date, '%d/%m/%Y')
    date_std = date_obj.strftime('%Y-%m-%d')

    try:
       close_price = float(record['GiaDongCua'])
       adjusted_price = float(record['GiaDieuChinh'])
       open_price = float(record['GiaMoCua'])
       high_price = float(record['GiaCaoNhat'])
       low_price = float(record['GiaThapNhat'])
       matched_volume = int(record['KhoiLuongKhopLenh'])
       matched_value = float(record['GiaTriKhopLenh'])
       agreed_volume = int(record['KLThoaThuan'])
       agreed_value = float(record['GtThoaThuan'])
    except ValueError:
        print(f"Bỏ qua bản ghi có giá trị số không hợp lệ: {date}")
        continue

    # Các rule lọc dữ liệu
    if (close_price < 0 or adjusted_price < 0 or open_price < 0 or
        high_price < 0 or low_price < 0 or matched_volume < 0 or
        matched_value < 0 or agreed_volume < 0 or agreed_value < 0):
        print(f"Bỏ qua bản ghi có giá trị âm: {date}")
        continue

    if high_price < low_price:
        print(f"Bỏ qua bản ghi có giá cao nhất < giá thấp nhất: {date}")
        continue

    if not (low_price <= close_price <= high_price):
        print(f"Bỏ qua bản ghi có giá đóng cửa không hợp lý: {date}")
        continue

    # Parse cột Thay đổi
    change_value, change_percent = parse_change(record['ThayDoi'])

    # Tạo id unique
    record_id = f"{date_std}_{i}"

    # Append bản ghi đã clean
    cleaned_data.append({
        'id': record_id,
        'date': date_std,
        'closePrice': close_price,
        'adjustedPrice': adjusted_price,
        'change': record['ThayDoi'],
        'changeValue': change_value,
        'changePercent': change_percent,
        'matchedVolume': matched_volume,
        'matchedValue': matched_value,
        'agreedVolume': agreed_volume,
        'agreedValue': agreed_value,
        'openPrice': open_price,
        'highPrice': high_price,
        'lowPrice': low_price
    })

# Xuất ra CSV
df = pd.DataFrame(cleaned_data)
df.to_csv("cleaned_data.csv", index=False, encoding="utf-8-sig")

# Xuất ra JSON
with open("cleaned_data.json", "w", encoding="utf-8") as f:
    json.dump(cleaned_data, f, ensure_ascii=False, indent=4)

print("Làm sạch dữ liệu xong! Đã lưu ra cleaned_data.csv và cleaned_data.json")
