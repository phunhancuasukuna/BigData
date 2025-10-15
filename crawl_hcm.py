from datetime import datetime
import pandas as pd
from meteostat import Daily, Stations

# 1. Chọn ngày bắt đầu/kết thúc
start = datetime(2010, 1, 1)
end = datetime(2025, 10, 15)

# 2. Chọn station Hồ Chí Minh (48900)
data = Daily(48900, start, end)
data = data.fetch()  # trả về DataFrame

print(data.head())
print(f"Tổng record: {len(data)}")

# 3. Lưu CSV
data.to_csv("ho_chi_minh_weather.csv")
