import pandas as pd
import matplotlib.pyplot as plt
from mplfinance.original_flavor import candlestick_ohlc
import matplotlib.dates as mdates
import numpy as np

# Đọc dữ liệu từ file CSV
df = pd.read_csv("cleaned_data.csv")

# Chuyển kiểu dữ liệu cột 'date' sang datetime
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values('date')

# ---------------------- 1️ Biểu đồ đường - Xu hướng giá đóng cửa ----------------------
plt.figure(figsize=(12,6))
plt.plot(df['date'], df['close_price'], color='blue', label='Close Price')
plt.title('Xu hướng giá đóng cửa cổ phiếu HPG (theo thời gian)', fontsize=14)
plt.xlabel('Ngày')
plt.ylabel('Giá đóng cửa (VNĐ)')
plt.grid(True, linestyle='--', alpha=0.6)
plt.legend()
plt.tight_layout()
plt.show()

# ---------------------- 2️ Biểu đồ nến (Candlestick) ----------------------
ohlc = df[['date', 'open_price', 'high_price', 'low_price', 'close_price']].copy()
ohlc['date'] = mdates.date2num(ohlc['date'])

fig, ax = plt.subplots(figsize=(12,6))
candlestick_ohlc(ax, ohlc.values, width=0.6, colorup='green', colordown='red', alpha=0.8)
ax.xaxis_date()
ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%m'))
ax.set_title('Biểu đồ nến giá cổ phiếu HPG')
ax.set_ylabel('Giá (VNĐ)')
plt.grid(True, linestyle='--', alpha=0.6)
plt.tight_layout()
plt.show()

# ---------------------- 3️ Biểu đồ khối lượng giao dịch ----------------------
plt.figure(figsize=(12,5))
plt.bar(df['date'], df['matched_volume']/1e6, color='orange', alpha=0.7)
plt.title('Khối lượng khớp lệnh cổ phiếu HPG (triệu cổ phiếu)', fontsize=14)
plt.xlabel('Ngày')
plt.ylabel('Khối lượng (triệu)')
plt.tight_layout()
plt.show()

# ---------------------- 4️ Biểu đồ phân tán - Quan hệ giữa Giá và Khối lượng ----------------------
plt.figure(figsize=(8,6))
plt.scatter(df['matched_volume']/1e6, df['close_price'], c='purple', alpha=0.6)
plt.title('Mối quan hệ giữa giá đóng cửa và khối lượng giao dịch HPG', fontsize=14)
plt.xlabel('Khối lượng khớp lệnh (triệu)')
plt.ylabel('Giá đóng cửa (VNĐ)')
plt.grid(True, linestyle='--', alpha=0.6)
plt.tight_layout()
plt.show()

# ---------------------- 5️ Biểu đồ kết hợp giá và khối lượng ----------------------
fig, ax1 = plt.subplots(figsize=(12,6))
ax2 = ax1.twinx()

ax1.plot(df['date'], df['close_price'], color='blue', label='Giá đóng cửa')
ax2.bar(df['date'], df['matched_volume']/1e6, color='gray', alpha=0.3, label='Khối lượng')

ax1.set_xlabel('Ngày')
ax1.set_ylabel('Giá (VNĐ)', color='blue')
ax2.set_ylabel('Khối lượng (triệu)', color='gray')
plt.title('Giá đóng cửa & Khối lượng giao dịch HPG', fontsize=14)
fig.tight_layout()
plt.show()

# ---------------------- 6️ Biểu đồ MA50 và MA200 ----------------------
df['MA50'] = df['close_price'].rolling(window=50).mean()
df['MA200'] = df['close_price'].rolling(window=200).mean()

plt.figure(figsize=(12,6))
plt.plot(df['date'], df['close_price'], label='Giá đóng cửa', color='blue', alpha=0.6)
plt.plot(df['date'], df['MA50'], label='MA50', color='green', linestyle='--')
plt.plot(df['date'], df['MA200'], label='MA200', color='red', linestyle='--')
plt.title('Đường trung bình động MA50 và MA200 - HPG', fontsize=14)
plt.xlabel('Ngày')
plt.ylabel('Giá (VNĐ)')
plt.legend()
plt.grid(True, linestyle='--', alpha=0.6)
plt.tight_layout()
plt.show()
