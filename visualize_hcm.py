import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Đọc dữ liệu
df = pd.read_json("hcm_cleaned.json")
df['time'] = pd.to_datetime(df['time'])
df['month'] = df['time'].dt.month
df['year'] = df['time'].dt.year

# Gom nhóm trung bình theo tháng (để vẽ biểu đồ tổng hợp)
monthly = df.groupby('month').agg({
    'tavg': 'mean',
    'prcp': 'sum',
    'wspd': 'mean'
}).reset_index()
# Biểu đồ cột — “Tổng lượng mưa theo tháng”
plt.figure(figsize=(10,6))
sns.barplot(data=monthly, x='month', y='prcp', color='skyblue', edgecolor='black')
plt.title("Biểu đồ cột: Tổng lượng mưa theo tháng tại TP.HCM", fontsize=14, weight='bold')
plt.xlabel("Tháng")
plt.ylabel("Tổng lượng mưa (mm)")
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.show()
# Biểu đồ phân tán — “Quan hệ giữa nhiệt độ và lượng mưa”
plt.figure(figsize=(8,6))
sns.scatterplot(data=df, x='tavg', y='prcp', alpha=0.6, color='orange', edgecolor='k')
plt.title("Biểu đồ phân tán: Quan hệ giữa Nhiệt độ trung bình và Lượng mưa", fontsize=14, weight='bold')
plt.xlabel("Nhiệt độ trung bình (°C)")
plt.ylabel("Lượng mưa (mm)")
plt.grid(True, linestyle='--', alpha=0.7)
plt.tight_layout()
plt.show()
# Biểu đồ tròn — “Tỷ lệ lượng mưa theo quý”
df['quarter'] = df['time'].dt.quarter
rain_by_quarter = df.groupby('quarter')['prcp'].sum()

plt.figure(figsize=(7,7))
colors = sns.color_palette("pastel")[0:4]
plt.pie(rain_by_quarter, labels=[f"Quý {i}" for i in range(1,5)],
        autopct='%1.1f%%', colors=colors, startangle=90)
plt.title("Biểu đồ tròn: Tỷ lệ lượng mưa theo quý tại TP.HCM", fontsize=14, weight='bold')
plt.tight_layout()
plt.show()
# Biểu đồ vùng (Area chart) – Dao động Tmin–Tmax
plt.figure(figsize=(12,5))
plt.fill_between(df['time'], df['tmin'], df['tmax'], color='lightblue', alpha=0.4, label='Khoảng dao động')
plt.plot(df['time'], df['tavg'], color='red', label='Trung bình')
plt.legend()
plt.title("Dao động nhiệt độ hằng ngày (Tmin–Tmax)", fontsize=14, weight='bold')
plt.xlabel("Thời gian")
plt.ylabel("°C")
plt.grid(alpha=0.3)
plt.show()
#Biểu đồ nhiệt — “Mối quan hệ Nhiệt độ – Lượng mưa – Gió”
plt.figure(figsize=(7,5))
corr = df[['tavg','tmin','tmax','prcp','wspd']].corr()
sns.heatmap(corr, annot=True, cmap='coolwarm', fmt=".2f")
plt.title("Biểu đồ nhiệt: Ma trận tương quan giữa các yếu tố thời tiết TP.HCM", fontsize=14, weight='bold')
plt.tight_layout()
plt.show()

