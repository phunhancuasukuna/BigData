import pandas as pd

# 1. Đọc CSV raw
df = pd.read_csv("ho_chi_minh_weather.csv")

# 2. Drop các cột không cần thiết
cols_to_drop = ['snow','wdir','wpgt','tsun','pres']  # bỏ pres luôn
df = df.drop(columns=cols_to_drop)

# 3. Xử lý tavg nếu null + round 1 chữ số
df['tavg'] = df.apply(
    lambda row: round((row['tmin'] + row['tmax'])/2, 1)
    if pd.isna(row['tavg']) and pd.notna(row['tmin']) and pd.notna(row['tmax'])
    else row['tavg'],
    axis=1
)

# 4. Xử lý tmin nếu null + round 1 chữ số
df['tmin'] = df.apply(
    lambda row: round(2*row['tavg'] - row['tmax'], 1)
    if pd.isna(row['tmin']) and pd.notna(row['tavg']) and pd.notna(row['tmax'])
    else row['tmin'],
    axis=1
)

# 5. Xử lý tmax nếu null + round 1 chữ số
df['tmax'] = df.apply(
    lambda row: round(2*row['tavg'] - row['tmin'], 1)
    if pd.isna(row['tmax']) and pd.notna(row['tavg']) and pd.notna(row['tmin'])
    else row['tmax'],
    axis=1
)

# 6. Điền 0 cho prcp và wspd nếu null
df['prcp'] = df['prcp'].fillna(0)
df['wspd'] = df['wspd'].fillna(0)

# 7. Chuẩn hóa None cho JSON
df = df.where(pd.notna(df), None)

# 8. Lưu CSV sạch
df.to_csv("hcm_cleaned.csv", index=False)

# 9. Lưu JSON lines
df.to_json("hcm_cleaned.json", orient="records", lines=True)

print("✅ Đã lưu CSV và JSON sạch + xử lý đầy đủ tmin/tmax/tavg + điền 0!")

