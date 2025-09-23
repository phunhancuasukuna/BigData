import pandas as pd
import json

# Đọc JSON lines
df = pd.read_json("books.json")

# 1. Title: rút gọn, strip
df['title'] = df['title'].astype(str).str.strip()
df['title'] = df['title'].str.split(":").str[0]   # lấy phần trước dấu ":"

# 2. Author: thay null = "Unknown"
df['author'] = df['author'].fillna("Unknown").astype(str).str.strip()

# 3. Downloads: đảm bảo kiểu int
df['downloads'] = pd.to_numeric(df['downloads'], errors='coerce').fillna(0).astype(int)

# 4. Xóa trùng lặp
df = df.drop_duplicates(subset=['title', 'author']).reset_index(drop=True)

# Xuất ra JSON sạch (dạng records, dễ đọc lại)
df.to_json("books_clean.json", orient="records", lines=True, force_ascii=False)

# Nếu muốn CSV thì thay bằng:
df.to_csv("books_clean.csv", index=False)
