from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
import time

# URL lịch sử giá vàng Investing
url = "https://www.investing.com/commodities/gold-historical-data"

# Mở trình duyệt Chrome
options = webdriver.ChromeOptions()
#options.add_argument("--headless")  # chạy ngầm, ko mở cửa sổ
driver = webdriver.Chrome(options=options)
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")


driver.get(url)
time.sleep(5)  # đợi trang load

# Lấy dữ liệu bảng
rows = driver.find_elements(By.CSS_SELECTOR, "#__next table tbody tr")

data = []
for row in rows:
    cols = row.find_elements(By.TAG_NAME, "td")
    if len(cols) >= 5:
        date = cols[0].text
        price = cols[1].text
        open_ = cols[2].text
        high = cols[3].text
        low = cols[4].text
        change = cols[5].text if len(cols) > 5 else None
        data.append([date, price, open_, high, low, change])

# Đóng trình duyệt
driver.quit()

# Đưa vào DataFrame
df = pd.DataFrame(data, columns=["Date", "Price", "Open", "High", "Low", "Change %"])

# Lưu ra CSV
df.to_csv("gold_prices.csv", index=False, encoding="utf-8-sig")

print("✅ Crawl xong, dữ liệu lưu ở gold_prices.csv")
