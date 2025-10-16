import jaydebeapi
import csv
import sys
import os

# Kết nối Phoenix
try:
    phoenix_home = "/usr/local/phoenix-hbase-2.5-5.2.1-bin"
    jars = [
        f"{phoenix_home}/phoenix-client-embedded-hbase-2.5-5.2.1.jar",
        f"{phoenix_home}/lib/log4j-api-2.18.0.jar",
        f"{phoenix_home}/lib/log4j-core-2.18.0.jar",
        f"{phoenix_home}/lib/log4j-1.2-api-2.18.0.jar",
        f"{phoenix_home}/lib/log4j-slf4j-impl-2.18.0.jar"
    ]

    for jar in jars:
        if not os.path.exists(jar):
            raise FileNotFoundError(f"Không tìm thấy file JAR: {jar}")

    conn = jaydebeapi.connect(
        'org.apache.phoenix.jdbc.PhoenixDriver',
        'jdbc:phoenix:localhost',
        jars=jars,
    )
    cursor = conn.cursor()
    print("Kết nối Phoenix thành công!")

except Exception as e:
    print(f"Lỗi kết nối Phoenix: {e}")
    sys.exit(1)

# File CSV nguồn
csv_file = '/mnt/c/Users/javiz/OneDrive/Documents/bigdata/cleaned_data.csv'

# Import dữ liệu
try:
    with open(csv_file, 'r', encoding='utf-8-sig') as f:  
        reader = csv.DictReader(f)
        reader.fieldnames = [h.strip() for h in reader.fieldnames]

        upsert_query = """
        UPSERT INTO HPG (
            id, date, close_price, adjusted_price, change, change_value, change_percent,
            matched_volume, matched_value, agreed_volume, agreed_value,
            open_price, high_price, low_price
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        count = 0
        for row in reader:
            try:
                cursor.execute(upsert_query, [
                    row.get('id'),
                    row.get('date'),
                    float(row.get('close_price') or 0),
                    float(row.get('adjusted_price') or 0),
                    row.get('change'),
                    float(row.get('change_value') or 0),
                    float(row.get('change_percent') or 0),
                    int(row.get('matched_volume') or 0),
                    float(row.get('matched_value') or 0),
                    int(row.get('agreed_volume') or 0),
                    float(row.get('agreed_value') or 0),
                    float(row.get('open_price') or 0),
                    float(row.get('high_price') or 0),
                    float(row.get('low_price') or 0)
                ])
                count += 1
            except Exception as e:
                print(f"Lỗi khi nhập bản ghi {row.get('id')}: {e}")
                continue

        conn.commit()
        print(f"Đã nhập {count} bản ghi từ {csv_file} vào Phoenix!")

except Exception as e:
    print(f"Lỗi khi nhập dữ liệu: {e}")
finally:
    cursor.close()
    conn.close()
    print("Đã đóng kết nối Phoenix.")
