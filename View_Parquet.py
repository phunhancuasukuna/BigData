#!/usr/bin/env python3
from pyspark.sql import SparkSession
import argparse, sys

def main():
    parser = argparse.ArgumentParser(
        description="Xem nội dung Parquet (schema, show N dòng, optional SQL) từ HDFS/local."
    )
    parser.add_argument("paths", nargs="+", help="Đường dẫn Parquet (ví dụ: hdfs:///output/hpg_1/monthly_avg)")
    parser.add_argument("-n", "--rows", type=int, default=50, help="Số dòng hiển thị (mặc định 50)")
    parser.add_argument("--count", action="store_true", help="Đếm số dòng (có thể tốn thời gian)")
    parser.add_argument("--sql", type=str, default=None,
                        help="Chạy SQL trên dataset (temp view tên 't'), ví dụ: \"select * from t limit 10\"")
    args = parser.parse_args()

    spark = (SparkSession.builder
             .appName("View_Parquet")
             .getOrCreate())

    for p in args.paths:
        print("\n" + "="*80)
        print(f"PATH: {p}")
        print("="*80)

        try:
            df = spark.read.parquet(p)
        except Exception as e:
            print(f"[!] Không đọc được {p}: {e}", file=sys.stderr)
            continue

        print("\n[Schema]")
        df.printSchema()

        if args.count:
            try:
                c = df.count()
                print(f"\n[Count] {c}")
            except Exception as e:
                print(f"[!] Lỗi đếm dòng: {e}", file=sys.stderr)

        print(f"\n[Preview] show({args.rows}, truncate=False)")
        try:
            df.show(args.rows, truncate=False)
        except Exception as e:
            print(f"[!] Lỗi show: {e}", file=sys.stderr)

        if args.sql:
            try:
                df.createOrReplaceTempView("t")
                print(f"\n[SQL] {args.sql}")
                spark.sql(args.sql).show(args.rows, truncate=False)
            except Exception as e:
                print(f"[!] Lỗi chạy SQL: {e}", file=sys.stderr)

    spark.stop()

if __name__ == "__main__":
    main()
