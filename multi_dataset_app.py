import re
import io
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType

# ----------------- UI CONFIG -----------------
st.set_page_config(page_title="HPG + Weather UI", layout="wide")
st.markdown("""
<style>
.big-title{font-weight:700;font-size:28px;margin:0 0 6px;}
.subtle{color:#9ca3af;margin-bottom:22px;}
/* làm nút popover nhỏ, gọn */
.small-right {
  display:flex; justify-content:flex-end; align-items:center; gap:8px;
  margin: 6px 0 8px;
}
</style>
""", unsafe_allow_html=True)

# ----------------- SPARK -----------------
@st.cache_resource(show_spinner=False)
def get_spark():
    return (
        SparkSession.builder
        .appName("HPG_Weather_UI")
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .getOrCreate()
    )
spark = get_spark()

# ----------------- LOADERS -----------------
def read_hpg_json(path):
    return (
        spark.read.option("multiLine", True).json(path)
        .withColumn("Date", F.to_date("Ngay", "dd/MM/yyyy"))
        .withColumn("Year", F.year("Date"))
        .withColumn("Month", F.month("Date"))
        .withColumn("Week", F.weekofyear("Date"))
        .withColumn("GiaDongCua", F.col("GiaDongCua").cast("double"))
        .withColumn("KhoiLuongKhopLenh", F.col("KhoiLuongKhopLenh").cast("double"))
        .withColumn("KLThoaThuan", F.col("KLThoaThuan").cast("double"))
    )

def read_weather_json(path):
    return (
        spark.read.option("multiLine", True).json(path)
        .withColumn("Date", F.to_date("time", "yyyy-MM-dd"))
        .withColumn("Year", F.year("Date"))
        .withColumn("Month", F.month("Date"))
        .withColumn("Week", F.weekofyear("Date"))
        .withColumn("tavg", F.col("tavg").cast("double"))
        .withColumn("tmin", F.col("tmin").cast("double"))
        .withColumn("tmax", F.col("tmax").cast("double"))
        .withColumn("prcp", F.col("prcp").cast("double"))
        .withColumn("wspd", F.col("wspd").cast("double"))
    )

@st.cache_resource(show_spinner=True)
def load_df(dataset, path):
    return read_hpg_json(path) if dataset == "HPG" else read_weather_json(path)

def show_parquet_df(p, n=50):
    ddf = spark.read.parquet(p)
    st.code(ddf._jdf.schema().treeString())
    pdf = ddf.limit(n).toPandas()
    # popover export nằm trên đầu bảng
    show_export_popover(_base_name_from_path(p, "parquet_export"), pdf)
    st.dataframe(pdf, use_container_width=True)
    return ddf, pdf

# ----------------- EXPORT (POPOVER PICKER) -----------------
def _base_name_from_path(p: str, fallback: str = "export"):
    try:
        name = Path(p).name
        return name if name else fallback
    except Exception:
        return fallback

def show_export_popover(base_name: str, pdf: pd.DataFrame):
    """Nút nhỏ mở popover → chọn CSV/JSON → 1 nút Download."""
    if pdf is None or pdf.empty:
        return
    # hàng chứa nút nhỏ bên phải
    st.markdown('<div class="small-right">', unsafe_allow_html=True)
    with st.popover("Xuất File⬇", help="Tải về CSV/JSON"):
        fmt = st.radio("Định dạng", ["CSV", "JSON"], horizontal=True, index=0)
        if fmt == "CSV":
            data = pdf.to_csv(index=False).encode("utf-8-sig")
            mime, ext = "text/csv", "csv"
        else:
            data = pdf.to_json(orient="records", force_ascii=False, indent=2).encode("utf-8")
            mime, ext = "application/json", "json"
        st.download_button(
            "Download",
            data=data,
            file_name=f"{base_name}.{ext}",
            mime=mime,
            use_container_width=True,
        )
    st.markdown('</div>', unsafe_allow_html=True)

# ----------------- SIDEBAR -----------------
st.sidebar.title("HPG + Weather UI")
dataset = st.sidebar.selectbox("Dataset", ["HPG", "Weather"])
mode    = st.sidebar.selectbox("Nguồn dữ liệu", ["Raw JSON", "Parquet (MR)"])

# JSON path (luôn cho phép nhập để chủ động chuyển chế độ khi cần)
default_json = "hdfs:///input/hpg.json" if dataset == "HPG" else "hdfs:///input/hcm_cleaned.json"
json_path    = st.sidebar.text_input("Đường dẫn JSON", default_json)

# Parquet path: chỉ hiện khi chọn Parquet (MR)
if mode == "Parquet (MR)":
    pq_suggestions = ([  # HPG
        "hdfs:///output/hpg_2/percentage",
        "hdfs:///output/hpg_1/monthly_avg",
        "hdfs:///output/hpg_1/yearly_extremes",
        "hdfs:///output/hpg_3/weekly_volume",
        "hdfs:///output/hpg_4/yearly_extreme_dates",
    ] if dataset == "HPG" else [               # Weather
        "hdfs:///output/weather/weather_1_monthly_stats/monthly_avg",
        "hdfs:///output/weather/weather_1_monthly_stats/yearly_extremes",
        "hdfs:///output/weather/weather_2_percent_temp_move/rain_share",
        "hdfs:///output/weather/weather_3_weekly_prcp/weekly_prcp",
        "hdfs:///output/weather/weather_4_yearly_extremes/yearly_extreme_dates",
    ])
    pq_path = st.sidebar.selectbox("Parquet path (MR)", pq_suggestions)
else:
    pq_path = None  # không hiển thị / không dùng trong Raw JSON

# Nút reload chỉ cần cho JSON
if mode == "Raw JSON":
    if st.sidebar.button("Load / Reload"):
        st.session_state.pop("df", None)

# JSON cache (đọc khi chọn Raw JSON)
if mode == "Raw JSON":
    if "df" not in st.session_state:
        try:
            st.session_state["df"] = load_df(dataset, json_path)
            st.success("Đã nạp dữ liệu từ JSON.")
        except Exception as e:
            st.error(f"Không đọc được JSON '{json_path}': {e}")
            st.stop()
    df = st.session_state["df"]
else:
    df = None

# ----------------- TITLES -----------------
if dataset == "HPG":
    subline = "Lịch sử giá cổ phiếu Tập đoàn Hòa Phát (15/11/2007–05/09/2025)"
else:
    subline = "Dữ liệu thời tiết Thành phố Hồ Chí Minh (01/01/2010–15/10/2025)"

# ----------------- PAGES (ẩn/hiện theo yêu cầu) -----------------
if dataset == "HPG":
    if mode == "Raw JSON":
        pages = ["Dữ liệu", "Tìm kiếm"]
    else:  # Parquet (MR)
        pages = ["Xem Parquet"]
else:  # Weather
    pages = ["Dữ liệu", "Tìm kiếm"] if mode == "Raw JSON" else ["Xem Parquet"]

page = st.sidebar.radio("Trang", pages, index=0)

st.markdown(f'<div class="big-title">{dataset} — {page}</div>', unsafe_allow_html=True)
st.markdown(
    f'<div class="subtle">{subline} • Nguồn: {"Raw JSON" if mode=="Raw JSON" else "Parquet (MR)"}'
    '</div>',
    unsafe_allow_html=True
)

# ----------------- PAGES -----------------
# ---- HPG • JSON ----
if dataset == "HPG" and mode == "Raw JSON":
    if page == "Dữ liệu":
        pdf_show = df.orderBy(F.desc("Date")).toPandas()
        show_export_popover("hpg_raw_full", pdf_show)
        st.dataframe(pdf_show, use_container_width=True)

    elif page == "Tìm kiếm":
        exact = st.text_input("Ngày chính xác (dd/MM/yyyy)", "")
        contains = st.text_input("Chuỗi cần chứa (vd '06/2019')", "")
        out = df
        if exact:    out = out.filter(F.col("Ngay") == exact)
        if contains: out = out.filter(F.col("Ngay").contains(contains))
        pdf_show = out.orderBy(F.desc("Date")).toPandas()
        show_export_popover("hpg_raw_filtered", pdf_show)
        st.dataframe(pdf_show, use_container_width=True)

# ---- HPG • PARQUET ----
if dataset == "HPG" and mode == "Parquet (MR)" and page == "Xem Parquet":
    p = st.text_input("Parquet path", pq_path or "")
    n = st.slider("Số dòng xem", 5, 200, 50)
    if st.button("Đọc Parquet"):
        try:
            ddf, pdf = show_parquet_df(p, n)

            # Vẽ donut nếu là percentage
            lower_cols = [c.lower() for c in ddf.columns]
            percentage_like = ("percentage" in (p or "").lower()) or (
                "phantram" in lower_cols and "loaibiendong" in lower_cols
            )
            if percentage_like:
                if "phantram" in lower_cols and "loaibiendong" in lower_cols:
                    plot_df = ddf.select(
                        "LoaiBienDong", F.round(F.col("PhanTram"), 2).alias("PhanTram")
                    ).toPandas()
                else:
                    tmp = ddf.groupBy("LoaiBienDong").count().toPandas()
                    total = tmp["count"].sum()
                    tmp["PhanTram"] = (tmp["count"] / total * 100).round(2)
                    plot_df = tmp[["LoaiBienDong", "PhanTram"]]

                st.altair_chart(
                    alt.Chart(plot_df).mark_arc(innerRadius=60)
                      .encode(theta="PhanTram:Q",
                              color="LoaiBienDong:N",
                              tooltip=["LoaiBienDong", "PhanTram"]),
                    use_container_width=True
                )
        except Exception as e:
            st.error(f"Không đọc được: {e}")

# ---- WEATHER ----
if dataset == "Weather":
    if mode == "Raw JSON" and page == "Dữ liệu":
        pdf_show = st.session_state["df"].orderBy(F.desc("Date")).toPandas()
        show_export_popover("weather_raw_full", pdf_show)
        st.dataframe(pdf_show, use_container_width=True)

    elif mode == "Raw JSON" and page == "Tìm kiếm":
        exact = st.text_input("Ngày chính xác (yyyy-MM-dd)", "")
        contains = st.text_input("Chuỗi cần chứa (vd '2024-07')", "")
        out = st.session_state["df"]
        if exact:    out = out.filter(F.col("time") == exact)
        if contains: out = out.filter(F.col("time").contains(contains))
        pdf_show = out.orderBy(F.desc("Date")).toPandas()
        show_export_popover("weather_raw_filtered", pdf_show)
        st.dataframe(pdf_show, use_container_width=True)

    elif mode == "Parquet (MR)" and page == "Xem Parquet":
        p = st.text_input("Parquet path", pq_path or "")
        n = st.slider("Số dòng xem", 5, 200, 50)
        if st.button("Đọc Parquet"):
            try:
                ddf, pdf = show_parquet_df(p, n)  # popover + bảng nằm trong hàm
            except Exception as e:
                st.error(f"Không đọc được: {e}")
