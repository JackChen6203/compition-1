import pandas as pd

# 資料來源
df1 = pd.read_csv("data1.csv", low_memory=False)
df2 = pd.read_csv("data2.csv", low_memory=False)
df3 = pd.read_csv("data3.csv", low_memory=False)

# 合併資料
combined_df = pd.concat([df1, df2, df3])

# 創建新的DataFrame
new_df = combined_df.drop_duplicates(subset="PseudoID").loc[
    :, ["PseudoID", "city_code", "organization_id", "grade", "class", "month"]
]
new_df.columns = ["PseudoID", "city_code", "organization_id", "grade", "class", "month"]

# 計算影片觀看數量和瀏覽次數等
video_counts = combined_df.groupby("PseudoID")["video_item_sn"].nunique()
review_counts = combined_df.groupby("PseudoID")["review_sn"].nunique()
review_finish_rate = (
    combined_df.drop_duplicates(subset="review_sn")
    .groupby("PseudoID")["review_finish_rate"]
    .mean()
)


# 轉換時間戳為秒數
def excel_timestamp_to_seconds(timestamp):
    try:
        return (
            pd.to_datetime(timestamp, format="%Y/%m/%d %H:%M")
            - pd.Timestamp("1899-12-30")
        ) // pd.Timedelta("1s")
    except ValueError:
        return None


review_time = combined_df.drop_duplicates(subset="review_sn").copy()
review_time["review_start_seconds"] = review_time["review_start_time"].apply(
    excel_timestamp_to_seconds
)
review_time["review_end_seconds"] = review_time["review_end_time"].apply(
    excel_timestamp_to_seconds
)
review_time["review_duration"] = (
    review_time["review_end_seconds"] - review_time["review_start_seconds"]
)

# 計算總觀看時間（秒）
total_review_time = review_time.groupby("PseudoID")["review_duration"].sum()

# 將計算結果合併到new_df
new_df = new_df.set_index("PseudoID")
new_df = new_df.join(video_counts.rename("影片觀看數量"))
new_df = new_df.join(review_counts.rename("影片瀏覽次數"))
new_df = new_df.join(review_finish_rate.rename("平均影片完成率"))
new_df = new_df.join(total_review_time.rename("影片瀏覽總時間"))

# 計算平均觀看影片時間和平均影片瀏覽時間
new_df["平均觀看影片時間"] = new_df["影片瀏覽總時間"] / new_df["影片觀看數量"]
new_df["平均影片瀏覽時間"] = new_df["影片瀏覽總時間"] / new_df["影片瀏覽次數"]

# 輸出結果為CSV
new_df.to_csv("output.csv")
