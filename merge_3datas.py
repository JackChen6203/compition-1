import pandas as pd

# # 資料來源
df1 = pd.read_csv("data1.csv", low_memory=False)
df2 = pd.read_csv("data2.csv", low_memory=False)
df3 = pd.read_csv("data3.csv", low_memory=False)

# # 合併資料
combined_df = pd.concat([df1, df2, df3])

# 轉換日期格式
combined_df["review_end_time"] = pd.to_datetime(combined_df["review_end_time"])
combined_df["review_start_time"] = pd.to_datetime(combined_df["review_start_time"])

# 創建 new_df 並選取所需欄位，去除重複
new_df = combined_df.drop_duplicates(subset="PseudoID")[
    ["PseudoID", "city_code", "organization_id", "grade", "class", "month"]
].reset_index(drop=True)

# 計算影片觀看數量
video_count = (
    combined_df.groupby("PseudoID")["video_item_sn"].nunique().rename("影片觀看數量")
)
new_df = new_df.join(video_count, on="PseudoID")

# 計算影片瀏覽次數
review_count = combined_df.groupby("PseudoID")["review_sn"].nunique().rename("影片瀏覽次數")
new_df = new_df.join(review_count, on="PseudoID")

# 計算平均影片完成率
avg_finish_rate = (
    combined_df.drop_duplicates("review_sn")
    .groupby("PseudoID")["review_finish_rate"]
    .mean()
    .rename("平均影片完成率")
)
new_df = new_df.join(avg_finish_rate, on="PseudoID")


# 計算影片瀏覽總時間
def total_seconds(series):
    return series.dt.total_seconds()


watch_time_total = (
    combined_df.drop_duplicates("review_sn")
    .groupby("PseudoID")
    .apply(lambda x: total_seconds(x["review_end_time"] - x["review_start_time"]).sum())
    .rename("影片瀏覽總時間")
)
new_df = new_df.join(watch_time_total, on="PseudoID")

# 計算平均觀看影片時間
new_df["平均觀看影片時間"] = new_df["影片瀏覽總時間"] / new_df["影片觀看數量"]

# 計算平均影片瀏覽時間
new_df["平均影片瀏覽時間"] = new_df["影片瀏覽總時間"] / new_df["影片瀏覽次數"]

# ...（继续之前的代码）

# 确保所有新计算的栏位，空白结果以0表示
new_df["avg_prac_ratio"] = new_df["avg_prac_ratio"].fillna(0)
new_df["total_prac_cost_time"] = new_df["total_prac_cost_time"].fillna(0)
new_df["total_prac_counts"] = new_df["total_prac_counts"].fillna(0)
new_df["avg_prac_time"] = new_df["avg_prac_time"].fillna(0)
new_df["total_check_ans_counts"] = new_df["total_check_ans_counts"].fillna(0)
new_df["total_check_ans_time"] = new_df["total_check_ans_time"].fillna(0)
new_df["total_check_ans_result"] = new_df["total_check_ans_result"].fillna(0)
new_df["check_correct_ratio"] = new_df["check_correct_ratio"].fillna(0)
new_df["avg_check_ans_time"] = new_df["avg_check_ans_time"].fillna(0)

# 将 new_df 输出为 CSV 文件
new_df.to_csv("output.csv", index=False)
