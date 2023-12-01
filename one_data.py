import pandas as pd

# # 資料來源
# df1 = pd.read_csv("data1.csv", low_memory=False)
# df2 = pd.read_csv("data2.csv", low_memory=False)
# df3 = pd.read_csv("data3.csv", low_memory=False)

# # 合併資料
# combined_df = pd.concat([df1, df2, df3])

combined_df = pd.read_csv("data.csv", low_memory=False, encoding="iso-8859-1")
# 轉換日期格式
combined_df["review_end_time"] = pd.to_datetime(combined_df["review_end_time"])
combined_df["review_start_time"] = pd.to_datetime(combined_df["review_start_time"])

# 創建 new_df 並選取所需欄位，去除重複
new_df = combined_df.drop_duplicates(subset="PseudoID")[
    ["PseudoID", "city_code", "organization_id", "grade", "class", "month"]
].reset_index(drop=True)

# 計算total_video_watched
video_count = (
    combined_df.groupby("PseudoID")["video_item_sn"]
    .nunique()
    .rename("total_video_watched")
)
new_df = new_df.join(video_count, on="PseudoID")

# 計算total_reviewed_count
review_count = (
    combined_df.groupby("PseudoID")["review_sn"]
    .nunique()
    .rename("total_reviewed_count")
)
new_df = new_df.join(review_count, on="PseudoID")

# 計算average_finished_rate
avg_finish_rate = (
    combined_df.drop_duplicates("review_sn")
    .groupby("PseudoID")["review_finish_rate"]
    .mean()
    .rename("average_finished_rate")
)
new_df = new_df.join(avg_finish_rate, on="PseudoID")


# 計算total_review_time
def total_seconds(series):
    return series.dt.total_seconds()


watch_time_total = (
    combined_df.drop_duplicates("review_sn")
    .groupby("PseudoID")
    .apply(lambda x: total_seconds(x["review_end_time"] - x["review_start_time"]).sum())
    .rename("total_review_time")
)
new_df = new_df.join(watch_time_total, on="PseudoID")

# 計算avg_video_watch_time
new_df["avg_video_watch_time"] = (
    new_df["total_review_time"] / new_df["total_video_watched"]
)

# 計算avg_video_review_time
new_df["avg_video_review_time"] = (
    new_df["total_review_time"] / new_df["total_reviewed_count"]
)

# 第13欄 - avg_prac_ratio
avg_practice_accuracy = (
    combined_df.drop_duplicates("prac_sn")
    .groupby("PseudoID")["prac_score_rate"]
    .mean()
    .rename("avg_prac_ratio")
)
new_df = new_df.join(avg_practice_accuracy, on="PseudoID")

# 第14欄 - total_prac_cost_time
total_practice_time = (
    combined_df.drop_duplicates("prac_sn")
    .groupby("PseudoID")["prac_during_time"]
    .sum()
    .rename("total_prac_cost_time ")
)
new_df = new_df.join(total_practice_time, on="PseudoID")

# 第15欄 - -total_prac_counts
total_practice_questions = (
    combined_df.groupby("PseudoID")
    .apply(lambda x: x["prac_questions"].explode().nunique())
    .rename("-total_prac_counts")
)
new_df = new_df.join(total_practice_questions, on="PseudoID")

# 第16欄 - avg_prac_time
new_df["avg_prac_time"] = new_df["total_prac_cost_time "] / new_df["-total_prac_counts"]

# 第17欄 - -total_check_ans_counts
total_exam_questions = (
    combined_df.drop_duplicates("exam_video_exam_sn")
    .groupby("PseudoID")["exam_video_exam_sn"]
    .count()
    .fillna(0)
    .rename("-total_check_ans_counts")
)
new_df = new_df.join(total_exam_questions, on="PseudoID")

# 第18欄 - total_check_ans_time
total_exam_time = (
    combined_df.drop_duplicates("exam_video_exam_sn")
    .groupby("PseudoID")["exam_ans_time"]
    .sum()
    .rename("total_check_ans_time")
)
new_df = new_df.join(total_exam_time, on="PseudoID")

# 第19欄 - total_check_ans_result)
total_exam_results = (
    combined_df.drop_duplicates("exam_video_exam_sn")
    .groupby("PseudoID")["exam_binary_res"]
    .sum()
    .rename("total_check_ans_result)")
)
new_df = new_df.join(total_exam_results, on="PseudoID")

# 第20欄 - check_correct_ratio
new_df["check_correct_ratio"] = (
    new_df["total_check_ans_result)"] / new_df["-total_check_ans_counts"]
)
new_df["check_correct_ratio"] = new_df["check_correct_ratio"].apply(
    lambda x: f"{x:.2%}"
)

# 第21欄 - avg_check_ans_time
new_df["avg_check_ans_time"] = (
    new_df["total_check_ans_time"] / new_df["-total_check_ans_counts"]
)
new_df["avg_check_ans_time"] = new_df["avg_check_ans_time"].apply(lambda x: f"{x:.2f}")

# 將 new_df 輸出為 CSV 檔案
new_df.to_csv("output2.csv", index=False)
