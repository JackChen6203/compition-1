import pandas as pd
from datetime import datetime
import numpy as np

# # 資料來源
df1 = pd.read_csv("data1.csv", low_memory=False)
df2 = pd.read_csv("data2.csv", low_memory=False)
df3 = pd.read_csv("data3.csv", low_memory=False)

# # 合併資料
combined_df = pd.concat([df1, df2, df3])


# 將Excel日期轉換為Python datetime
def excel_date_to_datetime(excel_date_str):
    try:
        # 假設日期已是 yyyy/mm/dd hh:mm:ss 格式的字符串
        return pd.to_datetime(excel_date_str)
    except ValueError:
        # 如果轉換失敗，則處理為Excel數字格式的日期
        excel_date_num = float(excel_date_str)
        return pd.to_datetime("1899-12-30") + pd.to_timedelta(excel_date_num, "D")


# 轉換 review_end_time 和 review_start_time
combined_df["review_end_time"] = combined_df["review_end_time"].apply(
    excel_date_to_datetime
)
combined_df["review_start_time"] = combined_df["review_start_time"].apply(
    excel_date_to_datetime
)
# 轉換 review_end_time 和 review_start_time
combined_df["review_end_time"] = combined_df["review_end_time"].apply(
    excel_date_to_datetime
)
combined_df["review_start_time"] = combined_df["review_start_time"].apply(
    excel_date_to_datetime
)

# 建立 new_df 並選擇欄位
new_df = combined_df[
    ["PseudoID", "city_code", "organization_id", "grade", "class", "month"]
].drop_duplicates(subset=["PseudoID"])

# 計算 total_video_watched
video_count = combined_df.groupby("PseudoID")["video_item_sn"].nunique().reset_index()
video_count.columns = ["PseudoID", "total_video_watched"]
new_df = new_df.merge(video_count, on="PseudoID", how="left")

# 計算 total_reviewed_count
review_count = combined_df.groupby("PseudoID")["review_sn"].nunique().reset_index()
review_count.columns = ["PseudoID", "total_reviewed_count"]
new_df = new_df.merge(review_count, on="PseudoID", how="left")

# 計算 avg_finished_rate
finished_rate = (
    combined_df.drop_duplicates(subset=["review_sn"])
    .groupby("PseudoID")["review_finish_rate"]
    .sum()
    .reset_index()
)
finished_rate.columns = ["PseudoID", "sum_finished_rate"]
new_df = new_df.merge(finished_rate, on="PseudoID", how="left")
new_df["avg_finished_rate"] = (
    new_df["sum_finished_rate"] / new_df["total_reviewed_count"]
)
new_df.drop(columns=["sum_finished_rate"], inplace=True)

# 計算 total_review_time
combined_df["review_time"] = (
    combined_df["review_end_time"] - combined_df["review_start_time"]
).dt.total_seconds()
review_time = (
    combined_df.drop_duplicates(subset=["review_sn"])
    .groupby("PseudoID")["review_time"]
    .sum()
    .reset_index()
)
review_time.columns = ["PseudoID", "total_review_time"]
new_df = new_df.merge(review_time, on="PseudoID", how="left")

# 計算 avg_video_watch_time 和 avg_video_review_time
new_df["avg_video_watch_time"] = (
    new_df["total_review_time"] / new_df["total_video_watched"]
)
new_df["avg_video_review_time"] = (
    new_df["total_review_time"] / new_df["total_reviewed_count"]
)

# 其餘程式碼維持不變...

# 計算 avg_prac_ratio
prac_ratio = (
    combined_df.drop_duplicates(subset=["prac_sn"])
    .groupby("PseudoID")["prac_score_rate"]
    .mean()
    .reset_index()
)
prac_ratio.columns = ["PseudoID", "avg_prac_ratio"]
prac_ratio["avg_prac_ratio"] = prac_ratio["avg_prac_ratio"] / 100
new_df = new_df.merge(prac_ratio, on="PseudoID", how="left")
new_df["avg_prac_ratio"].fillna(0, inplace=True)

# 計算 total_prac_cost_time
prac_cost_time = (
    combined_df.drop_duplicates(subset=["prac_sn"])
    .groupby("PseudoID")["prac_during_time"]
    .sum()
    .reset_index()
)
prac_cost_time.columns = ["PseudoID", "total_prac_cost_time"]
new_df = new_df.merge(prac_cost_time, on="PseudoID", how="left")
new_df["total_prac_cost_time"].fillna(0, inplace=True)


# 計算 total_prac_counts
# 计算 total_prac_counts 的函数
def extract_prac_questions_count(prac_questions_str):
    # 确保 prac_questions_str 不是 NaN
    if pd.isna(prac_questions_str):
        return 0
    else:
        # 将所有元素转换为字符串，然后分割
        prac_questions = set(prac_questions_str.split("@XX@"))
        # 移除空字符串
        prac_questions.discard("")
        return len(prac_questions)


# 计算 total_prac_counts
prac_counts = (
    combined_df.groupby("PseudoID")["prac_questions"]
    .apply(
        lambda x: extract_prac_questions_count(
            "@XX@".join(x.dropna().astype(str).unique())
        )
    )
    .reset_index()
)
prac_counts.columns = ["PseudoID", "total_prac_counts"]
new_df = new_df.merge(prac_counts, on="PseudoID", how="left")


# 計算 avg_prac_time
new_df["avg_prac_time"] = new_df["total_prac_cost_time"] / new_df["total_prac_counts"]
new_df["avg_prac_time"].fillna(0, inplace=True)

# 計算 total_check_ans_counts
check_ans_counts = (
    combined_df.drop_duplicates(subset=["exam_video_exam_sn"])
    .groupby("PseudoID")["exam_video_exam_sn"]
    .count()
    .reset_index()
)
check_ans_counts.columns = ["PseudoID", "total_check_ans_counts"]
new_df = new_df.merge(check_ans_counts, on="PseudoID", how="left")
new_df["total_check_ans_counts"].fillna(0, inplace=True)

# 計算 total_check_ans_time
check_ans_time = (
    combined_df.drop_duplicates(subset=["exam_video_exam_sn"])
    .groupby("PseudoID")["exam_ans_time"]
    .sum()
    .reset_index()
)
check_ans_time.columns = ["PseudoID", "total_check_ans_time"]
new_df = new_df.merge(check_ans_time, on="PseudoID", how="left")
new_df["total_check_ans_time"].fillna(0, inplace=True)

# 計算 total_check_ans_result
check_ans_result = (
    combined_df.drop_duplicates(subset=["exam_video_exam_sn"])
    .groupby("PseudoID")["exam_binary_res"]
    .sum()
    .reset_index()
)
check_ans_result.columns = ["PseudoID", "total_check_ans_result"]
new_df = new_df.merge(check_ans_result, on="PseudoID", how="left")
new_df["total_check_ans_result"].fillna(0, inplace=True)

# 計算 check_correct_ratio
new_df["check_correct_ratio"] = (
    new_df["total_check_ans_result"] / new_df["total_check_ans_counts"]
)
new_df["check_correct_ratio"] = new_df["check_correct_ratio"].fillna(0) * 100

# 計算 avg_check_ans_time
new_df["avg_check_ans_time"] = (
    new_df["total_check_ans_time"] / new_df["total_check_ans_counts"]
)
new_df["avg_check_ans_time"] = new_df["avg_check_ans_time"].fillna(0)

# 以前的代碼保持不變...


op_counts = (
    combined_df.drop_duplicates(subset=["review_sn", "record_plus_sn"])
    .groupby("PseudoID")
    .size()
    .reset_index(name="total_op_counts")
)
new_df = new_df.merge(op_counts, on="PseudoID", how="left")


# 计算 avg_op_counts
new_df["avg_op_counts"] = new_df["total_op_counts"] / new_df["total_reviewed_count"]


# 定義一個函數來計算特定動作的次數
def calculate_action_count(combined_df, action):
    action_df = combined_df[combined_df["record_plus_view_action"] == action]
    action_count = (
        action_df.drop_duplicates(subset=["review_sn", "record_plus_sn"])
        .groupby("PseudoID")
        .size()
        .reset_index(name=f"total_{action}")
    )
    return action_count


# 計算 play, paused, end, normal, slowdown, speedup, chkptstart, chkptend, continue, note, dragleft, dragright, dragstart, browse, replay, fuscreenoff, fuscreenon 的次數
for action in [
    "play",
    "paused",
    "end",
    "normal",
    "slowdown",
    "speedup",
    "chkptstart",
    "chkptend",
    "continue",
    "note",
    "dragleft",
    "dragright",
    "dragstart",
    "browse",
    "replay",
    "fuscreenoff",
    "fuscreenon",
]:
    action_count = calculate_action_count(combined_df, action)
    new_df = new_df.merge(action_count, on="PseudoID", how="left").fillna(0)


# 儲存 new_df 為CSV
new_df.to_csv("clean.csv", index=False)
