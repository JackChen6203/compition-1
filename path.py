import pandas as pd

# 1. 讀取CSV
combined_df = pd.read_csv("data.csv", low_memory=False, encoding="iso-8859-1")

# 2. 將review_end_time和review_start_time從Excel日期格式轉換為標準日期格式（如果需要）

# 3. 創建一個新的DataFrame
new_df = pd.DataFrame()

# 4. 根據combined_df填充new_df的欄位
new_df["PseudoID"] = combined_df["PseudoID"]  # PseudoID
new_df["city_code"] = combined_df["city_code"]  # city_code
new_df["organization_id"] = combined_df["organization_id"]  # organization_id
new_df["grade"] = combined_df["grade"]  # grade
new_df["class"] = combined_df["class"]  # class
new_df["month"] = combined_df["month"]  # month
new_df["review_sn"] = combined_df["review_sn"]  # review_sn
new_df["record_plus_sn"] = combined_df["record_plus_sn"]  # record_plus_sn
new_df["record_plus_view_time"] = combined_df[
    "record_plus_view_time"
]  # record_plus_view_time
new_df["record_plus_view_action"] = combined_df[
    "record_plus_view_action"
]  # record_plus_view_action

# 5. 移除record_plus_sn的重複值
new_df = new_df.drop_duplicates(subset=["record_plus_sn"])

# 6. 建立record_action_code欄位並根據record_plus_view_action進行轉換
action_code_mapping = {
    "play": "A",
    "paused": "B",
    "end": "C",
    "normal": "D",
    "slowdown": "E",
    "speedup": "F",
    "chkptstart": "G",
    "chkptend": "H",
    "continue": "I",
    "note": "J",
    "dragleft": "K",
    "dragright": "L",
    "dragstart": "M",
    "browse": "N",
    "replay": "O",
    "fuscreenoff": "P",
    "fuscreenon": "Q",
}
new_df["record_action_code"] = new_df["record_plus_view_action"].map(
    action_code_mapping
)

# 7. 根據PseudoID和record_plus_view_time進行排序
new_df = new_df.sort_values(by=["PseudoID", "record_plus_view_time"])

# 8. 儲存新的DataFrame為CSV
new_df.to_csv("PATH.csv", index=False)
