import csv
from collections import defaultdict

# 使用defaultdict儲存每個PseudoID的獨立prac_questions
prac_questions_dict = defaultdict(set)

# 打開CSV檔案
with open("data.csv", "r", encoding="ISO-8859-1") as csvfile:
    # 創建CSV讀取器
    reader = csv.reader(csvfile)

    # 忽略標題行
    next(reader)

    # 遍歷CSV中的每一行
    for row in reader:
        pseudo_id = row[0]
        # 使用@XX@拆解prac_questions
        prac_items = row[1].split("@XX@")

        # 將每個項目加入到對應的PseudoID的set中
        for item in prac_items:
            if item:  # 忽略空項目
                prac_questions_dict[pseudo_id].add(item)

# 輸出每個PseudoID的獨立prac_questions數量
for pseudo_id, prac_set in prac_questions_dict.items():
    print(f"PseudoID {pseudo_id} 的不重複prac_questions數量為：{len(prac_set)}")
