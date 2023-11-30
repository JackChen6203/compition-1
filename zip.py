import csv


def read_and_process_csv(file_path):
    with open(file_path, encoding="ISO-8859-1") as file:
        reader = csv.DictReader(file)
        for row in reader:
            if row["prac_score_rate"] == "100":
                ae_data = row["prac_questions"].split("@XX@")
                ag_data = row["prac_binary_res"].split("@XX@")

                zip_data = list(zip(ae_data, ag_data))
                # 確保數據被正確切割
                print(f"原始 AE 數據: {row['prac_questions']}")
                print(f"切割後 AE 數據: {ae_data}")
                print(f"原始 AG 數據: {row['prac_binary_res']}")
                print(f"切割後 AG 數據: {ag_data}")

                return zip_data
    return None


# 替換以下路徑為你的 CSV 文件路徑
file_path = "edu_bigdata_imp.csv"
result = read_and_process_csv(file_path)

if result:
    print("結果：", result)
else:
    print("沒有找到 AA 為 100 的數據")
