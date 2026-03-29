"""
専用カルテ整形処理
作成文書（肺血栓塞栓症予防管理料）からリスクレベルを抽出
"""

import pandas as pd

def extract_risk_surgery(record_text):
    in_final_block = False
    RISK_LEVELS = ["リスクなし", "低リスク", "中リスク", "高リスク", "最高リスク"]

    for line in record_text.splitlines():
        if "最終の決定" in line:
            in_final_block = True
            continue

        if not in_final_block:
            continue

        parts = line.split(",")
        for i in range(len(parts) - 1):
            current  = parts[i].strip()
            next_val = parts[i + 1].strip()
            if current in RISK_LEVELS and next_val in RISK_LEVELS:
                return next_val  # 隣り合う2つ目を返す

    return "未設定"

# =============================================
# 内科領域：合計点数からリスクレベルに変換
# =============================================
def score_to_risk(score):
    if score == 0:
        return "リスクなし"
    elif score == 1:
        return "低リスク"
    elif 2 <= score <= 4:
        return "中リスク"
    elif 5 <= score <= 6:
        return "高リスク"
    else:
        return "最高リスク"

def extract_risk_internal(record_text):
    for line in record_text.splitlines():
        if "合計点数" in line:
            parts = line.split(",")
            found_keyword = False
            for p in parts:
                if "合計点数" in p:
                    found_keyword = True
                    continue
                if found_keyword:
                    val = p.strip()
                    if val.lstrip("-").isdigit():
                        return score_to_risk(int(val))

    return "未設定"

# =============================================
# 行ごとにリスクレベルを判定
# =============================================
def extract_risk(row):
    doc_name    = str(row["文書名"])
    record_text = str(row["文書テキストデータ（全文）"])

    if "深部静脈血栓症リスク評価と予防指示書.手術・外傷例" in doc_name:
        return extract_risk_surgery(record_text)
    elif "深部静脈血栓症リスク評価と予防指示書.内科領域" in doc_name:
        return extract_risk_internal(record_text)
    else:
        return "対象外"

# =============================================
# メイン処理
# =============================================
def main(df: pd.DataFrame):

    # 対象列のnanを除去
    df = df.dropna(subset=["文書テキストデータ（全文）"])

    # リスクレベル列を生成
    df["リスクレベル"] = df.apply(extract_risk, axis=1)
    return df

# if __name__ == "__main__":
#     main()