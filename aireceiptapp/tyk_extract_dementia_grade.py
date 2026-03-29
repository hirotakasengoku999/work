"""
専用カルテ整形処理
作成文書（認知症ケア加算対象者スコア）から判定ランクを抽出
"""
import pandas as pd


# =============================================
# 判定ランクを抽出
# =============================================
def extract_hantei(record_text) -> str:
    for line in record_text.splitlines():
        parts = line.split(",")
        # 行の先頭が「判定」でかつ隣に値がある
        if parts[0].strip() == "判定" and len(parts) > 1:
            val = parts[1].strip()
            if val:
                return f"判定ランク{val}"
    return "未設定"

# =============================================
# メイン処理
# =============================================
def main(df: pd.DataFrame) -> pd.DataFrame:
    # 対象列のnanを除去
    df = df.dropna(subset=["文書テキストデータ（全文）"])

    # 判定列を生成
    df["判定"] = df["文書テキストデータ（全文）"].apply(extract_hantei)

    # 未設定行を除去
    df = df[df["判定"] != "未設定"]

    return df

# if __name__ == "__main__":
#     main()