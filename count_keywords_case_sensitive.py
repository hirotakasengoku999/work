import pandas as pd
import re
from pathlib import Path

# ===== 設定 =====
CSV_PATH = "input.csv"   # 対象CSV
ENCODING = "utf-8"       # 文字化けする場合は "cp932" などに変更

KEYWORDS = [
    "nppv",
    "RTX",
    "血尿",
    "カテ留置",
    "透析関連診察　障害者",
    "褥瘡分類D3",
    "褥瘡分類D4",
    "褥瘡分類D5",
    "VAC",
    "レナシス",
    "PICO",
    "縫合",
    "切開排膿",
    "ハーモニック",
    "リガシュア",
    "FFP",
    "アルブミン",
    "ＡＲナビゲーション",
    "ナビゲーション",
    "トライステープル",
    "ジアグノグリーン",
    "MEP",
    "SEP",
    "ABR",
    "VEPモニタリング",
    "気管支鏡　ブロンコ",
    "髄液検査",
    "ルンバール",
    "Tapテスト",
    "SpyGlss",
    "嚥下内視鏡",
    "膀胱鏡",
    "EBUS-GS下肺生検",
    "アンギオ",
    "○○より紹介",
    "早期プロトコール",
    "脱水",
    "嘔吐",
    "JCS",
    "OD（Overdose)",
    "ショック",
    "ｱﾅﾌｨﾗｷｼ-",
    "NYHA",
    "P/F比",
    "酸素投与",
    "呼吸苦",
]

# どの列を検索するか。空なら全列を検索
TARGET_COLUMNS = []  # 例: ["記事内容", "診療メモ"]


def build_search_text(df: pd.DataFrame, target_cols: list[str]) -> pd.Series:
    if target_cols:
        use_cols = [c for c in target_cols if c in df.columns]
        if not use_cols:
            raise ValueError(f"TARGET_COLUMNS がCSV列名に存在しません: {target_cols}")
        sub = df[use_cols]
    else:
        sub = df

    return sub.fillna("").astype(str).agg(" ".join, axis=1)


def main():
    df = pd.read_csv(CSV_PATH, encoding=ENCODING)
    text = build_search_text(df, TARGET_COLUMNS)

    results = []
    hit_masks = []

    for kw in KEYWORDS:
        # 完全一致（ケースセンシティブ）
        pattern = re.compile(re.escape(kw))
        hit = text.str.contains(pattern, na=False)
        hit_masks.append(hit)
        results.append({"keyword": kw, "count_rows": int(hit.sum())})

    res_df = pd.DataFrame(results).sort_values("count_rows", ascending=False)

    print("=== キーワード別ヒット行数（行単位でカウント） ===")
    print(res_df.to_string(index=False))

    total_any = int(pd.concat(hit_masks, axis=1).any(axis=1).sum())
    print("\n=== いずれかのキーワードにヒットした行数 ===")
    print(total_any)

    out_path = Path("keyword_counts.csv")
    res_df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n出力: {out_path.resolve()}")


if __name__ == "__main__":
    main()