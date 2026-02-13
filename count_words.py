import pandas as pd
from pathlib import Path

targets = ["カテ留置",
    "透析関連診察",
    "障害者",
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
    "気管支鏡",
    "ブロンコ",
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
    "呼吸苦"]



in_dir = Path("C:/aibrain_main/aireceiptproject/data/createmodel_in/karte/")

l = []
for file in in_dir.glob('**/*.csv'):
    df = pd.read_csv(file, engine="python", encoding='cp932', dtype='object')
    l.append(df)


all_df = pd.concat(l)

for target in targets:
    count = all_df['カルテ内容'].str.contains(target, regex=False).sum()
    print(f"{target}: {count}")