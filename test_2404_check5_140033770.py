# -*- coding: utf-8 -*-
# Check5処理前後の 140033770_result.csv 検証テスト
#
# 検証内容:
#   1. required_codesが1件もない行 → 予測値が0になっているか
#   2. required_codesが1件以上ある行 → 予測値が変わっていないか（予測値_old と一致）
#
# 前提:
#   s3_change_prediction.py（Check5）実行済みの 140033770_result.csv が存在すること
#   （予測値_old 列に処理前の値が保存されている）

import pandas as pd
from pathlib import Path

# ---- パス設定（環境に応じて変更） ----
BASE_DIR = Path(r"C:\aibrain_main\aireceiptproject")
result_path = BASE_DIR / "work/prediction/7_1/140033770_result.csv"
si_path = BASE_DIR / "work/prediction/3_2/診療行為.pickle"

# ---- Check5 の required_codes (p8_predictive_execution.json drops[4][0]) ----
required_codes = [
    "140057810", "140057910", "140058010", "140058110", "140058210",
    "140058310", "140058410", "140058510", "140058610", "140007710", "140029850"
]

col_karute  = "カルテ番号等"
col_date    = "_date"
col_yosoku  = "予測値"
col_yosoku_old = "予測値_old"

# ---- result.csv 読み込み（処理後） ----
if not result_path.exists():
    print(f"[ERROR] ファイルが見つかりません: {result_path}")
    exit(1)
result_df = pd.read_csv(result_path, engine='python', encoding='cp932', dtype={col_karute: 'object'})

# ---- 診療行為.pickle から required_sum を計算（option="" = 日単位） ----
if not si_path.exists():
    print(f"[ERROR] ファイルが見つかりません: {si_path}")
    exit(1)
indf_s = pd.read_pickle(si_path)

usec = [col_date, col_karute] + required_codes
df_tmp = indf_s.loc[:, indf_s.columns.intersection(usec)].reindex(columns=usec)
df_tmp[required_codes] = df_tmp[required_codes].fillna(0).astype('int')
df_tmp = df_tmp.fillna(0)
df_tmp['required_sum'] = df_tmp[required_codes].sum(axis=1)

# ---- result_df とマージして required_sum を付与 ----
merged = pd.merge(
    result_df,
    df_tmp[[col_date, col_karute, 'required_sum']],
    on=[col_date, col_karute],
    how='left'
)
merged['required_sum'] = merged['required_sum'].fillna(0)
merged[col_yosoku] = merged[col_yosoku].astype(float)
merged[col_yosoku_old] = merged[col_yosoku_old].astype(float)

result_str = "OK"

# ---- 検証1: required_sum == 0 の行は予測値が 0 になっているか ----
no_required = merged[merged['required_sum'] == 0]
not_zeroed = no_required[no_required[col_yosoku] != 0]
if len(not_zeroed) > 0:
    print(f"[NG] required_sum==0 なのに予測値が0でない行が {len(not_zeroed)} 件あります")
    print(not_zeroed[[col_karute, col_date, col_yosoku, col_yosoku_old, 'required_sum']].head(10).to_string())
    result_str = "NG"
else:
    print(f"[OK] required_sum==0 の行({len(no_required)}件) の予測値はすべて0です")

# ---- 検証2: required_sum > 0 の行は予測値が変わっていないか ----
has_required = merged[merged['required_sum'] > 0]
changed = has_required[has_required[col_yosoku] != has_required[col_yosoku_old]]
if len(changed) > 0:
    print(f"[NG] required_sum>0 なのに予測値が変更されている行が {len(changed)} 件あります")
    print(changed[[col_karute, col_date, col_yosoku, col_yosoku_old, 'required_sum']].head(10).to_string())
    result_str = "NG"
else:
    print(f"[OK] required_sum>0 の行({len(has_required)}件) の予測値はすべて変更なしです")

print(f"\nテスト結果: {result_str}")