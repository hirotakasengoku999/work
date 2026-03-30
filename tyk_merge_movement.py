"""
入院履歴・移動情報 統合変換スクリプト

【入力】
  - 入院履歴ファイル (.xlsx or .csv)  ※「患者番号」列を含むこと
  - 移動情報ファイル (.xlsx or .csv)

【出力】
  - 統合後の時系列 CSV ファイル (UTF-8 BOM付き、Excel で開ける)
    カラム: 患者番号, 入院番号, 移動日, 移動時刻, カテゴリ, 診療科, 病棟

【ロジック】
  - 入院履歴から: 入院行（入院日・入院科・入院病棟）、退院行（退院日・退院科・退院病棟）
  - 移動情報から: 転棟行（転区分="転棟" のみ、転ベッドは除外）
  - 退院日がNaN（入院中）の場合は退院行を出力しない
  - 入院番号をキーに結合し、患者番号を付与、日付・時刻順に並べる
"""

import pandas as pd
from pathlib import Path

# ============================================================
# ファイルパス設定（環境に合わせて変更してください）
# ============================================================
ADMISSION_FILE = "入院履歴.csv"        # 入院履歴ファイル
MOVEMENT_FILE  = "移動情報.csv"        # 移動情報ファイル
OUTPUT_FILE    = "tmp_移動情報.csv"   # 出力ファイル
# ============================================================


def read_file(path):
    """xlsx / csv 両対応で読み込む"""
    if str(path).endswith(".csv"):
        for enc in ("cp932", "utf8", "utf-8-sig", "shift-jis"):
            try:
                return pd.read_csv(path, engine='python', encoding=enc, dtype='object')
            except UnicodeDecodeError:
                continue
        raise ValueError(f"文字コードを判定できませんでした: {path}")
    else:
        return pd.read_excel(path)


def build_records(df_adm, df_mov):
    records = []

    # 患者番号・入院番号は前ゼロを保持するため必ず文字列に
    # 患者番号は8桁ゼロ埋め
    for col in ("患者番号", "入院番号"):
        if col in df_adm.columns:
            df_adm[col] = df_adm[col].astype(str).str.strip()
        if col in df_mov.columns:
            df_mov[col] = df_mov[col].astype(str).str.strip()
    if "患者番号" in df_adm.columns:
        df_adm["患者番号"] = df_adm["患者番号"].str.zfill(8)
    if "患者番号" in df_mov.columns:
        df_mov["患者番号"] = df_mov["患者番号"].str.zfill(8)

    # dtype='object' で読んだ場合に備えて日付・数値列を変換
    for col in ("入院日付", "退院日付", "転入日"):
        if col in df_adm.columns:
            df_adm[col] = pd.to_datetime(df_adm[col], errors="coerce")
    for col in ("入院時刻", "退院時刻"):
        if col in df_adm.columns:
            df_adm[col] = pd.to_numeric(df_adm[col], errors="coerce")

    if "移動日付" in df_mov.columns:
        df_mov["移動日付"] = pd.to_datetime(df_mov["移動日付"], errors="coerce")
    if "移動時刻" in df_mov.columns:
        df_mov["移動時刻"] = pd.to_numeric(df_mov["移動時刻"], errors="coerce")

    # 入院番号 → 患者番号 のマッピングを入院履歴から作成
    nyuin_to_patient = (
        df_adm.drop_duplicates("入院番号")
              .set_index("入院番号")["患者番号"]
              .to_dict()
    )

    # ----------------------------------------------------------
    # 入院履歴: 入院番号ごとに入院行・退院行を作成
    # 同一入院番号で複数行ある場合は先頭行（入院情報共通）を使用
    # ----------------------------------------------------------
    for nyuin_no, grp in df_adm.groupby("入院番号", sort=False):
        first  = grp.iloc[0]                            # 入院情報は先頭行
        latest = grp.sort_values("転入日").iloc[-1]     # 退院時の状態は転入日が最新の行

        # 入院行
        records.append({
            "患者番号": first["患者番号"],
            "入院番号": nyuin_no,
            "移動日":   first["入院日付"],
            "移動時刻": first.get("入院時刻", None),
            "カテゴリ": "入院",
            "診療科":   first["入院科名称"],
            "病棟":     first["入院病棟名称"],
        })

        # 退院行（退院日がNaN = 入院中 → スキップ）
        if pd.notna(latest["退院日付"]):
            records.append({
                "患者番号": latest["患者番号"],
                "入院番号": nyuin_no,
                "移動日":   latest["退院日付"],
                "移動時刻": latest.get("退院時刻", None),
                "カテゴリ": "退院",
                "診療科":   latest["カレント科名称"],
                "病棟":     latest["カレント病棟名称"],
            })

    # ----------------------------------------------------------
    # 移動情報: 転棟のみ抽出（転ベッドは除外）
    # 患者番号は入院番号経由でマッピング
    # ----------------------------------------------------------
    # 入院履歴に存在しない入院番号を警告表示
    adm_nyuin_nos = set(nyuin_to_patient.keys())
    mov_nyuin_nos = set(df_mov[df_mov["転区分"] == "転棟"]["入院番号"].unique())
    missing = mov_nyuin_nos - adm_nyuin_nos
    if missing:
        print(f"[WARNING] 移動情報にあるが入院履歴にない入院番号（患者番号が欠損します）: {missing}")

    df_tenbo = df_mov[df_mov["転区分"].isin(["転棟", "転科", "転室"])].copy()

    for _, row in df_tenbo.iterrows():
        nyuin_no = row["入院番号"]
        records.append({
            "患者番号": nyuin_to_patient.get(nyuin_no) or row["患者番号"],
            "入院番号": nyuin_no,
            "移動日":   row["移動日付"],
            "移動時刻": row.get("移動時刻", None),
            "カテゴリ": "転棟",
            "診療科":   row["移動科名称"],
            "病棟":     row["移動病棟名称"],
        })

    df = pd.DataFrame(records)

    # 入院番号・患者番号・移動日でソート
    # 同日の場合: 入院 > 転棟 > 退院
    cat_order = {"入院": 0, "転棟": 1, "退院": 2}
    df["_cat_order"] = df["カテゴリ"].map(cat_order)
    df["移動日"] = pd.to_datetime(df["移動日"])

    df = (
        df.sort_values(["入院番号", "患者番号", "移動日", "_cat_order"])
          .drop(columns=["移動時刻", "_cat_order"])
          .reset_index(drop=True)
    )

    return df


def write_csv(df, out_path):
    df["移動日"] = df["移動日"].dt.strftime("%Y/%m/%d")
    # utf-8-sig = BOM付きUTF-8（Excelで開いても文字化けしない）
    df.to_csv(out_path, index=False, encoding="cp932")
    print(f"出力完了: {out_path}  ({len(df)} 行)")


def find_file(folder: Path, base_name: str) -> Path:
    """
    ベース名の stem で前方一致するファイルを特定して返す。
    例: base_name="入院履歴.csv" → "入院履歴_20260330.csv" を返す
    複数ある場合は更新日時が最新のものを使用する。
    """
    stem = Path(base_name).stem
    suffix = Path(base_name).suffix
    candidates = sorted(
        [p for p in folder.iterdir() if p.is_file() and p.name.startswith(stem) and p.suffix == suffix],
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )
    if not candidates:
        raise FileNotFoundError(f"'{base_name}' に対応するファイルが見つかりません: {folder}")
    if len(candidates) > 1:
        print(f"[WARNING] 複数ファイルが見つかりました。最新を使用します: {candidates[0].name}")
    return candidates[0]


def main(user_karute_folder: str):
    user_karute_folder_path = Path(user_karute_folder)

    admission_file_path = find_file(user_karute_folder_path, ADMISSION_FILE)
    print(f"入院履歴読み込み: {admission_file_path.name}")
    df_adm = read_file(admission_file_path)

    movement_file_path = find_file(user_karute_folder_path, MOVEMENT_FILE)
    print(f"移動情報読み込み: {movement_file_path.name}")
    df_mov = read_file(movement_file_path)

    print("データ変換中...")
    df_result = build_records(df_adm, df_mov)

    output_file_path = user_karute_folder_path/OUTPUT_FILE
    write_csv(df_result, output_file_path)


if __name__ == "__main__":
    main("C:/aiブレーン/検知用データ/カルテ")
