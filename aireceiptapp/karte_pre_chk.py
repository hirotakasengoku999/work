import os, datetime, pandas as pd, numpy as np
from pathlib import Path

# カルテ処理
# フォルダ一覧
class KarteFolderInfo:
    def __init__(self):
        self.ITEM = ""
        self.STATUS = ""
        self.NGFLAG = 0

# 指定されたカルテファイルがあるかをチェック    
def GetKarteFolderList(path, process):
    # フォルダ下一覧取得
    results = []

    from aireceiptapp import read_kartefile, aireceipt_karte_pp
    karte_pre_file_obj = read_kartefile.create_karte_file_obj(process)
    target_folder_karte_csvfiles = list(Path(path).glob('**/*.csv'))
    for chkF in karte_pre_file_obj:
        status = 'ファイルを格納してください'
        ngflag = 1
        item = chkF.FILENAME
        datecol = chkF.DATECOL
        period_list = []
        for target_file in target_folder_karte_csvfiles:
            if item.replace('.csv', '') in target_file.name:
                ngflag = 0
                target_df = aireceipt_karte_pp.read_creaned_data(target_file)
                target_df = target_df[[datecol]]
                target_df = target_df[target_df[datecol]!=""]
                target_df = target_df.dropna(subset=[datecol])
                target_df[datecol] = target_df[datecol].str.replace('R6', '2024')
                target_df[datecol] = pd.to_datetime(target_df[datecol]).dt.strftime('%Y/%m/%d')
                for peu in target_df[datecol].unique():
                    period_list.append(peu)
                period_list = list(set(period_list))
        period_list = list(set(period_list))
        if len(period_list) > 0:
            month_str = period_list[0] if len(period_list) == 1 else f"{min(period_list)} - {max(period_list)}"
        else:
            month_str = ""
        # 出力をセット
        result = KarteFolderInfo()
        result.ITEM = chkF.FILENAME
        result.STATUS = status if ngflag == 1 else month_str
        result.NGFLAG = ngflag

        # 出力リストに追加
        results.append(result)
    return results


# ここから期間のチェック
def karte_period(karte_pre_object_list):
    # カルテデータ期間の最初の年月と最後の年月を取得する
    karte_start_period_list = []
    karte_end_period_list = []
    karte_period_ng_flag = 0
    work_list = []
    for karte in karte_pre_object_list:
        try:
            work_list = karte.STATUS.split('-')
            # 文字列を日付型に変換
            start_monthdate = datetime.datetime.strptime(work_list[0], '%Y/%m')
            end_monthdate = datetime.datetime.strptime(work_list[1], '%Y/%m')
            karte_start_period_list.append(start_monthdate)
            karte_end_period_list.append(end_monthdate)
        except:
            pass

    # カルテデータの各ファイルの期間が合致しているかをチェックする
    kartefiles_period_chk_start = all(
        x == karte_start_period_list[0] for x in karte_start_period_list)
    kartefiles_period_chk_end = all(
        x == karte_end_period_list[0] for x in karte_end_period_list)
    if kartefiles_period_chk_start and kartefiles_period_chk_end:
        pass
    else:
        karte_period_ng_flag = 1

    return (
        karte_start_period_list, karte_end_period_list, karte_period_ng_flag)

# 単体テスト用
if __name__ == '__main__':
    import folder
    user_karte_pre_folder = folder.user_karte_folder("predict")
    result = GetKarteFolderList(user_karte_pre_folder, 'predict')
    for i in result:
        print(f'{i.ITEM}  {i.STATUS}  {i.NGFLAG}')
