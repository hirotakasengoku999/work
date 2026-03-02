from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
import os
import sys
import pandas as pd

# 請求年月が５桁だった場合、和暦から西暦に変換する
def years_conversion(year):
    # 和暦とコードの対応表df作成
    master_df = pd.DataFrame({
        'コード': ["1","2","3","4","5"],
        '年': ["1867","1911","1925","1988","2018"],
        '内容': ["明治","大正","昭和","平成","令和"]
    })
    target_year = master_df["年"][master_df["コード"]==year[0]].values[0]
    seireki = int(target_year)  + int(year[1:3])
    seireki_gappi = str(seireki) + year[3:]
    return(seireki_gappi)

# UKEファイルのIE列に記載されている請求年月を取得する
def get_billing_year_month(file):
    fileobj = open(file, "r", encoding="cp932")
    year_month = ""
    while True:
        line = fileobj.readline()
        line_list = line.split(',')
        if line_list[0] == "IR":
            year_month = line_list[7]
        else:
            break
    return(year_month)

# 請求年月の前の月を取得する
def date_str_conversion(year_month):
    result_date = dt.strptime(year_month,'%Y%m') # 文字列を日付型に変換
    result_date = result_date + relativedelta(months=-1) # 1カ月前にする
    return(result_date)
    
# フォルダ内のファイルリストを取得する
def getUKEfiles(path):
    files = os.listdir(path)
    UKE_file = [f for f in files if f[-3:] == 'UKE']
    return(UKE_file)

# フォルダ内の全てのUKEファイルの請求年月が合致しているかどうかをチェックする
def chk_billing_year_month(path):
    try:
        UKE_files = getUKEfiles(path)
        UKE_billing_year_month_list = []
        for file in UKE_files:
            file_path = os.path.join(path,file)
            year_month = get_billing_year_month(file_path)

            # 取得した請求年月が和暦だった場合は西暦に変換する
            if len(year_month) == 5:
                year_month = years_conversion(year_month)

            year_month = date_str_conversion(year_month)
            UKE_billing_year_month_list.append(year_month)
        ngflg = all(x==UKE_billing_year_month_list[0] for x in UKE_billing_year_month_list)
        billing_year_month = UKE_billing_year_month_list[0]
        return(ngflg,billing_year_month)

    except:
        pass  
    


# 単体テスト用コマンド呼び出し関数
# if __name__ == "__main__":
#     """
#     引数
#         path : レセプトフォルダー
#     """
#     base_dir = 'C:/Users/Administrator/Desktop/work/'
#     dirs = os.listdir(base_dir)
#     folders_dirs = [f for f in dirs if os.path.isdir(os.path.join(base_dir, f))]
#     for f in folders_dirs:
#         target_dir = os.path.join(folders_dirs, f)
#         ngflg, billing_year_month = chk_billing_year_month(target_dir)
#         print('ngflg = {}  billing_year_month = {}'.format(ngflg, billing_year_month))

