import pandas as pd
from sqlalchemy import create_engine

db_passwd = 'administrator'
db_name = 'aireceiptdb'

def check_ent(seikyunengetsu):
    """
    インサートする検知結果が仮レセの検知か、そうでないかを判定する
    テーブルに入っている検知結果の請求年月と引数で渡ってきた（これからいれようとしている結果の）請求年月が同じならFalse
    違ったらTrueを返す
    """
    con = create_engine(f'mysql://root:{db_passwd}@127.0.0.1/{db_name}?charset=utf8')
    sql = "SELECT DISTINCT SEIKYUNENGETSU FROM no_calculation_results"
    df = pd.read_sql_query(sql=sql, con=con)
    con.dispose()
    if df.empty:
        result = False
    else:
        result = False if seikyunengetsu == df.iloc[0]['SEIKYUNENGETSU'] else True
    return(result)

def get_updated_records():
    """
    仮レセでステータスかメモ欄を更新したレコードのみを抽出
    """
    con = create_engine(f'mysql://root:{db_passwd}@127.0.0.1/{db_name}?charset=utf8')
    sql = "SELECT * FROM no_calculation_results WHERE USER_CHECK <> '未' OR KENCHI50 <> ''"
    df = pd.read_sql_query(sql=sql, con=con)
    con.dispose()
    return(df)

def reinsert_updated_records(df):
    """
    本レセの検知結果に対して仮レセ時のステータスとメモを更新する
    """
    add_data = []
    con = create_engine(f'mysql://root:{db_passwd}@127.0.0.1/{db_name}?charset=utf8')
    for index, row in df.iterrows():
        unique_key = (row['PATIENT_ID'], row['MEDICAL_CARE_DATE'], row['DEPT'], row['CODE'])
        existing_data = con.execute(
            "SELECT * FROM no_calculation_results WHERE PATIENT_ID = %s AND MEDICAL_CARE_DATE = %s AND DEPT = %s AND CODE = %s",
            unique_key
        ).fetchone()

        if existing_data:
            # すでにデータが存在する場合は、USER_CHECKとKENCHI50以外を更新
            update_query = """
            UPDATE no_calculation_results
            SET USER_CHECK = %s, KENCHI50 = %s
            WHERE PATIENT_ID = %s AND MEDICAL_CARE_DATE = %s AND DEPT = %s AND CODE = %s
            """
            update_data = (row['USER_CHECK'], row['KENCHI50'], *unique_key)
            con.execute(update_query, update_data)
            print('データを更新しました')
        else:
            # データが存在しない場合は新規追加
            add_data.append(list(row))
            print('データを追加しました')

    if add_data:
        add_df = pd.DataFrame(add_data, columns=df.columns)
        add_df = add_df.drop(['id'], axis=1)
        add_df.to_sql('no_calculation_results', con=con, if_exists='append', index=None)
    con.dispose()

if __name__ == '__main__':
    print(get_updated_records())