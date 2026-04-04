import os
import MySQLdb

def model_list(path):
    data_list = os.listdir(path)    #カレントディレクトリを指定
    sav_list = []
    pikkle_list = []
    m_list = []
    for data in data_list:
        if '_result.sav' in data:
            sav_list.append(data[:-11])
        elif '.pickle' in data:
            pikkle_list.append(data[:-7])
    for sa in sav_list:
        if sa in pikkle_list:
            m_list.append(sa)
    master_use_code_list = get_item_use_flag()
    results = []
    if master_use_code_list:
        for m in m_list:
            if m in master_use_code_list:
                results.append(m)
    else:
        results = m_list
    for i, v in master_use_code_list.items():
        if v == 1:
            results.append(i)
    return(results)

# 使用フラグが１の検知項目を取得する
def get_item_use_flag():
    # データベースへの()接続とカーソルの生成
    connection=MySQLdb.connect(
      host='127.0.0.1',
      user='root',
      passwd='administrator',
      db='aireceiptdb',
      charset='utf8'
      )
    cursor = connection.cursor(MySQLdb.cursors.DictCursor)

    sql = ("SELECT CODE,CODE2,RULE_FLAG FROM aireceiptapp_kenchi WHERE USE_FLAG = '1'")
    cursor.execute(sql)
    rows = cursor.fetchall()
    connection.close()
    result = {}
    if rows:
        for row in rows:
            if row['CODE2']:
                result[row['CODE2']] = row['RULE_FLAG']
            else:
                result[row['CODE']] = row['RULE_FLAG']
    return(result)

# リストをカンマ区切りの文字列に変換
def list_str_conversion(conversion_list):
    s = ""
    for i in conversion_list:
        s += i + ","
    if s != "":
        s = s[:-1]
    return(s)

# 単体テスト用
if __name__ == '__main__':
    path = "C:/aibrain_main/aireceiptproject/data//model/model"
    conversion_list = model_list(path)
    print(len(conversion_list))
    print(conversion_list)
    for c in conversion_list:
        print(c)
