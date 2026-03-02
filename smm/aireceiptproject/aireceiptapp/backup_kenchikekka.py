import MySQLdb
import os

# 検知結果をバックアップ
def write_backup_result():
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	sql = (
		"INSERT INTO aireceiptapp_backup_kenchikekka \
		(USER_CHECK, SEIKYUNENGETSU, PATIENT_ID,PATIENT_NAMEJ,\
		PATIENT_NAMEK, MEDICAL_CARE_DATE, DEPT, WARD, IN_OUT, CODE, CODENAME,\
		CODE2, CODE2NAME, POINTS, PREDICT_PROBA, ZISSEKI,\
		KENCHI01, KENCHI02, KENCHI03, KENCHI04, KENCHI05,\
		KENCHI06, KENCHI07, KENCHI08, KENCHI09, KENCHI10,\
		KENCHI11, KENCHI12, KENCHI13, KENCHI14, KENCHI15,\
		KENCHI16, KENCHI17, KENCHI18, KENCHI19, KENCHI20,\
		KENCHI21, KENCHI22, KENCHI23, KENCHI24, KENCHI25,\
		KENCHI26, KENCHI27, KENCHI28, KENCHI29, KENCHI30,\
		KENCHI31, KENCHI32, KENCHI33, KENCHI34, KENCHI35,\
		KENCHI36, KENCHI37, KENCHI38, KENCHI39, KENCHI40,\
		KENCHI41, KENCHI42, KENCHI43, KENCHI44, KENCHI45,\
		KENCHI46, KENCHI47, KENCHI48, KENCHI49, KENCHI50,\
		OUTPUTDATE)\
		SELECT\
		USER_CHECK, SEIKYUNENGETSU, PATIENT_ID, PATIENT_NAMEJ,\
		PATIENT_NAMEK, MEDICAL_CARE_DATE, DEPT, WARD, IN_OUT, CODE, CODENAME,\
		CODE2, CODE2NAME, POINTS, PREDICT_PROBA, ZISSEKI,\
		KENCHI01, KENCHI02, KENCHI03, KENCHI04, KENCHI05,\
		KENCHI06, KENCHI07, KENCHI08, KENCHI09, KENCHI10,\
		KENCHI11, KENCHI12, KENCHI13, KENCHI14, KENCHI15,\
		KENCHI16, KENCHI17, KENCHI18, KENCHI19, KENCHI20,\
		KENCHI21, KENCHI22, KENCHI23, KENCHI24, KENCHI25,\
		KENCHI26, KENCHI27, KENCHI28, KENCHI29, KENCHI30,\
		KENCHI31, KENCHI32, KENCHI33, KENCHI34, KENCHI35,\
		KENCHI36, KENCHI37, KENCHI38, KENCHI39, KENCHI40,\
		KENCHI41, KENCHI42, KENCHI43, KENCHI44, KENCHI45,\
		KENCHI46, KENCHI47, KENCHI48, KENCHI49, KENCHI50,\
		OUTPUTDATE\
		FROM\
		aireceiptapp_kenchikekka"
		)

	cursor.execute(sql)
	connection.commit()
	connection.close()

# 検知結果を空にする
def delete_result():
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	sql = ("DELETE FROM aireceiptapp_kenchikekka")

	cursor.execute(sql)
	connection.commit()
	connection.close()



if __name__ == '__main__':
	write_backup_result()