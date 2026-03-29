from django.db import models
from django.utils import timezone
from django.core.validators import MinValueValidator, MaxValueValidator

# 汎用
class HANYO(models.Model):
	CODE1 = models.CharField(max_length=20)
	CODE2 = models.CharField(max_length=20)
	CNAME = models.CharField(max_length=60)
	NAME = models.CharField(max_length=60)
	STARTDATE = models.DateField(default=timezone.now)
	ENDDATE = models.DateField(default='2099-12-31')
	CONTROLTEXT1 = models.CharField(max_length=50, null=True, blank=True)
	CONTROLTEXT2 = models.CharField(max_length=50, null=True, blank=True)
	CONTROLINT1 = models.IntegerField(null=True, blank=True, default=0)
	CONTROLINT2 = models.IntegerField(null=True, blank=True, default=0)
	AUTHORITY = models.CharField(max_length=20)
	CREATEUSER = models.CharField(max_length=20)
	CREATEDATE = models.DateTimeField(auto_now_add=True)
	UPDATEUSER = models.CharField(max_length=20)
	UPDATEDATE = models.DateTimeField(auto_now=True)
	def __str__(self):
		return self.CNAME

class KENCHI(models.Model):
	CODE = models.CharField(max_length=19)
	CODE2 = models.CharField(max_length=19, null=True, blank=True)
	NAME = models.CharField(max_length=60)
	USE_FLAG = models.CharField(max_length=1)
	PREDICT_PROBA = models.DecimalField(max_digits=4, decimal_places=1)
	POINTS = models.DecimalField(max_digits=9, decimal_places=3)
	RULE_FLAG = models.BooleanField()
	STARTDATE = models.DateField(default=timezone.now)
	ENDDATE = models.DateField(default='2099-12-31')
	NOTE = models.CharField(max_length=100, null=True, blank=True)
	SPARE = models.CharField(max_length=200, null=True, blank=True)
	CREATEUSER = models.CharField(max_length=20)
	CREATEDATE = models.DateTimeField(auto_now_add=True)
	UPDATEUSER = models.CharField(max_length=20)
	UPDATEDATE = models.DateTimeField(auto_now=True)
	def __str__(self):
		return self.NAME
		
# ログ出力テーブル
class USERLOG(models.Model):
	CODE = models.CharField(max_length=30)
	STARTDATE = models.DateTimeField(default=timezone.now)
	ENDDATE = models.DateTimeField(default=timezone.now, null=True, blank=True)
	STATUS_FLAG = models.CharField(max_length=1)
	COMMENT = models.CharField(max_length=50)
	def __str__(self):
		return self.UPDATETIME

class MOVEMENT_INFO(models.Model):
	PATIENT_ID = models.CharField(max_length=10)
	MOVE_DATETIME = models.DateTimeField(default=timezone.now)
	MOVE_CATEGORY = models.CharField(max_length=10)
	WARD = models.CharField(max_length=10, null=True, blank=True)
	DEPT = models.CharField(max_length=10, null=True, blank=True)
	def __str__(self):
		return self.PATIENT_ID

class KARTE_FILE(models.Model):
	FILE_CODE = models.CharField(max_length=10, primary_key=True)
	FILE_NAME = models.CharField(max_length=30)
	CONVERSION_FLAG = models.BooleanField(default=False)
	STARTDATE = models.DateField(default=timezone.now)
	ENDDATE = models.DateField(default='2099-12-31')
	CREATEUSER = models.CharField(max_length=20)
	CREATEDATE = models.DateTimeField(auto_now_add=True)
	UPDATEUSER = models.CharField(max_length=20)
	UPDATEDATE = models.DateTimeField(auto_now=True)
	def __str__(self):
		return self.FILE_CODE

class KARTE_FILE_COLUMN(models.Model):
	FILE_CODE = models.CharField(max_length=10)
	COLUMN_NAME = models.CharField(max_length=50)
	BEFORE_COLUMN_NAME = models.CharField(max_length=50, null=True, blank=True)
	DATA_TYPE = models.CharField(max_length=10)
	ADD_COLUMN_FLAG = models.BooleanField()
	ADD_COLUMN_TEXT = models.CharField(max_length=50, null=True, blank=True)
	STARTDATE = models.DateField(default=timezone.now)
	ENDDATE = models.DateField(default='2099-12-31')
	CREATEUSER = models.CharField(max_length=20)
	CREATEDATE = models.DateTimeField(auto_now_add=True)
	UPDATEUSER = models.CharField(max_length=20)
	UPDATEDATE = models.DateTimeField(auto_now=True)
	def __str__(self):
		return self.FILE_CODE

class RESULT_COLUMN(models.Model):
	COLUMN_NAME = models.CharField(max_length=50)
	ORIGINAL_NAME = models.CharField(max_length=50)
	CREATEUSER = models.CharField(max_length=20)
	CREATEDATE = models.DateTimeField(auto_now_add=True)
	UPDATEUSER = models.CharField(max_length=20)
	UPDATEDATE = models.DateTimeField(auto_now=True)
	def __str__(self):
		return self.COULUMN_NAME
