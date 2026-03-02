from django import forms
from .models import RESULT_COLUMN

class HanyoForm(forms.Form):
	code1 = forms.CharField(
		max_length=20,
		widget=forms.TextInput(attrs={'class':'form-control','readonly':'readonly'}),
		)
	code2 = forms.CharField(
		max_length=20,
		widget=forms.TextInput(attrs={'class':'form-control','pattern':'[A-Z,a-z,0-9,_]+'}),
		)
	cname = forms.CharField(
		max_length=60,
		widget=forms.TextInput(attrs={'class':'form-control'}),
		)
	name = forms.CharField(
		max_length=60,
		widget=forms.TextInput(attrs={'class':'form-control'}),
		)
	startdate = forms.CharField(
		max_length=10,
		widget=forms.TextInput(attrs={'class':'form-control datetimepicker-input','data-target':"#datetimepicker-startdate"}),
		)
	enddate = forms.CharField(
		max_length=10,
		widget=forms.TextInput(attrs={'class':'form-control datetimepicker-input','data-target':"#datetimepicker-enddate"}),
		)
	controltext1 = forms.CharField(
		max_length=50,
		required=False,
		widget=forms.TextInput(attrs={'class':'form-control'}),
		)
	controltext2 = forms.CharField(
		max_length=50,
		required=False,
		widget=forms.TextInput(attrs={'class':'form-control'}),
		)
	controlint1 = forms.IntegerField(
		required=False,
		widget=forms.NumberInput(attrs={'class':'form-control'}),
		)
	controlint2 = forms.IntegerField(
		required=False,
		widget=forms.NumberInput(attrs={'class':'form-control'}),
		)
	authority = forms.ChoiceField(
		widget=forms.Select(attrs={'class':'form-control'}),
		)
	createuser = forms.CharField(
		max_length=20,
		widget=forms.TextInput(attrs={'class':'form-control','readonly':'readonly'}),
		)
	updateuser = forms.CharField(
		max_length=20,
		widget=forms.TextInput(attrs={'class':'form-control','readonly':'readonly'}),
		)

class ResultForm(forms.Form):
	check = forms.ChoiceField(
		label='修正',
		choices=(('未','未'),('算定可','算定可'),('算定不可','算定不可'),('','全て')),
		required=False,
		widget=forms.Select(attrs={'class':'form-control'}),
		)
	seikyunengetsu = forms.CharField(
		label='請求年月',
	    max_length=8,
	    required=False,
		widget=forms.TextInput(attrs={'class':'form-control datetimepicker-input','data-target':"#datetimepicker-seikyugetsu"}),
		)
	dept = forms.MultipleChoiceField(
		label='診療科',
		required=False,
		widget=forms.SelectMultiple(attrs={'class':'form-control multiple', 'multiple':'multiple'}),
		)
	ward = forms.MultipleChoiceField(
		label='病棟',
		required=False,
		widget=forms.SelectMultiple(attrs={'class':'form-control multiple', 'multiple':'multiple'}),
		)
	in_out = forms.ChoiceField(
		label='入外',
		required=False,
		widget=forms.Select(attrs={'class':'form-control'}),
		choices=(('','全て'),('入院','入院'),('外来','外来')),
		)
	codename = forms.MultipleChoiceField(
		label='算定項目',
		required=False,
		widget=forms.SelectMultiple(attrs={'class':'form-control multiple', 'multiple':'multiple'}),
		)
	zisseki = forms.ChoiceField(
		label='算定実績',
		required=False,
		choices=(('0','無し'),('1','有')),
		widget=forms.Select(attrs={'class':'form-control'}),
		)

# 算定項目マスタ
class KenchiForm(forms.Form):
	code1 = forms.CharField(
		max_length=19,
		widget=forms.TextInput(attrs={'class':'form-control','readonly':'readonly'}),
		)
	code2 = forms.CharField(
		max_length=19,
		required=False,
		widget=forms.TextInput(attrs={'class':'form-control','readonly':'readonly'}),
		)
	name = forms.CharField(
		max_length=60,
		widget=forms.TextInput(attrs={'class':'form-control'}),
		)
	use_flag = forms.CharField(
		max_length=1,
		widget=forms.TextInput(attrs={'class':'form-control'}),
		)
	predict_proba = forms.DecimalField(
		max_digits=4,
		decimal_places=1,
		widget=forms.NumberInput(attrs={'class':'form-control'}),
		)
	points = forms.DecimalField(
		max_digits=9,
		decimal_places=3,
		widget=forms.NumberInput(attrs={'class':'form-control','readonly':'readonly'}),
		)
	rule_flag = forms.ChoiceField(
		choices=((False,'機械学習'),(True,'ルール')),
		widget=forms.Select(attrs={'class':'form-control','readonly':'readonly'}),
		)
	startdate = forms.CharField(
		max_length=10,
		widget=forms.TextInput(attrs={'class':'form-control datetimepicker-input','data-target':"#datetimepicker-startdate",'readonly':'readonly'}),
		)
	enddate = forms.CharField(
		max_length=10,
		widget=forms.TextInput(attrs={'class':'form-control datetimepicker-input','data-target':"#datetimepicker-enddate",'readonly':'readonly'}),
		)
	note = forms.CharField(
		max_length=100,
		required=False,
		widget=forms.TextInput(attrs={'class':'form-control'}),
		)
	createuser = forms.CharField(
		max_length=20,
		widget=forms.TextInput(attrs={'class':'form-control','readonly':'readonly'}),
		)
	updateuser = forms.CharField(
		max_length=20,
		widget=forms.TextInput(attrs={'class':'form-control','readonly':'readonly'}),
		)

class SystemLogSelectForm(forms.Form):
	log_date = forms.ChoiceField(
		label='日付',
		widget=forms.Select(attrs={'class':'form-control'}),
		)

class UserListSelectForm(forms.Form):
	target_date = forms.CharField(
		label='基準日',
		widget=forms.TextInput(attrs={'class':'form-control datetimepicker-input','data-target':"#datetimepicker-targetdate"}),
		)
	userid = forms.CharField(
		label='利用者ID',
		max_length = 30,
		required = False,
		widget=forms.TextInput(attrs={'class':'form-control'}),
		)
	username = forms.CharField(
		label='利用者氏名',
		max_length=30,
		required=False,
		widget=forms.TextInput(attrs={'class':'form-control'}),
		)
		
class KarteFileForm(forms.Form):
	file_code = forms.CharField(
		label='ファイルコード',
		max_length=10,
		widget=forms.TextInput(attrs={'class':'form-control'}),
		)
	file_name = forms. CharField(
		label='ファイル名',
		max_length=50,
		widget=forms.TextInput(attrs={'class':'form-control'}),
		)
	conversion_flag = forms.ChoiceField(
		label='列の変換',
		choices=((False, 'しない'), (True, 'する')),
		widget=forms.Select(attrs={'class':'form-control'}),
	)
	startdate = forms.CharField(
		label='開始日',
		max_length=10,
		widget=forms.TextInput(attrs={'class':'form-control datetimepicker-input','data-target':"#datetimepicker-startdate"}),
		)
	enddate = forms.CharField(
		label='終了日',
		max_length=10,
		widget=forms.TextInput(attrs={'class':'form-control datetimepicker-input','data-target':"#datetimepicker-enddate"}),
		)
	createuser = forms.CharField(
		label='作成者',
		max_length=20,
		widget=forms.TextInput(attrs={'class':'form-control','readonly':'readonly'}),
		)
	updateuser = forms.CharField(
		label='更新者',
		max_length=20,
		widget=forms.TextInput(attrs={'class':'form-control','readonly':'readonly'}),
		)

class KarteFileFormColumn(forms.Form):
	file_code = forms.CharField(
		label='ファイルコード',
		max_length=10,
		widget=forms.TextInput(attrs={'class':'form-control','readonly':'readonly'}),
		)
	column_name = forms.CharField(
		label='列名',
		max_length=50,
		widget=forms.TextInput(attrs={'class':'form-control'}),
		)
	before_column_name = forms. CharField(
		label='修正前の列名',
		max_length=50,
		required=False,
		widget=forms.TextInput(attrs={'class':'form-control'}),
		)
	data_type = forms.ChoiceField(
		label='データの種類',
		choices=(('テキスト','テキスト'),('日付','日付'),('時刻','時刻'),('患者ID','患者ID')),
		widget=forms.Select(attrs={'class':'form-control'}),
		)
	add_column_flag = forms.ChoiceField(
		label='列追加フラグ',
		choices=((0,'追加しない'),(1,'追加する')),
		widget=forms.Select(attrs={'class':'form-control'}),
		)
	add_column_text = forms.CharField(
		label='追加テキスト',
		max_length=50,
		required=False,
		widget=forms.TextInput(attrs={'class':'form-control'}),
		)
	startdate = forms.CharField(
		label='開始日',
		max_length=10,
		widget=forms.TextInput(attrs={'class':'form-control datetimepicker-input','data-target':"#datetimepicker-startdate"}),
		)
	enddate = forms.CharField(
		label='終了日',
		max_length=10,
		widget=forms.TextInput(attrs={'class':'form-control datetimepicker-input','data-target':"#datetimepicker-enddate"}),
		)
	createuser = forms.CharField(
		label='作成者',
		max_length=20,
		widget=forms.TextInput(attrs={'class':'form-control','readonly':'readonly'}),
		)
	updateuser = forms.CharField(
		label='更新者',
		max_length=20,
		widget=forms.TextInput(attrs={'class':'form-control','readonly':'readonly'}),
		)

class ResultColumnForm(forms.ModelForm):
	class Meta:
		model = RESULT_COLUMN
		fields = ('COLUMN_NAME', 'ORIGINAL_NAME', 'UPDATEUSER', 'CREATEUSER')
		labels = {
			'COLUMN_NAME':'列名称',
			'ORIGINAL_NAME':'オリジナル列名称',
			'UPDATEUSER':'更新者',
			'CREATEUSER':'作成者',
		}
		widgets = {
			'COLUMN_NAME': forms.TextInput(attrs={'class':'form-control'}),
			'ORIGINAL_NAME': forms.TextInput(attrs={'class':'form-control', 'readonly':'readonly'}),
			'UPDATEUSER': forms.TextInput(attrs={'class':'form-control', 'readonly':'readonly'}),
			'CREATEUSER': forms.TextInput(attrs={'class':'form-control', 'readonly':'readonly'}),
		}

class TestMulti(forms.Form):
	testcodename = forms.MultipleChoiceField(
	label='算定項目',
	widget=forms.SelectMultiple(attrs={'class':'form-control multiple', 'multiple':'multiple'}),
	)

class ChartSelect(forms.Form):
	point_count = forms.ChoiceField(
		label='点数/件数',
		required=False,
		choices=[
			('predict_counts','追加算定予測件数'),
			('predict_billing_amount','追加算定予測額'),
			('add_counts','追加算定件数'),
			('add_billing_amount','追加算定額')
		],
		initial=['predict_counts'],
		widget=forms.RadioSelect(attrs={'class':'form-check-input form-check-inline'}),
		)
	seikyunengetsu = forms.CharField(
		label='請求年月',
	    max_length=8,
	    required=False,
		widget=forms.TextInput(attrs={'class':'form-control datetimepicker-input','data-target':"#datetimepicker-seikyugetsu"}),
		)

class AddItemAllForm(forms.Form):
	startdate = forms.CharField(
		max_length=10,
		label='開始日',
		widget=forms.TextInput(attrs={'class':'form-control datetimepicker-input','data-target':"#datetimepicker-startdate"}),
		)
