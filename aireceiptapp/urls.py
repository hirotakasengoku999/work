from django.urls import path
from .views import loginfunc, logoutfunc, predict, MaintenanceIndex, userlog, \
    SystemLog, createmodel, run_create_model, run_predict, hanyoview, \
    hanyo_leaf, detail_hanyo, delete_hanyo, add_hanyo, edit_hanyo, indexview, \
    test_paginator, update_result, maintenance_item, detail_item, edit_item, \
    maintenance_item, create_item, delete_item, user_list, user_detail, \
    user_edit, user_add, user_delete, karte_file, add_karte_file, \
    detail_karte_file, edit_karte_file, delete_karte_file, update_result,\
    detail_karte_file_column, add_karte_file_column, edit_karte_file_column, \
    delete_karte_file_column, result_column_list, edit_result_column, \
    chart, line_chart, request_amount, inspection, add_allitem, inspection_index, \
    word_search

urlpatterns = [
    path('', loginfunc, name='login'), # ポータル画面へのパス
    path('logout/',logoutfunc,name='logout'), # ﾛｸﾞｱｳﾄ画
    path('indexview/',indexview,name='indexview'), # インデックス画面
    path('predict/', predict, name='predict'), # 検知画面
    path('maintenanceIndex/', MaintenanceIndex, name='maintenanceIndex'), # システム管理画面へのパス
    path('userlog/', userlog, name='userlog'), # ログ照会へのパス
    path('SystemLog/',SystemLog, name='SystemLog'), #システムログ照会へのパス 
    path('createmodel/',createmodel,name='createmodel'), # モデル作成画面
    path('run_create_model/',run_create_model,name='run_create_model'), # モデル作成実行
    path('run_predict/',run_predict,name='run_predict'), # 検知実行
    path('hanyoview/',hanyoview,name='hanyoview'), # 汎用画面
    path('detail_hanyo/<int:num>', detail_hanyo, name='detail_hanyo'), # 汎用ヘッダー項目詳細
    path('hanyo_leaf/<str:header_code>',hanyo_leaf,name='hanyo_leaf'), # ヘッダー項目編集
    path('delete_hanyo/<int:num>',delete_hanyo,name='delete_hanyo'), # ヘッダー項目削除
    path('add_hanyo/<str:header_code>',add_hanyo,name='add_hanyo'), # 汎用項目詳細追加
    path('edit_hanyo/<int:num>',edit_hanyo,name='edit_hanyo'),
    path('test_paginator/',test_paginator,name='test_paginator'),
    path('update_result/', update_result, name='update_result'), 
    path('maintenance_item/', maintenance_item, name='maintenance_item'), # 算定項目マスタメンテナンス
    path('detail_item/<int:num>', detail_item, name='detail_item'), #算定項目マスタメンテナンス詳細
    path('edit_item/<int:num>', edit_item, name='edit_item'), #算定項目マスタメンテナンス編集
    path('create_item', create_item,name='create_item'), # 算定項目追加
    path('delete_item/<int:num>',delete_item,name='delete_item'), # ヘッダー項目削除
    path('user_list/', user_list, name='user_list'), # 利用者一覧
    path('user_detail/<int:num>', user_detail, name='user_detail'), # 利用者詳細
    path('user_edit/<int:num>', user_edit, name='user_edit'), # 利用者編集
    path('user_add/', user_add, name='user_add'), # 利用者追加
    path('user_delete/<int:num>', user_delete, name='user_delete'), # 利用者削除
    path('karte_file/<str:table_name>', karte_file, name='karte_file'), # カルテファイル設定
    path('add_karte_file/', add_karte_file, name='add_karte_file'),
    path('detail_karte_file/<str:file_code>', detail_karte_file, name='detail_karte_file'),
    path('edit_karte_file/<str:file_code>', edit_karte_file, name='edit_karte_file'),
    path('delete_karte_file/<str:file_code>', delete_karte_file, name='delete_karte_file'),
    path('add_karte_file_column/<str:file_code>', add_karte_file_column,
         name='add_karte_file_column'),
    path('detail_karte_file_column/<int:num>', detail_karte_file_column,
         name='detail_karte_file_column'),
    path('edit_karte_file_column/<int:num>', edit_karte_file_column,
         name='edit_karte_file_column'),
    path('delete_karte_file_column/<int:num>', delete_karte_file_column,
         name='delete_karte_file_column'),
    path('result_column_list/', result_column_list, name='result_column_list'),
    path('edit_result_column/<int:num>', edit_result_column, name='edit_result_column'),
    path('chart/<str:chart_category>', chart, name='chart'),
    path('line_chart/<str:chart_category>', line_chart, name='line_chart'),
    path('request_amount/', request_amount, name='request_amount'),
    path('inspection/', inspection, name='inspection'),
    path('add_allitem/', add_allitem, name='add_allitem'),
    path('inspection_index/', inspection_index, name='inspection_index'),
    path('word_search/', word_search, name='word_search'),
    path('test_paginator/update_result/', update_result, name='update_result_new')
]
