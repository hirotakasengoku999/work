import update_chart

max_seikyunengtsu = update_chart.get_max_seikyunengetsu()

table_name = 'aireceiptapp_backup_kenchikekka'
update_chart.write_add_calc(table_name, max_seikyunengtsu)