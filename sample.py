import pandas as pd
import cx_Oracle

# プライマリキーのカラム名
primary_key_columns = ['MachineSN', 'PlantID']

# 社内版データベース接続情報
internal_username = 'your_internal_username'
internal_password = 'your_internal_password'
internal_host = 'your_internal_host'
internal_port = 'your_internal_port'
internal_service_name = 'your_internal_service_name'

# 社外版データベース接続情報
external_username = 'your_external_username'
external_password = 'your_external_password'
external_host = 'your_external_host'
external_port = 'your_external_port'
external_service_name = 'your_external_service_name'

# データベースへの接続とデータの読み込み
def get_data(username, password, host, port, service_name, table_name):
    dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
    connection = cx_Oracle.connect(username, password, dsn)
    query = f"SELECT * FROM {table_name}"
    data = pd.read_sql(query, connection)
    connection.close()
    return data

# 各テーブルのデータを比較
table_name = 'MachineSN'

# 社内版データの取得
internal_connection = get_data(internal_username, internal_password, internal_host, internal_port, internal_service_name, table_name)

# 社外版データの取得
external_connection = get_data(external_username, external_password, external_host, external_port, external_service_name, table_name)

# データのマージと差分の抽出
merged_data = pd.merge(internal_connection, external_connection, on=primary_key_columns, how='outer', suffixes=('_internal', '_external'))
new_data = merged_data[merged_data['_external'].isnull()]
updated_data = merged_data[merged_data['_internal'] != merged_data['_external']]

# 新規追加データの表示
if len(new_data) > 0:
    print("新規追加データ:")
    print(new_data[primary_key_columns])

# 更新データの表示
if len(updated_data) > 0:
    print("更新データ:")
    print(updated_data[primary_key_columns])
