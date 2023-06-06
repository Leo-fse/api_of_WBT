import cx_Oracle

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

# プライマリキーのカラム名
primary_key_columns = ['MachineSN', 'PlantID']

# データの取得と比較
def compare_data(internal_data, external_data, table_name):
    primary_key_indices = [internal_data[0].index(column) for column in primary_key_columns]
    for external_row in external_data:
        primary_key_external = tuple(external_row[i] for i in primary_key_indices)
        if primary_key_external not in [tuple(internal_row[i] for i in primary_key_indices) for internal_row in internal_data]:
            print(f"新規追加: {table_name} - {primary_key_external}")
        else:
            internal_row = next(internal_row for internal_row in internal_data if tuple(internal_row[i] for i in primary_key_indices) == primary_key_external)
            if external_row != internal_row:
                print(f"更新データ: {table_name} - {primary_key_external}")

# データベースへの接続とデータの取得
def get_data(username, password, host, port, service_name, table_name):
    dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
    connection = cx_Oracle.connect(username, password, dsn)
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {table_name}")
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
    connection.close()
    return columns, data

# データの比較と差分の抽出
def compare_tables(internal_data, external_data, table_name):
    internal_columns, internal_rows = internal_data
    external_columns, external_rows = external_data
    compare_data(internal_rows, external_rows, table_name)

# 各テーブルのデータを比較
table_name = 'MachineSN'

# 社内版データの取得
internal_data = get_data(internal_username, internal_password, internal_host, internal_port, internal_service_name, table_name)

# 社外版データの取得
external_data = get_data(external_username, external_password, external_host, external_port, external_service_name, table_name)

# データの比較と差分の抽出
compare_tables(internal_data, external_data, table_name)
