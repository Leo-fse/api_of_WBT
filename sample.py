import cx_Oracle

# データベース接続情報
oracle_username = 'your_username'
oracle_password = 'your_password'
oracle_host = 'your_host'
oracle_port = 'your_port'
oracle_service_name = 'your_service_name'

# 接続文字列の作成
dsn = cx_Oracle.makedsn(oracle_host, oracle_port, service_name=oracle_service_name)
connection = cx_Oracle.connect(oracle_username, oracle_password, dsn)

# プライマリキーのカラム名
primary_key_columns = ['MachineSN', 'PlantID']

# データを取得する関数
def get_table_data(connection, schema, table_name):
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {schema}.{table_name}")
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
    return columns, data

# データの比較と差分の抽出
def compare_data(internal_data, external_data, table_name):
    primary_key_indices = [internal_data[0].index(column) for column in primary_key_columns]
    for external_row in external_data[1]:
        primary_key_external = tuple(external_row[i] for i in primary_key_indices)
        if primary_key_external not in [tuple(internal_row[i] for i in primary_key_indices) for internal_row in internal_data[1]]:
            print(f"新規追加: {table_name} - {primary_key_external}")
        else:
            internal_row = next(internal_row for internal_row in internal_data[1] if tuple(internal_row[i] for i in primary_key_indices) == primary_key_external)
            if external_row != internal_row:
                print(f"更新データ: {table_name} - {primary_key_external}")

# データの取得と比較
internal_schema = 'your_internal_schema'
external_schema = 'your_external_schema'
table_name = 'MachineSN'

# 社内版データの取得
internal_columns, internal_data = get_table_data(connection, internal_schema, table_name)

# 社外版データの取得
external_columns, external_data = get_table_data(connection, external_schema, table_name)

# データの比較と差分の抽出
compare_data((internal_columns, internal_data), (external_columns, external_data), table_name)

# 接続のクローズ
connection.close()
