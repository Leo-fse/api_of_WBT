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

# カーソルの作成
cursor = connection.cursor()

# MachineSNテーブルのデータを取得
cursor.execute("SELECT * FROM MachineSN@社内版")
machine_sn_data_internal = cursor.fetchall()

cursor.execute("SELECT * FROM MachineSN@社外版")
machine_sn_data_external = cursor.fetchall()

# プライマリキーのカラム名
primary_key_columns = ['MachineSN', 'PlantID']

# カラム名の取得
columns_internal = [column[0] for column in cursor.description]

# 新規追加データの検出
for row_external in machine_sn_data_external:
    primary_key_external = tuple(row_external[columns_internal.index(column)] for column in primary_key_columns)

    if primary_key_external not in [tuple(row_internal[columns_internal.index(column)] for column in primary_key_columns) for row_internal in machine_sn_data_internal]:
        print(f"新規追加: MachineSN={primary_key_external[0]}, PlantID={primary_key_external[1]}")

# 更新データの検出
for row_external in machine_sn_data_external:
    primary_key_external = tuple(row_external[columns_internal.index(column)] for column in primary_key_columns)
    external_columns = tuple(row_external[i] for i, column in enumerate(columns_internal) if column not in primary_key_columns)

    internal_rows = [row_internal for row_internal in machine_sn_data_internal if tuple(row_internal[columns_internal.index(column)] for column in primary_key_columns) == primary_key_external]

    if internal_rows:
        row_internal = internal_rows[0]
        internal_columns = tuple(row_internal[i] for i, column in enumerate(columns_internal) if column not in primary_key_columns)

        if external_columns != internal_columns:
            print(f"更新データ: MachineSN={primary_key_external[0]}, PlantID={primary_key_external[1]}")

# 接続のクローズ
cursor.close()
connection.close()
