import pandas as pd
import cx_Oracle
from pprint import pprint
import os
import configparser


config = configparser.ConfigParser()
ini_file_path = os.path.join(r"./config.ini")
if os.path.isfile(ini_file_path):
    config.read(ini_file_path)
else:
    raise FileNotFoundError(f"iniファイルが存在しません")

# 社内版データベース接続情報
internal_username = config["INTERNAL"]["ID"]
internal_password = config["INTERNAL"]["PW"]
internal_host = config["INTERNAL"]["HOST"]
internal_port = config["INTERNAL"]["PORT"]
internal_service_name = config["INTERNAL"]["SERVICE_NAME"]

# 社外版データベース接続情報
external_username = config["EXTERNAL"]["ID"]
external_password = config["EXTERNAL"]["PW"]
external_host = config["EXTERNAL"]["HOST"]
external_port = config["EXTERNAL"]["PORT"]
external_service_name = config["EXTERNAL"]["SERVICE_NAME"]

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
internal_data = get_data(internal_username, internal_password, internal_host, internal_port, internal_service_name, table_name)
# 社外版データの取得
external_data = get_data(external_username, external_password, external_host, external_port, external_service_name, table_name)

internal_col_list = list(internal_data.columns) 
external_col_list = list(external_data.columns) 
assert internal_col_list == external_col_list, "2つのデータのカラムが異なります"
col_list = internal_col_list

internal_data["source"] = "internal"
external_data["source"] = "external"
internal_data["key"] = [(m, p) for m, p in zip(internal_data["MACHINESN"], internal_data["PLANTID"])]
external_data["key"] = [(m, p) for m, p in zip(external_data["MACHINESN"], external_data["PLANTID"])]

# プライマリキーのカラム名
primary_key_columns = ['MACHINESN', 'PLANTID']

add_key = list(set(internal_data["key"]) - set(external_data["key"]))
del_key = list(set(external_data["key"]) - set(internal_data["key"]))

# 追加データ
add_data = internal_data[internal_data["key"].isin(add_key)]
# 削除データ
del_data = external_data[external_data["key"].isin(add_key)]
# 更新データ
update_data = pd.concat([internal_data, external_data, add_data, del_data]).drop_duplicates(subset=col_list, keep=False)

diff_data = {table_name: {"ADD": add_data[col_list].to_dict(orient="records"), "DEL": del_data[col_list].to_dict(orient="records"), "UPDATE": update_data[col_list].to_dict(orient="records")}}
