import pandas as pd
import cx_Oracle
import os
import configparser
from pprint import pprint

def get_data(username, password, host, port, service_name, table_name):
    dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
    connection = cx_Oracle.connect(username, password, dsn)
    query = f"SELECT * FROM {table_name}"
    data = pd.read_sql(query, connection)
    connection.close()
    return data


def compare_tables(internal_config, external_config, table_name, primary_key_columns):
    # 内部版データベース接続情報
    internal_username = internal_config["ID"]
    internal_password = internal_config["PW"]
    internal_host = internal_config["HOST"]
    internal_port = internal_config["PORT"]
    internal_service_name = internal_config["SERVICE_NAME"]

    # 外部版データベース接続情報
    external_username = external_config["ID"]
    external_password = external_config["PW"]
    external_host = external_config["HOST"]
    external_port = external_config["PORT"]
    external_service_name = external_config["SERVICE_NAME"]

    # 社内版データの取得
    internal_data = get_data(internal_username, internal_password, internal_host, internal_port, internal_service_name,
                             table_name)
    # 社外版データの取得
    external_data = get_data(external_username, external_password, external_host, external_port,
                             external_service_name, table_name)

    internal_col_list = list(internal_data.columns)
    external_col_list = list(external_data.columns)
    assert internal_col_list == external_col_list, "2つのデータのカラムが異なります"
    col_list = internal_col_list

    internal_data["source"] = "internal"
    external_data["source"] = "external"
    internal_data["key"] = [tuple(row) for row in internal_data[primary_key_columns].itertuples(index=False)]
    external_data["key"] = [tuple(row) for row in external_data[primary_key_columns].itertuples(index=False)]

    add_key = list(set(internal_data["key"]) - set(external_data["key"]))
    del_key = list(set(external_data["key"]) - set(internal_data["key"]))

    # 追加データ
    add_data = internal_data[internal_data["key"].isin(add_key)]
    # 削除データ
    del_data = external_data[external_data["key"].isin(del_key)]
    # 更新データ
    update_data = pd.concat([internal_data, external_data, add_data, del_data]).drop_duplicates(subset=col_list,
                                                                                              keep=False)

    diff_data = {
        table_name: {
            "ADD": add_data[col_list].to_dict(orient="records"),
            "DEL": del_data[col_list].to_dict(orient="records"),
            "UPDATE": update_data[col_list].to_dict(orient="records")
        }
    }

    return diff_data


# 設定ファイルから接続情報を読み込む
config = configparser.ConfigParser()
ini_file_path = os.path.join(r"./config.ini")
if os.path.isfile(ini_file_path):
    config.read(ini_file_path)
else:
    raise FileNotFoundError(f"iniファイルが存在しません")

target = [
    {"table_name": "MACHINESN", "primary_key": ["MACHINESN", "PLANTID"]},
    # 他のテーブルに関する情報も追加できます
]

diff_data = {}

for table_info in target:
    table_name = table_info["table_name"]
    primary_key_columns = table_info["primary_key"]

    # 社内版データベースと社外版データベースの接続情報を取得
    internal_config = config["INTERNAL"]
    external_config = config["EXTERNAL"]

    # テーブルのデータを比較
    table_diff = compare_tables(internal_config, external_config, table_name, primary_key_columns)

    diff_data.update(table_diff)

# 結果の出力
pprint(diff_data)
