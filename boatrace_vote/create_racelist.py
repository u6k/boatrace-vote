import io
import json
import os
import re
from datetime import datetime, timedelta

import joblib
import utils

#
# S3操作
#


def get_feed(s3_client, feed_url):
    key_re = re.fullmatch(r"^s3://(\w+)/(.*)$", feed_url)
    s3_key = key_re.group(2)

    with io.BytesIO(s3_client.get_object(s3_key)) as b:
        json_data = json.load(b)

    return json_data


def put_racelist(s3_client, df_arg, arg_vote_folder):
    key_base = f"{arg_vote_folder}/df_racelist"

    with io.BytesIO() as b:
        df_arg.to_csv(b)

        s3_client.put_object(key_base + ".csv", b.getvalue())

    with io.BytesIO() as b:
        joblib.dump(df_arg, b, compress=True)

        s3_client.put_object(key_base + ".joblib", b.getvalue())

    return key_base + ".joblib"


def get_racelist(s3_client, arg_vote_folder):
    key = f"{arg_vote_folder}/df_racelist.joblib"

    with io.BytesIO(s3_client.get_object(key)) as b:
        df = joblib.load(b)

    return df


#
# レースデータ
#


def extract_racelist(df_race_info, target_date):
    start_datetime = target_date
    end_datetime = start_datetime + timedelta(days=1)

    df = df_race_info.query(f"'{start_datetime}'<=start_datetime<'{end_datetime}'").sort_values("start_datetime").reset_index(drop=True)

    df = df[[
        "race_id",
        "place_id",
        "race_round",
        "start_datetime",
    ]]

    df["vote_timestamp"] = None

    df = df.astype({
        "vote_timestamp": "datetime64[ns]",
    })

    return df


def create_racelist(target_date, s3_feed_url, s3_vote_folder):
    L = utils.get_logger("create_racelist")

    #
    L.info("# フィードをダウンロード")
    #

    s3_client = utils.S3Storage()
    json_data = get_feed(s3_client, s3_feed_url)

    L.debug(s3_feed_url)

    #
    L.info("# フィードを変換")
    #

    _, _, df_race_info, _, _, _, _, _ = utils.parse_feed_json_to_dataframe(json_data)

    L.debug(df_race_info)

    #
    L.info("# レース一覧を抽出")
    #

    df_racelist = extract_racelist(df_race_info, target_date)

    L.debug(df_racelist)

    #
    L.info("# レース一覧をアップロード")
    #

    racelist_key = put_racelist(s3_client, df_racelist, s3_vote_folder)

    L.debug(racelist_key)


#
# メイン処理
#
if __name__ == "__main__":
    L = utils.get_logger("main")

    target_date = datetime.strptime(os.environ["VOTE_TARGET_DATE"], "%Y-%m-%d")
    L.info(f"対象日: {target_date}")

    s3_feed_url = os.environ["AWS_S3_FEED_URL"]
    L.info(f"S3フィードURL: {s3_feed_url}")

    s3_vote_folder = os.environ["AWS_S3_VOTE_FOLDER"]
    L.info(f"S3投票データフォルダ: {s3_vote_folder}")

    create_racelist(target_date, s3_feed_url, s3_vote_folder)
