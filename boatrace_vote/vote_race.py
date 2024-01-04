import io
import json
import os
import re
from datetime import datetime

import create_racelist
import joblib
import pandas as pd
import utils

#
# S3操作
#


def get_pred(s3_client, arg_pred_url):
    key_re = re.fullmatch(r"^s3://(\w+)/(.*)$", arg_pred_url)
    s3_key = key_re.group(2)

    with io.BytesIO(s3_client.get_object(s3_key)) as b:
        df = joblib.load(b)

    return df


def get_crawl_racelist(s3_client, arg_racelist_folder):
    key_re = re.fullmatch(r"^s3://(\w+)/(.*)$", arg_racelist_folder + "/df_racelist.joblib")
    s3_key = key_re.group(2)

    with io.BytesIO(s3_client.get_object(s3_key)) as b:
        df = joblib.load(b)

    return df


def get_crawl_race_before_5min(s3_client, arg_racelist_folder, arg_race_id):
    key_re = re.fullmatch(r"^s3://(\w+)/(.*)$", arg_racelist_folder + f"/race_{arg_race_id}_before_5minutes.json")
    s3_key = key_re.group(2)

    with io.BytesIO(s3_client.get_object(s3_key)) as b:
        json_data = json.load(b)

    _, _, _, _, _, _, df_race_odds, _ = utils.parse_feed_json_to_dataframe(json_data)

    return df_race_odds


def put_vote(s3_client, df_arg, arg_race_id, arg_vote_folder):
    key = f"{arg_vote_folder}/vote_{arg_race_id}.joblib"

    with io.BytesIO() as b:
        joblib.dump(df_arg, b, compress=True)

        s3_client.put_object(key, b.getvalue())


#
# レース関連
#

def merge_pred_and_odds(df_arg_pred, df_arg_odds, arg_race_id):
    df_vote = pd.merge(
        df_arg_pred, df_arg_odds,
        on=["race_id", "bet_type", "bracket_number_1", "bracket_number_2", "bracket_number_3"],
        how="left",
    )

    df_vote = df_vote[df_vote["race_id"] == arg_race_id]

    return df_vote


def vote__expected_return(df_arg_vote):
    """対象レースに期待値投票(均等払い戻し)(仮)する。
    """

    pred_threshold = float(os.environ["PRED_THRESHOLD"])
    expected_return_threshold = float(os.environ["EXPECTED_RETURN_THRESHOLD"])
    odds_rank_threshold = int(os.environ["ODDS_RANK_THRESHOLD"])

    df_vote = df_arg_vote.copy()

    # オッズ順、期待値を算出する
    df_vote["odds__rank"] = df_vote["odds_1"].rank(method="dense")
    df_vote["expected_return"] = df_vote["pred_ticket"] * df_vote["odds_1"]

    # 投票対象の舟券を抽出する
    df_vote = df_vote.query(f"pred_ticket>={pred_threshold} and expected_return>={expected_return_threshold} and odds__rank>{odds_rank_threshold}")

    # 舟券に投票する
    df_vote["vote_amount"] = 1

    return df_vote


def upload_vote(s3_client, df_arg_racelist, df_arg_vote, arg_race_id, arg_vote_folder):
    create_racelist.put_racelist(s3_client, df_arg_racelist, arg_vote_folder)

    if len(df_arg_vote) > 0:
        put_vote(s3_client, df_arg_vote, arg_race_id, arg_vote_folder)


def vote_race(s3_vote_folder, s3_pred_folder):
    """投票アクションのメイン処理。
    """

    L = utils.get_logger("vote_race")
    s3_client = utils.S3Storage()

    #
    L.info("# レース一覧データを取得する")
    #

    df_racelist = create_racelist.get_racelist(s3_client, s3_vote_folder)
    L.debug(df_racelist)

    #
    L.info("# 舟券予測データを取得する")
    #

    df_pred_ticket = get_pred(s3_client, s3_pred_url)
    L.debug(df_pred_ticket)

    while True:
        #
        L.info("# 全レースの投票が終了した場合、ループを抜ける")
        #

        df_not_voted_racelist = df_racelist[df_racelist["vote_timestamp"].isnull()]

        if len(df_not_voted_racelist) == 0:
            L.debug("全レースに投票したので、ループを終了する")
            break

        L.debug(df_not_voted_racelist)

        #
        L.info("# クロール一覧データを取得する")
        #

        df_crawl_racelist = get_crawl_racelist(s3_client, s3_racelist_folder)
        L.debug(df_crawl_racelist)

        #
        L.info("# 5分前データがあり、未投票のレースを抽出する")
        #

        target_race_id = df_not_voted_racelist["race_id"].values[0]
        L.debug(f"target_race_id={target_race_id}")

        df_crawl_target_race = df_crawl_racelist.query(f"race_id=='{target_race_id}' and diff_minutes==10 and not crawl_datetime.isnull()")
        L.debug(f"df_crawl_target_race={df_crawl_target_race.to_dict(orient='records')}")

        if len(df_crawl_target_race) == 0:
            L.debug("対象レースのクロールデータがまだ存在しない")
            time.sleep(1)
            continue

        #
        L.info("# 対象レースの5分前クロールデータを取得する")
        #

        df_race_odds = get_crawl_race_before_5min(s3_client, s3_racelist_folder, target_race_id)
        L.debug(df_race_odds)

        #
        L.info("# 対象レースの舟券予測データと5分前クロールデータを結合する")
        #

        df_vote = merge_pred_and_odds(df_pred_ticket, df_race_odds, target_race_id)
        L.debug(df_vote)

        #
        L.info("# 投票する")
        #

        df_vote = vote__expected_return(df_vote)
        L.debug(df_vote)

        df_racelist.loc[df_racelist["race_id"] == target_race_id, "vote_timestamp"] = datetime.now()

        #
        L.info("# 投票結果データと更新後レース一覧データをアップロードする")
        #

        upload_vote(s3_client, df_racelist, df_vote, target_race_id, s3_vote_folder)


if __name__ == "__main__":
    L = utils.get_logger("main")

    #
    # 設定を取得する
    #
    s3_vote_folder = os.environ["AWS_S3_VOTE_FOLDER"]
    L.info(f"S3投票データフォルダ: {s3_vote_folder}")

    s3_pred_url = os.environ["AWS_S3_PRED_URL"]
    L.info(f"S3予測データ: {s3_pred_url}")

    s3_racelist_folder = os.environ["AWS_S3_RACELIST_FOLDER"]
    L.info(f"S3レース一覧フォルダ: {s3_racelist_folder}")

    #
    # 投票アクションのメイン処理
    #
    vote_race(s3_vote_folder, s3_pred_url)
