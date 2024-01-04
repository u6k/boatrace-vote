import io
import os
from pathlib import Path

import create_racelist
import joblib
import pandas as pd
import utils


def get_vote(s3_client, arg_vote_folder):
    """
    投票データを取得して、統合する
    """
    L = utils.get_logger("get_vote")
    L.info(f"start: arg_vote_folder={arg_vote_folder}")

    # 投票データのキーを取得する
    vote_paths = [obj.key for obj in s3_client.list_objects(arg_vote_folder + "/vote_")]
    L.debug(vote_paths)

    # 投票データを取得する
    list_df_vote = []
    for vote_path in vote_paths:
        L.debug(f"downloading: {vote_path}")

        with io.BytesIO(s3_client.get_object(vote_path)) as b:
            df_tmp = joblib.load(b)
            list_df_vote.append(df_tmp)

    # 投票データを統合する
    df_vote = pd.concat(list_df_vote).sort_values("start_datetime")
    L.debug(df_vote)

    return df_vote


def get_payoff(s3_client, arg_feed_url):
    """
    払戻しデータを取得する
    """
    L = utils.get_logger("get_payoff")
    L.info(f"start: arg_feed_url={arg_feed_url}")

    # フィードデータを取得する
    json_data = create_racelist.get_feed(s3_client, arg_feed_url)

    # フィードを変換する
    _, _, _, _, _, df_race_payoff, _, _ = utils.parse_feed_json_to_dataframe(json_data)
    L.debug(df_race_payoff)

    return df_race_payoff


def merge_vote_and_payoff(df_arg_vote, df_arg_payoff):
    """
    投票データと払戻しデータを結合する
    """
    L = utils.get_logger("merge_vote_and_payoff")
    L.info("start")

    # 結合する
    df_vote_result = pd.merge(
        df_arg_vote, df_arg_payoff,
        on=["race_id", "bracket_number_1", "bracket_number_2", "bracket_number_3", "bet_type"],
        how="left",
    )

    # 払戻し量を算出する
    df_vote_result["payoff_amount"] = df_vote_result["vote_amount"] * df_vote_result["payoff"]

    L.debug(df_vote_result)

    return df_vote_result


def evaluate_vote(df_arg_vote_result):
    """
    投票結果を評価する
    """
    L = utils.get_logger("evaluate_vote")
    L.info("start")

    # 評価する
    vote_summary = {
        "投票数": len(df_arg_vote_result.query("vote_amount>0")),
        "的中数": len(df_arg_vote_result.query("payoff_amount>0")),
        "費用": df_arg_vote_result["vote_amount"].sum(),
        "収益": df_arg_vote_result["payoff_amount"].sum(),
    }

    vote_summary["利益"] = vote_summary["収益"] - vote_summary["費用"]

    L.debug(vote_summary)

    return vote_summary


def put_vote_result(s3_client, df_arg_vote_result, arg_output_folder, arg_vote_folder):
    """
    投票結果をアップロードする
    """
    L = utils.get_logger("put_vote_result")
    L.info(f"start: arg_output_folder={arg_output_folder}, arg_vote_folder={arg_vote_folder}")

    # csv
    vote_result_path = Path(arg_output_folder) / "df_vote_result.csv"
    df_arg_vote_result.to_csv(vote_result_path)

    vote_result_key = arg_vote_folder + "/df_vote_result.csv"

    s3_client.upload_file(vote_result_path, vote_result_key)

    L.debug(f"uploaded: {vote_result_key}")

    # joblib
    vote_result_path = Path(arg_output_folder) / "df_vote_result.joblib"
    joblib.dump(df_arg_vote_result, vote_result_path, compress=True)

    vote_result_key = arg_vote_folder + "/df_vote_result.joblib"

    s3_client.upload_file(vote_result_path, vote_result_key)

    L.debug(f"uploaded: {vote_result_key}")


def vote_result(arg_output_folder, arg_vote_folder, arg_feed_url):
    """
    投票結果を集計する。
    """

    L = utils.get_logger("vote_result")
    L.info(f"arg_output_folder={arg_output_folder}, arg_vote_folder={arg_vote_folder}, arg_feed_url={arg_feed_url}")

    s3_client = utils.S3Storage()

    # 投票データを取得して、統合する
    df_vote = get_vote(s3_client, arg_vote_folder)

    # 払戻しデータを取得する
    df_race_payoff = get_payoff(s3_client, arg_feed_url)

    # 投票データと払戻しデータを結合する
    df_vote_result = merge_vote_and_payoff(df_vote, df_race_payoff)

    # 投票結果を評価する
    evaluate_vote(df_vote_result)

    # 投票結果をアップロードする
    put_vote_result(s3_client, df_vote_result, arg_output_folder, arg_vote_folder)


if __name__ == "__main__":
    L = utils.get_logger("main")

    #
    # 設定を取得する
    #

    s3_vote_folder = os.environ["AWS_S3_VOTE_FOLDER"]
    L.info(f"S3投票データフォルダ: {s3_vote_folder}")

    s3_feed_url = os.environ["AWS_S3_FEED_URL"]
    L.info(f"S3フィードURL: {s3_feed_url}")

    output_folder = os.environ["OUTPUT_DIR"]
    L.info(f"出力フォルダ: {output_folder}")

    #
    # 投票結果集計のメイン処理
    #

    vote_result(output_folder, s3_vote_folder, s3_feed_url)
