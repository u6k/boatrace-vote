import os
from datetime import datetime

import pandas as pd
import utils

L = utils.get_logger("payoff_race")


def payoff_race(df_arg_racelist, df_arg_race, df_arg_vote, df_arg_odds, df_arg_payoff):
    """対象レースを清算(仮)する。
    """

    df_result = df_arg_vote.copy()

    #
    # 確定オッズ、払戻データを結合する
    #
    if len(df_result.query("bet_type==1")) > 0:
        # 単勝オッズ
        df_odds_t = df_arg_odds.query("bet_type==1")[[
            "race_id",
            "bracket_number_1",
            "odds_1",
        ]].rename(columns={
            "bracket_number_1": "bracket_number",
            "odds_1": "odds_fix",
        })

        df_result = pd.merge(
            df_result, df_odds_t,
            on=["race_id", "bracket_number"], how="left",
        )

        # 単勝払戻
        df_payoff_t = df_arg_payoff.query("bet_type==1")[[
            "race_id",
            "bracket_number_1",
            "payoff",
        ]].rename(columns={
            "bracket_number_1": "bracket_number",
        })

        df_result = pd.merge(
            df_result, df_payoff_t,
            on=["race_id", "bracket_number"], how="left",
        )

    elif len(df_result.query("bet_type==2")) > 0:
        # 複勝オッズ
        df_odds_f = df_arg_odds.query("bet_type==2")[[
            "race_id",
            "bracket_number_1",
            "odds_1",
            "odds_2",
        ]].rename(columns={
            "bracket_number_1": "bracket_number",
            "odds_1": "odds_fix_1",
            "odds_2": "odds_fix_2",
        })

        df_result = pd.merge(
            df_result, df_odds_f,
            on=["race_id", "bracket_number"], how="left",
        )

        # 複勝払戻
        df_payoff_f = df_arg_payoff.query("bet_type==2")[[
            "race_id",
            "bracket_number_1",
            "payoff",
        ]].rename(columns={
            "bracket_number_1": "bracket_number",
        })

        df_result = pd.merge(
            df_result, df_payoff_f,
            on=["race_id", "bracket_number"], how="left",
        )

    elif len(df_result.query("bet_type==3")) > 0:
        # 拡張複オッズ
        df_odds_k = df_arg_odds.query("bet_type==3")[[
            "race_id",
            "bracket_number_1",
            "bracket_number_2",
            "odds_1",
            "odds_2",
        ]].rename(columns={
            "odds_1": "odds_fix_1",
            "odds_2": "odds_fix_2",
        })

        df_result = pd.merge(
            df_result, df_odds_k,
            on=["race_id", "bracket_number_1", "bracket_number_2"], how="left",
        )

        # 拡張複払戻
        df_payoff_k = df_arg_payoff.query("bet_type==3")[[
            "race_id",
            "bracket_number_1",
            "bracket_number_2",
            "payoff",
        ]]

        df_result = pd.merge(
            df_result, df_payoff_k,
            on=["race_id", "bracket_number_1", "bracket_number_2"], how="left",
        )

    elif len(df_result.query("bet_type==4")) > 0:
        # 2連単オッズ
        df_odds_2t = df_arg_odds.query("bet_type==4")[[
            "race_id",
            "bracket_number_1",
            "bracket_number_2",
            "odds_1",
        ]].rename(columns={
            "odds_1": "odds_fix",
        })

        df_result = pd.merge(
            df_result, df_odds_2t,
            on=["race_id", "bracket_number_1", "bracket_number_2"], how="left",
        )

        # 2連単払戻
        df_payoff_2t = df_arg_payoff.query("bet_type==4")[[
            "race_id",
            "bracket_number_1",
            "bracket_number_2",
            "payoff",
        ]]

        df_result = pd.merge(
            df_result, df_payoff_2t,
            on=["race_id", "bracket_number_1", "bracket_number_2"], how="left",
        )

    elif len(df_result.query("bet_type==5")) > 0:
        # 2連複オッズ
        df_odds_2f = df_arg_odds.query("bet_type==5")[[
            "race_id",
            "bracket_number_1",
            "bracket_number_2",
            "odds_1",
        ]].rename(columns={
            "odds_1": "odds_fix",
        })

        df_result = pd.merge(
            df_result, df_odds_2f,
            on=["race_id", "bracket_number_1", "bracket_number_2"], how="left",
        )

        # 2連複払戻
        df_payoff_2f = df_arg_payoff.query("bet_type==5")[[
            "race_id",
            "bracket_number_1",
            "bracket_number_2",
            "payoff",
        ]]

        df_result = pd.merge(
            df_result, df_payoff_2f,
            on=["race_id", "bracket_number_1", "bracket_number_2"], how="left",
        )

    elif len(df_result.query("bet_type==6")) > 0:
        # 3連単オッズ
        df_odds_3t = df_arg_odds.query("bet_type==6")[[
            "race_id",
            "bracket_number_1",
            "bracket_number_2",
            "bracket_number_3",
            "odds_1",
        ]].rename(columns={
            "odds_1": "odds_fix",
        })

        df_result = pd.merge(
            df_result, df_odds_3t,
            on=["race_id", "bracket_number_1", "bracket_number_2", "bracket_number_3"], how="left",
        )

        # 3連単払戻
        df_payoff_3t = df_arg_payoff.query("bet_type==6")[[
            "race_id",
            "bracket_number_1",
            "bracket_number_2",
            "bracket_number_3",
            "payoff",
        ]]

        df_result = pd.merge(
            df_result, df_payoff_3t,
            on=["race_id", "bracket_number_1", "bracket_number_2", "bracket_number_3"], how="left",
        )

    elif len(df_result.query("bet_type==7")) > 0:
        # 3連複オッズ
        df_odds_3f = df_arg_odds.query("bet_type==7")[[
            "race_id",
            "bracket_number_1",
            "bracket_number_2",
            "bracket_number_3",
            "odds_1",
        ]].rename(columns={
            "odds_1": "odds_fix",
        })

        df_result = pd.merge(
            df_result, df_odds_3f,
            on=["race_id", "bracket_number_1", "bracket_number_2", "bracket_number_3"], how="left",
        )

        # 3連複払戻
        df_payoff_3f = df_arg_payoff.query("bet_type==7")[[
            "race_id",
            "bracket_number_1",
            "bracket_number_2",
            "bracket_number_3",
            "payoff",
        ]]

        df_result = pd.merge(
            df_result, df_payoff_3f,
            on=["race_id", "bracket_number_1", "bracket_number_2", "bracket_number_3"], how="left",
        )

    if len(df_result) > 0:
        df_result["payoff"] = df_result["payoff"].fillna(0.0)
    else:
        df_result["payoff"] = 0.0

    #
    # 清算し、レース一覧に記録する
    #
    df_result["payoff_amount"] = df_result["vote_amount"] * df_result["payoff"]

    df_arg_racelist.at[df_arg_race.index[0], "payoff_timestamp"] = datetime.now()
    df_arg_racelist.at[df_arg_race.index[0], "payoff_amount"] = df_result["payoff_amount"].sum()

    return df_result, df_arg_racelist


def main(s3_feed_folder, s3_vote_folder):
    """清算アクションのメイン処理。
    """

    #
    # レース一覧データを取得する
    #
    L.info("# レース一覧データを取得する")

    df_racelist = utils.get_racelist(s3_vote_folder)

    L.debug(df_racelist)

    #
    # 清算対象レースを抽出する
    #
    L.info("# 清算対象レースを抽出する")

    df_racelist_target = utils.get_not_paidoff_racelist(df_racelist, s3_feed_folder)

    L.debug("df_racelist_target")
    L.debug(df_racelist_target)

    if len(df_racelist_target) == 0:
        L.debug("清算対象レースがない")
        return

    df_race = df_racelist_target.tail(1)
    race_id = df_race["race_id"].values[0]

    L.debug("df_race")
    L.debug(df_race)

    L.debug("race_id")
    L.debug(race_id)

    #
    # 投票データを取得する
    #
    L.info("# 投票データを取得する")

    df_vote = utils.get_vote(race_id, s3_vote_folder)

    L.debug("df_vote")
    L.debug(df_vote)

    #
    # フィードjsonからレースデータを取得する
    #
    L.info("# フィードjsonからレースデータを取得する")

    df_race_bracket, df_race_info, df_race_result, df_race_payoff, df_race_odds = utils.get_feed_data(df_race, s3_feed_folder, "_after")

    L.debug("df_race_bracket")
    L.debug(df_race_bracket)

    L.debug("df_race_info")
    L.debug(df_race_info)

    L.debug("df_race_result")
    L.debug(df_race_result)

    L.debug("df_race_payoff")
    L.debug(df_race_payoff)

    L.debug("df_race_odds")
    L.debug(df_race_odds)

    #
    # 清算する
    #
    L.info("# 清算する")

    if df_race_odds is None:
        L.debug("レースが中止となったため、0で清算する")

        df_vote["payoff_amount"] = None
        df_vote = df_vote.dropna()

        df_racelist.at[df_race.index[0], "payoff_timestamp"] = datetime.now()
        df_racelist.at[df_race.index[0], "payoff_amount"] = 0
    elif df_race_payoff is None:
        L.debug("まだ結果が出ていないため、処理しない")
    else:
        df_vote, df_racelist = payoff_race(df_racelist, df_race, df_vote, df_race_odds, df_race_payoff)

    L.debug("df_vote")
    L.debug(df_vote)

    L.debug("df_racelist")
    L.debug(df_racelist)
    L.debug(df_racelist.loc[df_race.index[0]])

    #
    # 清算データをアップロードする
    #
    L.info("# 清算データをアップロードする")

    utils.put_racelist(df_racelist, s3_vote_folder)
    utils.put_vote(df_vote, race_id, s3_vote_folder)


if __name__ == "__main__":
    #
    # 設定を取得する
    #
    s3_feed_folder = os.environ["AWS_S3_FEED_FOLDER"]
    L.info(f"S3フィードデータフォルダ: {s3_feed_folder}")

    s3_vote_folder = os.environ["AWS_S3_VOTE_FOLDER"]
    L.info(f"S3投票データフォルダ: {s3_vote_folder}")

    #
    # 清算処理
    #
    main(s3_feed_folder, s3_vote_folder)
