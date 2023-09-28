import os
import time

import payoff_race
import utils
import vote_race

L = utils.get_logger("vote_loop")


def vote_loop(s3_feed_folder, s3_pred_folder, s3_vote_folder):
    while (True):
        # レース一覧を取得して、全てのレースが処理済みならbreakする
        df_racelist = utils.remaining_racelist(s3_vote_folder)
        if len(df_racelist) == 0:
            L.info("全てのレースを処理した")
            break

        # 投票、清算する
        vote_race.main(s3_feed_folder, s3_pred_folder, s3_vote_folder)
        payoff_race.main(s3_feed_folder, s3_vote_folder)

        time.sleep(10)


if __name__ == "__main__":
    #
    # 設定を取得する
    #
    s3_feed_folder = os.environ["AWS_S3_FEED_FOLDER"]
    L.info(f"S3フィードデータフォルダ: {s3_feed_folder}")

    s3_pred_folder = os.environ["AWS_S3_PRED_FOLDER"]
    L.info(f"S3予測データフォルダ: {s3_pred_folder}")

    s3_vote_folder = os.environ["AWS_S3_VOTE_FOLDER"]
    L.info(f"S3投票データフォルダ: {s3_vote_folder}")

    #
    # 投票・清算ループ
    #
    vote_loop(s3_feed_folder, s3_pred_folder, s3_vote_folder)
