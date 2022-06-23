# README.md

register.py
- 以下のコマンドで起動
    - python register.py
- 以下のURLにアクセス
    - http://127.0.0.1:12345/
- 以下に登録される
    - data/master.csv
    - 手動でdelete_flg=1にするとデータ取得が無効になる
    - start_dateとend_dateは実験開始日と終了日
    - fetch_start_dateとfetch_end_dateはデータ取得開始日と終了日
        - データ取得日のデータ取得対象は前日までのデータ
        - fetch_start_dateはデフォルトでstart_date + 1
        - fetch_end_dateはデフォルトでend_date + 14
- TODO
    - start_dateとend_dateと（別のマスタと紐付けるID）の入力が必要そう
    - 最初に1ページ挟んでフォームで入力したのをやるといいかも

data/ABCDEF/1_raw/heart_rate/1_raw_heart_rate_20220622_ABCDEF.csv
data/ABCDEF/2_preprocessed/heart_rate/2_preprocessed_heart_rate_ABCDEF.csv
data/ABCDEF/3_processed/heart_rate/3_processed_heart_rate_ABCDEF.csv
data/ABCDEF/1_raw/sleep/1_raw_sleep_20220622_ABCDEF.csv
data/ABCDEF/2_preprocessed/heart_rate/2_preprocessed_sleep_ABCDEF.csv
data/ABCDEF/3_processed/heart_rate/3_processed_sleep_ABCDEF.csv
