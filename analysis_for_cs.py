# -*- coding: utf-8 -*-
"""develop_analysis_for_CS.ipynb
main.pyの参考となるファイル

# #1:Import【編集不可】
"""

!pip install pymysql
!pip install jpholiday
!pip install japanize_matplotlib
!pip install google-generativeai pillow

import pymysql.cursors
import pandas as pd
import polars as pl
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import japanize_matplotlib
from google.colab import userdata
from google.colab import drive
import urllib.request
from bs4 import BeautifulSoup
import os
import jpholiday
import datetime
import re
import random
import math
import google.generativeai as genai
import PIL.Image
drive.mount('/content/drive')

# Commented out IPython magic to ensure Python compatibility.
# %run '/content/drive/Shareddrives/internal_Shared_engineer/500_Soft/Data_Algo/Indiv_AC_CS/analysis-workspace/repositories/bm-airplug_plus-analysis/csv_to_master.ipynb'

"""# #2:SQL関数定義【編集不可】"""

# データベースに接続する関数

## 旧DB
# def connectDB():
#   connection = pymysql.connect(
#     host='airplugprod.tidb-tk1.db.sakurausercontent.com',
#     user='airplugprod',
#     password='rsy77cd5euhhadc@rwrf',
#     db='airplugprod',
#     charset='utf8mb4',
#     cursorclass=pymysql.cursors.DictCursor
#   )

#   return connection

## 新DB
def connectDB():
  connection = pymysql.connect(
    host='gateway01.ap-northeast-1.prod.aws.tidbcloud.com',
    port=4000,
    user='2Dv1chx9hoFRkxE.analytics_user',
    password='QX7k8jm4e!M%6Pen',
    db='airplugprod',
    charset='utf8mb4',
    connect_timeout=1000,
    max_allowed_packet='1G',
    ssl={'ssl': {}},
    cursorclass=pymysql.cursors.DictCursor
  )

  return connection

def getDataFromDB(connection, sql):
  try:
     with connection.cursor() as cursor:

      cursor.execute(sql)
      result = cursor.fetchall()

  finally:
    connection.close()

  df = pl.DataFrame(result)
  return df

"""# #3:データ処理関数の定義【編集不可】

##温度・設備データ
"""

#----------Zoneidの取得
def get_zone_id(floor_id):

    sql = "SELECT * FROM system_temperaturecontrolzone WHERE floor_id = " + floor_id

    #南海用
    #sql = "SELECT * FROM system_temperaturecontrolzone WHERE floor_id = " + floor_id + " AND (display_name = '事務室1' OR display_name = '事務室2' OR display_name = '事務室3' OR display_name = '事務室4' OR display_name = '事務室5' OR display_name = '事務室9' OR display_name = '事務室10' OR display_name = '事務室11' OR display_name = '事務室12')"

    connection = connectDB()
    df_id = getDataFromDB(connection, sql)

    if df_id.shape[0] == 0:
        return df_id, True

    df_id = df_id.sort("display_name")

    return df_id, False

#-----------設備IDの取得
def get_airid(df_id):
    if df_id.shape[0] == 0:
        # zoneデータがない場合、空のDataFrameを返すのではなく、
        # 全設備データを取得する条件を修正
        sql = "SELECT * FROM system_airconditioner"
    else:
        sql = "SELECT * FROM system_airconditioner WHERE "
        for i, id in enumerate(df_id['id']):
            sql += "zone_id = '" + id + "'"
            if i < len(df_id['id']) - 1:
                sql += " OR "

    connection = connectDB()
    df_airid = getDataFromDB(connection, sql)

    # df_airidが空の場合、空のDataFrameを返すように修正
    if df_airid.shape[0] == 0:
        return pl.DataFrame(schema=['id', 'zone_id', 'display_name']), False  # 修正箇所

    # zone_id列が存在しない場合は、設備idと同じ値をダミーとして設定
    if 'zone_id' not in df_airid.columns:
        df_airid = df_airid.with_columns(pl.col('id').alias('zone_id'))

    df_airid.sort("display_name")

    return df_airid, False

#--------------AirPlugデータの取得
def get_df_raw(df_zid, notBizDays, si):
    if df_zid.shape[0] == 0:
        # zoneデータがないので、空のDataFrameを返す（後続のjoin処理がエラーにならないように measured_at_jst カラムは保持）
        # empty_df = pl.DataFrame({"measured_at_jst": []}) # Original line
        # Return an empty DataFrame with 'measured_at_jst' column cast to Datetime
        return pl.DataFrame(schema=[('measured_at_jst', pl.Datetime)]), False # Changed line

    sql = "SELECT * FROM system_zonetemperature WHERE (zone_id = '"
    next_str = "' OR zone_id = '"

    for id in df_zid['id']:
        sql += id + next_str

    sql = sql[:-len(next_str)] + "')"
    #sql += " AND measured_at > DATE_SUB(CONVERT_TZ(NOW(), @@session.time_zone, '+00:00'), INTERVAL " + dur + " MINUTE);"
    sql += " AND measured_at > '" + st_dt_ymdhms.strftime('%Y-%m-%d %H:%M:%S') + "' AND measured_at < '" + ed_dt_ymdhms.strftime('%Y-%m-%d %H:%M:%S') + "';"

    # Print the SQL query for debugging
    print("SQL Query:", sql)

    connection = connectDB()
    df = getDataFromDB(connection, sql)

    if df.shape[0] == 0:
        print("Warning: SQL query returned no data. Returning an empty DataFrame.")
        # Return an empty DataFrame with 'measured_at_jst' column cast to Datetime
        return pl.DataFrame(schema=[('measured_at_jst', pl.Datetime)]), True # Changed line

    df = df.with_columns(
        measured_at_jst=pl.col('measured_at').dt.offset_by(by='9h').alias('measured_at_jst')
    )

    df_pivot = df.pivot(values="value", index="measured_at_jst", on="zone_id").sort("measured_at_jst")
    df_pivot = df_pivot.sort("measured_at_jst")

    # x分ごとにリサンプリング
    df_resampled = df_pivot.group_by_dynamic("measured_at_jst", every=si+"m").agg(pl.col("*").mean())

    df_ex = excludeNotBizDays(df_resampled, notBizDays)

    return df_ex, False

#--------------設備データの取得
def get_df_air(df_airid, notBizDays, si):

  sql = "SELECT * FROM system_airconditionermeasurement WHERE (air_conditioner_id = '"
  next_str = "' OR air_conditioner_id = '"

  for id in df_airid['id']:
    sql += id + next_str

  sql = sql[:-len(next_str)] + "')"
  #sql += " AND measured_at > DATE_SUB(CONVERT_TZ(NOW(), @@session.time_zone, '+00:00'), INTERVAL " + dur + " MINUTE);"
  sql += " AND measured_at > '" + st_dt_ymdhms.strftime('%Y-%m-%d %H:%M:%S') + "' AND measured_at < '" + ed_dt_ymdhms.strftime('%Y-%m-%d %H:%M:%S') + "';"

  connection = connectDB()
  df = getDataFromDB(connection, sql)

  if df.is_empty():
    # Handle the empty DataFrame case, for example, by returning an empty DataFrame or raising an error
    print("Warning: get_df_air received an empty DataFrame. Returning an empty DataFrame.")
    # Return an empty DataFrame with a measured_at_jst column cast to Datetime
    return pl.DataFrame(schema=[('measured_at_jst', pl.Datetime)]), False  # Changed line

  df = df.with_columns(
    measured_at_jst=pl.col('measured_at').dt.offset_by(by='9h').alias('measured_at_jst')
  )

  # dfをpivotしてzone_idごとにカラムに展開
  df_pivot = df.pivot(values=["operation_mode", "fan_speed", "start_stop", "set_temperature", "process_temperature"], index="measured_at_jst", on="air_conditioner_id").sort("measured_at_jst")

  # 5分ごとにリサンプリング
  df_resampled_ac = df_pivot.group_by_dynamic("measured_at_jst", every=si+"m").agg(pl.col("*").mean())

  #0をnull
  df_resampled_ac = df_resampled_ac.with_columns([
    pl.when(pl.col(col) == 0).then(None).otherwise(pl.col(col)).alias(col)
    for col in df_resampled_ac.columns
  ])

  df_ex = excludeNotBizDays(df_resampled_ac, notBizDays)

  return df_ex, False

#--------目標温度の取得
def get_df_target(df_airid):

  sql = "SELECT measured_at, target_temperature, airplug_control_on, calculated_set_temperature, air_conditioner_id FROM system_airconditionerlog WHERE (air_conditioner_id = '"
  next_str = "' OR air_conditioner_id = '"

  for id in df_airid['id']:
    sql += id + next_str

  sql = sql[:-len(next_str)] + "')"
  #sql += " AND measured_at > DATE_SUB(CONVERT_TZ(NOW(), @@session.time_zone, '+00:00'), INTERVAL " + dur + " MINUTE);"
  sql += " AND measured_at > '" + st_dt_ymdhms.strftime('%Y-%m-%d %H:%M:%S') + "' AND measured_at < '" + ed_dt_ymdhms.strftime('%Y-%m-%d %H:%M:%S') + "';"

  #print(sql)

  connection = connectDB()
  df = getDataFromDB(connection, sql)

  # Check if the DataFrame is empty and return a DataFrame with necessary columns
  if df.is_empty():
    print("Warning: get_df_target received an empty DataFrame. Returning an empty DataFrame with necessary columns.")
    # Create an empty DataFrame with 'measured_at_jst', 'air_conditioner_id', 'target_temperature', and 'calculated_set_temperature' columns
    return pl.DataFrame(schema=['measured_at_jst', 'air_conditioner_id', 'target_temperature', 'calculated_set_temperature']), False

  df = df.with_columns(
    measured_at_jst=pl.col('measured_at').dt.offset_by(by='9h').alias('measured_at_jst')
  )

  # dfをpivotしてzone_idごとにカラムに展開
  df_pivot = df.pivot(values=["target_temperature", "calculated_set_temperature"], index="measured_at_jst", on="air_conditioner_id").sort("measured_at_jst")

  return df, False

#--------空調制御ログの取得
def get_df_aclog(df_airid, notBizDays, si):

  sql = "SELECT measured_at, target_temperature, airplug_control_on, calculated_set_temperature, air_conditioner_id FROM system_airconditionerlog WHERE (air_conditioner_id = '"
  next_str = "' OR air_conditioner_id = '"

  for id in df_airid['id']:
    sql += id + next_str

  sql = sql[:-len(next_str)] + "')"
  #sql += " AND measured_at > DATE_SUB(CONVERT_TZ(NOW(), @@session.time_zone, '+00:00'), INTERVAL " + dur + " MINUTE);"
  sql += " AND measured_at > '" + st_dt_ymdhms.strftime('%Y-%m-%d %H:%M:%S') + "' AND measured_at < '" + ed_dt_ymdhms.strftime('%Y-%m-%d %H:%M:%S') + "';"

  #print(sql)

  connection = connectDB()
  df = getDataFromDB(connection, sql)

  if df.is_empty():
        print("Warning: get_df_aclog received an empty DataFrame. Returning an empty DataFrame.")
        # Create an empty DataFrame with 'measured_at_jst' and the desired type
        return pl.DataFrame(schema=[('measured_at_jst', pl.Datetime)]), False

  df = df.with_columns(
    measured_at_jst=pl.col('measured_at').dt.offset_by(by='9h').alias('measured_at_jst')
  )

  # dfをpivotしてzone_idごとにカラムに展開
  df_pivot = df.pivot(values=["target_temperature", "airplug_control_on", "calculated_set_temperature"], index="measured_at_jst", on="air_conditioner_id").sort("measured_at_jst")
  df_pivot = df_pivot.with_columns(pl.col('measured_at_jst').cast(pl.Datetime))

  # x分ごとにリサンプリング
  df_resampled = df_pivot.group_by_dynamic("measured_at_jst", every=si+"m").agg(pl.col("*").mean())

  #ビジネスデーのみ抽出
  df_ex = excludeNotBizDays(df_resampled, notBizDays)

  return df_ex, False

#-------結果の可視化
def visualize(df_airplug, df_aircond, df_target):
    view_cols = ["set_temperature", "process_temperature"]
    color_list = ['orange', 'green']

    # df_airidは各エアコンの情報（id, zone_id, display_name など）を持つデータフレームと仮定
    for ai, airid in enumerate(df_airid['id']):
        # 時系列を合わせるために内部結合
        df_combine = df_airplug.join(df_aircond, on='measured_at_jst', how='inner')

        # 対象のzone_idが存在しない場合はスキップ
        if df_airid['zone_id'][ai] not in df_combine.columns:
            continue

        # off状態のマスク（start_stopが2の場合）
        mask = df_combine['start_stop' + '_' + df_airid['id'][ai]] == 2

        # １つのグラフに温度とop_modeを描画するため、twinxを利用
        fig, ax1 = plt.subplots(figsize=(50, 8))
        ax2 = ax1.twinx()

        # ── 左軸(ax1)：温度グラフ ──
        # エアプラグの温度データ（線＆散布図）
        op_mode_col = "operation_mode_" + airid
        op_mode_vals = df_combine[op_mode_col]
        op_mode_colors = [
            'grey' if off else ('cyan' if mode == 1 else ('pink' if mode == 2 else 'white'))
            for off, mode in zip(mask, op_mode_vals)
        ]

        # colors = ['grey' if flag else 'blue' for flag in mask]
        sizes = [50 if flag else 1 for flag in mask]

        #ax1.scatter(df_combine['measured_at_jst'], df_combine[df_airid['zone_id'][ai]], s=sizes, color=op_mode_colors)
        # ax1.plot(df_combine['measured_at_jst'], df_combine[df_airid['zone_id'][ai]], label='AirPlug', color='blue')
        # ax1.scatter(df_combine['measured_at_jst'], df_combine[df_airid['zone_id'][ai]], label='AirPlug',  s=sizes, c=op_mode_colors)

        # 運転モードの大きなドット（背面に描画するため zorder=1）
        ax1.scatter(
            df_combine['measured_at_jst'],
            df_combine[df_airid['zone_id'][ai]],
            s=[300 if flag else 100 for flag in mask],  # ドットのサイズを大きく設定
            c=op_mode_colors,
            zorder=1,
            label='Operation Mode'
        )

        # 温度の青ライン（前面に描画するため zorder=2）
        ax1.plot(
            df_combine['measured_at_jst'],
            df_combine[df_airid['zone_id'][ai]],
            label='AirPlug',
            color='blue',
            zorder=2
        )

        # set_temperature, process_temperature の描画
        for k, col in enumerate(view_cols):
            # colors = ['grey' if flag else color_list[k] for flag in mask]
            ax1.plot(df_combine['measured_at_jst'], df_combine[col + '_' + df_airid['id'][ai]], label=col, color=color_list[k])
            ax1.scatter(df_combine['measured_at_jst'], df_combine[col + '_' + df_airid['id'][ai]], s=sizes, color=op_mode_colors)

        # 目標温度の描画（df_targetはair_conditioner_idでフィルタ）
        df_pick = df_target.filter(pl.col("air_conditioner_id") == df_airid['id'][ai]).sort("measured_at_jst")
        ax1.plot(df_pick['measured_at_jst'], df_pick['target_temperature'],
                 label="target_temperature", color='black', lw=5)

        # 温度軸の設定
        min_temp = 20
        max_temp = 30
        ax1.grid(axis="y")
        ax1.set_ylim(min_temp, max_temp)
        ax1.set_yticks(np.arange(min_temp, max_temp + 1, 1))
        ax1.set_xlabel("Measured Time (JST)")
        ax1.set_title(df_airid['display_name'][ai])
        ax1.legend(loc='upper left')

        plt.show()

def visualize_temperature_only(df_temperature, df_target=None):
    """
    df_temperature: 温度データのDataFrame。必ず 'measured_at_jst' 列があり、
                    それ以外の各カラムが各センサー（または測定ポイント）の温度値とする。
    df_target:      （任意）目標温度データのDataFrame。'measured_at_jst' 列と
                    同じセンサー名またはカラム名を持つ列が存在する場合にプロット。
    """
    import matplotlib.pyplot as plt
    import numpy as np

    time = df_temperature['measured_at_jst']

    # df_temperature の各温度カラムをループ（'measured_at_jst' 列を除く）
    for col in df_temperature.columns:
        if col == 'measured_at_jst':
            continue

        plt.figure(figsize=(12, 4))
        # 温度データのプロット
        plt.plot(time, df_temperature[col], label=f'{col} Temperature', color='blue')
        plt.scatter(time, df_temperature[col], s=5, color='blue')

        # 対応する目標温度データがあればプロット
        if df_target is not None and col in df_target.columns:
            plt.plot(df_target['measured_at_jst'], df_target[col], label='Target Temperature', color='black', linewidth=2)

        plt.title(f'Temperature Data for {col}')
        plt.xlabel('Time')
        plt.ylabel('Temperature')
        plt.grid(True)
        # 温度の範囲（必要に応じて調整）
        plt.ylim(0, 30)
        plt.yticks(np.arange(20, 31, 1))
        plt.legend()
        plt.show()

#-------指標の計算
def calc_res(df_airid: pl.DataFrame, df_airplug: pl.DataFrame, df_aircond: pl.DataFrame, df_target: pl.DataFrame, df_aclog: pl.DataFrame, st_h: int, ed_h: int):
    """
    ゾーンごとの指標を計算する関数（1:N対応修正版 & diff エラー修正版）
    """
    df_combine = df_airplug.join(df_aircond, on='measured_at_jst', how='inner')
    df_combine = df_combine.join(df_aclog, on='measured_at_jst', how='left')

    df_combine = df_combine.filter(
        pl.col('measured_at_jst').is_not_null()  # Add this line to filter out null values
    ).filter(
        (pl.col('measured_at_jst').dt.hour() >= st_h) &
        (pl.col('measured_at_jst').dt.hour() <= ed_h)
    )

    unique_zone_ids = df_airid['zone_id'].unique().drop_nulls().to_list()

    zone_results = []

    for zone_id in unique_zone_ids:
        zone_id_str = str(zone_id)

        airconditioner_ids_in_zone = df_airid.filter(
            pl.col('zone_id') == zone_id
        )['id'].to_list()

        zone_temp_col = zone_id_str
        ac_start_cols = [f"start_stop_{ac_id}" for ac_id in airconditioner_ids_in_zone]
        ac_control_cols = [f"airplug_control_on_{ac_id}" for ac_id in airconditioner_ids_in_zone]
        ac_set_temp_cols = [f"set_temperature_{ac_id}" for ac_id in airconditioner_ids_in_zone]
        ac_target_temp_cols = [f"target_temperature_{ac_id}" for ac_id in airconditioner_ids_in_zone]

        available_zone_temp_col = zone_temp_col if zone_temp_col in df_combine.columns else None
        available_start_cols = [col for col in ac_start_cols if col in df_combine.columns]
        available_control_cols = [col for col in ac_control_cols if col in df_combine.columns]
        available_set_temp_cols = [col for col in ac_set_temp_cols if col in df_combine.columns]
        available_target_temp_cols = [col for col in ac_target_temp_cols if col in df_combine.columns]

        if not available_zone_temp_col or not available_start_cols:
            print(f"警告: ゾーン {zone_id} の必須データ (温度または運転状態) が不足しているためスキップします。")
            continue

        zone_onoff = df_combine.select(
            pl.any_horizontal([(pl.col(col) == 1) for col in available_start_cols])
        ).to_series()

        if available_control_cols:
             zone_control = df_combine.select(
                 pl.any_horizontal([(pl.col(col).fill_null(0) == 1) for col in available_control_cols])
             ).to_series()
        else:
             zone_control = pl.repeat(False, df_combine.height, eager=True)

        mask_on = (zone_onoff & zone_control)
        mask_off = (zone_onoff & ~zone_control)
        mask_start = zone_onoff

        zone_temp_series = df_combine.select(
            pl.col(available_zone_temp_col).cast(pl.Float64)
        ).to_series()

        temp_on = zone_temp_series.filter(mask_on).to_numpy()
        temp_off = zone_temp_series.filter(mask_off).to_numpy()
        temp_start = zone_temp_series.filter(mask_start).to_numpy()

        mean_on = np.nanmean(temp_on) if len(temp_on) > 0 else np.nan
        std_on = np.nanstd(temp_on) if len(temp_on) > 0 else np.nan
        mean_off = np.nanmean(temp_off) if len(temp_off) > 0 else np.nan
        std_off = np.nanstd(temp_off) if len(temp_off) > 0 else np.nan

        total_samples = df_combine.height
        on_samples = mask_start.sum()
        on_control_on_samples = mask_on.sum()
        on_control_off_samples = mask_off.sum()

        ac_rate_on_percent = (on_control_on_samples / on_samples * 100) if on_samples > 0 else 0
        ac_rate_off_percent = (on_control_off_samples / on_samples * 100) if on_samples > 0 else 0

        e_temp_on = np.nan
        e_temp_off = np.nan
        if available_target_temp_cols:
            target_temp_series_mean = df_combine.select(
                pl.mean_horizontal([pl.col(col).cast(pl.Float64).fill_null(strategy='forward') for col in available_target_temp_cols])
            ).to_series()
            target_temp_on_np = target_temp_series_mean.filter(mask_on).to_numpy()
            target_temp_off_np = target_temp_series_mean.filter(mask_off).to_numpy()

            if len(temp_on) == len(target_temp_on_np):
                 e_temp_on = np.nanmean(np.abs(temp_on - target_temp_on_np))
            if len(temp_off) == len(target_temp_off_np):
                 e_temp_off = np.nanmean(np.abs(temp_off - target_temp_off_np))
        else:
            print(f"警告: ゾーン {zone_id} の目標温度データが見つかりません。")

        count_on = 0
        count_off = 0
        if available_set_temp_cols:
             df_on_changes = df_combine.filter(mask_on).select(available_set_temp_cols)
             if df_on_changes.height > 1:
                 df_on_shifted = df_on_changes.shift(1)
                 changed_on_expr = pl.any_horizontal([
                     ((pl.col(c) - df_on_shifted[c]) != 0).fill_null(False)
                     for c in available_set_temp_cols
                 ])
                 count_on = df_on_changes.select(changed_on_expr.alias("changed")).sum().row(0)[0]

             df_off_changes = df_combine.filter(mask_off).select(available_set_temp_cols)
             if df_off_changes.height > 1:
                 df_off_shifted = df_off_changes.shift(1)
                 changed_off_expr = pl.any_horizontal([
                     ((pl.col(c) - df_off_shifted[c]) != 0).fill_null(False)
                     for c in available_set_temp_cols
                 ])
                 count_off = df_off_changes.select(changed_off_expr.alias("changed")).sum().row(0)[0]
        else:
             print(f"警告: ゾーン {zone_id} の設定温度データが見つかりません。")

        null_rate_percent = df_combine[available_zone_temp_col].null_count() / total_samples * 100 if total_samples > 0 else 0

        zone_results.append([
            mean_on, mean_off, std_on, std_off,
            e_temp_on, e_temp_off, count_on, count_off,
            ac_rate_on_percent, ac_rate_off_percent, null_rate_percent
        ])


    if not zone_results:
        print("警告: 有効なゾーンデータから統計量を計算できませんでした。NaNを返します。")
        return [np.nan] * 11, df_combine

    results_array = np.array(zone_results)
    final_values = np.nanmean(results_array, axis=0).tolist()

    return final_values, df_combine

def calc_res_temperature_only(df_aiplug, st_h, ed_h):
    df_filtered = df_aiplug.filter(
        (pl.col("measured_at_jst").dt.hour() >= st_h) &
        (pl.col("measured_at_jst").dt.hour() <= ed_h)
    )

    # 2. センサーの温度カラムを特定（'measured_at_jst'以外）
    sensor_cols = [col for col in df_filtered.columns if col != "measured_at_jst"]

    # 3. 欠損値の集計と欠損率の計算
    total_missing = 0
    for col in sensor_cols:
        total_missing += df_filtered[col].null_count()
    total_expected = df_filtered.height * len(sensor_cols) if sensor_cols else 0
    null_rate = (total_missing / total_expected) if total_expected > 0 else np.nan

    # 4. 欠損値を前方補完し、残った欠損行は除去
    df_filled = df_filtered.fill_null(strategy="forward").drop_nulls()

    # 5. 各センサーごとに平均と標準偏差を計算
    sensor_means = []
    sensor_stds = []
    for col in sensor_cols:
        sensor_means.append(df_filled[col].mean())
        sensor_stds.append(df_filled[col].std())

    overall_mean = np.mean(sensor_means) if sensor_means else np.nan
    overall_std = np.mean(sensor_stds) if sensor_stds else np.nan

    # 温度データのみの場合は on/off の区別がないため、
    # on, off 両方に同じ値を流用し、他は 0 として返す
    # 元の calc_res の返り値は以下の11要素:
    # [mean_on, mean_off, std_on, std_off, e_temp_on, e_temp_off, count_on, count_off, ac_rate_on*100, ac_rate_off*100, null_rate*100]
    result = [
        overall_mean,    # mean_on
        overall_mean,    # mean_off
        overall_std,     # std_on
        overall_std,     # std_off
        0,               # e_temp_on (温度誤差：計算しない)
        0,               # e_temp_off
        0,               # count_on (設定温度変化回数：計算しない)
        0,               # count_off
        0,               # ac_rate_on*100 (空調稼働率：計算しない)
        0,               # ac_rate_off*100
        null_rate * 100  # null_rate(%)
    ]

    return result, df_filled

#厳密には異なる
#手で操作→手で操作が反映されない
def _count_consecutive_zeros(arr):
  consecutive_zeros = 0
  interval = []

  for i in arr:
    if i == 0:
        consecutive_zeros += 1

    else:

        interval.append(consecutive_zeros)
        consecutive_zeros = 0

  count = np.sum(np.logical_and(np.array(interval[1:]) < 29 , np.array(interval[1:]) > 3))

  return count


#厳密には異なる
#手で操作→手で操作が反映されない
def _count_consecutive_zeros(arr):
  consecutive_zeros = 0
  interval = []

  for i in arr:
    if i == 0:
        consecutive_zeros += 1

    else:

        interval.append(consecutive_zeros)
        consecutive_zeros = 0

  count = np.sum(np.logical_and(np.array(interval[1:]) < 29 , np.array(interval[1:]) > 3))

  return count

def _check_ids(list_a, list_b):
    for item in list_a:
        if item == list_b:
            return False
    return True

#---------解析区間の抽出
def get_dt(ekind, skind, st, ed):
  base_dir = '/content/drive/Shareddrives/internal_Shared_engineer/500_Soft/Data_Algo/Indiv_AC_CS/Customer/'

  if ekind == 'hioki':
    energy_df = pl.read_csv(base_dir + customer_dir + add_dir + '/' + ekind + '.csv', skip_rows=26, null_values=["-"])[:, 3:]
    st_dt_jst = energy_df['DateTime'].min() #jst
    ed_dt_jst = energy_df['DateTime'].max() #jst
  elif ekind == 'master':
    try:
        energy_df = pl.read_csv(base_dir + customer_dir + add_dir + '/' + ekind + '.csv', null_values=["-"])[:, 2:]
        if energy_df.is_empty():
            raise ValueError("CSV is empty.")
    except:
        print("Master CSVが空または読み込めないため、デフォルトの日付範囲を使用します。")
        # デフォルトのDataFrameに'measured_at'列を追加
        energy_df = pl.DataFrame({
            "DateTime": ['2024-12-02 00:00:00', '2024-12-27 23:00:00'],
            "CH1(kW)": [0, 0],
            # 'measured_at'列を追加し、'DateTime'列と同じ値を設定
            "measured_at": ['2024-12-02 00:00:00', '2024-12-27 23:00:00']
        })

    st_dt_jst = energy_df['DateTime'].min() #jst
    ed_dt_jst = energy_df['DateTime'].max() #jst

  if skind != 'plus':
    st_dt_jst = st
    ed_dt_jst = ed

  # Corrected the way to call strptime
  st_jst = datetime.datetime.strptime(st_dt_jst, '%Y-%m-%d %H:%M:%S')
  st_utc = st_jst + datetime.timedelta(hours=-9)
  st_dt_utc = st_utc.strftime('%Y-%m-%d %H:%M:%S')

  ed_jst = datetime.datetime.strptime(ed_dt_jst, '%Y-%m-%d %H:%M:%S')
  ed_utc = ed_jst + datetime.timedelta(hours=-9)
  ed_dt_utc = ed_utc.strftime('%Y-%m-%d %H:%M:%S')

  return energy_df, st_dt_utc, ed_dt_utc, st_dt_jst, ed_dt_jst

#------休日のリスト作成
def _getNotBizDay(st, ed):

  date = datetime.datetime.strptime(st, '%Y-%m-%d %H:%M:%S')
  notBizDayList = []

  while True:

    if date.weekday() == 4 or jpholiday.is_holiday(date):
      notBizDayList.append(date.strftime('%Y-%m-%d %H:%M:%S'))

    date += datetime.timedelta(days=1)

    if date > datetime.datetime.strptime(ed, '%Y-%m-%d %H:%M:%S'):
      break

  return notBizDayList

def excludeNotBizDays(df, notBizDays):
    # notBizDaysから除外したい日付（"YYYY-MM-DD"形式）を抽出
    excluded_dates_from_notBiz = [
        datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
        for ts in notBizDays
    ]

    global exclusion_date_list

    if exclusion_date_list is None:
        exclusion_date_list = []

    all_excluded_dates = set(excluded_dates_from_notBiz + exclusion_date_list)

    df = df.with_columns(pl.col("measured_at_jst").dt.strftime("%Y-%m-%d").alias("date_only"))

    # 統合した除外日リストに含まれる日付の行を除外
    df = df.filter(~pl.col("date_only").is_in(list(all_excluded_dates)))

    return df.drop("date_only")

"""##湿度"""

def get_df_humidity(df_zid, notBizDays, si):
    sql = "SELECT * FROM system_zonehumidity WHERE (zone_id = '"
    next_str = "' OR zone_id = '"
    for id in df_zid['id']:
        sql += id + next_str
    sql = sql[:-len(next_str)] + "')"
    sql += " AND measured_at > '" + st_dt_ymd + "' AND measured_at < '" + ed_dt_ymd + "';"

    connection = connectDB()
    df = getDataFromDB(connection, sql)

    if df.shape[0] == 0:
        return df, True

    df = df.with_columns(
        measured_at_jst=pl.col('measured_at').dt.replace_time_zone("UTC").dt.convert_time_zone("Asia/Tokyo")
    )

    # ピボットしてzone_idごとに展開
    df_pivot = df.pivot(values="value", index="measured_at_jst", columns="zone_id").sort("measured_at_jst")
    # リサンプリング
    df_resampled = df_pivot.group_by_dynamic("measured_at_jst", every=si+"m").agg(pl.col("*").mean())
    df_ex = excludeNotBizDays(df_resampled, notBizDays)

    # ここで、湿度カラムにはプレフィックスを付与しておく
    humidity_columns = {col: "humidity_" + col for col in df_ex.columns if col != "measured_at_jst"}
    df_ex = df_ex.rename(humidity_columns)

    return df_ex, False

"""##電気使用量"""

def calc_energy(st_h, ed_h, ekind, df_combine):
    base_dir = '/content/drive/Shareddrives/internal_Shared_engineer/500_Soft/Data_Algo/Indiv_AC_CS/Customer/'

    # ※customer_dir, add_dir はグローバルに定義済みと仮定
    try:
        energy_df = pl.read_csv(base_dir + customer_dir + add_dir + '/' + ekind + '.csv', null_values=["-"])[:, 2:]
        if energy_df.is_empty():
            raise ValueError("CSV is empty.")
    except Exception as e:
        print("Master CSVが空または読み込めないため、デフォルトの日付範囲を使用します。")
        energy_df = pl.DataFrame({
            "DateTime": ['2024-12-02 00:00:00', '2024-12-27 23:00:00'],
            "CH1(kW)": [0, 0]
        })

    ch_num = energy_df.shape[1] - 1

    # 日時列の整形
    df_raw = energy_df.drop_nulls()
    df_raw = df_raw.with_columns(
        pl.col("DateTime").str.to_datetime("%Y-%m-%d %H:%M:%S").alias('measured_at_jst')
    ).drop('DateTime')

    # 指定時間帯でフィルタリング
    df = df_raw.filter(
        (pl.col('measured_at_jst').dt.hour() >= st_h) &
        (pl.col('measured_at_jst').dt.hour() <= ed_h)
    )
    # Total 列: 日時以外の全エネルギー列の水平合計
    df = df.with_columns([pl.sum_horizontal(pl.col(df.columns[:-1])).alias('Total')])

    # df_combine とエネルギー情報を measured_at_jst で内部結合
    df_ecombine = df_combine.join(df, on='measured_at_jst', how='left')

    # airplug_control_on 列が存在するか確認
    airplug_cols = [col for col in df_ecombine.columns if 'airplug_control_on' in col]
    if airplug_cols:
        column = airplug_cols[0]
    else:
        # 存在しない場合、全行 0 のデフォルト列を作成
        df_ecombine = df_ecombine.with_columns(pl.lit(0).alias('airplug_control_on_default'))
        column = 'airplug_control_on_default'

    # エネルギー df に airplug_control_on 列を結合
    df = df.join(df_ecombine.select(['measured_at_jst', column]), on='measured_at_jst', how='left')

    # 1時間ごとの平均集計
    df_h = df.sort("measured_at_jst").group_by_dynamic("measured_at_jst", every="1h").agg(pl.col("*").mean())
    # 日ごとに合計（1時間集計結果から）
    df_d = df_h.group_by_dynamic("measured_at_jst", every="1d").agg(pl.col("*").sum())

    # airplug_control_on 列により、Energy の平均をフィルタリングして出力
    result_conv = df_d.filter(pl.col(column) < 3).select(pl.col("Total").mean())
    print("conv:", result_conv.to_numpy()[0, 0])
    result_airplug = df_d.filter(pl.col(column) >= 3).select(pl.col("Total").mean())
    print("AirPlug:", result_airplug.to_numpy()[0, 0])

    # df_combine と df の内部結合（不要な右側のカラムは drop する）
    df_all = df_combine.join(df, on='measured_at_jst', how='left')
    if (column + '_right') in df_all.columns:
        df_all = df_all.drop(column + '_right')

    # 時間・日単位の集計も同様に、df_combine の情報で join しておく
    df_h_combined = df_combine.group_by_dynamic("measured_at_jst", every="1h").agg(pl.col("*").mean()).join(
        df_h, on='measured_at_jst', how='left'
    )
    if (column + '_right') in df_h_combined.columns:
        df_h_combined = df_h_combined.drop(column + '_right')

    df_d_combined = df_combine.group_by_dynamic("measured_at_jst", every="1d").agg(pl.col("*").mean()).join(
        df_d, on='measured_at_jst', how='left'
    )
    if (column + '_right') in df_d_combined.columns:
        df_d_combined = df_d_combined.drop(column + '_right')

    return df_all, df_h_combined, df_d_combined

def calc_energy_for_slim(st_h, ed_h, ekind):
    base_dir = '/content/drive/Shareddrives/internal_Shared_engineer/500_Soft/Data_Algo/Indiv_AC_CS/Customer/'

    try:
        # customer_dir, add_dir は外部定義されている前提
        energy_df = pl.read_csv(base_dir + customer_dir + add_dir + '/' + ekind + '.csv', null_values=["-"])[:, 2:]
        if energy_df.is_empty():
            raise ValueError("CSV is empty.")
    except Exception as e:
        print("Master CSVが空または読み込めないため、デフォルトの日付範囲を使用します。")
        energy_df = pl.DataFrame({
            "DateTime": ['2024-12-02 00:00:00', '2024-12-27 23:00:00'],
            "CH1(kW)": [0, 0]
        })

    ch_num = energy_df.shape[1] - 1

    df_raw = energy_df.drop_nulls()
    df_raw = df_raw.with_columns(
        pl.col("DateTime").str.to_datetime("%Y-%m-%d %H:%M:%S").alias('measured_at_jst')
    ).drop('DateTime')

    df = df_raw.filter(
        (pl.col('measured_at_jst').dt.hour() >= st_h) &
        (pl.col('measured_at_jst').dt.hour() <= ed_h)
    )

    df = df.with_columns([pl.sum_horizontal(pl.col(df.columns[:-1])).alias('Total')])

    df = df.with_columns(pl.lit(0).alias('airplug_control_on'))

    # ToDo: 外気温の追加（必要に応じて実装）

    df_h = df.sort("measured_at_jst").group_by_dynamic("measured_at_jst", every="1h").agg(pl.col("*").mean())

    df_d = df_h.group_by_dynamic("measured_at_jst", every="1d").agg(pl.col("*").sum())

    result_conv = df_d.filter(pl.col('airplug_control_on') < 3).select(pl.col("Total").mean())
    print("conv:", result_conv.to_numpy()[0,0])
    result_airplug = df_d.filter(pl.col('airplug_control_on') >= 3).select(pl.col("Total").mean())
    print("AirPlug:", result_airplug.to_numpy()[0,0])

    df_all = df

    return df_all, df_h, df_d

"""##ボタン"""

#--------------ボタンデータの取得
def get_df_bt(notBizDays, si, sign, fid):
  if sign == '+':
    sql = "SELECT * FROM system_devicemeasurementbuttonplus WHERE value != 0 AND floor_id ='" + fid + "'"
  else:
    sql = "SELECT * FROM system_devicemeasurementbuttonminus WHERE value != 0 AND floor_id ='" + fid + "'"

  #sql += " AND measured_at > DATE_SUB(CONVERT_TZ(NOW(), @@session.time_zone, '+00:00'), INTERVAL " + dur + " MINUTE);"
  sql += " AND measured_at > '" + st_dt_ymd + "' AND measured_at < '" + ed_dt_ymd + "';"

  connection = connectDB()
  df = getDataFromDB(connection, sql)

  if df.shape[0] == 0:
    return df, True

  df = df.filter(pl.col('value') < 10)

  if df.shape[0] == 0:
    return df, True

  df = df.with_columns(
    measured_at_jst=pl.col('measured_at').dt.replace_time_zone("UTC").dt.convert_time_zone("Asia/Tokyo")
  )

  #display(df)

  df_ex = excludeNotBizDays(df, notBizDays)

  return df, False

#------------------ゾーニングとテーブルへの結合
def zone_bt(df_all, df_h, df_d, df_airid, fid):
    df_btp, e_p = get_df_bt(notBizDayList, si, '+', fid)
    df_btm, e_m = get_df_bt(notBizDayList, si, '-', fid)

    # df_btp が空でなく、value カラムを持つ場合のみ処理
    if not e_p and 'value' in df_btp.columns:
        sort_col_p = sorted(df_btp.columns)
        df_btp = df_btp.select(sort_col_p)
    else:
        # 空のデータフレームまたは value がない場合は、スキーマを指定して空のDFを作成
        schema_p = {**{col: pl.Utf8 for col in df_all.columns if col not in ['measured_at_jst', 'value']}, 'measured_at_jst': pl.Datetime, 'value': pl.Int64} # スキーマを適切に設定
        df_btp = pl.DataFrame(schema=schema_p) # 空のDFを作成

    # df_btm が空でなく、value カラムを持つ場合のみ処理
    if not e_m and 'value' in df_btm.columns:
        df_btm = df_btm.with_columns(pl.col('value') * -1)
        sort_col_m = sorted(df_btm.columns)
        df_btm = df_btm.select(sort_col_m)
    else:
        # 空のデータフレームまたは value がない場合は、スキーマを指定して空のDFを作成
        schema_m = {**{col: pl.Utf8 for col in df_all.columns if col not in ['measured_at_jst', 'value']}, 'measured_at_jst': pl.Datetime, 'value': pl.Int64} # スキーマを適切に設定
        df_btm = pl.DataFrame(schema=schema_m) # 空のDFを作成

    # df_btp と df_btm のスキーマを合わせる (存在しない列をnullで埋める)
    common_cols = set(df_btp.columns) & set(df_btm.columns)
    df_btp = df_btp.select(common_cols)
    df_btm = df_btm.select(common_cols)

    # vstackを実行
    if not df_btp.is_empty() or not df_btm.is_empty():
        df_bt = pl.concat([df_btp, df_btm], how="diagonal") # concatに変更し、スキーマが異なっても結合可能に
    else:
        # 両方空なら空のDF
        df_bt = pl.DataFrame(schema=schema_p) # スキーマは df_btp のものを使う（どちらでも良い）

    # df_bt が空でなく、value カラムを持つ場合のみ clip 処理
    if not df_bt.is_empty() and 'value' in df_bt.columns:
        df_bt = df_bt.with_columns(pl.col('value').clip(-3, 3))

    # 以降の処理 (join など) でも df_bt が空の場合を考慮する必要がある

    for zi, id in enumerate(df_airid['zone_id']):
        zone_table = df_zid.filter(pl.col("id") == id).select(pl.col("x"), pl.col("y"), pl.col("width"), pl.col("height"))

        # df_bt が空でない場合のみフィルタリングと集計を行う
        if not df_bt.is_empty() and all(col in df_bt.columns for col in ["x", "y", "measured_at_jst", "value"]):
            df_bt_zone = df_bt.filter(
                pl.col("x") >= zone_table['x'][0],
                pl.col("x") <= zone_table['x'][0] + zone_table['width'][0],
                pl.col("y") >= zone_table['y'][0],
                pl.col("y") <= zone_table['y'][0] + zone_table['height'][0]
            ).sort('measured_at_jst')

            # フィルタリング結果が空でない場合のみ集計
            if not df_bt_zone.is_empty():
                 df_bt_zone = df_bt_zone.group_by_dynamic("measured_at_jst", every="1m").agg(pl.col("value").sum()) # valueのみ集計
                 df_bt_zone_h = df_bt_zone.group_by_dynamic("measured_at_jst", every="1h").agg(pl.col("value").sum())
                 df_bt_zone_d = df_bt_zone.group_by_dynamic("measured_at_jst", every="1d").agg(pl.col("value").sum())
            else:
                # 集計できない場合は空のDFを作成
                schema_agg = {'measured_at_jst': pl.Datetime, 'value': pl.Int64}
                df_bt_zone = pl.DataFrame(schema=schema_agg)
                df_bt_zone_h = pl.DataFrame(schema=schema_agg)
                df_bt_zone_d = pl.DataFrame(schema=schema_agg)

        else:
            # df_btが空、または必要なカラムがない場合は空のDFを作成
            schema_agg = {'measured_at_jst': pl.Datetime, 'value': pl.Int64}
            df_bt_zone = pl.DataFrame(schema=schema_agg)
            df_bt_zone_h = pl.DataFrame(schema=schema_agg)
            df_bt_zone_d = pl.DataFrame(schema=schema_agg)


        # --- 結合処理 ---
        # df_bt_zone が value カラムを持っているか確認してから join する
        join_cols = ['measured_at_jst', 'value'] if 'value' in df_bt_zone.columns else ['measured_at_jst']
        how_join = 'left'

        if not df_bt_zone.is_empty() and 'value' in df_bt_zone.columns:
            df_all = df_all.join(df_bt_zone.select(join_cols), on='measured_at_jst', how=how_join)
        else: # valueがない場合は measured_at_jst だけでjoinし、後でnull埋め
            df_all = df_all.join(df_bt_zone.select('measured_at_jst'), on='measured_at_jst', how=how_join)
            df_all = df_all.with_columns(pl.lit(None).cast(pl.Int64).alias("value")) # value列がない場合にNoneで追加

        df_all = df_all.rename({"value": "bt_" + df_airid['id'][zi]})
        df_all = df_all.with_columns(pl.col("bt_" + df_airid['id'][zi]).fill_null(0)) # fill_null を with_columns 内で行う

        # df_h, df_d についても同様に修正
        if not df_bt_zone_h.is_empty() and 'value' in df_bt_zone_h.columns:
             df_h = df_h.join(df_bt_zone_h.select(join_cols), on='measured_at_jst', how=how_join)
        else:
             df_h = df_h.join(df_bt_zone_h.select('measured_at_jst'), on='measured_at_jst', how=how_join)
             df_h = df_h.with_columns(pl.lit(None).cast(pl.Int64).alias("value"))

        df_h = df_h.rename({"value": "bt_" + df_airid['id'][zi]})
        df_h = df_h.with_columns(pl.col("bt_" + df_airid['id'][zi]).fill_null(0))

        if not df_bt_zone_d.is_empty() and 'value' in df_bt_zone_d.columns:
             df_d = df_d.join(df_bt_zone_d.select(join_cols), on='measured_at_jst', how=how_join)
        else:
             df_d = df_d.join(df_bt_zone_d.select('measured_at_jst'), on='measured_at_jst', how=how_join)
             df_d = df_d.with_columns(pl.lit(None).cast(pl.Int64).alias("value"))

        df_d = df_d.rename({"value": "bt_" + df_airid['id'][zi]})
        df_d = df_d.with_columns(pl.col("bt_" + df_airid['id'][zi]).fill_null(0))


    df_all = excludeNotBizDays(df_all, notBizDayList)
    df_h = excludeNotBizDays(df_h, notBizDayList)
    df_d = excludeNotBizDays(df_d, notBizDayList)

    return df_all, df_h, df_d

#------------ヒートマップ可視化
def visualize_bt(df_all, df_h, df_d):
  visualize_date = df_h.with_columns(pl.col('measured_at_jst').dt.date()).select(pl.col('measured_at_jst')).unique()

  for di in range(len(visualize_date)):

    df = df_h.filter(pl.col('measured_at_jst').dt.date() == visualize_date[di])
    df = df.select(pl.col('measured_at_jst'), pl.col("^*bt_.*$"))

    #df_airid = df_airid.sort('display_name')

    #for i, id in enumerate(df_airid['id']):

    #ToDo:ゾーンのソートと縦軸メモリ

    tmp = df.drop('measured_at_jst').to_numpy().T

    plt.figure(figsize=(24,10))
    plt.imshow(tmp, extent=(st_h,ed_h+1,len(df_d)+1,0), cmap='seismic_r', aspect=0.25)
    plt.colorbar(shrink=0.5)
    plt.clim(-10, 10)


#ボタンの結果演算
def calc_bt(df_all, df_d, df_h, df_airid):
  day_list = df_d.select(pl.col('measured_at_jst').dt.date()).select(pl.col('measured_at_jst')).unique()
  df_df = df_all.select(pl.col('measured_at_jst'), pl.col("^*airplug_control_on.*$"), pl.col("^*bt_.*$"))

  for di in range(len(day_list)):

    df = df_df.filter(pl.col('measured_at_jst').dt.date() == day_list[di])

    bt_array = df.select(pl.col("^*bt_.*$")).to_numpy()
    mask_p = bt_array > 0
    mask_m = bt_array < 0

    print('--')
    print(day_list[di])
    btp = bt_array[mask_p]
    btm = bt_array[mask_m]

    print('+回数：', np.sum(btp))
    print('-回数：', np.sum(btm))
    print('+頻度：', len(btp))
    print('-頻度：', len(btm))

"""##リモコン"""

def visualize_remote_control(df_all, df_h, df_d, st_dt, ed_dt):
    # Convert df_all to Pandas for easier manipulation
    temp_df = df_all.to_pandas()
    temp_df['measured_at_jst'] = pd.to_datetime(temp_df['measured_at_jst'])
    set_temperature_columns = [col for col in df_all.columns if col.startswith("set_temperature_")]

    # If no set_temperature columns exist, skip plotting
    if not set_temperature_columns:
        print("Warning: 'set_temperature_' columns not found. Skipping remote control visualization.")
        return

    # Calculate min/max for y-axis
    y_min = min([temp_df[col].min() for col in set_temperature_columns]) - 1
    y_max = max([temp_df[col].max() for col in set_temperature_columns]) + 1

    # Create date range
    date_range = pd.date_range(start=st_dt, end=ed_dt, freq='D')

    # Initialize daily summary list
    daily_summary = []

    # Process each date to count manual temperature changes
    for date in date_range:
        date_data = temp_df[temp_df['measured_at_jst'].dt.date == date.date()]
        total_up = 0
        total_down = 0

        if not date_data.empty:
            for room in set_temperature_columns:
                if room in date_data.columns:
                    temperature_data = date_data[['measured_at_jst', room]].copy()
                    temperature_data.rename(columns={room: 'set_temperature'}, inplace=True)
                    temperature_data['temp_change'] = temperature_data['set_temperature'].diff()

                    # Detect manual changes (0.5-degree increments)
                    temperature_data['manual_up'] = (
                        (temperature_data['temp_change'] > 0) &
                        (temperature_data['temp_change'] % 0.5 == 0)
                    )
                    temperature_data['manual_down'] = (
                        (temperature_data['temp_change'] < 0) &
                        (temperature_data['temp_change'] % 0.5 == 0)
                    )

                    total_up += temperature_data['manual_up'].sum()
                    total_down += temperature_data['manual_down'].sum()

        daily_summary.append({
            'Date': date,  # Store as datetime object
            'Manual Up': int(total_up),
            'Manual Down': int(total_down),
            'Total Changes': int(total_up + total_down)
        })

    # Create daily_summary_df
    daily_summary_df = pd.DataFrame(daily_summary)

    # If DataFrame is empty, skip plotting
    if daily_summary_df.empty:
        print("Warning: No manual temperature change data available. Skipping visualization.")
        return

    # Ensure 'Date' column is datetime
    daily_summary_df['Date'] = pd.to_datetime(daily_summary_df['Date'])

    """
    Plot 1: Hourly manual changes per day
    """
    n_plots = len(date_range)
    n_cols = math.ceil(math.sqrt(n_plots))
    n_rows = math.ceil(n_plots / n_cols)

    fig, axes = plt.subplots(n_rows, n_cols, figsize=(18, n_rows * 5))
    axes = axes.flatten()

    for idx, date in enumerate(date_range):
        ax = axes[idx]
        date_data = temp_df[temp_df['measured_at_jst'].dt.date == date.date()]

        # Initialize hourly series
        hourly_up_changes = pd.Series(0, index=pd.date_range(date, periods=24, freq='h'))
        hourly_down_changes = pd.Series(0, index=pd.date_range(date, periods=24, freq='h'))

        if not date_data.empty:
            for room in set_temperature_columns:
                if room in date_data.columns:
                    temperature_data = date_data[['measured_at_jst', room]].copy()
                    temperature_data.rename(columns={room: 'set_temperature'}, inplace=True)
                    temperature_data['temp_change'] = temperature_data['set_temperature'].diff()

                    temperature_data['manual_up'] = (
                        (temperature_data['temp_change'] > 0) &
                        (temperature_data['temp_change'] % 0.5 == 0)
                    )
                    temperature_data['manual_down'] = (
                        (temperature_data['temp_change'] < 0) &
                        (temperature_data['temp_change'] % 0.5 == 0)
                    )

                    temperature_data['hour'] = temperature_data['measured_at_jst'].dt.floor('h')
                    hourly_up = temperature_data.groupby('hour')['manual_up'].sum()
                    hourly_down = temperature_data.groupby('hour')['manual_down'].sum()

                    hourly_up_changes = hourly_up_changes.add(hourly_up, fill_value=0)
                    hourly_down_changes = hourly_down_changes.add(hourly_down, fill_value=0)

        ax.plot(hourly_up_changes.index, hourly_up_changes.values, marker='o', color='red', label='Manual Up (+0.5)')
        ax.plot(hourly_down_changes.index, hourly_down_changes.values, marker='o', color='blue', label='Manual Down (-0.5)')
        ax.set_title(f'{date.strftime("%Y-%m-%d")}', fontsize=12)
        ax.set_xlabel('Time', fontsize=10)
        ax.set_ylabel('Manual Changes', fontsize=10)
        ax.grid(True, linestyle='--', alpha=0.7)
        ax.legend(fontsize=8)
        ax.tick_params(axis='x', rotation=45)
        ax.set_ylim(0, max(hourly_up_changes.max(), hourly_down_changes.max(), 10))

    # Remove empty subplots
    for idx in range(len(date_range), len(axes)):
        fig.delaxes(axes[idx])

    plt.tight_layout()
    plt.show()

    """
    Plot 2: Daily manual change fluctuations
    """
    plt.figure(figsize=(12, 6))
    plt.plot(daily_summary_df['Date'], daily_summary_df['Manual Up'], marker='o', linestyle='-', color='red', label='Manual Up (+0.5)')
    plt.plot(daily_summary_df['Date'], daily_summary_df['Manual Down'], marker='o', linestyle='-', color='blue', label='Manual Down (-0.5)')
    plt.xlabel('Date')
    plt.ylabel('Count')
    plt.title('Daily Manual Temperature Change Fluctuations')
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.show()

    """
    Display summary table
    """
    print("\nDaily Manual Temperature Change Summary:")
    display(daily_summary_df)

def visualize_remote_control(df_all, df_h, df_d, st_dt, ed_dt):
    temp_df = df_all.to_pandas()
    temp_df['measured_at_jst'] = pd.to_datetime(temp_df['measured_at_jst'])
    set_temperature_columns = [col for col in df_all.columns if col.startswith("set_temperature_")]

    # 各カラムごとに最小値と最大値を取得し、全体の最小・最大値を計算
    if not set_temperature_columns:
        print("Warning: 'set_temperature_' で始まるカラムが存在しません。リモコンの温度データ描画はスキップします。")
        return  # もしくはデフォルト値を使用する場合は以下のように
        # y_min, y_max = 20, 30

    y_min = min([temp_df[col].min() for col in set_temperature_columns]) - 1
    y_max = max([temp_df[col].max() for col in set_temperature_columns]) + 1

    date_range = pd.date_range(start=st_dt, end=ed_dt)
    daily_summary = []

    # プロット数に応じたグリッドの行数・列数を自動計算
    n_plots_tmp_len = len(set_temperature_columns)
    n_cols_tmp_len = math.ceil(math.sqrt(n_plots_tmp_len))
    n_rows_tmp_len = math.ceil(n_plots_tmp_len / n_cols_tmp_len)

    n_plots_dt_range = len(date_range)
    n_cols_dt_range = math.ceil(math.sqrt(n_plots_dt_range))  # 列数
    n_rows_dt_range = math.ceil(n_plots_dt_range / n_cols_dt_range)  # 行数

    """
    各日付の各時間帯における温度上昇と下降の手動変更回数をプロット
    """
    fig, axes = plt.subplots(n_rows_dt_range, n_cols_dt_range, figsize=(18, n_rows_dt_range * 5))  # サイズ調整

    if not isinstance(axes, np.ndarray):
        axes = np.array([axes])
    axes = axes.flatten()  # サブプロットを1次元化

    # 各日付について処理
    for idx, date in enumerate(date_range):
        ax = axes[idx]  # 現在のサブプロットを取得
        date_data = temp_df[temp_df['measured_at_jst'].dt.date == date.date()]

        manual_up_count = 0
        manual_down_count = 0

        # 各設定温度カラムについて処理
        for col in set_temperature_columns:
            # 前の時間帯との差を計算
            temp_diff = date_data[col].diff()

            # 手動変更をカウント
            manual_up_count += temp_diff[temp_diff == 0.5].count()
            manual_down_count += temp_diff[temp_diff == -0.5].count()

        # daily_summary に日付と手動変更回数を追加
        daily_summary.append({'Date': date.date(), 'Manual Up': manual_up_count, 'Manual Down': manual_down_count})

    daily_summary_df = pd.DataFrame(daily_summary)

    # プロットの軸範囲を設定
    ax.set_xlim([st_dt, ed_dt])
    ax.set_ylim([0, max(daily_summary_df[['Manual Up', 'Manual Down']].max(axis=1)) + 1])

    # 日付ごとの上下変動を折れ線グラフでプロット
    plt.figure(figsize=(12, 6))
    plt.plot(daily_summary_df['Date'], daily_summary_df['Manual Up'], marker='o', linestyle='-', color='red', label='Manual Up (+0.5)')
    plt.plot(daily_summary_df['Date'], daily_summary_df['Manual Down'], marker='o', linestyle='-', color='blue', label='Manual Down (-0.5)')
    plt.xlabel('Date')
    plt.ylabel('Count')
    plt.title('Daily Manual Temperature Changes')
    plt.grid(True)
    plt.legend()
    plt.show()

    """
    Display summary table
    """
    print("\nDaily Manual Temperature Change Summary:")
    display(daily_summary_df)

"""##外気温"""

# 天気データを浮動小数点に変換
def str2float(weather_data):
    try:
        return float(weather_data)
    except:
        return 0

def scraping(url, date, data_type):
    html = urllib.request.urlopen(url).read()
    soup = BeautifulSoup(html, 'html.parser')
    trs = soup.find("table", {"class": "data2_s"})
    if trs is None:
        print(f"Failed to find data table for {data_type} on {date}. URL: {url}")
        return []
    data_list_per_hour = []
    for tr in trs.findAll('tr')[2:]:
        tds = tr.findAll('td')

        if int(tds[0].string[:2]) != 24:
          dt = datetime.datetime(date.year, date.month, date.day, int(tds[0].string[:2]), int(tds[0].string[3:5]))
        else:
          dt = datetime.datetime(date.year, date.month, date.day, 0, int(tds[0].string[3:5])) + datetime.timedelta(days=1)

        if not tds or tds[1].string is None:
            break
        if data_type == 'temperature':
            data_list = [dt, str2float(tds[4].string)]
        elif data_type == 'solar':
            data_list = [dt, str2float(tds[11].string)]
        data_list_per_hour.append(data_list)
    return data_list_per_hour

def set_out_temp(df_all, df_d, df_h, proc_no=44, block_no=47662):
  day_list = df_d.select(pl.col('measured_at_jst').dt.date()).select(pl.col('measured_at_jst')).unique()

  df_out = pl.DataFrame({"measured_at_jst": [], "outdoor_temp": []})

  for di in range(len(day_list)):
    date = day_list[di].to_series()[0]

    # 東京
    temp_url = f"https://www.data.jma.go.jp/obd/stats/etrn/view/10min_s1.php?prec_no={proc_no}&block_no={block_no}&year={date.year}&month={date.month}&day={date.day}&view=p1"

    # 南海
    #temp_url = f"https://www.data.jma.go.jp/obd/stats/etrn/view/10min_s1.php?prec_no=62&block_no=47772&year={date.year}&month={date.month}&day={date.day}&view=p1"

    data = scraping(temp_url, date, 'temperature')

    df = pl.DataFrame(data, schema=["measured_at_jst", "outdoor_temp"])

    if di == 0:
      df_out = df
    else:
      df_out = df_out.vstack(df)

  if len(df_out) > 0:
    df_all = df_all.join(df_out, on='measured_at_jst', how='left').fill_null(strategy='forward').fill_null(strategy='backward')

    df_out_d = df_out.group_by_dynamic("measured_at_jst", every="1d").agg(pl.col("*").mean())
    df_d = df_d.join(df_out_d['measured_at_jst', 'outdoor_temp'], on='measured_at_jst', how='left')

    df_out_h = df_out.group_by_dynamic("measured_at_jst", every="1h").agg(pl.col("*").mean())
    df_h = df_h.join(df_out_h['measured_at_jst', 'outdoor_temp'], on='measured_at_jst', how='left')

  return df_all, df_h, df_d

"""##可視化"""

def visualize_summury(df_all, df_h, df_d):
    day_list = df_d.select(pl.col('measured_at_jst').dt.date()).select(pl.col('measured_at_jst')).unique()
    zone_num = len(df_airid)

    ##------温度
    plt.figure(figsize=(24,10))

    for di in range(len(day_list)):
        df = df_h.filter(pl.col('measured_at_jst').dt.date() == day_list[di])
        df = df[:,:zone_num+1].with_columns(pl.mean_horizontal(pl.col(df.columns[1:zone_num+1])).alias('mean'))

        if df_d.select(pl.col("^*airplug_control_on.*$"))[di,0] is not None and df_d.select(pl.col("^*airplug_control_on.*$"))[di,0] > 0.3:
            color = 'blue'
        else:
            color = 'gray'

        plt.plot(df['measured_at_jst'].dt.hour(), df['mean'], label=day_list[di].to_numpy()[0], color=color)

    plt.xticks(np.arange(st_h,ed_h+1,1))
    plt.ylim([22,28])
    plt.grid(alpha=0.5)

    plt.figure()
    # Check if values has the expected shape and data type
    if values.shape[0] > 0 and values.ndim == 2:
        plt.bar(['AirPlug temrerature error', 'Conv. temrerature error'], values[0,4:6], color=['blue', 'gray'])
    else:
        print("Warning: 'values' has unexpected shape or data type. Skipping temperature error bar plot.")

    ##----指標
    print('AirPlug mean temrerature : ', values[0,0])
    print('Conv. mean temrerature : ', values[0,1])
    print('AirPlug std temrerature : ', values[0,2])
    print('Conv. std temrerature : ', values[0,3])
    print('AirPlug error temrerature : ', values[0,4])
    print('Conv. error temrerature : ', values[0,5])
    print('AirPlug count : ', values[0,6])
    print('Conv. count : ', values[0,7])
    print('AirPlug rate : ', values[0,8])
    print('Conv. rate : ', values[0,9])

    ##---電気使用量h
    plt.figure(figsize=(24,10))

    for di in range(len(day_list)):
        df = df_h.filter(pl.col('measured_at_jst').dt.date() == day_list[di])
        if df_d.select(pl.col("^*airplug_control_on.*$"))[di,0] is not None and df_d.select(pl.col("^*airplug_control_on.*$"))[di,0] > 0.3:

            color = 'blue'
        else:
            color = 'gray'

        plt.plot(df['measured_at_jst'].dt.hour(), df['Total'], label=day_list[di].to_numpy()[0], color=color)

    plt.xticks(np.arange(st_h,ed_h+1,1))
    plt.grid(alpha=0.5)

    ##---電気使用量day
    airplug_on_cols = [col for col in df_d.columns if 'airplug_control_on' in col]
    if not airplug_on_cols:
        print("Error: No column containing 'airplug_control_on' found in df_d.")
        return

    if 'outdoor_temp' not in df_d.columns:
            df_d = df_d.with_columns(pl.lit(None).cast(pl.Float64).alias('outdoor_temp'))

    airplug_on_col = airplug_on_cols[0]
    df_on = df_d.filter(pl.col(airplug_on_col) > 0.3).select('measured_at_jst', 'Total', 'outdoor_temp')
    df_off = df_d.filter(pl.col(airplug_on_col) < 0.3).select('measured_at_jst', 'Total', 'outdoor_temp')

    plt.figure(figsize=(24,10))
    fig, ax1 = plt.subplots(figsize=(24,10))
    df_on = df_on.with_columns(pl.col('Total').fill_null(0))
    df_off = df_off.with_columns(pl.col('Total').fill_null(0))

    ax1.bar(df_on['measured_at_jst'], df_on['Total'], label='AirPlug ON', color='blue')
    ax1.bar(df_off['measured_at_jst'], df_off['Total'], label='AirPlug OFF', color='gray')

    ax2 = ax1.twinx()
    ax2.plot(df_d['measured_at_jst'], df_d['outdoor_temp'], label='outdoor temp', color='black')
    plt.legend()

    display(df_on.mean())
    display(df_off.mean())

    visualize_daily_usage_CHx(df_d)

    ##---外気温vs使用量
    plt.figure()
    plt.scatter(df_on['outdoor_temp'], df_on['Total'], label='AirPlug ON', color='blue')
    plt.scatter(df_off['outdoor_temp'], df_off['Total'], label='AirPlug OFF', color='gray')
    plt.legend()

def visualize_summury_for_slim(df_all, df_h, df_d):
    # 日付リストを抽出
    day_list = df_d.select(pl.col('measured_at_jst').dt.date()).select(pl.col('measured_at_jst')).unique()
    zone_num = len(df_all)

    plt.figure(figsize=(24,10))

    # 各日ごとのグラフ描画
    for di in range(len(day_list)):
        # df_hから対象日付のデータを抽出
        df = df_h.filter(pl.col('measured_at_jst').dt.date() == day_list[di])
        df = df[:,:zone_num+1].with_columns(pl.mean_horizontal(pl.col(df.columns[1:zone_num+1])).alias('mean'))

        # df_d内のairplug_control_onの値に応じて色を変更
        if df_d.select(pl.col("^*airplug_control_on.*$"))[di, 0] > 0.3:
            color = 'blue'
        else:
            color = 'gray'

        plt.plot(df['measured_at_jst'].dt.hour(), df['Total'],
                 label=day_list[di].to_numpy()[0], color=color)


    # ※st_h, ed_hはコード内で定義されている前提です
    plt.xticks(np.arange(st_h, ed_h+1, 1))
    plt.grid(alpha=0.5)

    # airplug_on_colの取得はdf_dのカラムから行う
    airplug_on_col = [col for col in df_d.columns if 'airplug_control_on' in col][0]

    # AirPlug ON/OFFの日ごとのデータ抽出
    df_on = df_d.filter(pl.col(airplug_on_col) > 0.3).select('measured_at_jst', 'Total', 'outdoor_temp')
    df_off = df_d.filter(pl.col(airplug_on_col) < 0.3).select('measured_at_jst', 'Total', 'outdoor_temp')

    plt.figure(figsize=(24,10))
    fig, ax1 = plt.subplots(figsize=(24,10))

    # 'Total'のNone値を0に置換してから描画
    df_on = df_on.with_columns(pl.col('Total').fill_null(0))
    df_off = df_off.with_columns(pl.col('Total').fill_null(0))

    ax1.bar(df_on['measured_at_jst'], df_on['Total'], label='AirPlug ON', color='blue')
    ax1.bar(df_off['measured_at_jst'], df_off['Total'], label='AirPlug OFF', color='gray')

    ax2 = ax1.twinx()
    ax2.plot(df_d['measured_at_jst'], df_d['outdoor_temp'], label='outdoor temp', color='black')

    plt.legend()

    display(df_on.mean())
    display(df_off.mean())

    visualize_daily_usage_CHx(df_d)

    # 外気温と使用量の散布図
    plt.figure()
    plt.scatter(df_on['outdoor_temp'], df_on['Total'], label='AirPlug ON', color='blue')
    plt.scatter(df_off['outdoor_temp'], df_off['Total'], label='AirPlug OFF', color='gray')
    plt.legend()

def visualize_daily_usage_CHx(df_d):
    # 対象の airplug_control_on 列（例："airplug_control_on_〇〇"）を取得
    airplug_on_col = [col for col in df_d.columns if 'airplug_control_on' in col][0]

    # CHx(kW) 列を抽出（例："CH1(kW)", "CH2(kW)", ...）
    ch_cols = [col for col in df_d.columns if col.startswith("CH") and "(kW)" in col]

    # AL制御：airplug_control_on > 0.3、従来制御：airplug_control_on < 0.3
    df_AL = df_d.filter(pl.col(airplug_on_col) > 0.3).select(["measured_at_jst", *ch_cols, "outdoor_temp"])
    df_conv = df_d.filter(pl.col(airplug_on_col) < 0.3).select(["measured_at_jst", *ch_cols, "outdoor_temp"])

    # 各チャネルの None 値を 0 に置換
    for c in ch_cols:
        df_AL = df_AL.with_columns(pl.col(c).fill_null(0))
        df_conv = df_conv.with_columns(pl.col(c).fill_null(0))

    # 日時を matplotlib 用の数値に変換（日時軸表示用）
    dates_conv = mdates.date2num(df_conv['measured_at_jst'].to_list())
    dates_AL   = mdates.date2num(df_AL['measured_at_jst'].to_list())

    # matplotlib のデフォルトカラ―サイクルを取得
    default_colors = plt.rcParams['axes.prop_cycle'].by_key()['color']

    # サブプロット作成（左：従来制御、右：AL制御）
    fig, (ax_conv, ax_AL) = plt.subplots(1, 2, figsize=(24, 10))

    # ----- 従来制御のスタックドバーチャート -----
    bottom_conv = np.zeros(len(df_conv))
    for i, c in enumerate(ch_cols):
        color = default_colors[i % len(default_colors)]
        ax_conv.bar(dates_conv, df_conv[c].to_numpy(), bottom=bottom_conv,
                    label=c, color=color)
        bottom_conv += df_conv[c].to_numpy()

    ax_conv.xaxis_date()
    ax_conv.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
    ax_conv.set_title("従来制御 (AirPlug OFF)")
    ax_conv.set_xlabel("日時")
    ax_conv.set_ylabel("電気使用量 (kW)")
    ax_conv.grid(alpha=0.5)
    ax_conv.legend(loc='upper left')

    # ツイン軸で外気温をプロット（黒）
    ax_conv_twin = ax_conv.twinx()
    ax_conv_twin.plot(dates_conv, df_conv['outdoor_temp'], label='外気温', color='black')
    ax_conv_twin.set_ylabel("外気温")

    # ----- AL制御のスタックドバーチャート -----
    bottom_AL = np.zeros(len(df_AL))
    for i, c in enumerate(ch_cols):
        color = default_colors[i % len(default_colors)]
        ax_AL.bar(dates_AL, df_AL[c].to_numpy(), bottom=bottom_AL,
                  label=c, color=color)
        bottom_AL += df_AL[c].to_numpy()

    ax_AL.xaxis_date()
    ax_AL.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
    ax_AL.set_title("AL制御 (AirPlug ON)")
    ax_AL.set_xlabel("日時")
    ax_AL.set_ylabel("電気使用量 (kW)")
    ax_AL.grid(alpha=0.5)
    ax_AL.legend(loc='upper left')

    # ツイン軸で外気温をプロット（黒）
    ax_AL_twin = ax_AL.twinx()
    ax_AL_twin.plot(dates_AL, df_AL['outdoor_temp'], label='外気温', color='black')
    ax_AL_twin.set_ylabel("外気温")

    plt.tight_layout()
    plt.show()

"""##データフレームの保存"""

def save_df(df_all, df_h, df_d, df_airid, floor, st, ed, sys, energy):
    base_dir = '/content/drive/Shareddrives/internal_Shared_engineer/500_Soft/Data_Algo/Indiv_AC_CS/Customer'
    output_folder = os.path.join(base_dir, customer_dir.strip('/'), add_dir.strip('/'))

    os.makedirs(output_folder, exist_ok=True)
    base_filename_part = f"floor{floor}_start_{st}_ed_{ed}{sys}_{energy}"

    def rename_columns_with_display_name(df, airid_df):
        df_renamed = df.clone()
        airid_map = {row['id']: row['display_name'] for row in airid_df.to_dicts()}
        zoneid_map = {row['zone_id']: row['display_name'] for row in airid_df.to_dicts()}

        rename_dict = {}
        for current_col in df_renamed.columns:
            new_col = current_col
            for air_id, display_name in airid_map.items():
                if air_id in new_col:
                    new_col = new_col.replace(air_id, display_name)
            if new_col == current_col:
                 for zone_id, display_name in zoneid_map.items():
                     if zone_id in new_col:
                         new_col = new_col.replace(zone_id, display_name)

            if new_col != current_col:
                rename_dict[current_col] = new_col

        if rename_dict:
            df_renamed = df_renamed.rename(rename_dict)
        return df_renamed

    # --- df_d (日単位) の処理 ---
    try:
        df_d_renamed = rename_columns_with_display_name(df_d, df_airid)
        df_d_pd = df_d_renamed.to_pandas()
        file_path_d = os.path.join(output_folder, f"df_day_{base_filename_part}.csv")
        df_d_pd.to_csv(file_path_d, encoding='utf-8-sig', index=False)
        print(f"Daily data saved to: {file_path_d}")
    except Exception as e:
        print(f"Error saving daily data to {file_path_d}: {e}")

    # --- df_h (時間単位) の処理 ---
    try:
        df_h_renamed = rename_columns_with_display_name(df_h, df_airid)
        df_h_pd = df_h_renamed.to_pandas()
        file_path_h = os.path.join(output_folder, f"df_hour_{base_filename_part}.csv")
        df_h_pd.to_csv(file_path_h, encoding='utf-8-sig', index=False)
        print(f"Hourly data saved to: {file_path_h}")
    except Exception as e:
        print(f"Error saving hourly data to {file_path_h}: {e}")

    # --- df_all (分単位) の処理 ---
    try:
        df_all_renamed = rename_columns_with_display_name(df_all, df_airid)
        df_all_pd = df_all_renamed.to_pandas()
        file_path_min = os.path.join(output_folder, f"df_min_{base_filename_part}.csv")
        df_all_pd.to_csv(file_path_min, encoding='utf-8-sig', index=False)
        print(f"Minutely data saved to: {file_path_min}")
    except Exception as e:
        print(f"Error saving minutely data to {file_path_min}: {e}")

"""##ユーティリティ"""

def check_csv_exists(ekind='master'):
    base_dir = '/content/drive/Shareddrives/internal_Shared_engineer/500_Soft/Data_Algo/Indiv_AC_CS/Customer/'
    file_path = base_dir + customer_dir + add_dir + '/' + ekind + '.csv'

    if os.path.isfile(file_path):
        try:
            pl.read_csv(file_path)
            return True
        except Exception as e:
            return False
    else:
        return False

def convert_to_ymd(st_dt, ed_dt):
    st_dt_ymd = st_dt.strftime('%Y-%m-%d')
    ed_dt_ymd = ed_dt.strftime('%Y-%m-%d')
    return st_dt_ymd, ed_dt_ymd

def filter_df_by_ymdhms(df):
    return df.filter(
        (pl.col("measured_at_jst") >= st_dt_ymdhms) &
        (pl.col("measured_at_jst") <= ed_dt_ymdhms)
    )

def generate_datetime_from_str(time_str):
    jst = pytz.timezone('Europe/London')
    now = datetime.datetime.now(jst)

    if time_str == 'now':
        return now.strftime('%Y-%m-%d %H:%M:%S')

    if 'h' in time_str:
        num = int(time_str.replace('h', ''))
        delta = now - datetime.timedelta(hours=num)
        return delta.strftime('%Y-%m-%d %H:%M:%S')

    if 'd' in time_str:
        num = int(time_str.replace('d', ''))
        delta = now - datetime.timedelta(days=num)
        return delta.strftime('%Y-%m-%d %H:%M:%S')

    return now

def generate_llm_report(df_all, df_h, df_d, values, graph_paths, report_params):
    """LLMを使用して分析レポートを生成し保存する関数"""
    try:
        SECRET_KEY_NAME = 'AIzaSyAbUOYuYwT2xpt0E-gps4HSzbp44iywN6I' # <- ここをSecretsで設定した名前に置き換えてください

        try:
            GOOGLE_API_KEY = userdata.get(SECRET_KEY_NAME)
        except userdata.SecretNotFoundError:
            print(f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            print(f"エラー: Google Colab Secretsに '{SECRET_KEY_NAME}' という名前のシークレットが見つかりません。")
            print(f"Colabの左側にある鍵アイコンをクリックし、Gemini APIキーが正しい名前で登録されているか確認してください。")
            print(f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            # APIキーがない場合はレポート生成をスキップ
            print("LLMレポート生成をスキップします。")
            return # レポート生成処理を中断
        except Exception as e:
            print(f"Colab Secretsへのアクセス中にエラーが発生しました: {e}")
            print("LLMレポート生成をスキップします。")
            return

        # APIキーが正常に取得できた場合のみ configure を実行
        if GOOGLE_API_KEY:
            try:
                genai.configure(api_key=GOOGLE_API_KEY)
                print("Gemini API Key configured successfully.")
            except Exception as e:
                print(f"Gemini APIの設定中にエラーが発生しました: {e}")
                print("LLMレポート生成をスキップします。")
                return
        else:
            # userdata.get() が空文字列などを返した場合のフォールバック
            print(f"エラー: Secretsから取得したAPIキーが空です (名前: '{SECRET_KEY_NAME}')。")
            print("LLMレポート生成をスキップします。")
            return

        # --- 使用モデル選択 (Visionモデルで画像を扱えるように) ---
        model = genai.GenerativeModel('gemini-pro-vision')

        # --- LLMに渡す情報の準備 ---
        # データフレームをPandasに変換 (LLMに渡しやすくするため、必要に応じて)
        df_all_pd = df_all.to_pandas() if df_all is not None else pd.DataFrame()
        df_h_pd = df_h.to_pandas() if df_h is not None else pd.DataFrame()
        df_d_pd = df_d.to_pandas() if df_d is not None else pd.DataFrame()

        # 指標の取得 (values配列が存在し、要素数が十分か確認)
        mean_on = values[0, 0] if values is not None and values.shape[1] > 0 else 'N/A'
        mean_off = values[0, 1] if values is not None and values.shape[1] > 1 else 'N/A'
        std_on = values[0, 2] if values is not None and values.shape[1] > 2 else 'N/A'
        std_off = values[0, 3] if values is not None and values.shape[1] > 3 else 'N/A'
        null_rate = values[0, 10] if values is not None and values.shape[1] > 10 else 'N/A'

        # 日別エネルギー消費量の平均 (df_dから計算)
        airplug_on_col_d = next((col for col in df_d.columns if 'airplug_control_on' in col), None)
        energy_mean_on = 'N/A'
        energy_mean_off = 'N/A'
        if 'Total' in df_d.columns and airplug_on_col_d:
            try:
                on_mean = df_d.filter(pl.col(airplug_on_col_d) > 0.3)['Total'].mean()
                energy_mean_on = f"{on_mean:.2f}" if on_mean is not None else 'N/A'
            except: pass # フィルタ結果が空の場合など
            try:
                off_mean = df_d.filter(pl.col(airplug_on_col_d) < 0.3)['Total'].mean()
                energy_mean_off = f"{off_mean:.2f}" if off_mean is not None else 'N/A'
            except: pass

        # --- プロンプト作成 ---
        prompt = f"""
        ## 空調システム分析レポート

        **分析条件:**
        - **顧客名:** {report_params.get('customer_dir', 'N/A')}
        - **フロア:** {report_params.get('floor_name', 'N/A')}
        - **分析期間:** {report_params.get('st_dt_jst', 'N/A')} ～ {report_params.get('ed_dt_jst', 'N/A')}
        - **分析時間帯:** {report_params.get('st_h', 'N/A')}時 ～ {report_params.get('ed_h', 'N/A')}時
        - **システム種別:** {report_params.get('sys_kind', 'N/A')}
        - **データ欠損率(室内環境):** {null_rate if isinstance(null_rate, str) else f'{null_rate:.2f}'}%

        **主要指標 (AirPlug制御ON vs 従来制御OFF):**
        - **平均室温:** ON={mean_on if isinstance(mean_on, str) else f'{mean_on:.2f}'}℃, OFF={mean_off if isinstance(mean_off, str) else f'{mean_off:.2f}'}℃
        - **室温標準偏差:** ON={std_on if isinstance(std_on, str) else f'{std_on:.2f}'}, OFF={std_off if isinstance(std_off, str) else f'{std_off:.2f}'}
        - **日平均電力消費量(Total):** ON={energy_mean_on} kWh, OFF={energy_mean_off} kWh
        * (注: 上記指標は指定された分析時間帯における集計値です)

        **データ概要:**
        * **日別集計 (抜粋):**
        ```
        {df_d_pd.head().to_string()}
        ```
        * **時間別集計 (抜粋):**
        ```
        {df_h_pd.head().to_string()}
        ```

        **分析依頼:**
        上記データと添付のグラフに基づき、以下の観点から分析レポートを作成してください。

        1.  **温度環境:** AirPlug制御(ON)時と従来制御(OFF)時の室温変化、安定性、目標温度への追従性の違いを比較してください。グラフ(特に各ゾーンの温度推移やサマリー)から読み取れる特徴も記述してください。
        2.  **電力消費:** AirPlug ON/OFFでの電力消費量の違いを分析してください。特に、日別・時間別の傾向、外気温との相関（散布図や日別グラフ参照）について考察し、省エネ効果について言及してください。チャネル別(CHx)のグラフも参考にしてください。
        3.  **操作・快適性:** ボタン操作のヒートマップやリモコンの手動変更回数のグラフから、利用者の操作傾向（暑い/寒いと感じる頻度、時間帯など）を分析し、AirPlug制御が快適性や操作負担に与えた影響を考察してください。
        4.  **総合評価:** AirPlugシステムの導入効果（温度安定性、省エネ性、快適性向上など）を総合的に評価し、考察や今後の改善提案があれば記述してください。

        **添付グラフ:**
        (以下のグラフ画像の内容を考慮して分析してください)
        """

        # --- 画像の準備 ---
        input_parts = [prompt]
        image_count = 0
        print("Preparing images for LLM...")
        for graph_type, file_path in graph_paths.items():
             if file_path and os.path.exists(file_path):
                 try:
                     print(f"Loading image: {file_path}")
                     img = PIL.Image.open(file_path)
                     input_parts.append(f"\n- {graph_type}:") # グラフの種類をテキストで示す
                     input_parts.append(img)
                     image_count += 1
                 except Exception as e:
                     print(f"Warning: Failed to load image {file_path}: {e}")
             else:
                 print(f"Warning: Graph file path not found or invalid for {graph_type}: {file_path}")

        if image_count == 0:
             print("Warning: No valid images found. Report will be generated based on text data only.")
             # Visionモデルではなくテキストモデルを使う場合はここで切り替えも検討
             # model = genai.GenerativeModel('gemini-pro')
             # input_parts = [prompt + "\n(グラフ画像はありません)"]

        # --- LLM API呼び出し ---
        print("Generating report using Gemini API...")
        # response = model.generate_content(input_parts, request_options={"timeout": 600}) # 長時間かかる場合にタイムアウト設定
        response = model.generate_content(input_parts)


        # --- レポートの保存 ---
        base_dir = '/content/drive/Shareddrives/internal_Shared_engineer/500_Soft/Data_Algo/Indiv_AC_CS/Customer'
        output_folder = os.path.join(base_dir, report_params.get('customer_dir','').strip('/'), report_params.get('add_dir','').strip('/'))
        os.makedirs(output_folder, exist_ok=True) # 保存先フォルダがなければ作成
        base_filename_part = f"floor{report_params.get('floor_name','unknown')}_start_{report_params.get('st_dt','unknown').replace(':','-').replace(' ','_')}_ed_{report_params.get('ed_dt','unknown').replace(':','-').replace(' ','_')}_{report_params.get('sys_kind','')}_{report_params.get('energy_kind','')}"
        report_filepath = os.path.join(output_folder, f"report_{base_filename_part}.md") # Markdown形式で保存

        with open(report_filepath, 'w', encoding='utf-8') as f:
            f.write(response.text)
        print(f"\nReport successfully saved to: {report_filepath}")

        # --- 生成されたレポートの表示 (オプション) ---
        print("\n--- Generated Report (Preview) ---")
        print(response.text[:1000] + "...") # 長い場合に一部表示
        print("----------------------------------")

    except Exception as e:
        print(f"\nAn error occurred during LLM report generation: {e}")
        import traceback
        traceback.print_exc()

"""# #4:処理"""

def exec(
        custom_customer_dir='/三菱UFJ銀行',
        custom_add_dir='/10.データ解析',
        custom_sumit_id="120005",
        custom_floor_id="210001",
        custom_proc_no=44,
        custom_block_no=47662,
        custom_floor_name="11F",
        custom_sys_kind='plus',
        custom_energy_kind='master',
        custom_energy_format_type='mufg',
        custom_exclusion_date_list=None,
        custom_st_dt='2024-11-06 14:59:00',
        custom_ed_dt='2024-11-08 14:59:00',
        custom_st_h=7,
        custom_ed_h=18,
        custom_si='1'
    ):

    global customer_dir, add_dir, sumit_id, floor_id, proc_no, block_no, floor_name, sys_kind, energy_kind, energy_format_type, exclusion_date_list, st_dt, ed_dt, st_h, ed_h, energy_df, st_dt_utc, ed_dt_utc, st_dt_jst, ed_dt_jst, st_dt_ymd, ed_dt_ymd, st_dt_ymdhms, ed_dt_ymdhms, notBizDayList, si, df_airid, df_airplug, df_aircond, df_target, df_aclog, values, df_combine, df_zid, df_all, df_h, df_d, master_path, has_csv

    customer_dir = custom_customer_dir
    add_dir = custom_add_dir
    sumit_id = custom_sumit_id
    floor_id = custom_floor_id
    proc_no = custom_proc_no
    block_no = custom_block_no
    floor_name = custom_floor_name
    sys_kind = custom_sys_kind
    energy_kind = custom_energy_kind
    energy_format_type = custom_energy_format_type
    exclusion_date_list = custom_exclusion_date_list
    st_dt = custom_st_dt
    ed_dt = custom_ed_dt
    st_h = custom_st_h
    ed_h = custom_ed_h
    has_csv = check_csv_exists(energy_kind)

    if has_csv:
        create_master_csv(customer_dir, add_dir, energy_format_type)
        energy_df, st_dt_utc, ed_dt_utc, st_dt_jst, ed_dt_jst = get_dt(energy_kind, sys_kind, st_dt, ed_dt)
        notBizDayList = _getNotBizDay(st_dt_jst, ed_dt_jst)
        st_dt_ymdhms = datetime.datetime.strptime(st_dt_jst, '%Y-%m-%d %H:%M:%S')
        ed_dt_ymdhms = datetime.datetime.strptime(ed_dt_jst, '%Y-%m-%d %H:%M:%S')
    else:
        if ed_dt == 'now':
            st_dt = generate_datetime_from_str(st_dt)
            ed_dt = generate_datetime_from_str(ed_dt)

        notBizDayList = _getNotBizDay(st_dt, ed_dt)
        st_dt_ymdhms = datetime.datetime.strptime(st_dt, '%Y-%m-%d %H:%M:%S')
        ed_dt_ymdhms = datetime.datetime.strptime(ed_dt, '%Y-%m-%d %H:%M:%S')

    st_dt_ymd, ed_dt_ymd = convert_to_ymd(st_dt_ymdhms, ed_dt_ymdhms)

    si = '1' #リサンプリング間隔[min]

    params = {
        "customer_dir": customer_dir,
        "add_dir": add_dir,
        "sumit_id": sumit_id,
        "floor_id": floor_id,
        "proc_no": proc_no, # 外気温取得用都道府県番号
        "block_no": block_no, # 外気温取得用エリア番号
        "floor_name": floor_name,
        "sys_kind": sys_kind, # plus, slim
        "energy_kind": energy_kind,
        "energy_format_type": energy_format_type, # mufg, PRT, hioki_cloud, dk, hioki_cloud
        "exclusion_date_list": exclusion_date_list, # ['YYYY-MM-DD'...]
        "st_dt": st_dt,
        "ed_dt": ed_dt,
        "st_h": st_h,
        "ed_h": ed_h,
        "si": si # '1', '5', &60'
    }

    print("--- パラメータ一覧 ---")
    for key, value in params.items():
      print(f"{key}: {value}")
    print("--------------------")

    print("---------------------------------------------------------")
    print(floor_name)

    df_zid, e = get_zone_id(floor_id)

    if sys_kind == 'plus':
        print('sys_kind', sys_kind)
        if e:
          print("Error zone id")

        df_airplug, e = get_df_raw(df_zid, notBizDayList, si)
        if e:
            print("Error airplug")

        df_airid, e_airid = get_airid(df_zid)
        if e_airid:
            print("Error airrid")

        df_aircond, e_aircond = get_df_air(df_airid, notBizDayList, si)

        if e_aircond:
          print("Error airconditioner")

        df_target, e = get_df_target(df_airid)
        if e:
            print("Error target")

        df_aclog, e = get_df_aclog(df_airid, notBizDayList, si)

        if e:
          print("Error AC log")

        if e_airid and e_aircond:
            print('設備なし')
            visualize(df_aircond)

            # Initialize 'values'
            fi = 0

            df_aircond = df_aircond.filter(
                (pl.col("measured_at_jst") >= st_dt_ymdhms) &
                (pl.col("measured_at_jst") <= ed_dt_ymdhms)
            )

            values = np.zeros((1, 11))
            values[fi, :], df_combine = calc_res(df_airid, df_aircond, st_h, ed_h)

            df_all, df_h, df_d = calc_energy(st_h, ed_h, energy_kind, df_combine)
            df_all, df_h, df_d = zone_bt(df_all, df_h, df_d, df_airid, floor_id)
            df_all, df_h, df_d = set_out_temp(df_all, df_d, df_h, proc_no, block_no)

            df_all = filter_df_by_ymdhms(df_all)
            df_h = filter_df_by_ymdhms(df_h)
            df_d = filter_df_by_ymdhms(df_d)

            visualize_bt(df_all, df_h, df_d)
            visualize_remote_control(df_all, df_h, df_d, st_dt_ymdhms, ed_dt_ymdhms)
            visualize_summury(df_all, df_h, df_d)

            calc_bt(df_all, df_h, df_d, df_airid)

            save_df(df_all, df_h, df_d, df_airid, floor_name, st_dt, ed_dt, sys_kind, energy_kind)
        else:
            print('設備あり')
            visualize(df_airplug, df_aircond, df_target)

            # Initialize 'values'
            fi = 0

            df_airplug = filter_df_by_ymdhms(df_airplug)
            df_aircond = filter_df_by_ymdhms(df_aircond)
            df_target = filter_df_by_ymdhms(df_target)
            df_aclog = filter_df_by_ymdhms(df_aclog)

            values = np.zeros((1, 11))

            values[fi, :], df_combine = calc_res(df_airid, df_airplug, df_aircond, df_target, df_aclog, st_h, ed_h)

            df_all, df_h, df_d = calc_energy(st_h, ed_h, energy_kind, df_combine)
            df_all, df_h, df_d = zone_bt(df_all, df_h, df_d, df_airid, floor_id)
            df_all, df_h, df_d = set_out_temp(df_all, df_d, df_h, proc_no, block_no)

            df_all = filter_df_by_ymdhms(df_all)
            df_h = filter_df_by_ymdhms(df_h)
            df_d = filter_df_by_ymdhms(df_d)

            visualize_bt(df_all, df_h, df_d)
            visualize_remote_control(df_all, df_h, df_d, st_dt_ymdhms, ed_dt_ymdhms)
            visualize_summury(df_all, df_h, df_d)

            calc_bt(df_all, df_h, df_d, df_airid)

            save_df(df_all, df_h, df_d, df_airid, floor_name, st_dt, ed_dt, sys_kind, energy_kind)
    elif sys_kind == 'slim':
        print('sys_kind', sys_kind)
        si = '1' #リサンプリング間隔[min]
        df_zid, e = get_zone_id(floor_id)
        df_airid, e = get_airid(df_zid)

        fi = 0
        values = np.zeros((1, 11))
        df_combine = []

        df_all, df_h, df_d = calc_energy_for_slim(st_h, ed_h, energy_kind)
        df_all, df_h, df_d = set_out_temp(df_all, df_d, df_h, proc_no, block_no)

        df_all = excludeNotBizDays(df_all, notBizDayList)
        df_h = excludeNotBizDays(df_h, notBizDayList)
        df_d = excludeNotBizDays(df_d, notBizDayList)

        df_all = filter_df_by_ymdhms(df_all)
        df_h = filter_df_by_ymdhms(df_h)
        df_d = filter_df_by_ymdhms(df_d)

        visualize_summury_for_slim(df_all, df_h, df_d)
        visualize_remote_control(df_all, df_h, df_d, st_dt_ymdhms, ed_dt_ymdhms)
        save_df(df_all, df_h, df_d, df_airid, floor_name, st_dt, ed_dt, sys_kind, energy_kind)
    elif sys_kind == 'middle':
        mean_list = []
        std_list = []
        e_temp_list = []
        count_list = []
        ac_rate_list = []
        null_rate_list = []

        dur = str(1440*1) #表示期間[min]
        si = '1'

        print("---------------------------------------------------------")
        df_zid, e = get_zone_id(floor_id)
        df_airid, e = get_airid(df_zid)

        df_airplug, e = get_df_raw(df_airid, si)

        df_aircond, e = get_df_air(df_airid, si)

        #df_target, e = get_df_target(df_airid)

        #visualize(df_airplug, df_aircond, df_target)
        visualize(df_airplug, df_aircond)
        #visualize(df_airplug, df_aircond,  df_aircond)
    else:
        print('sys_kindが指定されていません')

"""# #5:パラメータ設定"""

customer_dir='/野村不動産'
add_dir='/Data'
sumit_id="210007" # 魔法（エンジニア）
floor_id="300003" # 魔法（エンジニア）
proc_no=44
block_no=47662
floor_name="2F"
energy_format_type = "dk" # mufg, PRT, hioki_cloud, dk, hioki_cloud
exclusion_date_list = [
    '2025-02-12',
    '2025-02-13',
]
sys_kind='plus' # 固定
energy_kind='master' # 固定
st_dt='2025-02-03 00:00:00' # 02-03～02-07：従来①、 # 02-10～02-14：AL①
ed_dt='2025-02-14 23:59:00'
st_h=8
ed_h=20
si='1' # 固定

customer_dir='/三菱UFJ銀行'
add_dir='/Data'
sumit_id="120005" # 魔法（エンジニア）
floor_id="210001" # 魔法（エンジニア）
proc_no=44
block_no=47662
floor_name="11F"
sys_kind='plus'
energy_kind='master' # 固定
energy_format_type='mufg'
exclusion_date_list = [
    '2025-01-13',
    '2025-01-14',
    '2025-01-15',
    '2025-01-16',
]
st_dt='2024-11-06 14:59:00'
ed_dt='2024-11-08 14:59:00'
st_h=7
ed_h=18
si='1'

customer_dir='/中央日土地'
add_dir='/Data'
sumit_id="210005"
floor_id="330001"
proc_no=44
block_no=47662
floor_name="9F"
energy_format_type = "hioki_local" # mufg, PRT, hioki_cloud, dk, hioki_cloud
sys_kind='slim' # plus, slim
energy_kind='master'
st_dt='2024-12-09 00:00:00'
ed_dt='2025-01-17 23:59:00'
st_h=8
ed_h=20
si='1' # 固定

customer_dir='/東急不動産/ビジネスエアポート新橋/6F'
add_dir='/raw_data'
sumit_id="270002"
floor_id="240003"
proc_no=62
block_no=47772
floor_name="6F"
energy_format_type = "hioki_local" # mufg, PRT, hioki_cloud, dk, hioki_cloud
sys_kind='plus'
energy_kind='master' # 固定
st_dt='2025-03-28 00:00:00'
ed_dt='2025-04-10 23:59:00'
st_h=8
ed_h=20
si='1' # 固定

customer_dir='/東急不動産/ビジネスエアポート新橋/8F'
add_dir='/raw_data'
sumit_id="210006"
floor_id="300002"
proc_no=62
block_no=47772
floor_name="8F"
energy_format_type = "hioki_local" # mufg, PRT, hioki_cloud, dk, hioki_cloud
sys_kind='plus'
energy_kind='master' # 固定
st_dt='2025-03-28 00:00:00'
ed_dt='2025-04-10 23:59:00'
st_h=8
ed_h=20
si='1' # 固定

customer_dir='/東京海上日動/虎ノ門東京海上日動ビル/4F'
add_dir='/raw_data'
sumit_id="390001"
floor_id="390001"
floor_name="4F"
energy_format_type = "hioki_local" # mufg, PRT, hioki_cloud, dk, hioki_cloud
sys_kind='plus'
energy_kind='master' # 固定
st_dt='2025-04-07 00:00:00'
ed_dt='2025-04-14 23:59:00'
st_h=8
ed_h=20
si='1' # 固定

customer_dir='/東京海上日動/虎ノ門東京海上日動ビル/4F-2'
add_dir='/raw_data'
sumit_id="390001"
floor_id="390001"
floor_name="4F"
energy_format_type = "hioki_local" # mufg, PRT, hioki_cloud, dk, hioki_cloud
sys_kind='plus'
energy_kind='master' # 固定
st_dt='2025-04-07 00:00:00'
ed_dt='2025-04-14 23:59:00'
st_h=8
ed_h=20
si='1' # 固定

customer_dir = '/東京建物/日本橋ビル/10F'
add_dir = '/raw_data'
sumit_id="120005"
floor_id="210002"
floor_name="10F"
energy_format_type = "PRT" # mufg, PRT, hioki_cloud, dk, hioki_cloud
sys_kind = 'plus'
energy_kind = 'master'
st_dt = '2025-02-10 08:00:00'
ed_dt = '2025-03-07 18:00:00'
st_h = 8
ed_h = 18
si = '1'

customer_dir='/東急不動産/ビジネスエアポート新橋/6-2F'
add_dir='/raw_data'
sumit_id="270002" # スプレッドシート「CS件名リスト」参照
floor_id="240003" # スプレッドシート「CS件名リスト」参照
proc_no=44  # 気象庁データの都道府県番号（エンジニア）
block_no=47662 # 気象庁データのエリア番号（エンジニア）
floor_name="6F"
sys_kind='plus' # plus, slim
energy_kind='master' # 固定
energy_format_type='hioki_local' # mufg, PRT, dk, hioki_local, hioki_cloud
exclusion_date_list = [] # 空リスト or 日付のリスト（例：['2024-11-06'...]）
st_dt='1h'
ed_dt='now'
st_h=8
ed_h=20
si='1' # 固定

customer_dir='/東急不動産/ビジネスエアポート新橋/6F'
add_dir='/raw_data'
sumit_id="270002" # スプレッドシート「CS件名リスト」参照
floor_id="240003" # スプレッドシート「CS件名リスト」参照
proc_no=44  # 気象庁データの都道府県番号（エンジニア）
block_no=47662 # 気象庁データのエリア番号（エンジニア）
floor_name="6F"
sys_kind='plus' # plus, slim
energy_kind='master' # 固定
energy_format_type='hioki_local' # mufg, PRT, dk, hioki_local, hioki_cloud
exclusion_date_list = [] # 空リスト or 日付のリスト（例：['2024-11-06'...]）
st_dt='2025-2-25 00:00:00'
ed_dt='2024-4-25 23:59:00'
st_h=6
ed_h=23
si='1' # 固定

# 稼働確認
customer_dir='/東京海上日動/虎ノ門東京海上日動ビル/4F-2'
add_dir='/'
sumit_id="390001"
floor_id="390001"
floor_name="4F"
energy_format_type = "hioki_local" # mufg, PRT, hioki_cloud, dk, hioki_cloud
sys_kind='plus'
energy_kind='master' # 固定
st_dt='3d'
ed_dt='now'
st_h=8
ed_h=20
si='1' # 固定

# 稼働確認
customer_dir='/慶応大学（Middle POC）/三田キャンパス北館B1'
add_dir='/Data'
sumit_id="570001"
floor_id="570001"
poc_no=44
block_no=47662
floor_name="B1"
energy_format_type = "mufg" # mufg, PRT, hioki_cloud, dk, hioki_cloud
sys_kind='plus'
exclusion_date_list = []
energy_kind='master' # 固定
st_dt='2025-03-28 00:00:00'
ed_dt='2025-04-10 23:59:00'
st_h=0
ed_h=24
si='1' # 固定

customer_dir='/慶應大学（Middle POC）/三田キャンパス北館B1'
add_dir='/Data'
sumit_id="570001" # スプレッドシート「CS件名リスト」参照
floor_id="570001" # スプレッドシート「CS件名リスト」参照
proc_no=44  # 気象庁データの都道府県番号（エンジニア）
block_no=47662 # 気象庁データのエリア番号（エンジニア）
floor_name="B1"
sys_kind='plus' # plus, slim
energy_kind='master' # 固定
energy_format_type='mufg' # mufg, PRT, dk, hioki_local, hioki_cloud
exclusion_date_list = [] # 空リスト or 日付のリスト（例：['2024-11-06'...]）
st_dt='2025-05-15 14:59:00'
ed_dt='2025-06-15 14:59:00'
st_h=0
ed_h=24
si='1' # 固定

customer_dir='/神戸アイセンター（Middle POC）/4F'
add_dir='/Data'
sumit_id='600001'
floor_id='600001'
proc_no=63
block_no=1587
floor_name='4F'
sys_kind='plus'
energy_kind='master'
energy_format_type='mufg'
exclusion_date_list=[]
st_dt='2025-05-10 13:59:00'
ed_dt='2025-05-10 15:25:00'
st_h=9
ed_h=17
si='1'

customer_dir='/KONAMIスポーツクラブ（Middle POC）/3F'
add_dir='/Data'
sumit_id="630001" # スプレッドシート「CS件名リスト」参照
floor_id="630001" # スプレッドシート「CS件名リスト」参照
proc_no=44  # 気象庁データの都道府県番号（エンジニア）
block_no=47662 # 気象庁データのエリア番号（エンジニア）
floor_name="3F"
sys_kind='plus' # plus, slim
energy_kind='master' # 固定
energy_format_type='mufg' # mufg, PRT, dk, hioki_local, hioki_cloud
exclusion_date_list = [] # 空リスト or 日付のリスト（例：['2024-11-06'...]）
st_dt='2025-05-24 00:00:00'
ed_dt='2025-06-06 23:00:00'
st_h=10
ed_h=23
si='1' # 固定

"""# #6:実行"""

exec(
    custom_customer_dir=customer_dir,
    custom_add_dir=add_dir,
    custom_sumit_id=sumit_id,
    custom_floor_id=floor_id,
    custom_proc_no=proc_no, # 外気温取得用都道府県番号
    custom_block_no=block_no, # 外気温取得用エリア番号
    custom_floor_name=floor_name,
    custom_sys_kind=sys_kind, # plus, slim
    custom_energy_kind=energy_kind,
    custom_energy_format_type=energy_format_type, # mufg, PRT, hioki_cloud, dk, hioki_cloud
    custom_exclusion_date_list=exclusion_date_list, # ['YYYY-MM-DD'...]
    custom_st_dt=st_dt,
    custom_ed_dt=ed_dt,
    custom_st_h=st_h,
    custom_ed_h=ed_h,
    custom_si=si # '1', '5', &60'
)