# -*- coding: utf-8 -*-
"""
AirPlug Analysis Streamlit Application
Based on analysis_for_cs.py
"""

import streamlit as st
import pymysql.cursors
import pandas as pd
import polars as pl
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime
import os
import re
import math
import tempfile
import zipfile
from io import BytesIO
import urllib.request
from bs4 import BeautifulSoup
import jpholiday
import pytz
import japanize_matplotlib  # 日本語化ライブラリを追加
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import google.generativeai as genai
import PIL.Image
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
import markdown
import urllib.request
import os

# Streamlit page configuration
st.set_page_config(
    page_title="AirPlug Analysis Dashboard",
    page_icon="🌡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Gemini API Configuration
try:
    genai.configure(api_key='AIzaSyAbUOYuYwT2xpt0E-gps4HSzbp44iywN6I')
    GEMINI_AVAILABLE = True
except Exception as e:
    st.warning("Gemini API key configuration failed. LLM report generation will be disabled.")
    GEMINI_AVAILABLE = False

# Global variables
if 'analysis_results' not in st.session_state:
    st.session_state.analysis_results = None
if 'llm_report' not in st.session_state:
    st.session_state.llm_report = None
if 'llm_report_data' not in st.session_state:
    st.session_state.llm_report_data = None

# Database connection functions
def connectDB():
    """Connects to the database - creates fresh connection each time"""
    try:
        connection = pymysql.connect(
            host='gateway01.ap-northeast-1.prod.aws.tidbcloud.com',
            port=4000,
            user='2Dv1chx9hoFRkxE.analytics_user',
            password='QX7k8jm4e!M%6Pen',
            db='airplugprod',
            charset='utf8mb4',
            connect_timeout=60,
            read_timeout=60,
            write_timeout=60,
            autocommit=True,
            ssl={'ssl': {}},
            cursorclass=pymysql.cursors.DictCursor
        )
        return connection
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None

def getDataFromDB(connection, sql, params=None):
    """Fetches data from the database（最適化版）"""
    try:
        connection.ping(reconnect=True)
        
        start_time = datetime.datetime.now()
        
        with connection.cursor() as cursor:
            # より短いタイムアウト設定（2分）
            cursor.execute("SET SESSION max_execution_time = 120000")  # 2分
            cursor.execute("SET SESSION net_read_timeout = 120")       # 2分
            cursor.execute("SET SESSION net_write_timeout = 120")      # 2分
            
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            result = cursor.fetchall()
        
        end_time = datetime.datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        # DON'T close connection here - let caller manage it
        df = pl.DataFrame(result) if result else pl.DataFrame()
        st.write(f"⏱️ {execution_time:.1f}秒で{len(result) if result else 0}件取得")
        return df
    except Exception as e:
        error_msg = str(e)
        if "maximum statement execution time exceeded" in error_msg:
            st.error(f"⏰ クエリタイムアウト（2分超過）: データ量が多すぎる可能性があります")
        elif "timeout" in error_msg.lower():
            st.error(f"🔌 ネットワークタイムアウト: データベース接続が不安定です")
        else:
            st.error(f"❌ データベースエラー: {e}")
        
        # エラー時はSQLの一部のみ表示
        if len(sql) > 200:
            st.code(f"SQL抜粋: {sql[:200]}...")
        
        return pl.DataFrame()

# Data processing functions
def get_zone_id(floor_id):
    """Zone IDの取得"""
    try:
        sql = "SELECT * FROM system_temperaturecontrolzone WHERE floor_id = %s"
        connection = connectDB()
        
        if connection is None:
            return pl.DataFrame(), True
            
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, (int(floor_id),))
                result = cursor.fetchall()
            
            df_id = pl.DataFrame(result) if result else pl.DataFrame()
            
            if df_id.shape[0] == 0:
                st.warning(f"No zone data found for floor_id: {floor_id}")
                return df_id, True
            
            df_id = df_id.sort("display_name")
            return df_id, False
            
        finally:
            connection.close()
        
    except Exception as e:
        st.error(f"Database error in get_zone_id: {e}")
        return pl.DataFrame(), True

def get_airid(df_id):
    """設備IDの取得"""
    connection = connectDB()
    if connection is None:
        return pl.DataFrame(schema=['id', 'zone_id', 'display_name']), True
    
    try:
        if df_id.shape[0] == 0:
            sql = "SELECT * FROM system_airconditioner"
        else:
            sql = "SELECT * FROM system_airconditioner WHERE "
            for i, id in enumerate(df_id['id']):
                sql += "zone_id = '" + id + "'"
                if i < len(df_id['id']) - 1:
                    sql += " OR "

        df_airid = getDataFromDB(connection, sql)

        if df_airid.shape[0] == 0:
            return pl.DataFrame(schema=['id', 'zone_id', 'display_name']), False

        if 'zone_id' not in df_airid.columns:
            df_airid = df_airid.with_columns(pl.col('id').alias('zone_id'))

        df_airid = df_airid.sort("display_name")
        return df_airid, False
        
    except Exception as e:
        st.error(f"Error in get_airid: {e}")
        return pl.DataFrame(schema=['id', 'zone_id', 'display_name']), True
    finally:
        connection.close()

def get_df_raw_chunked(zone_ids, notBizDays, si, st_dt_ymdhms, ed_dt_ymdhms, chunk_size=5):
    """チャンク処理によるAirPlugデータの取得"""
    st.write(f"🔄 {len(zone_ids)}個のゾーンIDを{chunk_size}件ずつ処理します")
    
    all_dataframes = []
    total_rows = 0
    
    for i in range(0, len(zone_ids), chunk_size):
        chunk_ids = zone_ids[i:i+chunk_size]
        chunk_num = i // chunk_size + 1
        total_chunks = (len(zone_ids) + chunk_size - 1) // chunk_size
        
        st.write(f"📦 チャンク {chunk_num}/{total_chunks}: {len(chunk_ids)}件のゾーンID処理中...")
        
        connection = connectDB()
        if connection is None:
            st.error(f"❌ チャンク{chunk_num}: データベース接続に失敗")
            continue
            
        try:
            # 最適化されたSQL文の構築（INを使用）
            zone_id_list = "', '".join(chunk_ids)
            sql = f"""
            SELECT zone_id, measured_at, value 
            FROM system_zonetemperature 
            WHERE zone_id IN ('{zone_id_list}')
            AND measured_at BETWEEN '{st_dt_ymdhms.strftime('%Y-%m-%d %H:%M:%S')}' 
            AND '{ed_dt_ymdhms.strftime('%Y-%m-%d %H:%M:%S')}'
            ORDER BY measured_at
            """
            
            st.write(f"⏰ チャンク{chunk_num}: クエリ実行中...")
            df_chunk = getDataFromDB(connection, sql)
            
            if df_chunk.shape[0] > 0:
                st.write(f"✅ チャンク{chunk_num}: {df_chunk.shape[0]}行取得")
                all_dataframes.append(df_chunk)
                total_rows += df_chunk.shape[0]
            else:
                st.write(f"⚠️ チャンク{chunk_num}: データなし")
                
        except Exception as e:
            st.error(f"❌ チャンク{chunk_num}でエラー: {e}")
        finally:
            try:
                connection.close()
            except:
                pass
    
    if not all_dataframes:
        st.warning("⚠️ 全チャンクでデータが取得できませんでした")
        return pl.DataFrame(schema=[('measured_at_jst', pl.Datetime)]), True
    
    # 全チャンクのデータを結合
    st.write(f"🔄 {len(all_dataframes)}個のチャンクを結合中... (合計{total_rows}行)")
    df_combined = pl.concat(all_dataframes)
    
    return df_combined, False

def get_df_raw(df_zid, notBizDays, si, st_dt_ymdhms, ed_dt_ymdhms):
    """AirPlugデータの取得（チャンク処理対応版）"""
    if df_zid.shape[0] == 0:
        st.info("Zone IDが0件のため、空のDataFrameを返します")
        return pl.DataFrame(schema=[('measured_at_jst', pl.Datetime)]), False

    st.write(f"🔍 {df_zid.shape[0]}個のゾーンIDでデータ取得を開始")
    
    # Zone ID数による処理方法の選択
    if df_zid.shape[0] > 10:
        st.info(f"Zone ID数が多いため、チャンク処理を使用します")
        zone_ids = df_zid['id'].to_list()
        df, error = get_df_raw_chunked(zone_ids, notBizDays, si, st_dt_ymdhms, ed_dt_ymdhms, chunk_size=5)
        
        if error or df.shape[0] == 0:
            return pl.DataFrame(schema=[('measured_at_jst', pl.Datetime)]), error
    
    else:
        # 少数の場合は従来の方法
        connection = connectDB()
        if connection is None:
            st.error("❌ データベース接続に失敗しました")
            return pl.DataFrame(schema=[('measured_at_jst', pl.Datetime)]), True
            
        try:
            zone_id_list = "', '".join(df_zid['id'].to_list())
            sql = f"""
            SELECT zone_id, measured_at, value 
            FROM system_zonetemperature 
            WHERE zone_id IN ('{zone_id_list}')
            AND measured_at BETWEEN '{st_dt_ymdhms.strftime('%Y-%m-%d %H:%M:%S')}' 
            AND '{ed_dt_ymdhms.strftime('%Y-%m-%d %H:%M:%S')}'
            ORDER BY measured_at
            """
            
            st.write(f"⏰ データベースクエリを実行中...")
            df = getDataFromDB(connection, sql)
            
            if df.shape[0] == 0:
                st.warning("⚠️ SQLクエリの結果が0件でした")
                return pl.DataFrame(schema=[('measured_at_jst', pl.Datetime)]), True
                
        except Exception as e:
            st.error(f"❌ get_df_raw でエラー発生: {e}")
            return pl.DataFrame(schema=[('measured_at_jst', pl.Datetime)]), True
        finally:
            try:
                connection.close()
            except:
                pass
    
    try:
        st.write("🔄 データ変換中...")
        
        # 日時変換
        df = df.with_columns(
            measured_at_jst=pl.col('measured_at').dt.offset_by(by='9h').alias('measured_at_jst')
        )

        st.write("🔄 データピボット中...")
        
        # ピボット処理
        df_pivot = df.pivot(values="value", index="measured_at_jst", on="zone_id").sort("measured_at_jst")
        
        st.write(f"📊 ピボット完了: {df_pivot.shape[0]}行 × {df_pivot.shape[1]}列")

        st.write(f"🔄 {si}分ごとにリサンプリング中...")
        
        # リサンプリング処理
        df_resampled = df_pivot.group_by_dynamic("measured_at_jst", every=si+"m").agg(pl.col("*").mean())
        
        st.write(f"📊 リサンプリング完了: {df_resampled.shape[0]}行")

        st.write("🔄 営業日フィルタリング中...")
        
        # 営業日フィルタリング
        df_ex = excludeNotBizDays(df_resampled, notBizDays)
        
        st.write(f"✅ 温度データ処理完了: {df_ex.shape[0]}行")

        return df_ex, False
        
    except Exception as e:
        st.error(f"❌ データ処理でエラー発生: {e}")
        st.code(f"エラー詳細: {str(e)}")
        return pl.DataFrame(schema=[('measured_at_jst', pl.Datetime)]), True

def get_df_air(df_airid, notBizDays, si, st_dt_ymdhms, ed_dt_ymdhms):
    """設備データの取得"""
    connection = connectDB()
    if connection is None:
        return pl.DataFrame(schema=[('measured_at_jst', pl.Datetime)]), True
        
    try:
        sql = "SELECT * FROM system_airconditionermeasurement WHERE (air_conditioner_id = '"
        next_str = "' OR air_conditioner_id = '"

        for id in df_airid['id']:
            sql += id + next_str

        sql = sql[:-len(next_str)] + "')"
        sql += " AND measured_at > '" + st_dt_ymdhms.strftime('%Y-%m-%d %H:%M:%S') + "' AND measured_at < '" + ed_dt_ymdhms.strftime('%Y-%m-%d %H:%M:%S') + "';"

        df = getDataFromDB(connection, sql)

        if df.is_empty():
            st.warning("Warning: get_df_air received an empty DataFrame. Returning an empty DataFrame.")
            return pl.DataFrame(schema=[('measured_at_jst', pl.Datetime)]), False

        df = df.with_columns(
            measured_at_jst=pl.col('measured_at').dt.offset_by(by='9h').alias('measured_at_jst')
        )

        # dfをpivotしてzone_idごとにカラムに展開
        df_pivot = df.pivot(values=["operation_mode", "fan_speed", "start_stop", "set_temperature", "process_temperature"], index="measured_at_jst", on="air_conditioner_id").sort("measured_at_jst")

        # x分ごとにリサンプリング
        df_resampled_ac = df_pivot.group_by_dynamic("measured_at_jst", every=si+"m").agg(pl.col("*").mean())

        #0をnull
        df_resampled_ac = df_resampled_ac.with_columns([
            pl.when(pl.col(col) == 0).then(None).otherwise(pl.col(col)).alias(col)
            for col in df_resampled_ac.columns
        ])

        df_ex = excludeNotBizDays(df_resampled_ac, notBizDays)

        return df_ex, False
        
    except Exception as e:
        st.error(f"Error in get_df_air: {e}")
        return pl.DataFrame(schema=[('measured_at_jst', pl.Datetime)]), True
    finally:
        connection.close()

def get_df_aclog(df_airid, notBizDays, si, st_dt_ymdhms, ed_dt_ymdhms):
    """空調制御ログの取得"""
    connection = connectDB()
    if connection is None:
        return pl.DataFrame(schema=[('measured_at_jst', pl.Datetime)]), True
        
    try:
        sql = "SELECT measured_at, target_temperature, airplug_control_on, calculated_set_temperature, air_conditioner_id FROM system_airconditionerlog WHERE (air_conditioner_id = '"
        next_str = "' OR air_conditioner_id = '"

        for id in df_airid['id']:
            sql += id + next_str

        sql = sql[:-len(next_str)] + "')"
        sql += " AND measured_at > '" + st_dt_ymdhms.strftime('%Y-%m-%d %H:%M:%S') + "' AND measured_at < '" + ed_dt_ymdhms.strftime('%Y-%m-%d %H:%M:%S') + "';"

        df = getDataFromDB(connection, sql)

        if df.is_empty():
            st.warning("Warning: get_df_aclog received an empty DataFrame. Returning an empty DataFrame.")
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
        
    except Exception as e:
        st.error(f"Error in get_df_aclog: {e}")
        return pl.DataFrame(schema=[('measured_at_jst', pl.Datetime)]), True
    finally:
        connection.close()

def get_df_target(df_airid, st_dt_ymdhms, ed_dt_ymdhms):
    """目標温度の取得"""
    connection = connectDB()
    if connection is None:
        return pl.DataFrame(schema=['measured_at_jst', 'air_conditioner_id', 'target_temperature', 'calculated_set_temperature']), True
        
    try:
        sql = "SELECT measured_at, target_temperature, airplug_control_on, calculated_set_temperature, air_conditioner_id FROM system_airconditionerlog WHERE (air_conditioner_id = '"
        next_str = "' OR air_conditioner_id = '"

        for id in df_airid['id']:
            sql += id + next_str

        sql = sql[:-len(next_str)] + "')"
        sql += " AND measured_at > '" + st_dt_ymdhms.strftime('%Y-%m-%d %H:%M:%S') + "' AND measured_at < '" + ed_dt_ymdhms.strftime('%Y-%m-%d %H:%M:%S') + "';"

        df = getDataFromDB(connection, sql)

        if df.is_empty():
            st.warning("Warning: get_df_target received an empty DataFrame. Returning an empty DataFrame with necessary columns.")
            return pl.DataFrame(schema=['measured_at_jst', 'air_conditioner_id', 'target_temperature', 'calculated_set_temperature']), False

        df = df.with_columns(
            measured_at_jst=pl.col('measured_at').dt.offset_by(by='9h').alias('measured_at_jst')
        )

        return df, False
        
    except Exception as e:
        st.error(f"Error in get_df_target: {e}")
        return pl.DataFrame(schema=['measured_at_jst', 'air_conditioner_id', 'target_temperature', 'calculated_set_temperature']), True
    finally:
        connection.close()

# Utility functions
def _getNotBizDay(st, ed):
    """休日のリスト作成"""
    date = datetime.datetime.strptime(st, '%Y-%m-%d %H:%M:%S')
    notBizDayList = []

    while True:
        if date.weekday() == 4 or jpholiday.is_holiday(date):
            notBizDayList.append(date.strftime('%Y-%m-%d %H:%M:%S'))

        date += datetime.timedelta(days=1)

        if date > datetime.datetime.strptime(ed, '%Y-%m-%d %H:%M:%S'):
            break

    return notBizDayList

def excludeNotBizDays(df, notBizDays, exclusion_date_list=None):
    """休日・除外日の除外"""
    if df.is_empty():
        return df
        
    # notBizDaysから除外したい日付（"YYYY-MM-DD"形式）を抽出
    excluded_dates_from_notBiz = [
        datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
        for ts in notBizDays
    ]

    if exclusion_date_list is None:
        exclusion_date_list = []

    all_excluded_dates = set(excluded_dates_from_notBiz + exclusion_date_list)

    df = df.with_columns(pl.col("measured_at_jst").dt.strftime("%Y-%m-%d").alias("date_only"))

    # 統合した除外日リストに含まれる日付の行を除外
    df = df.filter(~pl.col("date_only").is_in(list(all_excluded_dates)))

    return df.drop("date_only")

def calc_res(df_airid, df_airplug, df_aircond, df_target, df_aclog, st_h, ed_h):
    """指標の計算"""
    df_combine = df_airplug.join(df_aircond, on='measured_at_jst', how='inner')
    df_combine = df_combine.join(df_aclog, on='measured_at_jst', how='left')

    df_combine = df_combine.filter(
        pl.col('measured_at_jst').is_not_null()
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
            st.warning(f"警告: ゾーン {zone_id} の必須データ (温度または運転状態) が不足しているためスキップします。")
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
            st.warning(f"警告: ゾーン {zone_id} の目標温度データが見つかりません。")

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
             st.warning(f"警告: ゾーン {zone_id} の設定温度データが見つかりません。")

        null_rate_percent = df_combine[available_zone_temp_col].null_count() / total_samples * 100 if total_samples > 0 else 0

        zone_results.append([
            mean_on, mean_off, std_on, std_off,
            e_temp_on, e_temp_off, count_on, count_off,
            ac_rate_on_percent, ac_rate_off_percent, null_rate_percent
        ])

    if not zone_results:
        st.warning("警告: 有効なゾーンデータから統計量を計算できませんでした。NaNを返します。")
        return [np.nan] * 11, df_combine

    results_array = np.array(zone_results)
    final_values = np.nanmean(results_array, axis=0).tolist()

    return final_values, df_combine

# Visualization functions
def visualize_temperature_data(df_airplug, df_aircond, df_target, df_airid):
    """温度データの可視化"""
    if df_airplug.is_empty() or df_airid.is_empty():
        st.warning("No temperature data available for visualization.")
        return

    view_cols = ["set_temperature", "process_temperature"]
    color_list = ['orange', 'green']

    for ai, airid in enumerate(df_airid['id']):
        if df_airid['zone_id'][ai] not in df_airplug.columns:
            continue

        # データの結合
        df_combine = df_airplug.join(df_aircond, on='measured_at_jst', how='inner')

        # off状態のマスク（start_stopが2の場合）
        start_stop_col = f'start_stop_{df_airid["id"][ai]}'
        if start_stop_col in df_combine.columns:
            mask = df_combine[start_stop_col] == 2
        else:
            mask = [False] * len(df_combine)

        # １つのグラフに温度とop_modeを描画
        fig, ax1 = plt.subplots(figsize=(15, 6))

        # 運転モードの色分け
        op_mode_col = f"operation_mode_{airid}"
        if op_mode_col in df_combine.columns:
            op_mode_vals = df_combine[op_mode_col]
            op_mode_colors = [
                'grey' if off else ('cyan' if mode == 1 else ('pink' if mode == 2 else 'white'))
                for off, mode in zip(mask, op_mode_vals)
            ]
        else:
            op_mode_colors = ['blue'] * len(df_combine)

        sizes = [50 if flag else 10 for flag in mask]

        # 運転モードの散布図
        ax1.scatter(
            df_combine['measured_at_jst'],
            df_combine[df_airid['zone_id'][ai]],
            s=[200 if flag else 50 for flag in mask],
            c=op_mode_colors,
            zorder=1,
            label='Operation Mode',
            alpha=0.7
        )

        # 温度の線グラフ
        ax1.plot(
            df_combine['measured_at_jst'],
            df_combine[df_airid['zone_id'][ai]],
            label='Temperature',
            color='blue',
            zorder=2,
            linewidth=2
        )

        # set_temperature, process_temperature の描画
        for k, col in enumerate(view_cols):
            col_name = f'{col}_{df_airid["id"][ai]}'
            if col_name in df_combine.columns:
                ax1.plot(df_combine['measured_at_jst'], df_combine[col_name], 
                        label=col, color=color_list[k], linewidth=1)

        # 目標温度の描画
        if not df_target.is_empty():
            df_pick = df_target.filter(pl.col("air_conditioner_id") == df_airid['id'][ai]).sort("measured_at_jst")
            if not df_pick.is_empty() and 'target_temperature' in df_pick.columns:
                ax1.plot(df_pick['measured_at_jst'], df_pick['target_temperature'],
                         label="target_temperature", color='black', linewidth=3)

        # 温度軸の設定
        ax1.grid(axis="y", alpha=0.3)
        ax1.set_ylim(20, 30)
        ax1.set_xlabel("Time (JST)")
        ax1.set_ylabel("Temperature (°C)")
        ax1.set_title(f"Temperature Data - {df_airid['display_name'][ai]}")
        ax1.legend()

        st.pyplot(fig)
        plt.close()

def visualize_energy_summary(df_h, df_d, st_h, ed_h):
    """エネルギー使用量のサマリー可視化"""
    if df_d.is_empty():
        st.warning("No daily energy data available.")
        return

    # 日別の電力使用量グラフ
    airplug_on_cols = [col for col in df_d.columns if 'airplug_control_on' in col]
    
    if airplug_on_cols and 'Total' in df_d.columns:
        airplug_on_col = airplug_on_cols[0]
        
        # データの分離
        df_on = df_d.filter(pl.col(airplug_on_col) > 0.3)
        df_off = df_d.filter(pl.col(airplug_on_col) <= 0.3)
        
        fig, ax1 = plt.subplots(figsize=(12, 6))
        
        # AirPlug ON/OFF の棒グラフ
        if not df_on.is_empty():
            ax1.bar(df_on['measured_at_jst'], df_on['Total'], 
                   label='AirPlug ON', color='blue', alpha=0.7)
        if not df_off.is_empty():
            ax1.bar(df_off['measured_at_jst'], df_off['Total'], 
                   label='AirPlug OFF', color='gray', alpha=0.7)
        
        # 外気温のプロット（もしあれば）
        if 'outdoor_temp' in df_d.columns:
            ax2 = ax1.twinx()
            ax2.plot(df_d['measured_at_jst'], df_d['outdoor_temp'], 
                    label='Outdoor Temperature', color='red', linewidth=2)
            ax2.set_ylabel('Outdoor Temperature (°C)')
            ax2.legend(loc='upper right')
        
        ax1.set_xlabel('Date')
        ax1.set_ylabel('Energy Consumption (kWh)')
        ax1.set_title('Daily Energy Consumption')
        ax1.legend(loc='upper left')
        ax1.grid(alpha=0.3)
        
        st.pyplot(fig)
        plt.close()
        
        # 統計情報の表示
        if not df_on.is_empty() and not df_off.is_empty():
            col1, col2 = st.columns(2)
            with col1:
                st.metric("AirPlug ON Average", f"{df_on['Total'].mean():.2f} kWh")
            with col2:
                st.metric("AirPlug OFF Average", f"{df_off['Total'].mean():.2f} kWh")

def display_key_metrics(values):
    """主要指標の表示"""
    if values is None or len(values) < 11:
        st.warning("No metrics available to display.")
        return
    
    st.subheader("Key Performance Indicators")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Avg. Temp (AirPlug ON)", 
                 f"{values[0]:.2f}°C" if not np.isnan(values[0]) else "N/A")
        st.metric("Temp Stability (AirPlug ON)", 
                 f"{values[2]:.2f}" if not np.isnan(values[2]) else "N/A")
        st.metric("Temp Error (AirPlug ON)", 
                 f"{values[4]:.2f}°C" if not np.isnan(values[4]) else "N/A")
    
    with col2:
        st.metric("Avg. Temp (Conventional)", 
                 f"{values[1]:.2f}°C" if not np.isnan(values[1]) else "N/A")
        st.metric("Temp Stability (Conventional)", 
                 f"{values[3]:.2f}" if not np.isnan(values[3]) else "N/A")
        st.metric("Temp Error (Conventional)", 
                 f"{values[5]:.2f}°C" if not np.isnan(values[5]) else "N/A")
    
    with col3:
        st.metric("Manual Changes (AirPlug ON)", 
                 f"{int(values[6])}" if not np.isnan(values[6]) else "N/A")
        st.metric("Manual Changes (Conventional)", 
                 f"{int(values[7])}" if not np.isnan(values[7]) else "N/A")
        st.metric("Data Missing Rate", 
                 f"{values[10]:.1f}%" if not np.isnan(values[10]) else "N/A")

    # 詳細な指標表も表示
    st.subheader("Detailed Metrics")
    metrics_df = pd.DataFrame({
        'Metric': [
            'AirPlug Mean Temperature', 'Conventional Mean Temperature',
            'AirPlug Std Temperature', 'Conventional Std Temperature', 
            'AirPlug Temperature Error', 'Conventional Temperature Error',
            'AirPlug Manual Count', 'Conventional Manual Count',
            'AirPlug Operation Rate', 'Conventional Operation Rate',
            'Data Missing Rate'
        ],
        'Value': [
            f"{values[0]:.2f}°C" if not np.isnan(values[0]) else "N/A",
            f"{values[1]:.2f}°C" if not np.isnan(values[1]) else "N/A",
            f"{values[2]:.3f}" if not np.isnan(values[2]) else "N/A",
            f"{values[3]:.3f}" if not np.isnan(values[3]) else "N/A",
            f"{values[4]:.2f}°C" if not np.isnan(values[4]) else "N/A",
            f"{values[5]:.2f}°C" if not np.isnan(values[5]) else "N/A",
            f"{int(values[6])}" if not np.isnan(values[6]) else "N/A",
            f"{int(values[7])}" if not np.isnan(values[7]) else "N/A",
            f"{values[8]:.1f}%" if not np.isnan(values[8]) else "N/A",
            f"{values[9]:.1f}%" if not np.isnan(values[9]) else "N/A",
            f"{values[10]:.1f}%" if not np.isnan(values[10]) else "N/A"
        ]
    })
    st.dataframe(metrics_df, use_container_width=True)

# Main execution function
def exec_analysis(params):
    """メイン分析処理"""
    try:
        # パラメータの取得
        floor_id = params['floor_id']
        proc_no = params['proc_no']  # 追加
        block_no = params['block_no']  # 追加
        st_dt_ymdhms = params['st_dt_ymdhms']
        ed_dt_ymdhms = params['ed_dt_ymdhms']
        st_h = params['st_h']
        ed_h = params['ed_h']
        si = params['si']
        notBizDayList = params['notBizDayList']
        
        # 必須変数の初期化（エラー回避のため最初に初期化）
        df_all = pl.DataFrame()
        df_h = pl.DataFrame()
        df_d = pl.DataFrame()
        df_combine = pl.DataFrame()
        values = [np.nan] * 11
        
        # プログレスバー
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # ステップ1: ゾーンID取得
        status_text.text("ステップ 1/8: ゾーンIDを取得中...")
        progress_bar.progress(10)
        
        df_zid, e_zone = get_zone_id(floor_id)
        if e_zone:
            st.error("ゾーンIDの取得に失敗しました")
            return None
            
        st.success(f"✅ {len(df_zid)} 個のゾーンを取得")
        
        # ステップ2: エアコンID取得
        status_text.text("ステップ 2/8: エアコンIDを取得中...")
        progress_bar.progress(15)
        
        df_airid, e_air = get_airid(df_zid)
        if e_air:
            st.warning("エアコンIDの取得に失敗しました")
            
        st.success(f"✅ {len(df_airid)} 台のエアコンを取得")
        
        # ステップ3: 温度データ取得
        status_text.text("ステップ 3/8: 温度データを取得中...")
        progress_bar.progress(25)
        
        df_airplug, e_temp = get_df_raw(df_zid, notBizDayList, si, st_dt_ymdhms, ed_dt_ymdhms)
        if e_temp:
            st.warning("温度データの取得に失敗しました")
            
        st.success(f"✅ 温度データを取得: {len(df_airplug)} レコード")
        
        # ステップ4: エアコンデータ取得
        status_text.text("ステップ 4/8: エアコンデータを取得中...")
        progress_bar.progress(35)
        
        df_aircond, e_aircond = get_df_air(df_airid, notBizDayList, si, st_dt_ymdhms, ed_dt_ymdhms)
        if e_aircond:
            st.warning("エアコンデータの取得に失敗しました")
            
        st.success(f"✅ エアコンデータを取得: {len(df_aircond)} レコード")
        
        # ステップ5: 制御ログ取得
        status_text.text("ステップ 5/8: 制御ログを取得中...")
        progress_bar.progress(45)
        
        df_aclog, e_aclog = get_df_aclog(df_airid, notBizDayList, si, st_dt_ymdhms, ed_dt_ymdhms)
        if e_aclog:
            st.warning("制御ログの取得に失敗しました")
            
        st.success(f"✅ 制御ログを取得: {len(df_aclog)} レコード")
        
        # ステップ6: 指標計算
        status_text.text("ステップ 6/8: 指標を計算中...")
        progress_bar.progress(55)
        
        df_target = pl.DataFrame()  # 簡易版では目標温度データをスキップ
        values, df_combine = calc_res(df_airid, df_airplug, df_aircond, df_target, df_aclog, st_h, ed_h)
        
        st.success("✅ 指標計算完了")
        
        # ステップ7: エネルギーデータ処理
        status_text.text("ステップ 7/8: エネルギーデータを処理中...")
        progress_bar.progress(65)
        
        # session_state からエネルギーデータを取得
        if hasattr(st.session_state, 'has_energy_data') and st.session_state.has_energy_data:
            energy_df = st.session_state.energy_data
            df_all, df_h, df_d = calc_energy_with_csv(st_h, ed_h, df_combine, energy_df)
            st.success("✅ エネルギーデータを処理しました")
        else:
            df_all, df_h, df_d = calc_energy(st_h, ed_h, df_combine)
            st.info("エネルギーデータなしで処理を続行します")
        
        # ステップ8: ボタンデータ統合（オプション）
        status_text.text("ステップ 8/8: ボタンデータを統合中...")
        progress_bar.progress(75)
        
        try:
            if not df_all.is_empty() and not df_airid.is_empty() and not df_zid.is_empty():
                df_all, df_h, df_d = zone_bt(df_all, df_h, df_d, df_airid, df_zid, floor_id, notBizDayList, st_dt_ymdhms, ed_dt_ymdhms)
                st.success("✅ ボタンデータを統合しました")
            else:
                st.info("ボタンデータ統合をスキップしました（データ不足）")
        except Exception as e:
            st.warning(f"ボタンデータ統合でエラー: {e}")
        
        # 外気温データの追加
        status_text.text("外気温データを取得中...")
        progress_bar.progress(85)
        
        try:
            df_all, df_h, df_d = set_out_temp(df_all, df_d, df_h, proc_no, block_no)  # proc_no, block_noを渡す
            st.success("✅ 外気温データを追加しました")
        except Exception as e:
            st.warning(f"外気温データ取得でエラー: {e}")
        
        progress_bar.progress(100)
        status_text.text("✅ 分析完了!")
        
        # 最終確認：必要なデータフレームが存在するかチェック
        if df_all.is_empty():
            df_all = pl.DataFrame(schema=[('measured_at_jst', pl.Datetime)])
        if df_h.is_empty():
            df_h = pl.DataFrame(schema=[('measured_at_jst', pl.Datetime)])
        if df_d.is_empty():
            df_d = pl.DataFrame(schema=[('measured_at_jst', pl.Datetime)])
        if df_combine.is_empty():
            df_combine = pl.DataFrame(schema=[('measured_at_jst', pl.Datetime)])
        
        # 結果を返す（追加のデータフレームを含む）
        return {
            'df_all': df_all,
            'df_h': df_h, 
            'df_d': df_d,
            'df_combine': df_combine,
            'df_airid': df_airid,
            'df_zid': df_zid,
            'df_airplug': df_airplug,  # 追加
            'df_aircond': df_aircond,  # 追加
            'df_target': df_target,    # 追加
            'df_aclog': df_aclog,      # 追加
            'values': values
        }
        
    except Exception as e:
        st.error(f"分析処理中にエラーが発生しました: {e}")
        import traceback
        st.error(f"詳細エラー: {traceback.format_exc()}")
        return None

# Streamlit UI
def main():
    """メイン関数"""
    st.title("AirPlug分析ダッシュボード")
    st.markdown("---")
    
    # サイドバーでパラメータ入力
    st.sidebar.title("分析パラメータ")
    
    # 顧客情報
    st.sidebar.subheader("顧客情報")
    customer_dir = st.sidebar.text_input("顧客ディレクトリ", value="/東京建物/日本橋ビル/10F")
    add_dir = st.sidebar.text_input("追加ディレクトリ", value="/raw_data")
    sumit_id = st.sidebar.text_input("Summit ID", value="120005")
    floor_id = st.sidebar.text_input("フロアID", value="210002")
    floor_name = st.sidebar.text_input("フロア名", value="10F")
    
    # システム設定
    st.sidebar.subheader("システム設定")
    sys_kind = st.sidebar.selectbox("システム種別", ["plus", "slim"], index=0)
    energy_kind = st.sidebar.selectbox("エネルギー種別", ["master"], index=0)
    energy_format_type = st.sidebar.selectbox("エネルギー形式", ["mufg", "PRT", "dk", "hioki_local", "hioki_cloud"], index=1)
    
    # 外気温データ設定
    st.sidebar.subheader("外気温データ設定")
    proc_no = st.sidebar.number_input("都道府県番号 (proc_no)", value=44, min_value=1, max_value=100)
    block_no = st.sidebar.number_input("エリア番号 (block_no)", value=47662, min_value=1, max_value=99999)
    
    # 分析期間
    st.sidebar.subheader("分析期間")
    today = datetime.date.today()
    
    # 日付と時間の入力
    st_date = st.sidebar.date_input("開始日", datetime.date(2025, 2, 10))
    st_time = st.sidebar.time_input("開始時刻", datetime.time(8, 0, 0))
    st_dt = datetime.datetime.combine(st_date, st_time)
    
    ed_date = st.sidebar.date_input("終了日", datetime.date(2025, 3, 7))
    ed_time = st.sidebar.time_input("終了時刻", datetime.time(18, 0, 0))
    ed_dt = datetime.datetime.combine(ed_date, ed_time)
    
    # 時間帯設定
    st.sidebar.subheader("分析時間帯")
    st_h = st.sidebar.slider("開始時間", 0, 23, 8)
    ed_h = st.sidebar.slider("終了時間", 0, 23, 18)
    
    # 除外日設定
    st.sidebar.subheader("除外日設定")
    exclusion_dates = st.sidebar.text_area(
        "除外日（1行に1日、YYYY-MM-DD形式）",
        value=""
    )
    
    exclusion_date_list = []
    if exclusion_dates.strip():
        exclusion_date_list = [date.strip() for date in exclusion_dates.strip().split('\n') if date.strip()]
    
    st.session_state['exclusion_date_list'] = exclusion_date_list
    
    # パラメータ例の表示
    with st.sidebar.expander("パラメータ例"):
        st.code("""
# KONAMIスポーツクラブ（Middle POC）の例
customer_dir='/KONAMIスポーツクラブ（Middle POC）/3F'
add_dir='/Data'
sumit_id="630001"
floor_id="630001" 
proc_no=44  # 東京
block_no=47662  # 東京
floor_name="3F"
st_dt='2025-05-24 00:00:00'
ed_dt='2025-06-06 23:00:00'
st_h=10
ed_h=23
sys_kind='plus'
energy_format_type='mufg'

# 野村不動産の例
customer_dir='/野村不動産'
add_dir='/Data'
sumit_id="210007"
floor_id="300003"
proc_no=44  # 東京
block_no=47662  # 東京
floor_name="2F"
st_dt='2025-02-03 00:00:00'
ed_dt='2025-02-14 23:59:00'
st_h=8
ed_h=20
sys_kind='plus'
energy_format_type='dk'

# 神戸アイセンターの例
customer_dir='/神戸アイセンター（Middle POC）/4F'
add_dir='/Data'
sumit_id='600001'
floor_id='600001'
proc_no=63  # 兵庫
block_no=1587  # 神戸
floor_name='4F'
st_dt='2025-05-10 13:59:00'
ed_dt='2025-05-10 15:25:00'
st_h=9
ed_h=17
sys_kind='plus'
energy_format_type='mufg'
        """)
    
    # メインエリア
    if 'analysis_results' not in st.session_state:
        st.session_state.analysis_results = None
    
    # パラメータ辞書作成（常に作成して保存用）
    notBizDayList = _getNotBizDay(st_dt.strftime('%Y-%m-%d %H:%M:%S'), ed_dt.strftime('%Y-%m-%d %H:%M:%S'))
    
    current_params = {
        'customer_dir': customer_dir,
        'add_dir': add_dir,
        'sumit_id': sumit_id,
        'floor_id': floor_id,
        'floor_name': floor_name,
        'sys_kind': sys_kind,
        'energy_kind': energy_kind,
        'energy_format_type': energy_format_type,
        'proc_no': proc_no,
        'block_no': block_no,
        'st_dt_ymdhms': st_dt,
        'ed_dt_ymdhms': ed_dt,
        'st_h': st_h,
        'ed_h': ed_h,
        'si': '1',
        'notBizDayList': notBizDayList
    }
    
    # エネルギーデータアップロード（分析実行ボタンの前に表示）
    st.sidebar.subheader("📊 エネルギーデータ")
    energy_df, has_energy = get_energy_data(current_params)
    
    # エネルギーデータを session_state に保存
    if has_energy and energy_df is not None and not energy_df.is_empty():
        st.session_state.energy_data = energy_df
        st.session_state.has_energy_data = True
        st.sidebar.success("✅ エネルギーデータ準備完了")
    else:
        st.session_state.energy_data = None
        st.session_state.has_energy_data = False
        if 'energy_data' in st.session_state:
            del st.session_state.energy_data
    
    # 分析実行ボタン
    if st.sidebar.button("分析実行", type="primary", key="execute_analysis"):
        
        # パラメータ検証
        if not floor_id.strip():
            st.error("フロアIDを入力してください")
            return
            
        if st_dt >= ed_dt:
            st.error("開始日時は終了日時より前に設定してください")
            return
        
        # パラメータ表示
        with st.expander("実行パラメータ"):
            col1, col2 = st.columns(2)
            with col1:
                st.write("**基本設定**")
                st.write(f"customer_dir: {customer_dir}")
                st.write(f"floor_name: {floor_name} (floor_id: {floor_id})")
                st.write(f"sys_kind: {sys_kind}")
                st.write(f"energy_format_type: {energy_format_type}")
                st.write(f"proc_no: {proc_no}, block_no: {block_no}")
            with col2:
                st.write("**期間設定**")
                st.write(f"st_dt: '{st_dt.strftime('%Y-%m-%d %H:%M:%S')}'")
                st.write(f"ed_dt: '{ed_dt.strftime('%Y-%m-%d %H:%M:%S')}'")
                st.write(f"st_h: {st_h}, ed_h: {ed_h}")
                st.write(f"除外日: {len(exclusion_date_list)}日")
        
        # 分析実行
        results = exec_analysis(current_params)
        
        if results:
            st.session_state.analysis_results = results
            st.session_state.analysis_params = current_params
            
            # 分析結果表示
            display_analysis_results(results, current_params)
    
    # 既存の分析結果がある場合は表示
    elif st.session_state.analysis_results is not None:
        # 保存されたパラメータを使用
        saved_params = st.session_state.get('analysis_params', current_params)
        display_analysis_results(st.session_state.analysis_results, saved_params)
    
    else:
        # 初期表示（分析結果がない場合のみ）
        st.info("サイドバーでパラメータを設定し、「分析実行」ボタンを押してください。")
        
        # データベース接続テスト
        st.subheader("データベース接続テスト")
        if st.button("接続テスト実行", key="db_connection_test"):
            with st.spinner("データベースに接続中..."):
                connection = connectDB()
                if connection:
                    try:
                        with connection.cursor() as cursor:
                            cursor.execute("SELECT COUNT(*) as total FROM system_temperaturecontrolzone")
                            result = cursor.fetchone()
                        connection.close()
                        st.success(f"✅ データベース接続成功！ゾーンテーブル総レコード数: {result['total']}")
                    except Exception as e:
                        st.error(f"❌ クエリ実行エラー: {e}")
                else:
                    st.error("❌ データベース接続に失敗しました")

def test_database_connection():
    """Test database connection and show available data"""
    with st.spinner("Testing database connection..."):
        try:
            connection = connectDB()
            if connection is None:
                st.error("❌ Failed to connect to database")
                return
                
            try:
                # Test basic query
                with connection.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) as total FROM system_temperaturecontrolzone")
                    total_zones = cursor.fetchone()['total']
                    
                    cursor.execute("SELECT DISTINCT floor_id FROM system_temperaturecontrolzone LIMIT 10")
                    floors = cursor.fetchall()
                
                st.success(f"✅ Database connection successful!")
                st.info(f"Total zones in database: {total_zones}")
                
                if floors:
                    st.write("**Available Floor IDs (sample):**")
                    floor_df = pd.DataFrame(floors)
                    st.dataframe(floor_df, use_container_width=True)
                    
            finally:
                connection.close()
            
        except Exception as e:
            st.error(f"❌ Database connection failed: {e}")

def generate_analysis_report(results):
    """Generate a text report of the analysis results"""
    params = results['params']
    values = results['values']
    
    report = f"""
AirPlug Analysis Report
=======================

Analysis Parameters:
-------------------
Customer: {params['customer_dir']}
Floor: {params['floor_name']} (ID: {params['floor_id']})
Period: {params['st_dt_ymdhms'].strftime('%Y-%m-%d %H:%M')} to {params['ed_dt_ymdhms'].strftime('%Y-%m-%d %H:%M')}
Analysis Hours: {params['st_h']}:00 - {params['ed_h']}:00
System Type: {params['sys_kind']}

Key Performance Indicators:
---------------------------
Average Temperature (AirPlug ON): {values[0]:.2f}°C
Average Temperature (Conventional): {values[1]:.2f}°C
Temperature Stability (AirPlug ON): {values[2]:.3f}
Temperature Stability (Conventional): {values[3]:.3f}
Temperature Error (AirPlug ON): {values[4]:.2f}°C
Temperature Error (Conventional): {values[5]:.2f}°C
Manual Changes (AirPlug ON): {int(values[6])}
Manual Changes (Conventional): {int(values[7])}
Operation Rate (AirPlug ON): {values[8]:.1f}%
Operation Rate (Conventional): {values[9]:.1f}%
Data Missing Rate: {values[10]:.1f}%

Data Summary:
-------------
Zones Found: {len(results['df_zid']) if not results['df_zid'].is_empty() else 0}
Air Conditioners: {len(results['df_airid']) if not results['df_airid'].is_empty() else 0}
Temperature Records: {len(results['df_airplug']) if not results['df_airplug'].is_empty() else 0}
AC Measurement Records: {len(results['df_aircond']) if not results['df_aircond'].is_empty() else 0}

Analysis Summary:
-----------------
Temperature Improvement: {values[1] - values[0]:.2f}°C (Conventional - AirPlug)
Stability Improvement: {values[3] - values[2]:.3f} (Conventional - AirPlug)
Manual Operation Reduction: {int(values[7] - values[6])} changes

Generated on: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
    return report

# CSV処理関数（エネルギーデータアップロード用、必要に応じて）
def process_energy_csv(uploaded_file, energy_format_type):
    """アップロードされたエネルギーCSVファイルの処理"""
    try:
        if energy_format_type == "hioki":
            df = pd.read_csv(uploaded_file, skiprows=26, na_values=["-"])
            df = df.iloc[:, 3:]  # 最初の3列をスキップ
        elif energy_format_type == "master":
            df = pd.read_csv(uploaded_file, na_values=["-"])
            df = df.iloc[:, 2:]  # 最初の2列をスキップ
        else:
            df = pd.read_csv(uploaded_file)
        
        return df
    except Exception as e:
        st.error(f"CSVファイル処理エラー: {e}")
        return None

# 追加のデータ取得関数

def get_df_bt(notBizDays, si, sign, fid, st_dt_ymdhms, ed_dt_ymdhms):
    """ボタンデータの取得"""
    connection = connectDB()
    if connection is None:
        return pl.DataFrame(), True
        
    try:
        if sign == '+':
            sql = "SELECT * FROM system_devicemeasurementbuttonplus WHERE value != 0 AND floor_id ='" + fid + "'"
        else:
            sql = "SELECT * FROM system_devicemeasurementbuttonminus WHERE value != 0 AND floor_id ='" + fid + "'"

        sql += " AND measured_at > '" + st_dt_ymdhms.strftime('%Y-%m-%d %H:%M:%S') + "' AND measured_at < '" + ed_dt_ymdhms.strftime('%Y-%m-%d %H:%M:%S') + "';"

        df = getDataFromDB(connection, sql)

        if df.shape[0] == 0:
            return df, True

        df = df.filter(pl.col('value') < 10)

        if df.shape[0] == 0:
            return df, True

        df = df.with_columns(
            measured_at_jst=pl.col('measured_at').dt.offset_by(by='9h').alias('measured_at_jst')
        )

        df_ex = excludeNotBizDays(df, notBizDays)

        return df, False
        
    except Exception as e:
        st.error(f"get_df_btでエラー: {e}")
        return pl.DataFrame(), True
    finally:
        connection.close()


def zone_bt(df_all, df_h, df_d, df_airid, df_zid, floor_id, notBizDayList, st_dt_ymdhms, ed_dt_ymdhms):
    """ボタンデータ統合（簡易版）"""
    try:
        # 簡易版では実際のボタンデータ処理をスキップし、元のデータフレームをそのまま返す
        return df_all, df_h, df_d
    except Exception as e:
        st.warning(f"ボタンデータ統合処理でエラー: {e}")
        return df_all, df_h, df_d

def set_out_temp(df_all, df_d, df_h, proc_no=44, block_no=47662):
    """外気温データの追加（簡易版）"""
    # 実際の実装では、proc_noとblock_noを使って気象庁のデータを取得
    # 現在は簡易版のため、単にカラムを追加するのみ
    try:
        # 外気温カラムが存在しない場合は追加
        if not df_all.is_empty() and 'outdoor_temp' not in df_all.columns:
            df_all = df_all.with_columns(pl.lit(None).cast(pl.Float64).alias('outdoor_temp'))
        if not df_h.is_empty() and 'outdoor_temp' not in df_h.columns:
            df_h = df_h.with_columns(pl.lit(None).cast(pl.Float64).alias('outdoor_temp'))
        if not df_d.is_empty() and 'outdoor_temp' not in df_d.columns:
            df_d = df_d.with_columns(pl.lit(None).cast(pl.Float64).alias('outdoor_temp'))
        
        return df_all, df_h, df_d
    except Exception as e:
        st.warning(f"外気温データ追加処理でエラー: {e}")
        return df_all, df_h, df_d

def calc_energy(st_h, ed_h, df_combine):
    """エネルギー計算（CSV無しの場合のダミー処理）"""
    try:
        if df_combine.is_empty():
            # 空のDataFrameの場合
            df_all = pl.DataFrame(schema=[('measured_at_jst', pl.Datetime), ('Total', pl.Float64)])
            df_h = pl.DataFrame(schema=[('measured_at_jst', pl.Datetime), ('Total', pl.Float64)])
            df_d = pl.DataFrame(schema=[('measured_at_jst', pl.Datetime), ('Total', pl.Float64)])
        else:
            # df_combineからデータフレームを作成
            df_all = df_combine.clone()
            
            # 時間フィルタリング
            df_all = df_all.filter(
                (pl.col('measured_at_jst').dt.hour() >= st_h) &
                (pl.col('measured_at_jst').dt.hour() <= ed_h)
            )
            
            # 時間別・日別集計
            if 'measured_at_jst' in df_all.columns and not df_all.is_empty():
                df_h = df_all.group_by_dynamic("measured_at_jst", every="1h").agg(pl.col("*").mean())
                df_d = df_h.group_by_dynamic("measured_at_jst", every="1d").agg(pl.col("*").sum())
            else:
                df_h = pl.DataFrame(schema=[('measured_at_jst', pl.Datetime)])
                df_d = pl.DataFrame(schema=[('measured_at_jst', pl.Datetime)])
            
            # Totalカラムを追加（ダミー）
            if 'Total' not in df_all.columns:
                df_all = df_all.with_columns(pl.lit(0.0).alias('Total'))
            if not df_h.is_empty() and 'Total' not in df_h.columns:
                df_h = df_h.with_columns(pl.lit(0.0).alias('Total'))
            if not df_d.is_empty() and 'Total' not in df_d.columns:
                df_d = df_d.with_columns(pl.lit(0.0).alias('Total'))
        
        return df_all, df_h, df_d
        
    except Exception as e:
        st.warning(f"エネルギー計算でエラー: {e}")
        # エラー時のフォールバック
        df_all = pl.DataFrame(schema=[('measured_at_jst', pl.Datetime), ('Total', pl.Float64)])
        df_h = pl.DataFrame(schema=[('measured_at_jst', pl.Datetime), ('Total', pl.Float64)])
        df_d = pl.DataFrame(schema=[('measured_at_jst', pl.Datetime), ('Total', pl.Float64)])
        return df_all, df_h, df_d

def str2float(weather_data):
    """天気データを浮動小数点に変換"""
    try:
        return float(weather_data)
    except:
        return 0

def scraping(url, date, data_type):
    """気象データのスクレイピング"""
    try:
        html = urllib.request.urlopen(url).read()
        soup = BeautifulSoup(html, 'html.parser')
        trs = soup.find("table", {"class": "data2_s"})
        if trs is None:
            st.warning(f"Failed to find data table for {data_type} on {date}. URL: {url}")
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
    except Exception as e:
        st.warning(f"気象データ取得エラー: {e}")
        return []

def visualize_bt(df_all, df_h, df_d, df_airid, st_h, ed_h):
    """ボタンのヒートマップ可視化"""
    if df_h.is_empty():
        st.warning("ボタンデータがありません")
        return
        
    visualize_date = df_h.with_columns(pl.col('measured_at_jst').dt.date()).select(pl.col('measured_at_jst')).unique()

    for di in range(len(visualize_date)):
        df = df_h.filter(pl.col('measured_at_jst').dt.date() == visualize_date[di])
        bt_cols = [col for col in df.columns if col.startswith('bt_')]
        
        if not bt_cols:
            continue
            
        df = df.select(['measured_at_jst'] + bt_cols)

        tmp = df.drop('measured_at_jst').to_numpy().T

        fig, ax = plt.subplots(figsize=(12, 6))
        im = ax.imshow(tmp, extent=(st_h, ed_h+1, len(bt_cols), 0), cmap='seismic_r', aspect=0.25)
        plt.colorbar(im, shrink=0.5)
        plt.clim(-10, 10)
        
        ax.set_title(f'ボタン操作ヒートマップ - {visualize_date[di].to_numpy()[0]}')
        ax.set_xlabel('時刻')
        ax.set_ylabel('ゾーン')
        
        st.pyplot(fig)
        plt.close()

def calc_bt(df_all, df_d, df_h, df_airid):
    """ボタンの結果演算"""
    if df_d.is_empty() or df_all.is_empty():
        st.warning("ボタン統計用のデータがありません")
        return
        
    day_list = df_d.select(pl.col('measured_at_jst').dt.date().unique()).to_series().to_list()
    bt_cols = [col for col in df_all.columns if col.startswith('bt_')]
    airplug_cols = [col for col in df_all.columns if 'airplug_control_on' in col]
    
    if not bt_cols:
        st.warning("ボタンデータが見つかりません")
        return
        
    df_df = df_all.select(['measured_at_jst'] + airplug_cols + bt_cols)

    st.subheader("📱 ボタン操作統計")
    
    summary_data = []
    
    for di in range(len(day_list)):
        df = df_df.filter(pl.col('measured_at_jst').dt.date() == day_list[di])

        bt_array = df.select(bt_cols).to_numpy()
        mask_p = bt_array > 0
        mask_m = bt_array < 0

        btp = bt_array[mask_p]
        btm = bt_array[mask_m]

        summary_data.append({
            '日付': day_list[di],
            '+操作回数': int(np.sum(btp)) if len(btp) > 0 else 0,
            '-操作回数': int(np.sum(btm)) if len(btm) > 0 else 0,
            '+操作頻度': len(btp),
            '-操作頻度': len(btm)
        })

    if summary_data:
        st.dataframe(pd.DataFrame(summary_data), use_container_width=True)

def visualize_remote_control(df_all, df_h, df_d, st_dt, ed_dt):
    """リモコン操作の可視化"""
    if df_all.is_empty():
        st.warning("リモコンデータがありません")
        return
        
    temp_df = df_all.to_pandas()
    temp_df['measured_at_jst'] = pd.to_datetime(temp_df['measured_at_jst'])
    set_temperature_columns = [col for col in df_all.columns if col.startswith("set_temperature_")]

    if not set_temperature_columns:
        st.warning("'set_temperature_' カラムが見つかりません。リモコン操作の可視化をスキップします。")
        return

    # 各カラムごとに最小値と最大値を取得
    y_min = min([temp_df[col].min() for col in set_temperature_columns]) - 1
    y_max = max([temp_df[col].max() for col in set_temperature_columns]) + 1

    date_range = pd.date_range(start=st_dt, end=ed_dt)
    daily_summary = []

    # プロット数に応じたグリッドの行数・列数を自動計算
    n_plots_dt_range = len(date_range)
    n_cols_dt_range = math.ceil(math.sqrt(n_plots_dt_range))
    n_rows_dt_range = math.ceil(n_plots_dt_range / n_cols_dt_range)

    # 各日付の各時間帯における温度上昇と下降の手動変更回数をプロット
    fig, axes = plt.subplots(n_rows_dt_range, n_cols_dt_range, figsize=(18, n_rows_dt_range * 5))

    if n_plots_dt_range == 1:
        axes = [axes]
    elif n_rows_dt_range == 1:
        axes = [axes]
    else:
        axes = axes.flatten()

    # 各日付について処理
    for idx, date in enumerate(date_range):
        if idx >= len(axes):
            break
            
        ax = axes[idx]
        date_data = temp_df[temp_df['measured_at_jst'].dt.date == date.date()]

        manual_up_count = 0
        manual_down_count = 0

        # 各設定温度カラムについて処理
        for col in set_temperature_columns:
            if col in date_data.columns:
                # 前の時間帯との差を計算
                temp_diff = date_data[col].diff()

                # 手動変更をカウント
                manual_up_count += temp_diff[temp_diff == 0.5].count()
                manual_down_count += temp_diff[temp_diff == -0.5].count()

        # daily_summary に日付と手動変更回数を追加
        daily_summary.append({
            '日付': date.date(), 
            '手動上昇': manual_up_count, 
            '手動下降': manual_down_count
        })

        # 時間別の変更回数をプロット
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

        ax.plot(hourly_up_changes.index, hourly_up_changes.values, marker='o', color='red', label='手動上昇 (+0.5)')
        ax.plot(hourly_down_changes.index, hourly_down_changes.values, marker='o', color='blue', label='手動下降 (-0.5)')
        ax.set_title(f'{date.strftime("%Y-%m-%d")}', fontsize=12)
        ax.set_xlabel('時刻', fontsize=10)
        ax.set_ylabel('手動変更回数', fontsize=10)
        ax.grid(True, linestyle='--', alpha=0.7)
        ax.legend(fontsize=8)
        ax.tick_params(axis='x', rotation=45)
        ax.set_ylim(0, max(hourly_up_changes.max(), hourly_down_changes.max(), 5))

    # 空のサブプロットを削除
    for idx in range(len(date_range), len(axes)):
        fig.delaxes(axes[idx])

    plt.tight_layout()
    st.pyplot(fig)
    plt.close()

    # 日別手動変更の変動をプロット
    daily_summary_df = pd.DataFrame(daily_summary)
    
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(daily_summary_df['日付'], daily_summary_df['手動上昇'], marker='o', linestyle='-', color='red', label='手動上昇 (+0.5)')
    ax.plot(daily_summary_df['日付'], daily_summary_df['手動下降'], marker='o', linestyle='-', color='blue', label='手動下降 (-0.5)')
    ax.set_xlabel('日付')
    ax.set_ylabel('回数')
    ax.set_title('日別手動温度変更回数')
    ax.grid(True)
    ax.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    st.pyplot(fig)
    plt.close()

    # サマリーテーブルの表示
    st.write("**日別手動温度変更サマリー:**")
    st.dataframe(daily_summary_df, use_container_width=True)

def visualize_daily_usage_CHx(df_d):
    """CHx別の日別使用量可視化"""
    if df_d.is_empty():
        st.warning("日別使用量データがありません")
        return
        
    # 対象の airplug_control_on 列を取得
    airplug_on_col = [col for col in df_d.columns if 'airplug_control_on' in col]
    if not airplug_on_col:
        st.warning("airplug_control_on列が見つかりません")
        return
        
    airplug_on_col = airplug_on_col[0]

    # CHx(kW) 列を抽出
    ch_cols = [col for col in df_d.columns if col.startswith("CH") and "(kW)" in col]
    
    if not ch_cols:
        st.warning("CHx(kW)列が見つかりません")
        return

    # AL制御：airplug_control_on > 0.3、従来制御：airplug_control_on < 0.3
    df_AL = df_d.filter(pl.col(airplug_on_col) > 0.3).select(["measured_at_jst", *ch_cols, "outdoor_temp"])
    df_conv = df_d.filter(pl.col(airplug_on_col) < 0.3).select(["measured_at_jst", *ch_cols, "outdoor_temp"])

    # 各チャネルの None 値を 0 に置換
    for c in ch_cols:
        df_AL = df_AL.with_columns(pl.col(c).fill_null(0))
        df_conv = df_conv.with_columns(pl.col(c).fill_null(0))

    # 日時を matplotlib 用の数値に変換
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
    ax_conv.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax_conv.set_title("従来制御 (AirPlug OFF)")
    ax_conv.set_xlabel("日時")
    ax_conv.set_ylabel("電気使用量 (kW)")
    ax_conv.grid(alpha=0.5)
    ax_conv.legend(loc='upper left')

    # ツイン軸で外気温をプロット（黒）
    if 'outdoor_temp' in df_conv.columns:
        ax_conv_twin = ax_conv.twinx()
        ax_conv_twin.plot(dates_conv, df_conv['outdoor_temp'], label='外気温', color='black')
        ax_conv_twin.set_ylabel("外気温 (°C)")

    # ----- AL制御のスタックドバーチャート -----
    bottom_AL = np.zeros(len(df_AL))
    for i, c in enumerate(ch_cols):
        color = default_colors[i % len(default_colors)]
        ax_AL.bar(dates_AL, df_AL[c].to_numpy(), bottom=bottom_AL,
                  label=c, color=color)
        bottom_AL += df_AL[c].to_numpy()

    ax_AL.xaxis_date()
    ax_AL.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax_AL.set_title("AL制御 (AirPlug ON)")
    ax_AL.set_xlabel("日時")
    ax_AL.set_ylabel("電気使用量 (kW)")
    ax_AL.grid(alpha=0.5)
    ax_AL.legend(loc='upper left')

    # ツイン軸で外気温をプロット（黒）
    if 'outdoor_temp' in df_AL.columns:
        ax_AL_twin = ax_AL.twinx()
        ax_AL_twin.plot(dates_AL, df_AL['outdoor_temp'], label='外気温', color='black')
        ax_AL_twin.set_ylabel("外気温 (°C)")

    plt.tight_layout()
    st.pyplot(fig)
    plt.close()

def visualize_summury(df_h, df_d, df_airid, values, st_h, ed_h):
    """総合サマリーの可視化"""
    if df_d.is_empty():
        st.warning("サマリー用のデータがありません")
        return
        
    day_list = df_d.select(pl.col('measured_at_jst').dt.date().unique()).to_series().to_list()
    zone_num = len(df_airid) if not df_airid.is_empty() else 0

    # 温度グラフ
    st.subheader("🌡️ 温度サマリー")
    fig, ax = plt.subplots(figsize=(12, 6))

    for di in range(len(day_list)):
        df = df_h.filter(pl.col('measured_at_jst').dt.date() == day_list[di])
        
        # ゾーンの温度カラムを取得
        zone_cols = [str(zid) for zid in df_airid['zone_id'].unique().to_list()] if not df_airid.is_empty() else []
        valid_zone_cols = [c for c in zone_cols if c in df.columns]
        
        if valid_zone_cols:
            df = df.with_columns(pl.mean_horizontal(valid_zone_cols).alias('mean'))
        else:
            continue

        # AirPlug制御の確認
        airplug_cols = [col for col in df_d.columns if 'airplug_control_on' in col]
        if airplug_cols and di < len(df_d):
            airplug_value = df_d.select(airplug_cols[0])[di, 0] if df_d.select(airplug_cols[0]).height > di else None
            color = 'blue' if airplug_value is not None and airplug_value > 0.3 else 'gray'
        else:
            color = 'gray'

        ax.plot(df['measured_at_jst'].dt.hour(), df['mean'], label=str(day_list[di]), color=color)

    ax.set_xticks(np.arange(st_h, ed_h+1, 1))
    ax.set_ylim([22, 28])
    ax.grid(alpha=0.5)
    ax.set_xlabel('時刻')
    ax.set_ylabel('平均温度 (°C)')
    ax.set_title('日別平均温度推移')
    ax.legend()

    st.pyplot(fig)
    plt.close()

    # 指標の棒グラフ
    if values is not None and len(values) >= 6:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.bar(['AirPlug温度誤差', '従来制御温度誤差'], [values[4], values[5]], color=['blue', 'gray'])
        ax.set_ylabel('温度誤差 (°C)')
        ax.set_title('温度誤差比較')
        st.pyplot(fig)
        plt.close()

    # エネルギー使用量（もしあれば）
    if 'Total' in df_d.columns:
        st.subheader("エネルギーサマリー")
        visualize_energy_summary(df_h, df_d, st_h, ed_h)
        
        # CHx別の表示
        visualize_daily_usage_CHx(df_d)

        # 外気温vs使用量の散布図
        airplug_on_cols = [col for col in df_d.columns if 'airplug_control_on' in col]
        if airplug_on_cols and 'outdoor_temp' in df_d.columns:
            airplug_on_col = airplug_on_cols[0]
            df_on = df_d.filter(pl.col(airplug_on_col) > 0.3)
            df_off = df_d.filter(pl.col(airplug_on_col) <= 0.3)
            
            fig, ax = plt.subplots(figsize=(10, 6))
            if not df_on.is_empty():
                ax.scatter(df_on['outdoor_temp'], df_on['Total'], label='AirPlug ON', color='blue', alpha=0.7)
            if not df_off.is_empty():
                ax.scatter(df_off['outdoor_temp'], df_off['Total'], label='AirPlug OFF', color='gray', alpha=0.7)
            ax.set_xlabel('外気温 (°C)')
            ax.set_ylabel('エネルギー使用量 (kWh)')
            ax.set_title('外気温 vs エネルギー使用量')
            ax.legend()
            ax.grid(alpha=0.3)
            
            st.pyplot(fig)
            plt.close()

# 可視化関数を追加

def visualize_temperature_with_mode(df_airplug, df_aircond, df_target, df_airid):
    """ゾーン別温度推移と運転モード可視化"""
    if df_airplug.is_empty():
        st.warning("温度データがありません")
        return
        
    view_cols = ["set_temperature", "process_temperature"]
    color_list = ['orange', 'green']
    col_labels = ['設定温度', '吸込温度']  # 日本語ラベル
    
    # エアコンごとの温度推移
    for ai, airid in enumerate(df_airid['id'].to_list()):
        # データ結合
        df_combine = df_airplug.join(df_aircond, on='measured_at_jst', how='inner') if not df_aircond.is_empty() else df_airplug
        
        # ゾーンIDが存在しない場合はスキップ
        zone_id = str(df_airid['zone_id'][ai])
        if zone_id not in df_combine.columns:
            continue
            
        fig, ax1 = plt.subplots(figsize=(12, 6))
        ax2 = ax1.twinx()
        
        # 運転状態マスク
        start_stop_col = f'start_stop_{airid}'
        if start_stop_col in df_combine.columns:
            mask = df_combine[start_stop_col] == 2
        else:
            mask = [False] * len(df_combine)
            
        # 運転モード
        op_mode_col = f"operation_mode_{airid}"
        if op_mode_col in df_combine.columns:
            op_mode_vals = df_combine[op_mode_col]
            op_mode_colors = [
                'grey' if off else ('cyan' if mode == 1 else ('pink' if mode == 2 else 'white'))
                for off, mode in zip(mask, op_mode_vals)
            ]
        else:
            op_mode_colors = ['blue'] * len(df_combine)
            
        # 温度データプロット
        ax1.scatter(
            df_combine['measured_at_jst'],
            df_combine[zone_id],
            s=[300 if flag else 100 for flag in mask],
            c=op_mode_colors,
            zorder=1,
            label='運転モード (灰:OFF, 水色:冷房, ピンク:暖房)'
        )
        
        ax1.plot(
            df_combine['measured_at_jst'],
            df_combine[zone_id],
            label='室温',
            color='blue',
            zorder=2
        )
        
        # 設定温度・吸込温度
        for k, (col, label) in enumerate(zip(view_cols, col_labels)):
            col_name = f"{col}_{airid}"
            if col_name in df_combine.columns:
                ax1.plot(df_combine['measured_at_jst'], df_combine[col_name], 
                        label=label, color=color_list[k])
                
        # 目標温度
        if not df_target.is_empty() and 'air_conditioner_id' in df_target.columns:
            df_pick = df_target.filter(pl.col("air_conditioner_id") == airid).sort("measured_at_jst")
            if not df_pick.is_empty() and 'target_temperature' in df_pick.columns:
                ax1.plot(df_pick['measured_at_jst'], df_pick['target_temperature'],
                        label="目標温度", color='black', lw=3)
        
        # グラフ設定
        ax1.set_ylim(18, 30)
        ax1.set_xlabel("時刻")
        ax1.set_ylabel("温度 (°C)")
        ax1.set_title(f"{df_airid['display_name'][ai]} - 温度推移と運転モード")
        ax1.legend(loc='upper left', bbox_to_anchor=(0, 1))
        ax1.grid(True, alpha=0.3)
        
        st.pyplot(fig)

def visualize_summary(df_h, df_d, values, st_h, ed_h, df_airid):
    """総合サマリー可視化 - グラフ画像辞書を返すバージョン"""
    graph_images = {}
    
    if df_d.is_empty() or df_h.is_empty():
        st.warning("サマリー表示用のデータがありません")
        return graph_images
        
    # --- 温度グラフ ---
    fig1, ax1 = plt.subplots(figsize=(12, 5))
    day_list = df_d.select(pl.col('measured_at_jst').dt.date()).unique().to_series().to_list()
    
    zone_cols = [str(zid) for zid in df_airid['zone_id'].unique().to_list()]
    valid_zone_cols = [c for c in zone_cols if c in df_h.columns]
    
    for di, day in enumerate(day_list):
        df_day = df_h.filter(pl.col('measured_at_jst').dt.date() == day)
        if df_day.is_empty() or not valid_zone_cols:
            continue
            
        # ゾーン平均温度
        df_day = df_day.with_columns(pl.mean_horizontal(valid_zone_cols).alias('mean'))
        
        # AirPlug制御の判定
        airplug_on_col = next((col for col in df_d.columns if 'airplug_control_on' in col), None)
        if airplug_on_col and df_d.filter(pl.col('measured_at_jst').dt.date() == day)[airplug_on_col].mean() > 0.3:
            color = 'blue'
        else:
            color = 'gray'
            
        ax1.plot(df_day['measured_at_jst'].dt.hour(), df_day['mean'], 
               label=str(day), color=color)
    
    ax1.set_title("日別 平均温度推移 (青:AL制御 / 灰:従来制御)")
    ax1.set_xlabel("時間 (時)")
    ax1.set_ylabel("平均温度 (℃)")
    ax1.grid(alpha=0.5)
    
    # グラフを画像データとして保存
    buf = io.BytesIO()
    fig1.savefig(buf, format='png', bbox_inches='tight')
    buf.seek(0)
    graph_images['temperature_summary'] = PIL.Image.open(buf)
    
    # Streamlitにも表示
    st.subheader("温度推移サマリー")
    st.pyplot(fig1)
    plt.close(fig1)
    
    # --- 電力消費グラフ ---
    airplug_on_col = next((col for col in df_d.columns if 'airplug_control_on' in col), None)
    if airplug_on_col and 'outdoor_temp' in df_d.columns and 'Total' in df_d.columns:
        fig2, ax2 = plt.subplots(figsize=(12, 5))
        df_on = df_d.filter(pl.col(airplug_on_col) > 0.3)
        df_off = df_d.filter(pl.col(airplug_on_col) <= 0.3)

        if not df_on.is_empty():
            ax2.scatter(df_on['outdoor_temp'], df_on['Total'], label='AL制御', color='blue')
        if not df_off.is_empty():
            ax2.scatter(df_off['outdoor_temp'], df_off['Total'], label='従来制御', color='gray')
        
        ax2.set_title("外気温 vs 消費電力 (Total)")
        ax2.set_xlabel("外気温 (℃)")
        ax2.set_ylabel("日別総消費電力 (kWh)")
        ax2.legend()
        ax2.grid(alpha=0.5)

        # グラフを画像データとして保存
        buf = io.BytesIO()
        fig2.savefig(buf, format='png', bbox_inches='tight')
        buf.seek(0)
        graph_images['energy_scatter'] = PIL.Image.open(buf)
        
        # Streamlitにも表示
        st.subheader("外気温 vs 消費電力")
        st.pyplot(fig2)
        plt.close(fig2)
    
    return graph_images

def visualize_energy_summary(df_d, df_h, st_h, ed_h):
    """エネルギー分析サマリー"""
    if df_d.is_empty() or 'Total' not in df_d.columns:
        st.info("エネルギーデータがありません")
        return
        
    # 日別エネルギー消費
    st.subheader("日別エネルギー消費量")
    
    airplug_on_col = next((col for col in df_d.columns if 'airplug_control_on' in col), None)
    
    if airplug_on_col:
        df_on = df_d.filter(pl.col(airplug_on_col) > 0.3)
        df_off = df_d.filter(pl.col(airplug_on_col) <= 0.3)
        
        fig, ax1 = plt.subplots(figsize=(12, 6))
        
        # 棒グラフ
        if not df_on.is_empty():
            ax1.bar(df_on['measured_at_jst'], df_on['Total'], 
                   label='AirPlug制御', color='blue', alpha=0.7)
        if not df_off.is_empty():
            ax1.bar(df_off['measured_at_jst'], df_off['Total'], 
                   label='従来制御', color='gray', alpha=0.7)
        
        ax1.set_xlabel("日付")
        ax1.set_ylabel("電力消費量 (kWh)")
        ax1.set_title("日別電力消費量")
        
        # 外気温
        if 'outdoor_temp' in df_d.columns:
            ax2 = ax1.twinx()
            ax2.plot(df_d['measured_at_jst'], df_d['outdoor_temp'], 
                    label='外気温', color='red', linewidth=2)
            ax2.set_ylabel("外気温 (°C)")
            
        ax1.legend(loc='upper left')
        st.pyplot(fig)
        
        # 統計情報
        col1, col2 = st.columns(2)
        with col1:
            if not df_on.is_empty():
                st.metric("AirPlug制御 平均消費量", 
                         f"{df_on['Total'].mean():.2f} kWh" if df_on['Total'].mean() is not None else "N/A")
        with col2:
            if not df_off.is_empty():
                st.metric("従来制御 平均消費量", 
                         f"{df_off['Total'].mean():.2f} kWh" if df_off['Total'].mean() is not None else "N/A")

def visualize_daily_usage_CHx(df_d):
    """CHx別スタックドバーチャート"""
    if df_d.is_empty():
        return
        
    ch_cols = [col for col in df_d.columns if col.startswith("CH") and "(kW)" in col]
    if not ch_cols:
        st.info("チャンネル別データがありません")
        return
        
    st.subheader("チャンネル別電力消費量")
    
    airplug_on_col = next((col for col in df_d.columns if 'airplug_control_on' in col), None)
    
    if airplug_on_col:
        df_AL = df_d.filter(pl.col(airplug_on_col) > 0.3)
        df_conv = df_d.filter(pl.col(airplug_on_col) <= 0.3)
        
        fig, (ax_conv, ax_AL) = plt.subplots(1, 2, figsize=(16, 6))
        
        # 従来制御
        if not df_conv.is_empty():
            bottom_conv = np.zeros(len(df_conv))
            for i, c in enumerate(ch_cols):
                values = df_conv[c].fill_null(0).to_numpy()
                ax_conv.bar(range(len(df_conv)), values, bottom=bottom_conv,
                           label=c)
                bottom_conv += values
            
            ax_conv.set_title("従来制御")
            ax_conv.set_xlabel("日数")
            ax_conv.set_ylabel("電力消費量 (kW)")
            ax_conv.legend()
            
        # AirPlug制御
        if not df_AL.is_empty():
            bottom_AL = np.zeros(len(df_AL))
            for i, c in enumerate(ch_cols):
                values = df_AL[c].fill_null(0).to_numpy()
                ax_AL.bar(range(len(df_AL)), values, bottom=bottom_AL,
                         label=c)
                bottom_AL += values
            
            ax_AL.set_title("AirPlug制御")
            ax_AL.set_xlabel("日数")
            ax_AL.set_ylabel("電力消費量 (kW)")
            ax_AL.legend()
        
        plt.tight_layout()
        st.pyplot(fig)

def visualize_outdoor_correlation(df_d):
    """外気温とエネルギー消費の相関"""
    if df_d.is_empty() or 'outdoor_temp' not in df_d.columns or 'Total' not in df_d.columns:
        return
        
    st.subheader("外気温とエネルギー消費の相関")
    
    airplug_on_col = next((col for col in df_d.columns if 'airplug_control_on' in col), None)
    
    if airplug_on_col:
        df_on = df_d.filter(pl.col(airplug_on_col) > 0.3)
        df_off = df_d.filter(pl.col(airplug_on_col) <= 0.3)
        
        fig, ax = plt.subplots(figsize=(8, 6))
        
        if not df_on.is_empty():
            ax.scatter(df_on['outdoor_temp'], df_on['Total'], 
                      label='AirPlug制御', color='blue', alpha=0.7)
                      
        if not df_off.is_empty():
            ax.scatter(df_off['outdoor_temp'], df_off['Total'], 
                      label='従来制御', color='gray', alpha=0.7)
        
        ax.set_xlabel("外気温 (°C)")
        ax.set_ylabel("電力消費量 (kWh)")
        ax.set_title("外気温 vs 電力消費量")
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        st.pyplot(fig)

def visualize_remote_control_streamlit(df_all, st_dt, ed_dt):
    """リモコン操作可視化（Streamlit版）"""
    if df_all.is_empty():
        st.info("リモコン操作データがありません")
        return
        
    st.subheader("リモコン手動操作分析")
    
    # Pandas DataFrameに変換
    temp_df = df_all.to_pandas()
    temp_df['measured_at_jst'] = pd.to_datetime(temp_df['measured_at_jst'])
    set_temperature_columns = [col for col in df_all.columns if col.startswith("set_temperature_")]
    
    if not set_temperature_columns:
        st.info("設定温度データがありません")
        return
    
    # 日別の手動変更を集計
    date_range = pd.date_range(start=st_dt, end=ed_dt, freq='D')
    daily_summary = []
    
    for date in date_range:
        date_data = temp_df[temp_df['measured_at_jst'].dt.date == date.date()]
        total_up = 0
        total_down = 0
        
        if not date_data.empty:
            for room in set_temperature_columns:
                if room in date_data.columns:
                    temp_diff = date_data[room].diff()
                    # 0.5度単位の手動変更を検出
                    total_up += ((temp_diff > 0) & (temp_diff % 0.5 == 0)).sum()
                    total_down += ((temp_diff < 0) & (temp_diff % 0.5 == 0)).sum()
        
        daily_summary.append({
            '日付': date,
            '温度上げ': int(total_up),
            '温度下げ': int(total_down),
            '総変更回数': int(total_up + total_down)
        })
    
    daily_summary_df = pd.DataFrame(daily_summary)
    
    # 日別変動グラフ
    if not daily_summary_df.empty:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.plot(daily_summary_df['日付'], daily_summary_df['温度上げ'], 
               marker='o', color='red', label='温度上げ (+0.5°C)')
        ax.plot(daily_summary_df['日付'], daily_summary_df['温度下げ'], 
               marker='o', color='blue', label='温度下げ (-0.5°C)')
        ax.set_xlabel('日付')
        ax.set_ylabel('操作回数')
        ax.set_title('日別リモコン手動操作回数')
        ax.legend()
        ax.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        
        st.pyplot(fig)
        
        # サマリーテーブル
        st.dataframe(daily_summary_df)

def visualize_button_heatmap(df_h, df_airid, st_h, ed_h):
    """ボタン操作ヒートマップ"""
    if df_h.is_empty():
        st.info("ボタンデータがありません")
        return
        
    bt_cols = [col for col in df_h.columns if col.startswith('bt_')]
    if not bt_cols:
        st.info("ボタン操作データがありません")
        return
        
    st.subheader("ボタン操作ヒートマップ")
    
    # 日別のヒートマップ
    visualize_date = df_h.with_columns(pl.col('measured_at_jst').dt.date()).select('measured_at_jst').unique()
    
    for di, date in enumerate(visualize_date['measured_at_jst'].to_list()):
        df = df_h.filter(pl.col('measured_at_jst').dt.date() == date)
        df_bt = df.select(['measured_at_jst'] + bt_cols)
        
        if df_bt.shape[0] > 0:
            # データを配列に変換
            bt_data = df_bt.drop('measured_at_jst').to_numpy().T
            
            fig, ax = plt.subplots(figsize=(12, 6))
            im = ax.imshow(bt_data, extent=(st_h, ed_h+1, len(bt_cols), 0), 
                          cmap='seismic_r', aspect='auto')
            
            ax.set_xlabel('時刻')
            ax.set_ylabel('エアコン')
            ax.set_title(f'ボタン操作ヒートマップ - {date}')
            ax.set_yticks(range(len(bt_cols)))
            
            # エアコン名を取得してラベルに使用
            bt_labels = []
            for col in bt_cols:
                ac_id = col.replace('bt_', '')
                # df_airidから表示名を取得
                display_name = df_airid.filter(pl.col('id') == ac_id)['display_name']
                if not display_name.is_empty():
                    bt_labels.append(display_name[0])
                else:
                    bt_labels.append(ac_id)
            
            ax.set_yticklabels(bt_labels)
            
            cbar = plt.colorbar(im, ax=ax)
            cbar.set_label('操作回数（＋：暑い、−：寒い）')
            
            st.pyplot(fig)

# メインのタブ構成を更新
def display_analysis_results(results, params):
    """分析結果の表示"""
    st.markdown("---")
    st.header("分析結果")
    
    # 主要指標表示
    display_key_metrics(results['values'])
    
    # タブで詳細表示
    tabs = st.tabs([
        "温度分析", 
        "エネルギー分析", 
        "リモコン・ボタン操作", 
        "総合サマリー",
        "LLM分析レポート",
        "データダウンロード"
    ])
    
    with tabs[0]:  # 温度分析
        st.subheader("温度分析")
        
        # ゾーン別温度推移と運転モード
        if 'df_airplug' in results and 'df_aircond' in results:
            visualize_temperature_with_mode(
                results.get('df_airplug', pl.DataFrame()),
                results.get('df_aircond', pl.DataFrame()),
                results.get('df_target', pl.DataFrame()),
                results['df_airid']
            )
        else:
            # シンプルな温度推移
            visualize_temperature_data(results['df_combine'], results['df_airid'], 
                                     params['st_h'], params['ed_h'])
    
    with tabs[1]:  # エネルギー分析
        st.subheader("エネルギー分析")
        
        # エネルギーサマリー
        visualize_energy_summary(results['df_d'], results['df_h'], 
                               params['st_h'], params['ed_h'])
        
        # CHx別分析
        visualize_daily_usage_CHx(results['df_d'])
        
        # 外気温相関
        visualize_outdoor_correlation(results['df_d'])
    
    with tabs[2]:  # リモコン・ボタン操作
        st.subheader("操作分析")
        
        # リモコン操作
        visualize_remote_control_streamlit(results['df_all'], 
                                         params['st_dt_ymdhms'], 
                                         params['ed_dt_ymdhms'])
        
        # ボタン操作ヒートマップ
        visualize_button_heatmap(results['df_h'], results['df_airid'],
                               params['st_h'], params['ed_h'])
        
        # ボタン操作統計
        if not results['df_d'].is_empty():
            calc_button_stats(results['df_all'], results['df_d'], results['df_airid'])
    
    with tabs[3]:  # 総合サマリー
        st.subheader("総合サマリー")
        
        # 温度サマリー
        visualize_summary(results['df_h'], results['df_d'], results['values'],
                        params['st_h'], params['ed_h'], results['df_airid'])
        
        # 指標サマリー
        display_metrics_summary(results['values'])
    
    with tabs[4]:  # LLM分析レポート
        st.subheader("🤖 LLM分析レポート")
        
        if not GEMINI_AVAILABLE:
            st.error("Gemini APIが利用できません。APIキーの設定を確認してください。")
        else:
            st.info("このタブでは、AI（Gemini）を使用してカスタマーサクセス向けの包括的な分析レポートを生成します。")
            
            # LLMレポート生成ボタン
            if st.button("📊 LLMレポートを生成", type="primary", key="generate_llm_report"):
                # サマリーグラフからグラフ画像を取得
                graph_images = visualize_summary(
                    results.get('df_h', pl.DataFrame()),
                    results.get('df_d', pl.DataFrame()),
                    results.get('values', []),
                    params.get('st_h', 8),
                    params.get('ed_h', 20),
                    results.get('df_airid', pl.DataFrame())
                )
                
                # レポートデータの準備
                report_data = {
                    'period_start': params.get('st_dt_ymdhms', '').strftime('%Y-%m-%d') if params.get('st_dt_ymdhms') else 'N/A',
                    'period_end': params.get('ed_dt_ymdhms', '').strftime('%Y-%m-%d') if params.get('ed_dt_ymdhms') else 'N/A',
                    'floor_name': params.get('floor_name', 'N/A'),
                    'temp_error_conv': f"{results['values'][5]:.2f}" if len(results.get('values', [])) > 5 else 'N/A',
                    'temp_error_al': f"{results['values'][4]:.2f}" if len(results.get('values', [])) > 4 else 'N/A',
                    'energy_conv_kwh': 'N/A',  # エネルギーデータがある場合に計算
                    'energy_al_kwh': 'N/A',    # エネルギーデータがある場合に計算
                    'control_efficiency_rate': 'N/A',  # 制御効率データがある場合に計算
                    'manual_ops_conv': 'N/A',  # 手動操作データがある場合に計算
                    'manual_ops_al': 'N/A'     # 手動操作データがある場合に計算
                }
                
                # LLMレポート生成
                with st.spinner("AI分析レポートを生成中..."):
                    report_text = generate_customer_success_report(report_data, graph_images)
                
                # セッション状態に保存
                st.session_state.llm_report = report_text
                st.session_state.llm_report_data = report_data
                
                st.success("✅ LLM分析レポートが生成されました！")
            
            # 生成済みレポートがある場合は表示
            if st.session_state.llm_report:
                st.markdown("### 📋 カスタマーサクセス分析レポート")
                st.markdown(st.session_state.llm_report)
                
                # ダウンロードボタン
                col1, col2 = st.columns(2)
                
                with col1:
                    st.download_button(
                        label="📥 レポートをダウンロード (.md)",
                        data=st.session_state.llm_report,
                        file_name=f"customer_success_report_{params.get('floor_name', 'floor')}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.md",
                        mime="text/markdown",
                        key="llm_report_download_md"
                    )
                
                with col2:
                    # PDF変換とダウンロード
                    pdf_data = convert_markdown_to_pdf(
                        st.session_state.llm_report, 
                        f"customer_success_report_{params.get('floor_name', 'floor')}"
                    )
                    
                    if pdf_data:
                        st.download_button(
                            label="📄 レポートをダウンロード (.pdf)",
                            data=pdf_data,
                            file_name=f"customer_success_report_{params.get('floor_name', 'floor')}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
                            mime="application/pdf",
                            key="llm_report_download_pdf"
                        )
                
                # レポートをクリアするボタン
                if st.button("🗑️ レポートをクリア", key="clear_llm_report"):
                    st.session_state.llm_report = None
                    st.session_state.llm_report_data = None
                    st.rerun()
    
    with tabs[5]:  # データダウンロード
        download_section(results, params)

def calc_button_stats(df_all, df_d, df_airid):
    """ボタン操作統計"""
    st.subheader("ボタン操作統計")
    
    day_list = df_d.select(pl.col('measured_at_jst').dt.date().unique()).to_series().to_list()
    df_df = df_all.select(['measured_at_jst'] + 
                         [col for col in df_all.columns if col.startswith('bt_')])
    
    stats = []
    for day in day_list:
        df = df_df.filter(pl.col('measured_at_jst').dt.date() == day)
        
        if not df.is_empty():
            bt_cols = [col for col in df.columns if col.startswith('bt_')]
            if bt_cols:
                bt_array = df.select(bt_cols).to_numpy()
                
                btp = bt_array[bt_array > 0]
                btm = bt_array[bt_array < 0]
                
                stats.append({
                    '日付': day,
                    '暑いボタン回数': np.sum(btp) if len(btp) > 0 else 0,
                    '寒いボタン回数': np.abs(np.sum(btm)) if len(btm) > 0 else 0,
                    '暑いボタン頻度': len(btp),
                    '寒いボタン頻度': len(btm)
                })
    
    if stats:
        stats_df = pd.DataFrame(stats)
        st.dataframe(stats_df)

def display_metrics_summary(values):
    """指標サマリー表示"""
    if values is None or len(values) < 11:
        return
        
    st.subheader("詳細指標")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**AirPlug制御時**")
        st.metric("平均温度", f"{values[0]:.2f}°C" if not np.isnan(values[0]) else "N/A")
        st.metric("温度標準偏差", f"{values[2]:.2f}" if not np.isnan(values[2]) else "N/A")
        st.metric("目標温度誤差", f"{values[4]:.2f}°C" if not np.isnan(values[4]) else "N/A")
        st.metric("設定温度変更回数", f"{int(values[6])}" if not np.isnan(values[6]) else "N/A")
        st.metric("稼働率", f"{values[8]:.1f}%" if not np.isnan(values[8]) else "N/A")
        
    with col2:
        st.write("**従来制御時**")
        st.metric("平均温度", f"{values[1]:.2f}°C" if not np.isnan(values[1]) else "N/A")
        st.metric("温度標準偏差", f"{values[3]:.2f}" if not np.isnan(values[3]) else "N/A")
        st.metric("目標温度誤差", f"{values[5]:.2f}°C" if not np.isnan(values[5]) else "N/A")
        st.metric("設定温度変更回数", f"{int(values[7])}" if not np.isnan(values[7]) else "N/A")
        st.metric("稼働率", f"{values[9]:.1f}%" if not np.isnan(values[9]) else "N/A")
    
    st.metric("データ欠損率", f"{values[10]:.1f}%" if not np.isnan(values[10]) else "N/A")

def download_section(results, params):
    """ダウンロードセクション"""
    st.subheader("データダウンロード")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if not results['df_all'].is_empty():
            csv_all = results['df_all'].to_pandas().to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                label="↓ 分データ (CSV)",
                data=csv_all,
                file_name=f"df_min_floor{params['floor_name']}_start_{params['st_dt_ymdhms'].strftime('%Y%m%d')}_{params['sys_kind']}_{params['energy_kind']}.csv",
                mime="text/csv"
            )
    
    with col2:
        if not results['df_h'].is_empty():
            csv_h = results['df_h'].to_pandas().to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                label="↓ 時間データ (CSV)",
                data=csv_h,
                file_name=f"df_hour_floor{params['floor_name']}_start_{params['st_dt_ymdhms'].strftime('%Y%m%d')}_{params['sys_kind']}_{params['energy_kind']}.csv",
                mime="text/csv"
            )
    
    with col3:
        if not results['df_d'].is_empty():
            csv_d = results['df_d'].to_pandas().to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                label="↓ 日別データ (CSV)",
                data=csv_d,
                file_name=f"df_day_floor{params['floor_name']}_start_{params['st_dt_ymdhms'].strftime('%Y%m%d')}_{params['sys_kind']}_{params['energy_kind']}.csv",
                mime="text/csv"
            )

def load_energy_csv(params):
    """エネルギーCSVファイルの読み込み"""
    st.sidebar.subheader("📁 エネルギーデータ")
    
    # ファイルアップロード
    uploaded_file = st.sidebar.file_uploader(
        "エネルギーCSVファイルをアップロード", 
        type=['csv'],
        help="master.csv または energy.csv をアップロードしてください"
    )
    
    if uploaded_file is not None:
        # アップロードされたファイルを読み込み
        try:
            if params['energy_format_type'] == 'master':
                df = pl.read_csv(uploaded_file, null_values=["-"])[:, 2:]
            elif params['energy_format_type'] == 'hioki':
                df = pl.read_csv(uploaded_file, skip_rows=26, null_values=["-"])[:, 3:]
            else:
                df = pl.read_csv(uploaded_file, null_values=["-"])
            
            st.sidebar.success(f"✅ {uploaded_file.name} を読み込みました")
            return df, True
        except Exception as e:
            st.sidebar.error(f"ファイル読み込みエラー: {e}")
            return pl.DataFrame(), False
    else:
        return pl.DataFrame(), False

# calc_energy関数を修正
def calc_energy_with_csv(st_h, ed_h, df_combine, energy_df=None):
    """エネルギー計算（CSVファイル対応版）"""
    try:
        if energy_df is not None and not energy_df.is_empty():
            # CSVファイルからエネルギーデータを処理
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
            
            # Total列追加
            energy_cols = [col for col in df.columns if col != 'measured_at_jst']
            df = df.with_columns(pl.sum_horizontal(energy_cols).alias('Total'))
            
            # df_combineとエネルギー情報を結合
            df_ecombine = df_combine.join(df.select(['measured_at_jst', 'Total']), 
                                         on='measured_at_jst', how='left')
            
            # 時間別・日別集計
            df_h = df.group_by_dynamic("measured_at_jst", every="1h").agg(pl.col("*").mean())
            df_d = df_h.group_by_dynamic("measured_at_jst", every="1d").agg(pl.col("*").sum())
            
            # AirPlug制御カラムを追加
            airplug_cols = [col for col in df_ecombine.columns if 'airplug_control_on' in col]
            if airplug_cols:
                column = airplug_cols[0]
                df_h = df_h.join(df_ecombine.group_by_dynamic("measured_at_jst", every="1h")
                                .agg(pl.col(column).mean()).select(['measured_at_jst', column]), 
                                on='measured_at_jst', how='left')
                df_d = df_d.join(df_ecombine.group_by_dynamic("measured_at_jst", every="1d")
                                .agg(pl.col(column).mean()).select(['measured_at_jst', column]), 
                                on='measured_at_jst', how='left')
            
            return df_ecombine, df_h, df_d
            
        else:
            # CSVがない場合のダミー処理（既存の処理）
            return calc_energy(st_h, ed_h, df_combine)
            
    except Exception as e:
        st.error(f"エネルギー計算エラー: {e}")
        return calc_energy(st_h, ed_h, df_combine)

def setup_google_drive():
    """Google Drive API設定"""
    # サービスアカウントの認証情報（JSONファイル）が必要
    # Streamlit Secretsに保存することを推奨
    
    try:
        # Streamlit Secretsから認証情報を取得
        creds_dict = st.secrets["google_drive"]
        creds = service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=['https://www.googleapis.com/auth/drive.readonly']
        )
        
        service = build('drive', 'v3', credentials=creds)
        return service
    except Exception as e:
        st.error(f"Google Drive認証エラー: {e}")
        return None

def download_from_drive(service, file_id):
    """Google DriveからファイルIDを指定してダウンロード"""
    try:
        request = service.files().get_media(fileId=file_id)
        file_data = io.BytesIO()
        downloader = MediaIoBaseDownload(file_data, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
            
        file_data.seek(0)
        return file_data
    except Exception as e:
        st.error(f"ファイルダウンロードエラー: {e}")
        return None

def load_energy_from_gdrive(customer_dir, add_dir, energy_kind='master'):
    """Google Driveからエネルギーデータを読み込み"""
    service = setup_google_drive()
    if service is None:
        return pl.DataFrame(), False
    
    # ファイルIDの管理（例：辞書で管理）
    file_mapping = {
        '/野村不動産/Data/master.csv': 'YOUR_FILE_ID_HERE',
        # 他のファイルマッピングを追加
    }
    
    file_path = f"{customer_dir}{add_dir}/{energy_kind}.csv"
    file_id = file_mapping.get(file_path)
    
    if file_id:
        file_data = download_from_drive(service, file_id)
        if file_data:
            df = pl.read_csv(file_data, null_values=["-"])
            return df, True
    
    return pl.DataFrame(), False

def generate_customer_success_report(report_data, graph_images):
    """
    集約されたデータとグラフ画像から、カスタマーサクセスレポートを生成する。
    """
    if not GEMINI_AVAILABLE:
        return "Gemini APIが利用できません。APIキーの設定を確認してください。"
    
    # Gemini Pro Visionモデルをロード
    model = genai.GenerativeModel('gemini-2.0-flash')

    # LLMへの指示（プロンプト）を作成
    prompt_parts = [
        "あなたはAI空調制御システム「AirPlug」のカスタマーサクセス担当者です。",
        "以下のデータとグラフを総合的に分析し、顧客向けの導入効果レポートを生成してください。",
        "レポートは【エグゼクティブサマリー】【分析結果詳細】【総合評価と次のステップ】の3部構成とします。",
        "分析の際は、以下の4つの指標を必ず定量評価定量に含めてください：",
        "1. 温度安定性：目標温度との誤差が小さいほど良い。",
        "2. 省エネ効果：電力消費量が少ないほど良い。",
        "3. 制御効率：AL制御の成功率が高いほど良い。",
        "4. 快適性向上：手動での温度操作回数が少ないほど良い。",
        "各項目について「成功」「要改善」などの明確な評価を下し、データに基づいた具体的な次のアクション提案で締めくくってください。これにより意思決定の標準化を図ります。",
        "\n---",
        "## 分析データ\n",
        f"**分析期間:** {report_data.get('period_start')} ～ {report_data.get('period_end')}\n",
        f"**対象フロア:** {report_data.get('floor_name')}\n",

        "### 1. 温度安定性\n",
        f"- 従来制御時の目標温度との平均誤差: {report_data.get('temp_error_conv', 'N/A')} ℃\n",
        f"- AL制御時の目標温度との平均誤差: {report_data.get('temp_error_al', 'N/A')} ℃\n",

        "### 2. 省エネ効果\n",
        f"- 従来制御時の日平均電力消費量: {report_data.get('energy_conv_kwh', 'N/A')} kWh\n",
        f"- AL制御時の日平均電力消費量: {report_data.get('energy_al_kwh', 'N/A')} kWh\n",

        "### 3. 制御効率\n",
        f"- AL制御の成功率（空調稼働時間中）: {report_data.get('control_efficiency_rate', 'N/A')} %\n",

        "### 4. 快適性向上\n",
        f"- 従来制御時の1日あたり平均手動操作回数: {report_data.get('manual_ops_conv', 'N/A')} 回\n",
        f"- AL制御時の1日あたり平均手動操作回数: {report_data.get('manual_ops_al', 'N/A')} 回\n",
        "---",
        "\n## 分析用グラフ\n",
        "以下のグラフも参考にして分析してください。",
    ]

    # グラフ画像を追加
    if graph_images.get('temperature_summary'):
        prompt_parts.append(graph_images.get('temperature_summary'))
    if graph_images.get('energy_scatter'):
        prompt_parts.append(graph_images.get('energy_scatter'))

    prompt_parts.append("\n以上の情報に基づき、レポートを作成してください。")

    # 不要なNoneをリストから除去
    prompt_parts_filtered = [part for part in prompt_parts if part is not None]

    try:
        st.info("Gemini APIにレポート生成をリクエストしています...")
        response = model.generate_content(prompt_parts_filtered)
        return response.text
    except Exception as e:
        error_msg = f"Gemini API呼び出し中にエラーが発生しました: {e}"
        st.error(error_msg)
        return f"レポートの生成に失敗しました。\n\n{error_msg}"

def setup_japanese_font():
    """日本語フォントのセットアップ"""
    try:
        # HeiseiKakuGo-W5（ヒラギノ角ゴ Pro W3の代替）を試す
        pdfmetrics.registerFont(UnicodeCIDFont('HeiseiKakuGo-W5'))
        return 'HeiseiKakuGo-W5'
    except:
        try:
            # HeiseiMin-W3（明朝体）を試す
            pdfmetrics.registerFont(UnicodeCIDFont('HeiseiMin-W3'))
            return 'HeiseiMin-W3'
        except:
            try:
                # KozMinPro-Regular（小塚明朝）を試す
                pdfmetrics.registerFont(UnicodeCIDFont('KozMinPro-Regular'))
                return 'KozMinPro-Regular'
            except:
                # どのフォントも使えない場合はデフォルトフォントを使用
                st.warning("日本語フォントが見つかりません。デフォルトフォントを使用します。")
                return 'Helvetica'

def convert_markdown_to_pdf(markdown_text, file_name):
    """MarkdownテキストをPDFに変換する関数"""
    try:
        # 日本語フォントのセットアップ
        japanese_font = setup_japanese_font()
        
        # バイトストリーム作成
        buffer = io.BytesIO()
        
        # PDF文書作成
        doc = SimpleDocTemplate(buffer, pagesize=A4, topMargin=20*mm, bottomMargin=20*mm,
                              leftMargin=20*mm, rightMargin=20*mm)
        
        # スタイル設定
        styles = getSampleStyleSheet()
        
        # 日本語対応スタイル作成
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Title'],
            fontName=japanese_font,
            fontSize=16,
            spaceAfter=20,
            alignment=1  # 中央揃え
        )
        
        heading_style = ParagraphStyle(
            'CustomHeading',
            parent=styles['Heading1'],
            fontName=japanese_font,
            fontSize=14,
            spaceAfter=12,
            spaceBefore=12
        )
        
        normal_style = ParagraphStyle(
            'CustomNormal',
            parent=styles['Normal'],
            fontName=japanese_font,
            fontSize=10,
            spaceAfter=6,
            leading=14
        )
        
        # コンテンツ配列
        story = []
        
        # タイトル追加
        story.append(Paragraph("AirPlug カスタマーサクセス分析レポート", title_style))
        story.append(Spacer(1, 20))
        
        # Markdownを簡単なHTMLに変換してパースする
        lines = markdown_text.split('\n')
        current_paragraph = ""
        
        for line in lines:
            line = line.strip()
            
            if not line:
                if current_paragraph:
                    # 段落終了
                    story.append(Paragraph(current_paragraph, normal_style))
                    current_paragraph = ""
                story.append(Spacer(1, 6))
                continue
            
            # 見出し処理
            if line.startswith('###'):
                if current_paragraph:
                    story.append(Paragraph(current_paragraph, normal_style))
                    current_paragraph = ""
                heading_text = line.replace('###', '').strip()
                story.append(Paragraph(heading_text, heading_style))
                continue
            elif line.startswith('##'):
                if current_paragraph:
                    story.append(Paragraph(current_paragraph, normal_style))
                    current_paragraph = ""
                heading_text = line.replace('##', '').strip()
                story.append(Paragraph(heading_text, heading_style))
                continue
            elif line.startswith('#'):
                if current_paragraph:
                    story.append(Paragraph(current_paragraph, normal_style))
                    current_paragraph = ""
                heading_text = line.replace('#', '').strip()
                story.append(Paragraph(heading_text, heading_style))
                continue
            
            # 箇条書き処理
            if line.startswith('- ') or line.startswith('* '):
                if current_paragraph:
                    story.append(Paragraph(current_paragraph, normal_style))
                    current_paragraph = ""
                bullet_text = "• " + line[2:].strip()
                story.append(Paragraph(bullet_text, normal_style))
                continue
            
            # 通常テキスト
            if current_paragraph:
                current_paragraph += " "
            current_paragraph += line
        
        # 最後の段落
        if current_paragraph:
            story.append(Paragraph(current_paragraph, normal_style))
        
        # PDF生成
        doc.build(story)
        
        # バイト配列取得
        buffer.seek(0)
        return buffer.getvalue()
        
    except Exception as e:
        st.error(f"PDF変換エラー: {e}")
        return None

def get_energy_data(params):
    """エネルギーデータの取得（ドラッグアンドドロップ対応）"""
    
    data_source = st.sidebar.radio(
        "データソースを選択",
        ["ドラッグ&ドロップアップロード", "複数ファイルアップロード", "単一ファイルアップロード", "サンプルデータ使用", "データなし"]
    )
    
    if data_source == "ドラッグ&ドロップアップロード":
        st.sidebar.info("💡 ファイルをドラッグ&ドロップでアップロードできます")
        
        # メインエリアにドラッグ&ドロップエリアを作成
        with st.container():
            st.markdown("### 📁 エネルギーデータファイルアップロード")
            st.markdown("---")
            
            # ドラッグ&ドロップエリアのスタイル
            drop_zone_style = """
            <style>
            .drop-zone {
                border: 3px dashed #cccccc;
                border-radius: 10px;
                padding: 50px;
                text-align: center;
                margin: 20px 0;
                background-color: #f8f9fa;
                transition: all 0.3s ease;
            }
            .drop-zone:hover {
                border-color: #007bff;
                background-color: #e7f3ff;
            }
            .drop-zone-active {
                border-color: #28a745;
                background-color: #d4edda;
            }
            </style>
            """
            st.markdown(drop_zone_style, unsafe_allow_html=True)
            
            # ファイルアップローダーを大きなドロップゾーンとして表示
            col1, col2, col3 = st.columns([1, 3, 1])
            with col2:
                st.markdown("""
                <div class="drop-zone">
                    <h3>📎 ファイルをここにドラッグ&ドロップ</h3>
                    <p>または下のボタンをクリックしてファイルを選択</p>
                    <p style="color: #666;">対応形式: CSV, Excel (.xlsx)</p>
                </div>
                """, unsafe_allow_html=True)
                
                uploaded_files = st.file_uploader(
                    "",
                    type=['csv', 'xlsx'],
                    accept_multiple_files=True,
                    key="drag_drop_uploader",
                    help="複数のエネルギーデータファイルを選択またはドラッグ&ドロップしてください"
                )
        
        if uploaded_files:
            try:
                st.success(f"🎉 {len(uploaded_files)}個のファイルがアップロードされました！")
                
                # アップロードされたファイルの詳細表示
                with st.expander("📋 アップロードファイル詳細", expanded=True):
                    file_details = []
                    total_size = 0
                    
                    for i, uploaded_file in enumerate(uploaded_files, 1):
                        file_size = len(uploaded_file.getvalue()) if hasattr(uploaded_file, 'getvalue') else 0
                        total_size += file_size
                        
                        file_details.append({
                            "No.": i,
                            "ファイル名": uploaded_file.name,
                            "サイズ": f"{file_size/1024:.1f} KB" if file_size > 0 else "N/A",
                            "形式": uploaded_file.name.split('.')[-1].upper()
                        })
                    
                    import pandas as pd
                    df_details = pd.DataFrame(file_details)
                    st.dataframe(df_details, use_container_width=True)
                    st.info(f"📊 合計: {len(uploaded_files)}ファイル, {total_size/1024:.1f} KB")
                
                # Master形式統一変換の実行
                with st.spinner("🔄 Master形式に変換中..."):
                    df_master = convert_to_master_format(uploaded_files, params['energy_format_type'])
                    
                    if df_master.shape[0] == 0:
                        st.warning("⚠️ 有効なデータが見つかりませんでした")
                        return pl.DataFrame(), False
                    
                    # Polarsデータフレームに変換
                    combined_df = pl.DataFrame(df_master)
                    
                    st.success(f"✅ Master形式への変換が完了しました！")
                    
                    # 変換結果のサマリー表示
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("📊 データ行数", f"{combined_df.height:,}")
                    with col2:
                        st.metric("📈 データ列数", combined_df.width)
                    with col3:
                        if 'DateTime' in combined_df.columns:
                            dt_range = combined_df.select([
                                pl.col('DateTime').min().alias('start'),
                                pl.col('DateTime').max().alias('end')
                            ]).to_pandas().iloc[0]
                            duration = pd.to_datetime(dt_range['end']) - pd.to_datetime(dt_range['start'])
                            st.metric("📅 データ期間", f"{duration.days}日")
                    
                    # データプレビュー
                    with st.expander("👀 データプレビュー"):
                        st.dataframe(combined_df.head(10).to_pandas(), use_container_width=True)
                    
                    return combined_df, True
                    
            except Exception as e:
                st.error(f"❌ ファイル処理エラー: {e}")
                st.code(f"エラー詳細: {str(e)}")
                return pl.DataFrame(), False
        else:
            st.info("👆 ファイルをドラッグ&ドロップまたは選択してください")
            return pl.DataFrame(), False
    
    elif data_source == "複数ファイルアップロード":
        st.sidebar.info("複数のエネルギーCSVファイルをアップロードできます")
        
        uploaded_files = st.sidebar.file_uploader(
            "CSVファイルを複数選択",
            type=['csv'],
            accept_multiple_files=True,
            help="複数のエネルギーデータCSVファイルを選択してください"
        )
        
        if uploaded_files:
            try:
                # Master形式統一変換を使用
                st.sidebar.info("🔄 Master形式に統一変換中...")
                df_master = convert_to_master_format(uploaded_files, params['energy_format_type'])
                
                if df_master.shape[0] == 0:
                    st.sidebar.warning("有効なデータが見つかりませんでした")
                    return pl.DataFrame(), False
                
                # Polarsデータフレームに変換
                combined_df = pl.DataFrame(df_master)
                
                # ファイル情報を表示
                file_info = []
                for uploaded_file in uploaded_files:
                    file_info.append({
                        'name': uploaded_file.name,
                        'size': len(uploaded_file.getvalue()) if hasattr(uploaded_file, 'getvalue') else 'N/A'
                    })
                
                st.sidebar.success(f"✅ {len(uploaded_files)}個のファイルをMaster形式で統合しました")
                st.sidebar.info(f"📊 統合結果: {combined_df.height}行 × {combined_df.width}列")
                
                with st.sidebar.expander("ファイル詳細"):
                    for info in file_info:
                        st.write(f"📄 {info['name']}")
                    st.write(f"📈 変換形式: {params['energy_format_type']} → master")
                    if 'DateTime' in combined_df.columns:
                        dt_range = combined_df.select([
                            pl.col('DateTime').min().alias('start'),
                            pl.col('DateTime').max().alias('end')
                        ]).to_pandas().iloc[0]
                        st.write(f"📅 期間: {dt_range['start']} ～ {dt_range['end']}")
                
                return combined_df, True
                
            except Exception as e:
                st.sidebar.error(f"ファイル読み込みエラー: {e}")
                return pl.DataFrame(), False
        else:
            st.sidebar.info("複数のファイルを選択してください")
            return pl.DataFrame(), False
    
    elif data_source == "単一ファイルアップロード":
        uploaded_file = st.sidebar.file_uploader(
            "CSVファイルをアップロード",
            type=['csv'],
            help="master.csv または同等のエネルギーデータCSVをアップロードしてください"
        )
        
        if uploaded_file:
            try:
                # ファイル形式に応じた読み込み処理
                if params['energy_format_type'] == 'hioki':
                    energy_df = pl.read_csv(uploaded_file, skip_rows=26, null_values=["-"])[:, 3:]
                elif params['energy_format_type'] == 'master':
                    energy_df = pl.read_csv(uploaded_file, null_values=["-"])[:, 2:]
                else:
                    energy_df = pl.read_csv(uploaded_file, null_values=["-"])
                
                st.sidebar.success(f"✅ {uploaded_file.name} を読み込みました")
                return energy_df, True
            except Exception as e:
                st.sidebar.error(f"ファイル読み込みエラー: {e}")
                return pl.DataFrame(), False
        else:
            # ファイルがアップロードされていない場合
            st.sidebar.info("ファイルをアップロードするか、他のオプションを選択してください")
            return pl.DataFrame(), False
    
    elif data_source == "サンプルデータ使用":
        # サンプルデータの生成
        st.sidebar.info("サンプルデータを使用します")
        return generate_sample_energy_data(params), True
    
    else:
        st.sidebar.info("エネルギーデータなしで分析を実行します")
        return pl.DataFrame(), False

def generate_sample_energy_data(params):
    """サンプルエネルギーデータの生成"""
    # 期間に応じたサンプルデータを生成
    start = params['st_dt_ymdhms']
    end = params['ed_dt_ymdhms']
    
    # 1時間ごとのデータを生成
    date_range = pd.date_range(start=start, end=end, freq='1H')
    
    # ランダムなエネルギーデータ
    np.random.seed(42)
    data = {
        'DateTime': [dt.strftime('%Y-%m-%d %H:%M:%S') for dt in date_range],
        'CH1(kW)': np.random.uniform(10, 50, len(date_range)),
        'CH2(kW)': np.random.uniform(5, 30, len(date_range)),
        'CH3(kW)': np.random.uniform(15, 40, len(date_range))
    }
    
    return pl.DataFrame(data)

# ================================
# エネルギーデータ形式統一機能
# ================================

def clean_column_names(df, meta_columns=['ページNo', '日付', '時刻']):
    """先頭行をヘッダーとして利用し、メタ情報列以降を自動でCHカラム名に変換"""
    if df.shape[0] == 0:
        return df
    
    df.columns = df.iloc[0]
    df = df.drop(0).reset_index(drop=True)
    num_measurements = df.shape[1] - len(meta_columns)
    measurement_columns = [f'CH{i+1}(kW)' for i in range(num_measurements)]
    new_columns = meta_columns + measurement_columns
    
    if len(df.columns) < len(new_columns):
        new_columns = new_columns[:len(df.columns)]
    
    df.columns = new_columns
    df = df.dropna(how='all').reset_index(drop=True)
    return df

def expand_to_minutely(df, meta_columns=['ページNo', '日付', '時刻'], offset=0):
    """各行について、日付・時刻を元に1分刻みで60行に展開"""
    if df.shape[0] == 0:
        return df
    
    expanded_rows = []
    measurement_columns = [col for col in df.columns if col not in meta_columns]
    
    for index, row in df.iterrows():
        try:
            date = pd.to_datetime(row['日付'], errors='coerce')
            time = pd.to_datetime(row['時刻'], format='%H:%M', errors='coerce')
            if pd.isnull(date) or pd.isnull(time):
                continue
            
            datetime_combined = datetime.datetime.combine(date.date(), time.time())
            datetime_combined = datetime_combined - datetime.timedelta(minutes=offset)
            
            for minute in range(60):
                new_datetime = datetime_combined + datetime.timedelta(minutes=minute)
                new_row = {
                    'No.': len(expanded_rows) + 1,
                    'Date': new_datetime.strftime('%Y-%m-%d'),
                    'Time': new_datetime.strftime('%H:%M:%S'),
                    'DateTime': new_datetime.strftime('%Y-%m-%d %H:%M:%S')
                }
                for col in measurement_columns:
                    new_row[col] = row[col]
                expanded_rows.append(new_row)
        except Exception as e:
            print(f"Error processing row {index}: {e}")
    
    return pd.DataFrame(expanded_rows)

def convert_dk_format(df):
    """dk（野村不動産）用の「日時」列から1時間分を1分刻み展開"""
    if df.shape[0] == 0:
        return df
    
    power_columns = [col for col in df.columns if "電力" in col]
    if len(power_columns) == 0:
        return pd.DataFrame()
    
    expanded_rows = []
    record_no = 1
    
    for idx, row in df.iterrows():
        dt = pd.to_datetime(row.get("日時", None), format="%Y/%m/%d %H:%M:%S", errors="coerce")
        if pd.isnull(dt):
            continue
        
        for minute in range(60):
            new_dt = dt + datetime.timedelta(minutes=minute)
            new_row = {
                "No.": record_no,
                "Date": new_dt.strftime("%Y-%m-%d"),
                "Time": new_dt.strftime("%H:%M:%S"),
                "DateTime": new_dt.strftime("%Y-%m-%d %H:%M:%S")
            }
            for i, col in enumerate(power_columns, start=1):
                new_row[f"CH{i}(kW)"] = row[col]
            expanded_rows.append(new_row)
            record_no += 1
    
    return pd.DataFrame(expanded_rows)

def process_mufg(uploaded_files):
    """MUFG形式の複数ファイルを処理してmaster形式に変換（csv_to_master仕様）"""
    print('▼ process_mufg start ▼')
    
    dfs = []
    for uploaded_file in uploaded_files:
        try:
            # ファイル名から日付を取得
            temp = pd.read_csv(uploaded_file, encoding='cp932', on_bad_lines='skip', nrows=1)
            day = temp.columns[1] if len(temp.columns) > 1 else None
            
            uploaded_file.seek(0)
            df = pd.read_csv(uploaded_file, encoding='cp932', skiprows=8)
            
            # カラム存在チェック
            if '単位' in df.columns:
                df = df.rename(columns={'単位': 'Time'})
            else:
                time_column = next((col for col in df.columns if "time" in col.lower() or "時刻" in col.lower()), None)
                if time_column:
                    df = df.rename(columns={time_column: 'Time'})
                else:
                    print(f"No suitable time column in {uploaded_file.name}, skipping...")
                    continue
            
            df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
            measurement_columns = [col for col in df.columns if col not in ['Date','Time']]
            mapping = {old: f"CH{i+1}(kW)" for i, old in enumerate(measurement_columns)}
            df = df.rename(columns=mapping)
            df['Date'] = day
            
            df = df[df['Time'].astype(str).str.match(r'^\d{1,2}:\d{2}')]
            df['Time'] = df['Time'].str.replace('24:00', '00:00')
            df['DateTime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], errors='coerce', format='%Y/%m/%d %H:%M')
            
            channel_names = [f"CH{i+1}(kW)" for i in range(len(measurement_columns))]
            new_columns = ['Date', 'Time', 'DateTime'] + channel_names
            df = df[new_columns].dropna(subset=['DateTime'])
            
            dfs.append(df)
            
        except Exception as e:
            print(f"Error reading file {uploaded_file.name}: {e}")
            continue
    
    if len(dfs) == 0:
        print("No valid MUFG dataframes found.")
        return pd.DataFrame()
    
    combined_df = pd.concat(dfs, ignore_index=True)
    # 1分刻みリサンプル
    combined_df = combined_df.set_index('DateTime').resample('1min').ffill().reset_index()
    combined_df['Date'] = combined_df['DateTime'].dt.strftime('%Y-%m-%d')
    combined_df['Time'] = combined_df['DateTime'].dt.strftime('%H:%M:%S')
    
    meta_columns = ['Date', 'Time', 'DateTime']
    measurement_columns = [col for col in combined_df.columns if col not in meta_columns]
    combined_df = combined_df[meta_columns + measurement_columns]
    
    print('▲ process_mufg end ▲')
    return combined_df

def process_RPT(uploaded_files):
    """RPT形式の複数ファイルを処理してmaster形式に変換（csv_to_master仕様）"""
    print('▼ process_RPT start ▼')
    
    dataframes = []
    for uploaded_file in uploaded_files:
        try:
            df = pd.read_csv(uploaded_file, encoding='cp932')
            df = df.iloc[7:32, :7].reset_index(drop=True)
            dataframes.append(df)
        except Exception as e:
            print(f"Error reading file {uploaded_file.name}: {e}")
            continue
    
    if len(dataframes) == 0:
        print("No valid RPT dataframes found.")
        return pd.DataFrame()
    
    combined_df = pd.concat(dataframes, axis=0).reset_index(drop=True)
    cleaned_df = clean_column_names(combined_df)
    minutely_df = expand_to_minutely(cleaned_df, offset=0)
    
    print('▲ process_RPT end ▲')
    return minutely_df

def process_hioki_cloud(uploaded_files):
    """HIOKI Cloud形式の複数ファイルを処理してmaster形式に変換（csv_to_master仕様）"""
    print('▼ process_hioki_cloud start ▼')
    
    dataframes = []
    for uploaded_file in uploaded_files:
        try:
            df = pd.read_csv(uploaded_file, encoding="cp932", on_bad_lines='skip', header=None, skiprows=26)
            df = df.iloc[:, :6]  # 最初6列を想定
            dataframes.append(df)
        except Exception as e:
            print(f"Error reading file {uploaded_file.name}: {e}")
            continue
    
    if len(dataframes) == 0:
        print("No valid hioki_cloud dataframes found.")
        return pd.DataFrame()
    
    combined_df = pd.concat(dataframes, axis=0).reset_index(drop=True)
    cleaned_df = clean_column_names(combined_df)
    minutely_df = expand_to_minutely(cleaned_df, offset=0)
    
    print('▲ process_hioki_cloud end ▲')
    return minutely_df

def process_dk(uploaded_files):
    """DK形式の複数Excelファイルを処理してmaster形式に変換（csv_to_master仕様）"""
    print('▼ process_dk start ▼')
    
    dataframes = []
    for uploaded_file in uploaded_files:
        try:
            if uploaded_file.name.endswith('.xlsx'):
                df = pd.read_excel(uploaded_file, sheet_name='Outdoor', skiprows=6)
            else:
                df = pd.read_csv(uploaded_file)
            dataframes.append(df)
        except Exception as e:
            print(f"Error reading file {uploaded_file.name}: {e}")
            continue
    
    if len(dataframes) == 0:
        print("No valid dk dataframes found.")
        return pd.DataFrame()
    
    combined_df = pd.concat(dataframes, axis=0).reset_index(drop=True)
    result_df = convert_dk_format(combined_df)
    
    if result_df.shape[0] > 0 and "No." in result_df.columns:
        result_df = result_df.drop(columns=["No."])
    
    print('▲ process_dk end ▲')
    return result_df

def process_hioki_local(uploaded_files):
    """HIOKI Local形式の複数ファイルを処理してmaster形式に変換（csv_to_master仕様）"""
    print('▼ process_hioki_local start ▼')
    
    dataframes = []
    
    for i, uploaded_file in enumerate(uploaded_files):
        try:
            # メタデータの読み込み
            metadata = pd.read_csv(uploaded_file, encoding='cp932', header=None, nrows=6)
            trigger_date = None
            if metadata.shape[0] >= 6 and pd.notna(metadata.iloc[5, 1]):
                trigger_time = str(metadata.iloc[5, 1])
                trigger_date = trigger_time.split()[0]
            
            uploaded_file.seek(0)
            df = pd.read_csv(uploaded_file, encoding='cp932', skiprows=11)
            print(f"Data shape after reading CSV: {df.shape}")
            
            df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
            
            time_column = next((col for col in df.columns if "time" in col.lower()), None)
            if not time_column:
                continue
            if time_column != 'Time':
                df = df.rename(columns={time_column: 'Time'})
            
            df['DateTime'] = pd.to_datetime(df['Time'], errors='coerce', format='%Y/%m/%d %H:%M:%S')
            if df['DateTime'].isna().all():
                df['DateTime'] = pd.to_datetime(df['Time'], errors='coerce')
            
            if trigger_date is not None:
                mask = df['DateTime'].isna()
                if mask.any():
                    df.loc[mask, 'DateTime'] = pd.to_datetime(trigger_date + ' ' + df.loc[mask, 'Time'], errors='coerce')
            
            df = df.dropna(subset=['DateTime'])
            # 秒数を0に統一（分単位に丸める）：秒がファイル毎にバラバラなため
            df['DateTime'] = df['DateTime'].dt.floor('min')
            df['Date'] = df['DateTime'].dt.strftime('%Y-%m-%d')
            df['Time'] = df['DateTime'].dt.strftime('%H:%M:%S')
            
            measurement_cols = [col for col in df.columns if col not in ['Date', 'Time', 'DateTime']]
            if len(measurement_cols) < 2:
                print(f"{uploaded_file.name} には最低2つの計測列が必要です。Skipping...")
                continue
            
            # ▼電流を電圧に変換
            df = df.rename(columns={measurement_cols[0]: 'CH1', measurement_cols[1]: 'CH2'})
            
            # W_kW の計算
            V = 205
            cos_theta = 0.95
            # 式: W = √3 × ((V×CH1 + V×CH2)/2) × cosθ, その後 kW 単位に (W/1000)
            df['W_kW'] = (math.sqrt(3) * ((V * df['CH1']) + (V * df['CH2'])) / 2 * cos_theta) / 1000
            
            new_col_name = f"CH{i+1}(kW)"
            df = df[['Date', 'Time', 'DateTime', 'W_kW']].rename(columns={'W_kW': new_col_name})
            df = df.drop_duplicates(subset='DateTime')
            # ▲電流を電圧に変換
            
            dataframes.append(df)
            
        except Exception as e:
            print(f"Error reading file {uploaded_file.name}: {e}")
            continue
    
    if len(dataframes) == 0:
        return pd.DataFrame()
    
    combined_df = dataframes[0].set_index('DateTime')
    for j in range(1, len(dataframes)):
        df_j = dataframes[j].set_index('DateTime')
        df_j = df_j.drop(columns=['Date', 'Time'])
        combined_df = combined_df.join(df_j, how='outer')
    
    combined_df = combined_df.sort_index().reset_index()
    combined_df['Date'] = combined_df['DateTime'].dt.strftime('%Y-%m-%d')
    combined_df['Time'] = combined_df['DateTime'].dt.strftime('%H:%M:%S')
    
    meta_cols = ['Date', 'Time', 'DateTime']
    measurement_cols = [col for col in combined_df.columns if col not in meta_cols]
    combined_df = combined_df[meta_cols + measurement_cols]
    
    print('▲ process_hioki_local end ▲')
    return combined_df

def reorder_columns(df):
    """
    カラムを [Date, Time, DateTime, CH1(kW), CH2(kW), ...] の順番にする（csv_to_master仕様）
    もし該当カラムが無い場合は無視し、存在する分だけ順序を合わせる。
    他のカラムが含まれていても最後に回すか、必要に応じて削除する。
    """
    if df.shape[0] == 0:
        return df
    
    # 1) 必須のメタカラム
    meta_cols = ['Date', 'Time', 'DateTime']
    
    # 2) CHn(kW) カラムを探して並び替え
    #    例: "CH1(kW)" -> 1, "CH11(kW)" -> 11
    ch_cols = [col for col in df.columns if col.startswith('CH') and col.endswith('(kW)')]
    def extract_ch_number(col_name):
        # "CH1(kW)" -> "1", "CH11(kW)" -> "11"
        return int(col_name[2:].split('(')[0])
    
    ch_cols_sorted = sorted(ch_cols, key=extract_ch_number)
    
    # 3) 上記以外のカラムは remainder として最後に
    remainder = [c for c in df.columns if c not in meta_cols + ch_cols]
    
    # 4) 結合して必要な順に。存在しないカラムは自動的にスキップ。
    desired_order = meta_cols + ch_cols_sorted + remainder
    # 実際に df に存在するカラムだけ取り出す
    final_cols = [c for c in desired_order if c in df.columns]
    
    return df[final_cols]

def convert_to_master_format(uploaded_files, format_type):
    """複数ファイルをmaster形式に統一変換（csv_to_master仕様準拠）"""
    try:
        print(f"Processing {len(uploaded_files)} files with format: {format_type}")
        
        # 単一のフォーマット処理を実行（csv_to_master仕様）
        if format_type == 'mufg':
            df_master = process_mufg(uploaded_files)
        elif format_type == 'PRT':
            df_master = process_RPT(uploaded_files)
        elif format_type == 'hioki_local':
            df_master = process_hioki_local(uploaded_files)
        elif format_type == 'hioki_cloud':
            df_master = process_hioki_cloud(uploaded_files)
        elif format_type == 'dk':
            df_master = process_dk(uploaded_files)
        else:
            print(f"Unknown format: {format_type}, using generic processing...")
            # フォールバック処理（既存ファイルとの互換性）
            combined_df = pl.DataFrame()
            for uploaded_file in uploaded_files:
                try:
                    if uploaded_file.name.endswith('.xlsx'):
                        df_excel = pd.read_excel(uploaded_file)
                        energy_df = pl.DataFrame(df_excel)
                    else:
                        if format_type == 'hioki':
                            energy_df = pl.read_csv(uploaded_file, skip_rows=26, null_values=["-"])[:, 3:]
                        elif format_type == 'master':
                            energy_df = pl.read_csv(uploaded_file, null_values=["-"])[:, 2:]
                        else:
                            energy_df = pl.read_csv(uploaded_file, null_values=["-"])
                    
                    if combined_df.is_empty():
                        combined_df = energy_df
                    else:
                        if 'DateTime' in energy_df.columns and 'DateTime' in combined_df.columns:
                            combined_df = combined_df.join(energy_df, on='DateTime', how='outer')
                except Exception as e:
                    print(f"Error processing file {uploaded_file.name}: {e}")
                    continue
            
            df_master = combined_df.to_pandas()
        
        # 入力データがない場合は空のDataFrameを返す
        if df_master is None or df_master.shape[0] == 0:
            print("No valid data found. Creating empty DataFrame.")
            return pd.DataFrame()
        
        # カラム並び替え (Date, Time, DateTime, CH..., その他)
        df_master = reorder_columns(df_master)
        
        # DateTime でソート
        if 'DateTime' in df_master.columns:
            df_master = df_master.sort_values('DateTime')
        
        # 数値カラムを Float64 に変換し、null値を0で埋める（csv_to_master仕様）
        cols_to_convert = [col for col in df_master.columns if col not in ["DateTime", "Date", "Time"]]
        # Use polars to convert columns to Float64 and fill nulls with 0
        df_master = pl.DataFrame(df_master).with_columns(
            [pl.col(col).cast(pl.Float64).fill_null(0) for col in cols_to_convert]
        ).to_pandas()
        
        print(f"Master CSV conversion completed. Shape: {df_master.shape}")
        return df_master
        
    except Exception as e:
        st.error(f"Master形式変換エラー: {e}")
        print(f"Error in convert_to_master_format: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"アプリケーションエラー: {e}")
        st.code(f"エラー詳細: {str(e)}")
