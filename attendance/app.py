import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
import time
import io
import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- 配置区域 ---
APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")
USER_ID = os.getenv("USER_ID")
EMPLOYEE_TYPE = "employee_id"
        # 当前操作者ID
# --- 配置结束 ---

# 常量定义
TOKEN_URL = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
STATS_URL = "https://open.feishu.cn/open-apis/attendance/v1/user_stats_datas/query"
EXPECTED_SHIFT_END_TIME = "17:30"
DATE_FORMAT = "%Y%m%d"
OUTPUT_DATE_FORMAT = "%Y-%m-%d"
TOKEN_EXPIRE_BUFFER = 60

_token = None
_token_expire_time = 0

# ==================== 默认通讯录数据 ====================
DEFAULT_ADDRESS_BOOK = [
    {"用户ID": "10010006", "部门": "产研中心", "所属中心": "产研中心"},
    {"用户ID": "10010007", "部门": "产研中心", "所属中心": "产研中心"},
    {"用户ID": "10010014", "部门": "产研中心", "所属中心": "产研中心"},
    {"用户ID": "10010095", "部门": "产研中心", "所属中心": "产研中心"},
    {"用户ID": "10010121", "部门": "产研中心", "所属中心": "产研中心"},
    {"用户ID": "10010146", "部门": "产研中心", "所属中心": "产研中心"},
    {"用户ID": "10010044", "部门": "产品部", "所属中心": "产研中心"},
    {"用户ID": "10010045", "部门": "产品部", "所属中心": "产研中心"},
    {"用户ID": "10010046", "部门": "产品部", "所属中心": "产研中心"},
    {"用户ID": "10010049", "部门": "产品部", "所属中心": "产研中心"},
    {"用户ID": "10010050", "部门": "产品部", "所属中心": "产研中心"},
    {"用户ID": "10010051", "部门": "产品部", "所属中心": "产研中心"},
    {"用户ID": "10010127", "部门": "产品部", "所属中心": "产研中心"},
    {"用户ID": "10010147", "部门": "产品部", "所属中心": "产研中心"},
    {"用户ID": "10010055", "部门": "生态平台部", "所属中心": "产研中心"},
    {"用户ID": "10010069", "部门": "生态平台部", "所属中心": "产研中心"},
    {"用户ID": "10010071", "部门": "生态平台部", "所属中心": "产研中心"},
    {"用户ID": "10010072", "部门": "生态平台部", "所属中心": "产研中心"},
    {"用户ID": "10010073", "部门": "生态平台部", "所属中心": "产研中心"},
    {"用户ID": "10010074", "部门": "生态平台部", "所属中心": "产研中心"},
    {"用户ID": "10010075", "部门": "生态平台部", "所属中心": "产研中心"},
    {"用户ID": "10010077", "部门": "生态平台部", "所属中心": "产研中心"},
    {"用户ID": "10010078", "部门": "生态平台部", "所属中心": "产研中心"},
    {"用户ID": "10010129", "部门": "生态平台部", "所属中心": "产研中心"},
    {"用户ID": "10010052", "部门": "硬件平台部", "所属中心": "产研中心"},
    {"用户ID": "10010053", "部门": "硬件平台部", "所属中心": "产研中心"},
    {"用户ID": "10010054", "部门": "硬件平台部", "所属中心": "产研中心"},
    {"用户ID": "10010058", "部门": "硬件平台部", "所属中心": "产研中心"},
    {"用户ID": "10010059", "部门": "软件部", "所属中心": "产研中心"},
    {"用户ID": "10010061", "部门": "软件部", "所属中心": "产研中心"},
    {"用户ID": "10010062", "部门": "软件部", "所属中心": "产研中心"},
    {"用户ID": "10010064", "部门": "软件部", "所属中心": "产研中心"},
    {"用户ID": "10010066", "部门": "软件部", "所属中心": "产研中心"},
    {"用户ID": "10010067", "部门": "软件部", "所属中心": "产研中心"},
    {"用户ID": "10010128", "部门": "软件部", "所属中心": "产研中心"},
    {"用户ID": "10010011", "部门": "人力中心", "所属中心": "人力中心"},
    {"用户ID": "10010117", "部门": "人力资源部", "所属中心": "人力中心"},
    {"用户ID": "10010118", "部门": "人力资源部", "所属中心": "人力中心"},
    {"用户ID": "10010120", "部门": "人力资源部", "所属中心": "人力中心"},
    {"用户ID": "10010136", "部门": "人力资源部", "所属中心": "人力中心"},
    {"用户ID": "10010009", "部门": "总经办", "所属中心": "总经办"},
    {"用户ID": "10010015", "部门": "总经办", "所属中心": "总经办"},
    {"用户ID": "10010122", "部门": "总经办", "所属中心": "总经办"},
    {"用户ID": "10010012", "部门": "总经办", "所属中心": "总经办"},
    {"用户ID": "10010111", "部门": "行政部", "所属中心": "总经办"},
    {"用户ID": "10010112", "部门": "行政部", "所属中心": "总经办"},
    {"用户ID": "10010113", "部门": "行政部", "所属中心": "总经办"},
    {"用户ID": "10010115", "部门": "行政部", "所属中心": "总经办"},
    {"用户ID": "10010116", "部门": "行政部", "所属中心": "总经办"},
    {"用户ID": "10010125", "部门": "行政部", "所属中心": "总经办"},
    {"用户ID": "10010135", "部门": "行政部", "所属中心": "总经办"},
    {"用户ID": "10010068", "部门": "南京研究中心", "所属中心": "南京研究中心"},
    {"用户ID": "10010070", "部门": "南京研究中心", "所属中心": "南京研究中心"},
    {"用户ID": "10010138", "部门": "南京研究中心", "所属中心": "南京研究中心"},
    {"用户ID": "10010139", "部门": "南京研究中心", "所属中心": "南京研究中心"},
    {"用户ID": "10010140", "部门": "南京研究中心", "所属中心": "南京研究中心"},
    {"用户ID": "10010145", "部门": "南京研究中心", "所属中心": "南京研究中心"},
    {"用户ID": "10010123", "部门": "清华无锡院智能配电应用技术研究中心", "所属中心": "清华无锡院智能配电应用技术研究中心"},
    {"用户ID": "10010002", "部门": "营销中心", "所属中心": "营销中心"},
    {"用户ID": "10010003", "部门": "营销中心", "所属中心": "营销中心"},
    {"用户ID": "10010029", "部门": "商务部", "所属中心": "营销中心"},
    {"用户ID": "10010030", "部门": "商务部", "所属中心": "营销中心"},
    {"用户ID": "10010031", "部门": "商务部", "所属中心": "营销中心"},
    {"用户ID": "10010032", "部门": "商务部", "所属中心": "营销中心"},
    {"用户ID": "10010033", "部门": "商务部", "所属中心": "营销中心"},
    {"用户ID": "10010124", "部门": "商务部", "所属中心": "营销中心"},
    {"用户ID": "10010005", "部门": "市场部", "所属中心": "营销中心"},
    {"用户ID": "10010024", "部门": "市场部", "所属中心": "营销中心"},
    {"用户ID": "10010035", "部门": "技术服务部", "所属中心": "营销中心"},
    {"用户ID": "10010037", "部门": "技术服务部", "所属中心": "营销中心"},
    {"用户ID": "10010038", "部门": "技术服务部", "所属中心": "营销中心"},
    {"用户ID": "10010039", "部门": "技术服务部", "所属中心": "营销中心"},
    {"用户ID": "10010041", "部门": "技术服务部", "所属中心": "营销中心"},
    {"用户ID": "10010042", "部门": "技术服务部", "所属中心": "营销中心"},
    {"用户ID": "10010004", "部门": "销售部", "所属中心": "营销中心"},
    {"用户ID": "10010020", "部门": "销售部", "所属中心": "营销中心"},
    {"用户ID": "10010022", "部门": "销售部", "所属中心": "营销中心"},
    {"用户ID": "10010016", "部门": "销售东部大区", "所属中心": "营销中心"},
    {"用户ID": "10010017", "部门": "销售东部大区", "所属中心": "营销中心"},
    {"用户ID": "10010018", "部门": "销售东部大区", "所属中心": "营销中心"},
    {"用户ID": "10010026", "部门": "销售北部大区", "所属中心": "营销中心"},
    {"用户ID": "10010126", "部门": "销售北部大区", "所属中心": "营销中心"},
    {"用户ID": "10010019", "部门": "销售南部大区", "所属中心": "营销中心"},
    {"用户ID": "10010021", "部门": "销售南部大区", "所属中心": "营销中心"},
    {"用户ID": "10010023", "部门": "销售西部大区", "所属中心": "营销中心"},
    {"用户ID": "10010025", "部门": "销售西部大区", "所属中心": "营销中心"},
    {"用户ID": "10010008", "部门": "财务中心", "所属中心": "财务中心"},
    {"用户ID": "10010106", "部门": "财务部", "所属中心": "财务中心"},
    {"用户ID": "10010107", "部门": "财务部", "所属中心": "财务中心"},
    {"用户ID": "10010108", "部门": "财务部", "所属中心": "财务中心"},
    {"用户ID": "10010109", "部门": "财务部", "所属中心": "财务中心"},
    {"用户ID": "10010110", "部门": "财务部", "所属中心": "财务中心"},
    {"用户ID": "10010010", "部门": "运营中心", "所属中心": "运营中心"},
    {"用户ID": "10010013", "部门": "运营中心", "所属中心": "运营中心"},
    {"用户ID": "10010098", "部门": "仓储部", "所属中心": "运营中心"},
    {"用户ID": "10010099", "部门": "仓储部", "所属中心": "运营中心"},
    {"用户ID": "10010100", "部门": "仓储部", "所属中心": "运营中心"},
    {"用户ID": "10010101", "部门": "仓储部", "所属中心": "运营中心"},
    {"用户ID": "10010102", "部门": "仓储部", "所属中心": "运营中心"},
    {"用户ID": "10010083", "部门": "生产部", "所属中心": "运营中心"},
    {"用户ID": "10010084", "部门": "生产部", "所属中心": "运营中心"},
    {"用户ID": "10010085", "部门": "生产部", "所属中心": "运营中心"},
    {"用户ID": "10010086", "部门": "生产部", "所属中心": "运营中心"},
    {"用户ID": "10010087", "部门": "生产部", "所属中心": "运营中心"},
    {"用户ID": "10010088", "部门": "生产部", "所属中心": "运营中心"},
    {"用户ID": "10010089", "部门": "生产部", "所属中心": "运营中心"},
    {"用户ID": "10010090", "部门": "生产部", "所属中心": "运营中心"},
    {"用户ID": "10010091", "部门": "生产部", "所属中心": "运营中心"},
    {"用户ID": "10010092", "部门": "生产部", "所属中心": "运营中心"},
    {"用户ID": "10010093", "部门": "生产部", "所属中心": "运营中心"},
    {"用户ID": "10010094", "部门": "生产部", "所属中心": "运营中心"},
    {"用户ID": "10010131", "部门": "生产部", "所属中心": "运营中心"},
    {"用户ID": "10010134", "部门": "生产部", "所属中心": "运营中心"},
    {"用户ID": "10010081", "部门": "质量部", "所属中心": "运营中心"},
    {"用户ID": "10010082", "部门": "质量部", "所属中心": "运营中心"},
    {"用户ID": "10010103", "部门": "质量部", "所属中心": "运营中心"},
    {"用户ID": "10010105", "部门": "质量部", "所属中心": "运营中心"},
    {"用户ID": "10010132", "部门": "质量部", "所属中心": "运营中心"},
    {"用户ID": "10010097", "部门": "采购部", "所属中心": "运营中心"},
    {"用户ID": "10010137", "部门": "采购部", "所属中心": "运营中心"}
]
# ==================== 节假日定义 ====================
HOLIDAYS_BY_YEAR = {
    2025: {
        "holidays": [
            "2025-01-01",
            "2025-01-28", "2025-01-29", "2025-01-30", "2025-01-31",
            "2025-02-01", "2025-02-02", "2025-02-03",
            "2025-04-04", "2025-04-05", "2025-04-06",
            "2025-05-01", "2025-05-02", "2025-05-03",
            "2025-05-31", "2025-06-01", "2025-06-02",
            "2025-09-15", "2025-09-16", "2025-09-17",
            "2025-10-01", "2025-10-02", "2025-10-03", "2025-10-04", "2025-10-05", "2025-10-06", "2025-10-07",
        ],
        "workweekends": [
            "2025-01-26",
            "2025-02-08",
            "2025-04-27",
            "2025-09-28",
            "2025-10-11",
        ]
    },
    2026: {
        "holidays": [
            "2026-01-01", "2026-01-02", "2026-01-03",
            "2026-02-15", "2026-02-16", "2026-02-17", "2026-02-18", "2026-02-19",
            "2026-02-20", "2026-02-21", "2026-02-22", "2026-02-23",
            "2026-04-04", "2026-04-05", "2026-04-06",
            "2026-05-01", "2026-05-02", "2026-05-03", "2026-05-04", "2026-05-05",
            "2026-06-19", "2026-06-20", "2026-06-21",
            "2026-09-25", "2026-09-26", "2026-09-27",
            "2026-10-01", "2026-10-02", "2026-10-03", "2026-10-04",
            "2026-10-05", "2026-10-06", "2026-10-07",
        ],
        "workweekends": [
            "2026-01-04",
            "2026-02-14",
            "2026-02-28",
            "2026-05-09",
            "2026-09-20",
            "2026-10-10",
        ]
    },
    2027: {
        "holidays": [],
        "workweekends": []
    }
}
# ==================== 飞书API相关函数 ====================
def get_tenant_access_token():
    global _token, _token_expire_time
    if _token and time.time() < _token_expire_time - TOKEN_EXPIRE_BUFFER:
        return _token
    headers = {"Content-Type": "application/json; charset=utf-8"}
    payload = {"app_id": APP_ID, "app_secret": APP_SECRET}
    try:
        response = requests.post(TOKEN_URL, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        result = response.json()
        if result.get("code") == 0:
            _token = result.get("tenant_access_token")
            expire_seconds = result.get("expire", 7200)
            _token_expire_time = time.time() + expire_seconds
            return _token
        else:
            st.error(f"获取 token 失败: {result.get('msg')}")
            st.stop()
    except requests.exceptions.RequestException as e:
        st.error(f"网络请求异常: {e}")
        st.stop()

def fetch_daily_stats(date, user_ids):
    token = get_tenant_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8"
    }
    params = {"employee_type": EMPLOYEE_TYPE}
    payload = {
        "locale": "zh",
        "stats_type": "daily",
        "start_date": int(date),
        "end_date": int(date),
        "user_ids": user_ids,
        "need_history": True,
        "current_group_only": False,
        "user_id": USER_ID
    }
    try:
        response = requests.post(STATS_URL, headers=headers, params=params, json=payload, timeout=30)
        response.raise_for_status()
        result = response.json()
        if result.get("code") == 0:
            return result.get("data", {})
        else:
            st.warning(f"获取 {date} 数据失败: {result.get('msg')}")
            return None
    except requests.exceptions.RequestException as e:
        st.warning(f"网络请求异常: {e}")
        return None

def extract_value(datas, code, default="-"):
    for item in datas:
        if item.get("code") == code:
            return item.get("value", default)
    return default

def extract_duration_hour(datas, code):
    for item in datas:
        if item.get("code") == code:
            duration = item.get("duration_num", {})
            if duration:
                return float(duration.get("hour", 0))
    return 0.0

def extract_punch_time_and_status(datas, punch_sequence):
    time_code = f"51502-{punch_sequence}"
    result_code = f"51503-{punch_sequence}"
    punch_time = extract_value(datas, time_code, "-")
    punch_status = "-"
    for item in datas:
        if item.get("code") == result_code:
            features = item.get("features", [])
            for feature in features:
                if feature.get("key") == "StatusMsg":
                    punch_status = feature.get("value", "-")
                    break
            break
    return punch_time, punch_status

def calculate_overtime(actual_shift_end):
    if not actual_shift_end or actual_shift_end == "-":
        return 0.0
    try:
        end_time = datetime.strptime(actual_shift_end, "%H:%M")
        expected_end = datetime.strptime(EXPECTED_SHIFT_END_TIME, "%H:%M")
        if end_time > expected_end:
            diff = end_time - expected_end
            minutes = int(diff.total_seconds() / 60)
            return round(minutes / 60, 2)
        else:
            return 0.0
    except ValueError:
        return 0.0

def parse_daily_data(api_data):
    records = []
    user_datas = api_data.get("user_datas", [])
    for user_data in user_datas:
        datas = user_data.get("datas", [])
        name = user_data.get("name", "")
        user_id = user_data.get("user_id", "")
        dept = extract_value(datas, "50102")
        emp_no = extract_value(datas, "50103")
        date = extract_value(datas, "51201")
        shift = extract_value(datas, "51202")
        group = extract_value(datas, "51203")
        scheduled_hours = extract_duration_hour(datas, "51302")
        actual_hours_api = extract_duration_hour(datas, "51303")
        leave_hours = extract_duration_hour(datas, "51401")
        intra_shift_hours = round(scheduled_hours - leave_hours, 2)

        on_time, on_status = extract_punch_time_and_status(datas, "1-1")
        off_time, off_status = extract_punch_time_and_status(datas, "1-2")
        overtime_hours = 0.0
        if off_status == "正常" and off_time != "-":
            overtime_hours = calculate_overtime(off_time)

        if date != "-":
            try:
                formatted_date = datetime.strptime(date, DATE_FORMAT).strftime(OUTPUT_DATE_FORMAT)
            except ValueError:
                formatted_date = date
        else:
            formatted_date = date

        record = {
            "姓名": name,
            "工号": emp_no,
            "部门": dept,
            "日期": formatted_date,
            "班次": shift,
            "上班打卡": on_time,
            "下班打卡": off_time,
            "应出勤(小时)": scheduled_hours,
            "实际出勤(小时)": actual_hours_api,
            "加班时长(小时)": overtime_hours,
            "请假时长": leave_hours if leave_hours > 0 else "-",
            "班内工作时长(小时)": intra_shift_hours,
            "上午打卡结果": on_status,
            "下午打卡结果": off_status,
            "考勤组": group,
            "_原始日期": date,
            "用户ID": user_id
        }
        records.append(record)
    return records


def fetch_daily_stats(date, user_ids):
    """返回 (api_data, error_message)，如果成功 error_message 为 None"""
    token = get_tenant_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8"
    }
    params = {"employee_type": EMPLOYEE_TYPE}
    payload = {
        "locale": "zh",
        "stats_type": "daily",
        "start_date": int(date),
        "end_date": int(date),
        "user_ids": user_ids,
        "need_history": True,
        "current_group_only": False,
        "user_id": USER_ID
    }
    try:
        response = requests.post(STATS_URL, headers=headers, params=params, json=payload, timeout=30)
        response.raise_for_status()
        result = response.json()
        if result.get("code") == 0:
            return result.get("data", {}), None
        else:
            return None, f"获取 {date} 数据失败: {result.get('msg')}"
    except requests.exceptions.RequestException as e:
        return None, f"网络请求异常 {date}: {e}"


def extract_value(datas, code, default="-"):
    for item in datas:
        if item.get("code") == code:
            return item.get("value", default)
    return default


def extract_duration_hour(datas, code):
    for item in datas:
        if item.get("code") == code:
            duration = item.get("duration_num", {})
            if duration:
                return float(duration.get("hour", 0))
    return 0.0


def extract_punch_time_and_status(datas, punch_sequence):
    time_code = f"51502-{punch_sequence}"
    result_code = f"51503-{punch_sequence}"
    punch_time = extract_value(datas, time_code, "-")
    punch_status = "-"
    for item in datas:
        if item.get("code") == result_code:
            features = item.get("features", [])
            for feature in features:
                if feature.get("key") == "StatusMsg":
                    punch_status = feature.get("value", "-")
                    break
            break
    return punch_time, punch_status


def calculate_overtime(actual_shift_end):
    if not actual_shift_end or actual_shift_end == "-":
        return 0.0
    try:
        end_time = datetime.strptime(actual_shift_end, "%H:%M")
        expected_end = datetime.strptime(EXPECTED_SHIFT_END_TIME, "%H:%M")
        if end_time > expected_end:
            diff = end_time - expected_end
            minutes = int(diff.total_seconds() / 60)
            return round(minutes / 60, 2)
        else:
            return 0.0
    except ValueError:
        return 0.0


def parse_daily_data(api_data):
    records = []
    user_datas = api_data.get("user_datas", [])
    for user_data in user_datas:
        datas = user_data.get("datas", [])
        name = user_data.get("name", "")
        user_id = user_data.get("user_id", "")
        dept = extract_value(datas, "50102")
        emp_no = extract_value(datas, "50103")
        date = extract_value(datas, "51201")
        shift = extract_value(datas, "51202")
        group = extract_value(datas, "51203")
        scheduled_hours = extract_duration_hour(datas, "51302")
        actual_hours_api = extract_duration_hour(datas, "51303")
        leave_hours = extract_duration_hour(datas, "51401")
        intra_shift_hours = round(scheduled_hours - leave_hours, 2)

        on_time, on_status = extract_punch_time_and_status(datas, "1-1")
        off_time, off_status = extract_punch_time_and_status(datas, "1-2")
        overtime_hours = 0.0
        if off_status == "正常" and off_time != "-":
            overtime_hours = calculate_overtime(off_time)

        if date != "-":
            try:
                formatted_date = datetime.strptime(date, DATE_FORMAT).strftime(OUTPUT_DATE_FORMAT)
            except ValueError:
                formatted_date = date
        else:
            formatted_date = date

        record = {
            "姓名": name,
            "工号": emp_no,
            "部门": dept,
            "日期": formatted_date,
            "班次": shift,
            "上班打卡": on_time,
            "下班打卡": off_time,
            "应出勤(小时)": scheduled_hours,
            "实际出勤(小时)": actual_hours_api,
            "加班时长(小时)": overtime_hours,
            "请假时长": leave_hours if leave_hours > 0 else "-",
            "班内工作时长(小时)": intra_shift_hours,
            "上午打卡结果": on_status,
            "下午打卡结果": off_status,
            "考勤组": group,
            "_原始日期": date,
            "用户ID": user_id
        }
        records.append(record)
    return records


def fetch_all_records(start_date_str, end_date_str, user_ids):
    """根据日期范围和用户ID列表获取所有原始记录（并发请求）"""
    current = datetime.strptime(start_date_str, DATE_FORMAT)
    end = datetime.strptime(end_date_str, DATE_FORMAT)
    date_list = []
    while current <= end:
        date_list.append(current.strftime(DATE_FORMAT))
        current += timedelta(days=1)

    all_records = []
    failed_dates = []
    progress_bar = st.progress(0)
    total_dates = len(date_list)

    # 使用线程池并发请求
    def fetch_one(date):
        api_data, err = fetch_daily_stats(date, user_ids)
        return date, api_data, err

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_one, date) for date in date_list]
        for i, future in enumerate(as_completed(futures)):
            date, api_data, err = future.result()
            if api_data and api_data.get("user_datas"):
                daily_records = parse_daily_data(api_data)
                all_records.extend(daily_records)
            elif err:
                failed_dates.append((date, err))
            # 更新进度条
            progress_bar.progress((i + 1) / total_dates)

    progress_bar.empty()
    if failed_dates:
        # 汇总显示失败日期
        st.warning(f"以下日期获取失败：{', '.join([d for d, _ in failed_dates])}")
    return all_records


def generate_daily_report(all_records):
    """生成日统计报表"""
    daily_report = []
    for record in all_records:
        daily_report.append({
            "用户名称": record["姓名"],
            "用户ID": record["用户ID"],
            "部门": record["部门"],
            "所属中心": record.get("所属中心", ""),
            "日期": record["日期"],
            "总应出勤(小时)": record["应出勤(小时)"],
            "总实际出勤(小时)": record["实际出勤(小时)"],
            "班内工作时长(小时)": record["班内工作时长(小时)"],
            "上班打卡时间": record["上班打卡"],
            "下班打卡时间": record["下班打卡"],
            "加班时间(小时)": record["加班时长(小时)"],
            "请假时长(小时)": record["请假时长"] if record["请假时长"] != "-" else 0.0
        })
    daily_report.sort(key=lambda x: (
        x["部门"],
        x["用户名称"],
        -int(x["日期"].replace("-", "")) if x["日期"] != "-" else 0
    ))
    return daily_report


def generate_monthly_report(all_records, month):
    """生成月统计报表"""
    user_stats = defaultdict(lambda: {
        "总应出勤": 0.0,
        "总实际出勤_api": 0.0,
        "总班内": 0.0,
        "总加班": 0.0,
        "总请假": 0.0,
        "部门": "",
        "姓名": "",
        "所属中心": ""
    })
    for record in all_records:
        user_id = record["用户ID"]
        name = record["姓名"]
        dept = record["部门"]
        center = record.get("所属中心", "")
        user_stats[user_id]["姓名"] = name
        user_stats[user_id]["部门"] = dept
        user_stats[user_id]["所属中心"] = center
        user_stats[user_id]["总应出勤"] += record["应出勤(小时)"]
        user_stats[user_id]["总实际出勤_api"] += record["实际出勤(小时)"]
        user_stats[user_id]["总班内"] += record["班内工作时长(小时)"]
        user_stats[user_id]["总加班"] += record["加班时长(小时)"]
        leave_val = record["请假时长"]
        if leave_val != "-":
            user_stats[user_id]["总请假"] += float(leave_val)
        else:
            user_stats[user_id]["总请假"] += 0.0

    monthly_report = []
    for user_id, stats in user_stats.items():
        monthly_report.append({
            "用户名称": stats["姓名"],
            "用户ID": user_id,
            "部门": stats["部门"],
            "所属中心": stats["所属中心"],
            "月份": month,
            "总应出勤时长(小时)": round(stats["总应出勤"], 2),
            "总实际出勤(小时)": round(stats["总实际出勤_api"], 2),
            "总班内工作时长(小时)": round(stats["总班内"], 2),
            "总加班时间(小时)": round(stats["总加班"], 2),
            "总请假时长(小时)": round(stats["总请假"], 2)
        })
    monthly_report.sort(key=lambda x: (x["部门"], -x["总加班时间(小时)"]))
    return monthly_report


def parse_address_book(uploaded_file):
    """从上传的Excel中读取用户ID、部门、所属中心"""
    try:
        df = pd.read_excel(uploaded_file)
        required_cols = ["用户ID", "部门", "所属中心"]
        if not all(col in df.columns for col in required_cols):
            st.error("通讯录文件必须包含列：用户ID、部门、所属中心")
            return None, None
        df = df.dropna(subset=["用户ID"]).copy()
        df["用户ID"] = df["用户ID"].astype(str).str.strip()
        df["部门"] = df["部门"].astype(str).str.strip()
        df["所属中心"] = df["所属中心"].astype(str).str.strip()
        user_ids = df["用户ID"].unique().tolist()
        return df, user_ids
    except Exception as e:
        st.error(f"解析通讯录失败: {e}")
        return None, None


# ==================== 工作日计算函数 ====================
def count_workdays(start_date, end_date):
    if start_date > end_date:
        return 0
    all_dates = pd.date_range(start=start_date, end=end_date, freq='D')
    workdays = 0
    for date in all_dates:
        year = date.year
        date_str = date.strftime("%Y-%m-%d")
        weekday = date.weekday()
        year_config = HOLIDAYS_BY_YEAR.get(year, {"holidays": [], "workweekends": []})
        holidays_set = set(year_config["holidays"])
        workweekends_set = set(year_config["workweekends"])
        if date_str in workweekends_set:
            workdays += 1
        elif date_str in holidays_set:
            continue
        elif weekday < 5:
            workdays += 1
        else:
            continue
    return workdays


# ==================== Streamlit 界面 ====================
st.set_page_config(page_title="考勤报表系统", layout="wide")

st.markdown("""
<style>
    /* ========== 全局变量 ========== */
    :root {
        --bg-primary: #ffffff;
        --bg-card: #f8f9fa;  /* 卡片背景色改为浅灰色，与白色背景区分 */
        --text-primary: #0A0A0A;
        --text-secondary: #6c757d;
        --border-light: #f0f2f6;
        --border-table: #dee2e6;
        --table-header-bg: #f8f9fa;
        --table-row-odd: #fcfcfc;
        --table-row-hover: #f0f7ff;
        --button-bg: white;
        --button-border: #dee2e6;
        --button-hover-bg: #e9ecef;
        --button-hover-border: #adb5bd;
        --shadow-card: 0 4px 6px rgba(0,0,0,0.02), 0 1px 3px rgba(0,0,0,0.05);
        --shadow-card-hover: 0 8px 12px rgba(0,0,0,0.05);
        --color-blue: #1E88E5;
        --color-red: #dc3545;
        --disabled-bg: #f1f3f5;
    }

    [data-theme="dark"] {
        --bg-primary: #0e1117;
        --bg-card: #1e1e1e;
        --text-primary: #ffffff;
        --text-secondary: #aaaaaa;
        --border-light: #333333;
        --border-table: #444444;
        --table-header-bg: #2d2d2d;
        --table-row-odd: #1e1e1e;
        --table-row-hover: #333333;
        --button-bg: #2d2d2d;
        --button-border: #444444;
        --button-hover-bg: #3d3d3d;
        --button-hover-border: #666666;
        --shadow-card: 0 4px 6px rgba(0,0,0,0.4);
        --shadow-card-hover: 0 8px 12px rgba(0,0,0,0.5);
        --color-blue: #66b0ff;
        --color-red: #ff6b6b;
        --disabled-bg: #2d2d2d;
    }

    /* ========== 标题样式 ========== */
    h1 {
        margin-top: -20px !important;
        font-size: 32px !important;
        font-weight: 700 !important;
        margin-bottom: 0.3rem !important;
        color: var(--text-primary);
        border-bottom: 2px solid var(--border-light);
        padding-bottom: 0.5rem;
    }

    /* 筛选容器压缩 */
    .filter-container {
        margin-bottom: 0 !important;
        padding-top: 0 !important;
        padding-bottom: 0 !important;
    }
    .filter-container .stSelectbox,
    .filter-container .stMultiSelect,
    .filter-container .stTextInput {
        margin-bottom: 0.1rem !important;
        margin-top: 0 !important;
    }
    .filter-container .stSelectbox label,
    .filter-container .stMultiSelect label,
    .filter-container .stTextInput label {
        margin-bottom: 0 !important;
        padding-bottom: 0 !important;
        line-height: 1.2 !important;
        font-size: 0.85rem !important;
    }

    /* 分隔线压缩 */
    hr {
        margin-top: 0.2rem !important;
        margin-bottom: 0.2rem !important;
        border: 0;
        border-top: 1px solid var(--border-light);
    }

    /* 指标卡片 */
    .metric-card {
        background: var(--bg-card);
        border-radius: 12px;
        padding: 16px;
        box-shadow: var(--shadow-card);
        transition: all 0.2s ease;
        height: 100%;
    }
    .metric-card:hover {
        box-shadow: var(--shadow-card-hover);
    }
    .metric-label {
        font-size: 14px;
        color: var(--text-secondary);
        margin-bottom: 8px;
        font-weight: 500;
    }
    .metric-value {
        font-size: 28px;
        font-weight: 700;
        line-height: 1.2;
        color: var(--text-primary);
    }
    .metric-value-blue {
        color: var(--color-blue);
    }
    .metric-value-red {
        color: var(--color-red);
    }
    .metric-warning {
        background-color: color-mix(in srgb, var(--color-red) 10%, var(--bg-card));
        border-left: 4px solid var(--color-red);
    }
    .metric-value span {
        font-size: 16px;
        font-weight: 400;
        color: var(--text-secondary);
        margin-left: 4px;
    }

    /* 卡片列间距 */
    div[data-testid="column"] {
        gap: 1rem;
    }

    /* 表格样式 */
    .stDataFrame {
        font-size: 13px;
        border-radius: 8px;
        overflow: hidden;
        box-shadow: 0 2px 4px rgba(0,0,0,0.02);
    }
    .stDataFrame thead tr th {
        background-color: var(--table-header-bg) !important;
        font-weight: 600 !important;
        border-bottom: 2px solid var(--border-table) !important;
        padding: 12px 8px !important;
        white-space: nowrap;
        color: var(--text-primary);
    }
    .stDataFrame tbody tr:nth-child(odd) {
        background-color: var(--table-row-odd);
    }
    .stDataFrame tbody tr:hover {
        background-color: var(--table-row-hover) !important;
        transition: background-color 0.15s;
    }
    .stDataFrame td {
        padding: 10px 8px !important;
        border-bottom: 1px solid var(--border-light);
        color: var(--text-primary);
    }
    /* 数值列右对齐 */
    .stDataFrame td:nth-child(8), .stDataFrame td:nth-child(9),
    .stDataFrame td:nth-child(10), .stDataFrame td:nth-child(11),
    .stDataFrame td:nth-child(12) {
        text-align: right;
        font-family: 'Courier New', monospace;
    }

    /* 分页按钮 */
    div[data-testid="stHorizontalBlock"] > div {
        display: flex;
        align-items: center;
        gap: 8px;
    }
    .stButton button:not([kind="primary"]) {
        background: var(--button-bg);
        border: 1px solid var(--button-border);
        border-radius: 6px;
        padding: 4px 12px;
        font-size: 13px;
        font-weight: 400;
        transition: all 0.15s;
        color: var(--text-primary);
    }
    .stButton button:not([kind="primary"]):hover:not(:disabled) {
        background: var(--button-hover-bg);
        border-color: var(--button-hover-border);
    }
    .stButton button:disabled {
        opacity: 0.5;
        cursor: not-allowed;
    }

    /* ========== 排序 radio 样式 ========== */
    div.sort-buttons {
        margin-bottom: 0.3rem;
    }
    div.sort-buttons .stRadio > div {
        gap: 0.2rem !important;
        justify-content: flex-start !important;
    }
    div.sort-buttons .stRadio label {
        font-size: 0.7rem !important;
        align-items: center !important;
        margin-right: 0 !important;
    }
    div.sort-buttons .stRadio label span:first-child {
        transform: scale(0.6) !important;
        margin-right: 2px !important;
    }
    /* 移除可能的按钮样式残留 */
    div.sort-buttons .stButton {
        display: none;
    }

    /* 部门多选框禁用样式 */
    .stMultiSelect[disabled] {
        background-color: var(--disabled-bg);
        border-radius: 4px;
    }

    /* 侧边栏统计卡片 */
    .sidebar-stats-card {
        background-color: var(--bg-card);
        border-radius: 12px;
        padding: 16px 12px;
        margin-top: 16px;
        box-shadow: var(--shadow-card);
        display: flex;
        justify-content: space-around;
        align-items: center;
    }
    .sidebar-stat-item {
        text-align: center;
        flex: 1;
    }
    .sidebar-stat-label {
        font-size: 12px;
        color: var(--text-secondary);
        margin-bottom: 4px;
    }
    .sidebar-stat-value {
        font-size: 24px;
        font-weight: 700;
        color: var(--color-blue);
        line-height: 1.2;
    }
    .sidebar-stat-unit {
        font-size: 14px;
        color: var(--text-secondary);
        margin-left: 4px;
        font-weight: 400;
    }

    /* 标签页上边距 */
    .stTabs {
        margin-top: 0.5rem !important;
        margin-bottom: 0 !important;
    }


</style>
""", unsafe_allow_html=True)

st.title("📊 北斗考勤报表")

# 初始化session_state
if "address_book_df" not in st.session_state:
    df_default = pd.DataFrame(DEFAULT_ADDRESS_BOOK)
    st.session_state.address_book_df = df_default
    st.session_state.user_ids = df_default["用户ID"].unique().tolist()

if "raw_records" not in st.session_state:
    st.session_state.raw_records = None
if "df_daily_raw" not in st.session_state:
    st.session_state.df_daily_raw = None
if "df_monthly_raw" not in st.session_state:
    st.session_state.df_monthly_raw = None
if "df_detail_raw" not in st.session_state:
    st.session_state.df_detail_raw = None
if "data_loaded" not in st.session_state:
    st.session_state.data_loaded = False
if "auto_load_attempted" not in st.session_state:
    st.session_state.auto_load_attempted = False
if "page_monthly" not in st.session_state:
    st.session_state.page_monthly = 1
if "page_size_monthly" not in st.session_state:
    st.session_state.page_size_monthly = 10
if "page_daily" not in st.session_state:
    st.session_state.page_daily = 1
if "page_size_daily" not in st.session_state:
    st.session_state.page_size_daily = 10
if "page_detail" not in st.session_state:
    st.session_state.page_detail = 1
if "page_size_detail" not in st.session_state:
    st.session_state.page_size_detail = 10

# 排序状态初始化（默认加班倒序）
if "sort_monthly" not in st.session_state:
    st.session_state.sort_monthly = "加班倒序"
if "sort_daily" not in st.session_state:
    st.session_state.sort_daily = "加班倒序"
if "sort_detail" not in st.session_state:
    st.session_state.sort_detail = "加班倒序"


# 快捷日期辅助函数
def get_today():
    return datetime.now().date()


def get_first_day_of_month(d):
    return d.replace(day=1)


def get_first_day_of_last_month():
    today = get_today()
    first_this = today.replace(day=1)
    last_month_last_day = first_this - timedelta(days=1)
    return last_month_last_day.replace(day=1)


def get_last_day_of_last_month():
    today = get_today()
    first_this = today.replace(day=1)
    return first_this - timedelta(days=1)


# ==================== 侧边栏 ====================
with st.sidebar:
    st.header("📋 通讯录")
    st.info(f"已加载 {len(st.session_state.user_ids)} 名员工")
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        st.session_state.address_book_df.to_excel(writer, index=False, sheet_name='通讯录')
    st.download_button(
        label="📥 下载当前通讯录",
        data=output.getvalue(),
        file_name=f"通讯录_{datetime.now().strftime('%Y%m%d')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )

    with st.expander("更换通讯录（上传Excel覆盖）"):
        uploaded_file = st.file_uploader("上传通讯录Excel文件", type=["xlsx"])
        if uploaded_file is not None:
            df_addr, ids = parse_address_book(uploaded_file)
            if df_addr is not None:
                st.session_state.address_book_df = df_addr
                st.session_state.user_ids = ids
                st.success(f"已更新通讯录，共 {len(ids)} 人")
                st.session_state.raw_records = None
                st.session_state.data_loaded = False
                st.rerun()

    st.header("📅 日期范围")

    col_a, col_b, col_c = st.columns(3)
    with col_a:
        if st.button("上月", use_container_width=True):
            st.session_state.start_date = get_first_day_of_last_month()
            st.session_state.end_date = get_last_day_of_last_month()
            st.rerun()
    with col_b:
        if st.button("本月", use_container_width=True):
            today = get_today()
            st.session_state.start_date = today.replace(day=1)
            st.session_state.end_date = today
            st.rerun()
    with col_c:
        if st.button("本年", use_container_width=True):
            today = get_today()
            st.session_state.start_date = today.replace(month=1, day=1)
            st.session_state.end_date = today
            st.rerun()

    today = get_today()
    default_start = today.replace(day=1)
    default_end = today

    start_date = st.date_input("开始日期",
                               value=st.session_state.get("start_date", default_start),
                               key="start_date")
    end_date = st.date_input("结束日期",
                             value=st.session_state.get("end_date", default_end),
                             key="end_date")

    fetch_btn = st.button("获取数据", type="primary", use_container_width=True)

    # ===== 统计周期和筛选人数卡片（动态计算） =====
    if st.session_state.data_loaded:
        # 使用侧边栏中的 start_date 和 end_date 计算工作日
        workdays_sidebar = count_workdays(start_date, end_date)
        df_month_raw = st.session_state.df_monthly_raw
        if df_month_raw is not None and not df_month_raw.empty:
            df_filtered = df_month_raw.copy()
            current_center = st.session_state.get("center_filter", "全部中心")
            current_depts = st.session_state.get("dept_filter", [])
            name_query = st.session_state.get("name_filter", "").strip()

            if current_center != "全部中心":
                df_filtered = df_filtered[df_filtered["所属中心"] == current_center]
            if current_depts:
                # 对部门列进行多部门匹配
                mask = df_filtered["部门"].apply(
                    lambda x: any(dept.strip() in current_depts for dept in str(x).split('|'))
                )
                df_filtered = df_filtered[mask]
            if name_query:
                # 姓名模糊匹配
                if "用户名称" in df_filtered.columns:
                    name_col = "用户名称"
                elif "姓名" in df_filtered.columns:
                    name_col = "姓名"
                else:
                    name_col = None
                if name_col:
                    df_filtered = df_filtered[
                        df_filtered[name_col].astype(str).str.contains(name_query, case=False, na=False)]
            person_count = len(df_filtered)
        else:
            person_count = 0

        st.markdown(f"""
        <div class="sidebar-stats-card">
            <div class="sidebar-stat-item">
                <div class="sidebar-stat-label">📆 统计周期</div>
                <div class="sidebar-stat-value">{workdays_sidebar}<span class="sidebar-stat-unit">工作日</span></div>
            </div>
            <div class="sidebar-stat-item">
                <div class="sidebar-stat-label">👥 筛选人数</div>
                <div class="sidebar-stat-value">{person_count}<span class="sidebar-stat-unit">人</span></div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("---")
        st.caption("加载数据后显示统计信息")

# ==================== 获取数据（手动） ====================
if fetch_btn:
    start_str = start_date.strftime(DATE_FORMAT)
    end_str = end_date.strftime(DATE_FORMAT)
    month_str = start_date.strftime("%Y%m")

    with st.spinner("🌞 考勤数据正在飞奔而来... 请稍候"):
        raw_records = fetch_all_records(start_str, end_str, st.session_state.user_ids)

    if not raw_records:
        st.warning("未获取到任何数据，请检查参数或网络")
        st.stop()

    addr_df = st.session_state.address_book_df
    user_to_center = dict(zip(addr_df["用户ID"], addr_df["所属中心"]))
    for r in raw_records:
        r["所属中心"] = user_to_center.get(r["用户ID"], "")

    daily_raw = generate_daily_report(raw_records)
    monthly_raw = generate_monthly_report(raw_records, month_str)
    detail_raw = sorted(raw_records, key=lambda x: (
        x["部门"],
        x["姓名"],
        -int(x["日期"].replace("-", "")) if x["日期"] != "-" else 0
    ))

    st.session_state.df_daily_raw = pd.DataFrame(daily_raw)
    st.session_state.df_monthly_raw = pd.DataFrame(monthly_raw)
    st.session_state.df_detail_raw = pd.DataFrame(detail_raw)
    st.session_state.raw_records = raw_records
    st.session_state.data_loaded = True

    st.rerun()

# ==================== 自动加载数据（首次进入） ====================
if not st.session_state.data_loaded and not st.session_state.auto_load_attempted:
    st.session_state.auto_load_attempted = True
    start_str = start_date.strftime(DATE_FORMAT)
    end_str = end_date.strftime(DATE_FORMAT)
    month_str = start_date.strftime("%Y%m")

    with st.spinner("🌞 考勤数据正在飞奔而来... 请稍候"):
        raw_records = fetch_all_records(start_str, end_str, st.session_state.user_ids)

    if raw_records:
        addr_df = st.session_state.address_book_df
        user_to_center = dict(zip(addr_df["用户ID"], addr_df["所属中心"]))
        for r in raw_records:
            r["所属中心"] = user_to_center.get(r["用户ID"], "")

        daily_raw = generate_daily_report(raw_records)
        monthly_raw = generate_monthly_report(raw_records, month_str)
        detail_raw = sorted(raw_records, key=lambda x: (
            x["部门"],
            x["姓名"],
            -int(x["日期"].replace("-", "")) if x["日期"] != "-" else 0
        ))

        st.session_state.df_daily_raw = pd.DataFrame(daily_raw)
        st.session_state.df_monthly_raw = pd.DataFrame(monthly_raw)
        st.session_state.df_detail_raw = pd.DataFrame(detail_raw)
        st.session_state.raw_records = raw_records
        st.session_state.data_loaded = True

        st.rerun()
    else:
        st.warning("自动加载数据失败，请稍后手动点击获取数据")

# ==================== 主内容区 ====================
if st.session_state.data_loaded:
    addr_df = st.session_state.address_book_df

    # ===== 筛选控件（三列布局，增加姓名搜索） =====
    with st.container():
        st.markdown('<div class="filter-container">', unsafe_allow_html=True)
        col_center, col_dept, col_name = st.columns([1, 1.5, 0.8])
        with col_center:
            centers = sorted(addr_df["所属中心"].unique())
            center_options = ["全部中心"] + centers


            def reset_dept_filter():
                center = st.session_state.center_filter
                if center == "全部中心":
                    st.session_state.dept_filter = []
                else:
                    dept_opts = sorted(addr_df[addr_df["所属中心"] == center]["部门"].unique())
                    st.session_state.dept_filter = dept_opts


            selected_center = st.selectbox(
                "🏢 选择所属中心",
                center_options,
                key="center_filter",
                on_change=reset_dept_filter
            )
        with col_dept:
            if selected_center == "全部中心":
                dept_options = sorted(addr_df["部门"].unique())
                disabled = True
                st.session_state.dept_filter = []
            else:
                dept_options = sorted(addr_df[addr_df["所属中心"] == selected_center]["部门"].unique())
                disabled = False
                if not st.session_state.get("dept_filter"):
                    st.session_state.dept_filter = dept_options
            selected_depts = st.multiselect(
                "🏷️ 选择部门",
                dept_options,
                key="dept_filter",
                disabled=disabled
            )
        with col_name:
            st.text_input(
                "🔍 姓名",
                key="name_filter",
                placeholder="输入姓名模糊匹配",
                value=st.session_state.get("name_filter", "")
            )
        st.markdown('</div>', unsafe_allow_html=True)

    # 更新当前筛选值
    current_center = selected_center
    current_depts = selected_depts
    name_query = st.session_state.get("name_filter", "").strip()


    # 过滤函数（支持多部门匹配 + 姓名模糊）
    def filter_df(df, center_col="所属中心", dept_col="部门"):
        filtered = df.copy()
        if current_center != "全部中心":
            filtered = filtered[filtered[center_col] == current_center]
        if current_depts:
            mask = filtered[dept_col].apply(
                lambda x: any(dept.strip() in current_depts for dept in str(x).split('|'))
            )
            filtered = filtered[mask]
        if name_query:
            if "用户名称" in filtered.columns:
                name_col = "用户名称"
            elif "姓名" in filtered.columns:
                name_col = "姓名"
            else:
                name_col = None
            if name_col:
                filtered = filtered[filtered[name_col].astype(str).str.contains(name_query, case=False, na=False)]
        return filtered


    # 计算工作日（用于指标卡片）
    workdays = count_workdays(st.session_state.start_date, st.session_state.end_date)

    # 过滤后的各报表
    df_daily = filter_df(st.session_state.df_daily_raw)
    df_monthly = filter_df(st.session_state.df_monthly_raw)
    df_detail = filter_df(st.session_state.df_detail_raw)

    # 计算加班TOP3（基于当前筛选后的月报表）
    if not df_monthly.empty:
        top3_list = df_monthly.nlargest(3, '总加班时间(小时)')[['用户名称', '总加班时间(小时)']].values.tolist()
    else:
        top3_list = []

    # --- 全局统计（加班指标） ---
    df_month_raw = st.session_state.df_monthly_raw
    if df_month_raw is not None and not df_month_raw.empty:
        avg_all = df_month_raw["总加班时间(小时)"].mean()
        df_rd = df_month_raw[df_month_raw["所属中心"] == "产研中心"]
        avg_rd = df_rd["总加班时间(小时)"].mean() if not df_rd.empty else 0.0

        show_selected_center = current_center != "全部中心" and current_center != "产研中心"
        if show_selected_center:
            df_selected = df_month_raw[df_month_raw["所属中心"] == current_center]
            avg_selected = df_selected["总加班时间(小时)"].mean() if not df_selected.empty else 0.0
        else:
            avg_selected = None

        daily_avg_all = avg_all / workdays if workdays > 0 else 0.0
        daily_avg_rd = avg_rd / workdays if workdays > 0 else 0.0
        if show_selected_center:
            daily_avg_selected = avg_selected / workdays if workdays > 0 else 0.0
    else:
        avg_all = avg_rd = daily_avg_all = daily_avg_rd = 0.0
        show_selected_center = False
        avg_selected = daily_avg_selected = None


    # 指标卡片生成函数
    def metric_card(label, value, unit="小时", color=None, warning=False, blue=False):
        color_class = "metric-value-red" if color == "red" else ("metric-value-blue" if blue else "")
        warning_class = " metric-warning" if warning else ""
        unit_display = f"<span style='font-size: 16px; font-weight: 400; color: #6c757d; margin-left: 4px;'>{unit}</span>" if unit else ""
        return f"""
        <div class="metric-card{warning_class}">
            <div class="metric-label">{label}</div>
            <div class="metric-value {color_class}">{value:.2f}{unit_display}</div>
        </div>
        """


    # TOP3卡片函数
    def top3_card(top3_data):
        top3_html = '<div class="metric-card" style="height: 100%; display: flex; flex-direction: column; justify-content: center;"><div class="metric-label" style="margin-bottom: 16px; font-size: 18px;">🐝 小蜜蜂TOP3</div>'
        if top3_data and len(top3_data) > 0:
            for i, (name, hours) in enumerate(top3_data, 1):
                medal = "🥇" if i == 1 else ("🥈" if i == 2 else "🥉")
                top3_html += f'<div style="display: flex; justify-content: space-between; align-items: baseline; font-size: 16px; margin-bottom: 10px;"><span>{medal} {name}</span><span class="metric-value-blue" style="font-size: 18px;">{hours:.1f} 小时</span></div>'
        else:
            top3_html += '<div style="color: var(--text-secondary); font-size: 16px;">暂无数据</div>'
        top3_html += '</div>'
        return top3_html


    # 确定列数及比例
    if show_selected_center:
        col_widths = [1.2, 1, 1, 1]
    else:
        col_widths = [1.2, 1, 1]

    cols = st.columns(col_widths)

    # 第一列：TOP3卡片
    with cols[0]:
        st.markdown(top3_card(top3_list), unsafe_allow_html=True)

    # 第二列：全公司指标
    with cols[1]:
        st.markdown(metric_card("🏢 全公司人均加班", avg_all), unsafe_allow_html=True)
        st.markdown(metric_card("🏢 全公司人均日均加班", daily_avg_all, unit="小时/日"), unsafe_allow_html=True)

    # 第三列：产研中心指标
    warning_rd = (avg_rd < avg_all) or (show_selected_center and avg_rd < avg_selected)
    with cols[2]:
        st.markdown(metric_card("⚙️ 产研中心人均加班", avg_rd, color="red" if warning_rd else None, warning=warning_rd),
                    unsafe_allow_html=True)
        st.markdown(
            metric_card("⚙️ 产研中心人均日均加班", daily_avg_rd, unit="小时/日", color="red" if warning_rd else None,
                        warning=warning_rd), unsafe_allow_html=True)

    # 第四列（如果有）：选中中心指标
    if show_selected_center:
        with cols[3]:
            st.markdown(metric_card(f"🏷️ {current_center}人均加班", avg_selected), unsafe_allow_html=True)
            st.markdown(metric_card(f"🏷️ {current_center}人均日均加班", daily_avg_selected, unit="小时/日"),
                        unsafe_allow_html=True)

    # 标签页
    tab1, tab2, tab3 = st.tabs(["📅 月报表", "📆 日报表", "📋 明细数据"])

    # ---------- 月报表 ----------
    with tab1:
        # 排序 radio
        st.markdown('<div class="sort-buttons">', unsafe_allow_html=True)
        st.radio(
            "排序",
            options=["加班倒序", "请假倒序"],
            horizontal=True,
            key="sort_monthly",
            label_visibility="collapsed"
        )
        st.markdown('</div>', unsafe_allow_html=True)

        # 根据排序状态排序
        df_display = df_monthly.copy()
        if st.session_state.sort_monthly == "加班倒序":
            df_display = df_display.sort_values(by="总加班时间(小时)", ascending=False)
        else:
            df_display = df_display.sort_values(by="总请假时长(小时)", ascending=False)

        if not df_display.empty:
            df_display.insert(0, '序号', range(1, len(df_display) + 1))

        # 分页切片
        total_rows = len(df_display)
        start_idx = (st.session_state.page_monthly - 1) * st.session_state.page_size_monthly
        end_idx = min(start_idx + st.session_state.page_size_monthly, total_rows)
        df_page = df_display.iloc[start_idx:end_idx] if total_rows > 0 else df_display

        column_config = {
            "序号": st.column_config.NumberColumn(width=80),
            "用户名称": st.column_config.TextColumn(width=150),
            "用户ID": st.column_config.TextColumn(width=100),
            "部门": st.column_config.TextColumn(width=150),
            "所属中心": st.column_config.TextColumn(width=150),
            "月份": st.column_config.TextColumn(width=80),
            "总应出勤时长(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
            "总实际出勤(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
            "总班内工作时长(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
            "总加班时间(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
            "总请假时长(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
        }
        st.dataframe(df_page, column_config=column_config, use_container_width=True, hide_index=True)

        # 分页控件
        total_pages = (total_rows - 1) // st.session_state.page_size_monthly + 1 if total_rows > 0 else 1
        if st.session_state.page_monthly > total_pages:
            st.session_state.page_monthly = total_pages
        if st.session_state.page_monthly < 1:
            st.session_state.page_monthly = 1

        col_left, col_right = st.columns([1, 2])
        with col_left:
            left_inner_cols = st.columns([1, 1])
            with left_inner_cols[0]:
                st.markdown(f"**共 {len(df_monthly)} 条记录**")
            with left_inner_cols[1]:
                page_size_options = [10, 20, 50, 100]
                selected_size = st.selectbox(
                    "每页行数",
                    options=page_size_options,
                    index=page_size_options.index(st.session_state.page_size_monthly),
                    key="page_size_monthly_select",
                    label_visibility="collapsed"
                )
                if selected_size != st.session_state.page_size_monthly:
                    st.session_state.page_size_monthly = selected_size
                    st.session_state.page_monthly = 1
                    st.rerun()

        with col_right:
            pagination_cols = st.columns([1, 1, 1, 1, 1])
            with pagination_cols[0]:
                if st.button("⏮️ 首页", disabled=(st.session_state.page_monthly == 1), key="first_monthly"):
                    st.session_state.page_monthly = 1
                    st.rerun()
            with pagination_cols[1]:
                if st.button("← 上一页", disabled=(st.session_state.page_monthly == 1), key="prev_monthly"):
                    st.session_state.page_monthly -= 1
                    st.rerun()
            with pagination_cols[2]:
                st.markdown(
                    f"<div style='text-align: center; font-weight: 600;'>{st.session_state.page_monthly} / {total_pages}</div>",
                    unsafe_allow_html=True)
            with pagination_cols[3]:
                if st.button("下一页 →", disabled=(st.session_state.page_monthly == total_pages), key="next_monthly"):
                    st.session_state.page_monthly += 1
                    st.rerun()
            with pagination_cols[4]:
                if st.button("⏭️ 末页", disabled=(st.session_state.page_monthly == total_pages), key="last_monthly"):
                    st.session_state.page_monthly = total_pages
                    st.rerun()

    # ---------- 日报表 ----------
    with tab2:
        # 排序 radio
        st.markdown('<div class="sort-buttons">', unsafe_allow_html=True)
        st.radio(
            "排序",
            options=["加班倒序", "请假倒序"],
            horizontal=True,
            key="sort_daily",
            label_visibility="collapsed"
        )
        st.markdown('</div>', unsafe_allow_html=True)

        df_display = df_daily.copy()
        if st.session_state.sort_daily == "加班倒序":
            df_display = df_display.sort_values(by="加班时间(小时)", ascending=False)
        else:
            df_display = df_display.sort_values(by="请假时长(小时)", ascending=False)

        if not df_display.empty:
            df_display.insert(0, '序号', range(1, len(df_display) + 1))

        total_rows = len(df_display)
        start_idx = (st.session_state.page_daily - 1) * st.session_state.page_size_daily
        end_idx = min(start_idx + st.session_state.page_size_daily, total_rows)
        df_page = df_display.iloc[start_idx:end_idx] if total_rows > 0 else df_display

        column_config = {
            "序号": st.column_config.NumberColumn(width=80),
            "用户名称": st.column_config.TextColumn(width=150),
            "用户ID": st.column_config.TextColumn(width=100),
            "部门": st.column_config.TextColumn(width=150),
            "所属中心": st.column_config.TextColumn(width=150),
            "日期": st.column_config.TextColumn(width=100),
            "总应出勤(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
            "总实际出勤(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
            "班内工作时长(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
            "加班时间(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
            "请假时长(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
        }
        st.dataframe(df_page, column_config=column_config, use_container_width=True, hide_index=True)

        total_pages = (total_rows - 1) // st.session_state.page_size_daily + 1 if total_rows > 0 else 1
        if st.session_state.page_daily > total_pages:
            st.session_state.page_daily = total_pages
        if st.session_state.page_daily < 1:
            st.session_state.page_daily = 1

        col_left, col_right = st.columns([1, 2])
        with col_left:
            left_inner_cols = st.columns([1, 1])
            with left_inner_cols[0]:
                st.markdown(f"**共 {len(df_daily)} 条记录**")
            with left_inner_cols[1]:
                page_size_options = [10, 20, 50, 100]
                selected_size = st.selectbox(
                    "每页行数",
                    options=page_size_options,
                    index=page_size_options.index(st.session_state.page_size_daily),
                    key="page_size_daily_select",
                    label_visibility="collapsed"
                )
                if selected_size != st.session_state.page_size_daily:
                    st.session_state.page_size_daily = selected_size
                    st.session_state.page_daily = 1
                    st.rerun()

        with col_right:
            pagination_cols = st.columns([1, 1, 1, 1, 1])
            with pagination_cols[0]:
                if st.button("⏮️ 首页", disabled=(st.session_state.page_daily == 1), key="first_daily"):
                    st.session_state.page_daily = 1
                    st.rerun()
            with pagination_cols[1]:
                if st.button("← 上一页", disabled=(st.session_state.page_daily == 1), key="prev_daily"):
                    st.session_state.page_daily -= 1
                    st.rerun()
            with pagination_cols[2]:
                st.markdown(
                    f"<div style='text-align: center; font-weight: 600;'>{st.session_state.page_daily} / {total_pages}</div>",
                    unsafe_allow_html=True)
            with pagination_cols[3]:
                if st.button("下一页 →", disabled=(st.session_state.page_daily == total_pages), key="next_daily"):
                    st.session_state.page_daily += 1
                    st.rerun()
            with pagination_cols[4]:
                if st.button("⏭️ 末页", disabled=(st.session_state.page_daily == total_pages), key="last_daily"):
                    st.session_state.page_daily = total_pages
                    st.rerun()

    # ---------- 明细数据 ----------
    with tab3:
        # 排序 radio
        st.markdown('<div class="sort-buttons">', unsafe_allow_html=True)
        st.radio(
            "排序",
            options=["加班倒序", "请假倒序"],
            horizontal=True,
            key="sort_detail",
            label_visibility="collapsed"
        )
        st.markdown('</div>', unsafe_allow_html=True)

        df_display = df_detail.copy()
        # 将“请假时长”列中的字符串转换为数值
        df_display['请假时长'] = pd.to_numeric(df_display['请假时长'], errors='coerce').fillna(0)

        if st.session_state.sort_detail == "加班倒序":
            df_display = df_display.sort_values(by="加班时长(小时)", ascending=False)
        else:
            df_display = df_display.sort_values(by="请假时长", ascending=False)

        if not df_display.empty:
            df_display.insert(0, '序号', range(1, len(df_display) + 1))

        total_rows = len(df_display)
        start_idx = (st.session_state.page_detail - 1) * st.session_state.page_size_detail
        end_idx = min(start_idx + st.session_state.page_size_detail, total_rows)
        df_page = df_display.iloc[start_idx:end_idx] if total_rows > 0 else df_display

        column_config = {
            "序号": st.column_config.NumberColumn(width=80),
            "姓名": st.column_config.TextColumn(width=120),
            "工号": st.column_config.TextColumn(width=100),
            "部门": st.column_config.TextColumn(width=150),
            "日期": st.column_config.TextColumn(width=100),
            "班次": st.column_config.TextColumn(width=100),
            "上班打卡": st.column_config.TextColumn(width=100),
            "下班打卡": st.column_config.TextColumn(width=100),
            "应出勤(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
            "实际出勤(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
            "加班时长(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
            "请假时长": st.column_config.NumberColumn(width=80, format="%.2f"),
            "班内工作时长(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
            "上午打卡结果": st.column_config.TextColumn(width=100),
            "下午打卡结果": st.column_config.TextColumn(width=100),
            "考勤组": st.column_config.TextColumn(width=120),
            "用户ID": st.column_config.TextColumn(width=100),
        }
        st.dataframe(df_page, column_config=column_config, use_container_width=True, hide_index=True)

        total_pages = (total_rows - 1) // st.session_state.page_size_detail + 1 if total_rows > 0 else 1
        if st.session_state.page_detail > total_pages:
            st.session_state.page_detail = total_pages
        if st.session_state.page_detail < 1:
            st.session_state.page_detail = 1

        col_left, col_right = st.columns([1, 2])
        with col_left:
            left_inner_cols = st.columns([1, 1])
            with left_inner_cols[0]:
                st.markdown(f"**共 {len(df_detail)} 条记录**")
            with left_inner_cols[1]:
                page_size_options = [10, 20, 50, 100]
                selected_size = st.selectbox(
                    "每页行数",
                    options=page_size_options,
                    index=page_size_options.index(st.session_state.page_size_detail),
                    key="page_size_detail_select",
                    label_visibility="collapsed"
                )
                if selected_size != st.session_state.page_size_detail:
                    st.session_state.page_size_detail = selected_size
                    st.session_state.page_detail = 1
                    st.rerun()

        with col_right:
            pagination_cols = st.columns([1, 1, 1, 1, 1])
            with pagination_cols[0]:
                if st.button("⏮️ 首页", disabled=(st.session_state.page_detail == 1), key="first_detail"):
                    st.session_state.page_detail = 1
                    st.rerun()
            with pagination_cols[1]:
                if st.button("← 上一页", disabled=(st.session_state.page_detail == 1), key="prev_detail"):
                    st.session_state.page_detail -= 1
                    st.rerun()
            with pagination_cols[2]:
                st.markdown(
                    f"<div style='text-align: center; font-weight: 600;'>{st.session_state.page_detail} / {total_pages}</div>",
                    unsafe_allow_html=True)
            with pagination_cols[3]:
                if st.button("下一页 →", disabled=(st.session_state.page_detail == total_pages), key="next_detail"):
                    st.session_state.page_detail += 1
                    st.rerun()
            with pagination_cols[4]:
                if st.button("⏭️ 末页", disabled=(st.session_state.page_detail == total_pages), key="last_detail"):
                    st.session_state.page_detail = total_pages
                    st.rerun()

else:
    st.info("🌞 打工人，考勤数据正在飞奔而来... 请稍候或点击「获取数据」")