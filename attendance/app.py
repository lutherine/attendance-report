import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
import time
import io
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- 配置区域 ---
APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")
USER_ID = os.getenv("USER_ID")
EMPLOYEE_TYPE = "employee_id"

TOKEN_URL = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
STATS_URL = "https://open.feishu.cn/open-apis/attendance/v1/user_stats_datas/query"
EXPECTED_SHIFT_END_TIME = "17:30"
DATE_FORMAT = "%Y%m%d"
OUTPUT_DATE_FORMAT = "%Y-%m-%d"
TOKEN_EXPIRE_BUFFER = 60

_token = None
_token_expire_time = 0

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

# ==================== 部门简化函数 ====================
def simplify_dept_path(dept_str):
    if not isinstance(dept_str, str) or dept_str == "-" or dept_str == "":
        return dept_str
    if ',' in dept_str:
        parts = dept_str.split(',')
        simplified_parts = []
        for part in parts:
            part = part.strip()
            if '/' in part:
                leaf = part.split('/')[-1].strip()
            else:
                leaf = part
            simplified_parts.append(leaf)
        return ','.join(simplified_parts)
    else:
        if '/' in dept_str:
            return dept_str.split('/')[-1].strip()
        else:
            return dept_str.strip()

# ==================== 原始完整路径的默认通讯录 ====================
RAW_DEFAULT_ADDRESS_BOOK = [
    {"用户ID": "10010006", "姓名": "康习梅", "部门": "北京北斗银河科技有限公司/产研中心", "所属中心": "产研中心"},
    {"用户ID": "10010007", "姓名": "李珉", "部门": "北京北斗银河科技有限公司/产研中心", "所属中心": "产研中心"},
    {"用户ID": "10010014", "姓名": "赵婷", "部门": "北京北斗银河科技有限公司/产研中心", "所属中心": "产研中心"},
    {"用户ID": "10010095", "姓名": "丁霞", "部门": "北京北斗银河科技有限公司/产研中心", "所属中心": "产研中心"},
    {"用户ID": "10010121", "姓名": "张自强", "部门": "北京北斗银河科技有限公司/产研中心", "所属中心": "产研中心"},
    {"用户ID": "10010146", "姓名": "江轶", "部门": "北京北斗银河科技有限公司/产研中心", "所属中心": "产研中心"},
    {"用户ID": "10010044", "姓名": "王宗飒", "部门": "北京北斗银河科技有限公司/产研中心/产品部", "所属中心": "产研中心"},
    {"用户ID": "10010045", "姓名": "梁子岳", "部门": "北京北斗银河科技有限公司/产研中心/产品部", "所属中心": "产研中心"},
    {"用户ID": "10010046", "姓名": "刘金格", "部门": "北京北斗银河科技有限公司/产研中心/产品部", "所属中心": "产研中心"},
    {"用户ID": "10010049", "姓名": "王俊杰", "部门": "北京北斗银河科技有限公司/产研中心/产品部", "所属中心": "产研中心"},
    {"用户ID": "10010050", "姓名": "韩敬轩", "部门": "北京北斗银河科技有限公司/产研中心/产品部", "所属中心": "产研中心"},
    {"用户ID": "10010051", "姓名": "梁瑞", "部门": "北京北斗银河科技有限公司/产研中心/产品部", "所属中心": "产研中心"},
    {"用户ID": "10010147", "姓名": "徐智达", "部门": "北京北斗银河科技有限公司/产研中心/产品部", "所属中心": "产研中心"},
    {"用户ID": "10010153", "姓名": "吴亮", "部门": "北京北斗银河科技有限公司/产研中心/产品部", "所属中心": "产研中心"},
    {"用户ID": "10010037", "姓名": "王振业", "部门": "北京北斗银河科技有限公司/产研中心/生态平台部", "所属中心": "产研中心"},
    {"用户ID": "10010055", "姓名": "唐嘉璐", "部门": "北京北斗银河科技有限公司/产研中心/生态平台部", "所属中心": "产研中心"},
    {"用户ID": "10010069", "姓名": "李鹏旭", "部门": "北京北斗银河科技有限公司/产研中心/生态平台部", "所属中心": "产研中心"},
    {"用户ID": "10010071", "姓名": "顾晓虎", "部门": "北京北斗银河科技有限公司/产研中心/生态平台部", "所属中心": "产研中心"},
    {"用户ID": "10010072", "姓名": "郑露", "部门": "北京北斗银河科技有限公司/产研中心/生态平台部", "所属中心": "产研中心"},
    {"用户ID": "10010073", "姓名": "苗春雨", "部门": "北京北斗银河科技有限公司/产研中心/生态平台部", "所属中心": "产研中心"},
    {"用户ID": "10010075", "姓名": "杜婉纯", "部门": "北京北斗银河科技有限公司/产研中心/生态平台部", "所属中心": "产研中心"},
    {"用户ID": "10010077", "姓名": "沈晓文", "部门": "北京北斗银河科技有限公司/产研中心/生态平台部", "所属中心": "产研中心"},
    {"用户ID": "10010078", "姓名": "郑逸梅", "部门": "北京北斗银河科技有限公司/产研中心/生态平台部", "所属中心": "产研中心"},
    {"用户ID": "10010129", "姓名": "王梓旭", "部门": "北京北斗银河科技有限公司/产研中心/生态平台部", "所属中心": "产研中心"},
    {"用户ID": "10010157", "姓名": "徐九伍", "部门": "北京北斗银河科技有限公司/产研中心/生态平台部", "所属中心": "产研中心"},
    {"用户ID": "10010160", "姓名": "郝柯翔", "部门": "北京北斗银河科技有限公司/产研中心/生态平台部", "所属中心": "产研中心"},
    {"用户ID": "10010161", "姓名": "周恩琴", "部门": "北京北斗银河科技有限公司/产研中心/生态平台部", "所属中心": "产研中心"},
    {"用户ID": "10010052", "姓名": "任庆", "部门": "北京北斗银河科技有限公司/产研中心/硬件平台部", "所属中心": "产研中心"},
    {"用户ID": "10010053", "姓名": "梁臣", "部门": "北京北斗银河科技有限公司/产研中心/硬件平台部", "所属中心": "产研中心"},
    {"用户ID": "10010054", "姓名": "陈召", "部门": "北京北斗银河科技有限公司/产研中心/硬件平台部", "所属中心": "产研中心"},
    {"用户ID": "10010058", "姓名": "王佳旻", "部门": "北京北斗银河科技有限公司/产研中心/硬件平台部", "所属中心": "产研中心"},
    {"用户ID": "10010151", "姓名": "潘申杰", "部门": "北京北斗银河科技有限公司/产研中心/硬件平台部", "所属中心": "产研中心"},
    {"用户ID": "10010059", "姓名": "朱玉佳", "部门": "北京北斗银河科技有限公司/产研中心/软件部", "所属中心": "产研中心"},
    {"用户ID": "10010061", "姓名": "丁新文", "部门": "北京北斗银河科技有限公司/产研中心/软件部", "所属中心": "产研中心"},
    {"用户ID": "10010062", "姓名": "李大祥", "部门": "北京北斗银河科技有限公司/产研中心/软件部", "所属中心": "产研中心"},
    {"用户ID": "10010064", "姓名": "任志强", "部门": "北京北斗银河科技有限公司/产研中心/软件部", "所属中心": "产研中心"},
    {"用户ID": "10010067", "姓名": "朱言付", "部门": "北京北斗银河科技有限公司/产研中心/软件部", "所属中心": "产研中心"},
    {"用户ID": "10010128", "姓名": "季娟", "部门": "北京北斗银河科技有限公司/产研中心/软件部", "所属中心": "产研中心"},
    {"用户ID": "10010011", "姓名": "徐海静", "部门": "北京北斗银河科技有限公司/人力中心", "所属中心": "人力中心"},
    {"用户ID": "10010117", "姓名": "任杰", "部门": "北京北斗银河科技有限公司/人力中心/人力资源部", "所属中心": "人力中心"},
    {"用户ID": "10010118", "姓名": "任泽盈", "部门": "北京北斗银河科技有限公司/人力中心/人力资源部", "所属中心": "人力中心"},
    {"用户ID": "10010120", "姓名": "赵敏婷", "部门": "北京北斗银河科技有限公司/人力中心/人力资源部", "所属中心": "人力中心"},
    {"用户ID": "10010136", "姓名": "高海丽", "部门": "北京北斗银河科技有限公司/人力中心/人力资源部", "所属中心": "人力中心"},
    {"用户ID": "10010009", "姓名": "孙晓萍", "部门": "北京北斗银河科技有限公司/总经办", "所属中心": "总经办"},
    {"用户ID": "10010015", "姓名": "闫涛", "部门": "北京北斗银河科技有限公司/总经办", "所属中心": "总经办"},
    {"用户ID": "10010122", "姓名": "郭春林", "部门": "北京北斗银河科技有限公司/总经办", "所属中心": "总经办"},
    {"用户ID": "10010012", "姓名": "张永", "部门": "北京北斗银河科技有限公司/总经办,北京北斗银河科技有限公司/技术研究中心", "所属中心": "总经办"},
    {"用户ID": "10010111", "姓名": "田晗", "部门": "北京北斗银河科技有限公司/总经办/行政部", "所属中心": "总经办"},
    {"用户ID": "10010112", "姓名": "邵平", "部门": "北京北斗银河科技有限公司/总经办/行政部", "所属中心": "总经办"},
    {"用户ID": "10010113", "姓名": "陈晨", "部门": "北京北斗银河科技有限公司/总经办/行政部", "所属中心": "总经办"},
    {"用户ID": "10010115", "姓名": "杜雪茹", "部门": "北京北斗银河科技有限公司/总经办/行政部", "所属中心": "总经办"},
    {"用户ID": "10010116", "姓名": "丁信允", "部门": "北京北斗银河科技有限公司/总经办/行政部", "所属中心": "总经办"},
    {"用户ID": "10010125", "姓名": "何桂芳", "部门": "北京北斗银河科技有限公司/总经办/行政部", "所属中心": "总经办"},
    {"用户ID": "10010135", "姓名": "胡婕", "部门": "北京北斗银河科技有限公司/总经办/行政部", "所属中心": "总经办"},
    {"用户ID": "10010068", "姓名": "聂赛赛", "部门": "北京北斗银河科技有限公司/技术研究中心/南京研究中心", "所属中心": "技术研究中心"},
    {"用户ID": "10010070", "姓名": "甘奕", "部门": "北京北斗银河科技有限公司/技术研究中心/南京研究中心", "所属中心": "技术研究中心"},
    {"用户ID": "10010138", "姓名": "柳彬", "部门": "北京北斗银河科技有限公司/技术研究中心/南京研究中心", "所属中心": "技术研究中心"},
    {"用户ID": "10010139", "姓名": "胡一苇", "部门": "北京北斗银河科技有限公司/技术研究中心/南京研究中心", "所属中心": "技术研究中心"},
    {"用户ID": "10010140", "姓名": "哈依沙尔·木哈达", "部门": "北京北斗银河科技有限公司/技术研究中心/南京研究中心", "所属中心": "技术研究中心"},
    {"用户ID": "10010145", "姓名": "郭龙浦", "部门": "北京北斗银河科技有限公司/技术研究中心/南京研究中心", "所属中心": "技术研究中心"},
    {"用户ID": "10010123", "姓名": "石启月", "部门": "北京北斗银河科技有限公司/技术研究中心/清华无锡院智能配电应用技术研究中心", "所属中心": "技术研究中心"},
    {"用户ID": "10010002", "姓名": "毕陆超", "部门": "北京北斗银河科技有限公司/营销中心", "所属中心": "营销中心"},
    {"用户ID": "10010159", "姓名": "梅星星", "部门": "北京北斗银河科技有限公司/营销中心", "所属中心": "营销中心"},
    {"用户ID": "10010003", "姓名": "刘立成", "部门": "北京北斗银河科技有限公司/营销中心,北京北斗银河科技有限公司/营销中心/销售部", "所属中心": "营销中心"},
    {"用户ID": "10010029", "姓名": "刘雅莉", "部门": "北京北斗银河科技有限公司/营销中心/商务部", "所属中心": "营销中心"},
    {"用户ID": "10010030", "姓名": "贺蕾蕾", "部门": "北京北斗银河科技有限公司/营销中心/商务部", "所属中心": "营销中心"},
    {"用户ID": "10010031", "姓名": "郑秀雨", "部门": "北京北斗银河科技有限公司/营销中心/商务部", "所属中心": "营销中心"},
    {"用户ID": "10010032", "姓名": "邓丽莉", "部门": "北京北斗银河科技有限公司/营销中心/商务部", "所属中心": "营销中心"},
    {"用户ID": "10010033", "姓名": "包喜乐", "部门": "北京北斗银河科技有限公司/营销中心/商务部", "所属中心": "营销中心"},
    {"用户ID": "10010124", "姓名": "张梦渊", "部门": "北京北斗银河科技有限公司/营销中心/商务部", "所属中心": "营销中心"},
    {"用户ID": "10010005", "姓名": "周媛", "部门": "北京北斗银河科技有限公司/营销中心/市场部", "所属中心": "营销中心"},
    {"用户ID": "10010024", "姓名": "潘广树", "部门": "北京北斗银河科技有限公司/营销中心/市场部", "所属中心": "营销中心"},
    {"用户ID": "10010025", "姓名": "杨浩", "部门": "北京北斗银河科技有限公司/营销中心/市场部", "所属中心": "营销中心"},
    {"用户ID": "10010035", "姓名": "谢春峰", "部门": "北京北斗银河科技有限公司/营销中心/市场部", "所属中心": "营销中心"},
    {"用户ID": "10010041", "姓名": "田雨", "部门": "北京北斗银河科技有限公司/营销中心/市场部", "所属中心": "营销中心"},
    {"用户ID": "10010154", "姓名": "王明旭", "部门": "北京北斗银河科技有限公司/营销中心/市场部", "所属中心": "营销中心"},
    {"用户ID": "10010155", "姓名": "范德伟", "部门": "北京北斗银河科技有限公司/营销中心/市场部", "所属中心": "营销中心"},
    {"用户ID": "10010004", "姓名": "凌洁", "部门": "北京北斗银河科技有限公司/营销中心/销售部", "所属中心": "营销中心"},
    {"用户ID": "10010020", "姓名": "吕孟樊", "部门": "北京北斗银河科技有限公司/营销中心/销售部", "所属中心": "营销中心"},
    {"用户ID": "10010022", "姓名": "刘岚", "部门": "北京北斗银河科技有限公司/营销中心/销售部", "所属中心": "营销中心"},
    {"用户ID": "10010016", "姓名": "王磊传", "部门": "北京北斗银河科技有限公司/营销中心/销售部/销售东部大区", "所属中心": "营销中心"},
    {"用户ID": "10010017", "姓名": "李岩", "部门": "北京北斗银河科技有限公司/营销中心/销售部/销售东部大区", "所属中心": "营销中心"},
    {"用户ID": "10010018", "姓名": "王鹏程", "部门": "北京北斗银河科技有限公司/营销中心/销售部/销售东部大区", "所属中心": "营销中心"},
    {"用户ID": "10010026", "姓名": "张海刚", "部门": "北京北斗银河科技有限公司/营销中心/销售部/销售北部大区", "所属中心": "营销中心"},
    {"用户ID": "10010126", "姓名": "曾凯", "部门": "北京北斗银河科技有限公司/营销中心/销售部/销售北部大区", "所属中心": "营销中心"},
    {"用户ID": "10010019", "姓名": "毕鲁驹", "部门": "北京北斗银河科技有限公司/营销中心/销售部/销售南部大区", "所属中心": "营销中心"},
    {"用户ID": "10010021", "姓名": "陈宇轩", "部门": "北京北斗银河科技有限公司/营销中心/销售部/销售南部大区", "所属中心": "营销中心"},
    {"用户ID": "10010023", "姓名": "张波棋", "部门": "北京北斗银河科技有限公司/营销中心/销售部/销售西部大区", "所属中心": "营销中心"},
    {"用户ID": "10010152", "姓名": "王浩东", "部门": "北京北斗银河科技有限公司/营销中心/销售部/销售西部大区", "所属中心": "营销中心"},
    {"用户ID": "10010008", "姓名": "姜士会", "部门": "北京北斗银河科技有限公司/财务中心", "所属中心": "财务中心"},
    {"用户ID": "10010106", "姓名": "范晓燕", "部门": "北京北斗银河科技有限公司/财务中心/财务部", "所属中心": "财务中心"},
    {"用户ID": "10010107", "姓名": "贾爱君", "部门": "北京北斗银河科技有限公司/财务中心/财务部", "所属中心": "财务中心"},
    {"用户ID": "10010108", "姓名": "孔海燕", "部门": "北京北斗银河科技有限公司/财务中心/财务部", "所属中心": "财务中心"},
    {"用户ID": "10010109", "姓名": "刘凤", "部门": "北京北斗银河科技有限公司/财务中心/财务部", "所属中心": "财务中心"},
    {"用户ID": "10010110", "姓名": "高梦博", "部门": "北京北斗银河科技有限公司/财务中心/财务部", "所属中心": "财务中心"},
    {"用户ID": "10010158", "姓名": "孙如霞", "部门": "北京北斗银河科技有限公司/财务中心/财务部", "所属中心": "财务中心"},
    {"用户ID": "10010010", "姓名": "姜佳村", "部门": "北京北斗银河科技有限公司/运营中心", "所属中心": "运营中心"},
    {"用户ID": "10010013", "姓名": "李永年", "部门": "北京北斗银河科技有限公司/运营中心", "所属中心": "运营中心"},
    {"用户ID": "10010098", "姓名": "孙海勇", "部门": "北京北斗银河科技有限公司/运营中心/仓储部", "所属中心": "运营中心"},
    {"用户ID": "10010099", "姓名": "李孟军", "部门": "北京北斗银河科技有限公司/运营中心/仓储部", "所属中心": "运营中心"},
    {"用户ID": "10010100", "姓名": "章磊", "部门": "北京北斗银河科技有限公司/运营中心/仓储部", "所属中心": "运营中心"},
    {"用户ID": "10010101", "姓名": "孟园奇", "部门": "北京北斗银河科技有限公司/运营中心/仓储部", "所属中心": "运营中心"},
    {"用户ID": "10010102", "姓名": "陆连富", "部门": "北京北斗银河科技有限公司/运营中心/仓储部", "所属中心": "运营中心"},
    {"用户ID": "10010156", "姓名": "谢文艳", "部门": "北京北斗银河科技有限公司/运营中心/仓储部", "所属中心": "运营中心"},
    {"用户ID": "10010083", "姓名": "孟令桃", "部门": "北京北斗银河科技有限公司/运营中心/生产部", "所属中心": "运营中心"},
    {"用户ID": "10010084", "姓名": "周媛媛", "部门": "北京北斗银河科技有限公司/运营中心/生产部", "所属中心": "运营中心"},
    {"用户ID": "10010085", "姓名": "陈志玲", "部门": "北京北斗银河科技有限公司/运营中心/生产部", "所属中心": "运营中心"},
    {"用户ID": "10010086", "姓名": "嵇峰", "部门": "北京北斗银河科技有限公司/运营中心/生产部", "所属中心": "运营中心"},
    {"用户ID": "10010087", "姓名": "高守艳", "部门": "北京北斗银河科技有限公司/运营中心/生产部", "所属中心": "运营中心"},
    {"用户ID": "10010088", "姓名": "周红霞", "部门": "北京北斗银河科技有限公司/运营中心/生产部", "所属中心": "运营中心"},
    {"用户ID": "10010089", "姓名": "马燕", "部门": "北京北斗银河科技有限公司/运营中心/生产部", "所属中心": "运营中心"},
    {"用户ID": "10010090", "姓名": "邹沭城", "部门": "北京北斗银河科技有限公司/运营中心/生产部", "所属中心": "运营中心"},
    {"用户ID": "10010091", "姓名": "潘文", "部门": "北京北斗银河科技有限公司/运营中心/生产部", "所属中心": "运营中心"},
    {"用户ID": "10010092", "姓名": "申雅莉", "部门": "北京北斗银河科技有限公司/运营中心/生产部", "所属中心": "运营中心"},
    {"用户ID": "10010093", "姓名": "吴星彤", "部门": "北京北斗银河科技有限公司/运营中心/生产部", "所属中心": "运营中心"},
    {"用户ID": "10010094", "姓名": "刘冰", "部门": "北京北斗银河科技有限公司/运营中心/生产部", "所属中心": "运营中心"},
    {"用户ID": "10010131", "姓名": "李左", "部门": "北京北斗银河科技有限公司/运营中心/生产部", "所属中心": "运营中心"},
    {"用户ID": "10010134", "姓名": "杨洋", "部门": "北京北斗银河科技有限公司/运营中心/生产部", "所属中心": "运营中心"},
    {"用户ID": "10010081", "姓名": "潘顺宁", "部门": "北京北斗银河科技有限公司/运营中心/质量部", "所属中心": "运营中心"},
    {"用户ID": "10010082", "姓名": "张蓓", "部门": "北京北斗银河科技有限公司/运营中心/质量部", "所属中心": "运营中心"},
    {"用户ID": "10010103", "姓名": "王林纳", "部门": "北京北斗银河科技有限公司/运营中心/质量部", "所属中心": "运营中心"},
    {"用户ID": "10010105", "姓名": "翟晓翠", "部门": "北京北斗银河科技有限公司/运营中心/质量部", "所属中心": "运营中心"},
    {"用户ID": "10010132", "姓名": "夏艳芳", "部门": "北京北斗银河科技有限公司/运营中心/质量部", "所属中心": "运营中心"},
    {"用户ID": "10010097", "姓名": "李娟", "部门": "北京北斗银河科技有限公司/运营中心/采购部", "所属中心": "运营中心"},
    {"用户ID": "10010137", "姓名": "马文超", "部门": "北京北斗银河科技有限公司/运营中心/采购部", "所属中心": "运营中心"}
]

def build_default_address_book():
    simplified = []
    for item in RAW_DEFAULT_ADDRESS_BOOK:
        new_item = item.copy()
        new_item["部门"] = simplify_dept_path(item["部门"])
        simplified.append(new_item)
    return simplified

DEFAULT_ADDRESS_BOOK = build_default_address_book()

# ==================== 飞书API相关函数 ====================
_token = None
_token_expire_time = 0

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
        "stats_type": "month",          # 改为 month，因为新 API 按月份返回日报表明细
        "start_date": int(date),
        "end_date": int(date),
        "user_ids": user_ids,
        "need_history": True,
        "current_group_only": True,
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

def calculate_non_workday_overtime(on_time_str, off_time_str):
    if on_time_str == "-" or off_time_str == "-":
        return 0.0
    try:
        base_date = datetime(1900, 1, 1)
        on_time = datetime.strptime(on_time_str, "%H:%M").replace(year=base_date.year, month=base_date.month, day=base_date.day)
        off_time = datetime.strptime(off_time_str, "%H:%M").replace(year=base_date.year, month=base_date.month, day=base_date.day)
        if off_time < on_time:
            off_time += timedelta(days=1)
        total_seconds = (off_time - on_time).total_seconds()
        total_hours = total_seconds / 3600
        lunch_start = base_date.replace(hour=11, minute=45)
        lunch_end = base_date.replace(hour=13, minute=0)
        overlap_start = max(on_time, lunch_start)
        overlap_end = min(off_time, lunch_end)
        overlap_minutes = 0
        if overlap_start < overlap_end:
            overlap_minutes = (overlap_end - overlap_start).total_seconds() / 60
        valid_hours = total_hours - overlap_minutes / 60
        return round(max(valid_hours, 0), 2)
    except Exception:
        return 0.0

def is_workday(date_str):
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        year = dt.year
        weekday = dt.weekday()
        year_config = HOLIDAYS_BY_YEAR.get(year, {"holidays": [], "workweekends": []})
        holidays_set = set(year_config["holidays"])
        workweekends_set = set(year_config["workweekends"])
        if date_str in workweekends_set:
            return True
        if date_str in holidays_set:
            return False
        if weekday < 5:
            return True
        else:
            return False
    except:
        return True

def parse_business_trip(value_str):
    """解析外勤打卡，返回 (是否出差, 出差时长小时)"""
    if not isinstance(value_str, str) or value_str == "-":
        return False, 0.0
    pattern1 = r'外勤[^\(]*\((\d{2}:\d{2})\)'
    pattern2 = r'外勤\s*(\d{2}:\d{2})'
    times = re.findall(pattern1, value_str)
    if not times:
        times = re.findall(pattern2, value_str)
    if not times:
        return False, 0.0

    def to_min(t):
        h, m = map(int, t.split(':'))
        return h * 60 + m

    minutes = [to_min(t) for t in times]
    minutes.sort()
    earliest = minutes[0]
    latest = minutes[-1]

    work_start = to_min("08:45")
    work_end = to_min("17:30")
    lunch_start = to_min("11:45")
    lunch_end = to_min("13:00")

    if len(minutes) == 1:
        t = minutes[0]
        if t < to_min("12:00"):
            return True, 3.0
        else:
            return True, 4.5

    start = max(earliest, work_start)
    end = min(latest, work_end)
    if end <= start:
        return True, 0.0

    total_min = end - start
    lunch_overlap_start = max(start, lunch_start)
    lunch_overlap_end = min(end, lunch_end)
    if lunch_overlap_end > lunch_overlap_start:
        total_min -= (lunch_overlap_end - lunch_overlap_start)

    hours = round(total_min / 60.0, 2)
    return True, hours

def parse_attendance_value(value_str):
    """解析请假信息，返回各类假期时长字典"""
    leave_hours = {
        "年假": 0.0, "病假": 0.0, "事假": 0.0,
        "婚假": 0.0, "产假": 0.0, "产检假": 0.0, "丧假": 0.0,
    }
    if not isinstance(value_str, str) or value_str == "-":
        return leave_hours

    pattern = r'([^\(;]+?)\(([^)]+)\)'
    matches = re.findall(pattern, value_str)

    for leave_type, time_str in matches:
        leave_type = leave_type.strip()
        if leave_type not in leave_hours:
            continue
        segments = time_str.split(',')
        total_hours = 0.0
        for seg in segments:
            seg = seg.strip()
            if '-' not in seg:
                continue
            start_str, end_str = re.split(r'\s*-\s*', seg)
            try:
                start = datetime.strptime(start_str, "%H:%M")
                end = datetime.strptime(end_str, "%H:%M")
                if end < start:
                    end = end.replace(day=start.day + 1)
                total_hours += (end - start).total_seconds() / 3600.0
            except Exception:
                continue
        leave_hours[leave_type] += total_hours

    return leave_hours

def parse_daily_value_string(value_str):
    """
    解析日报表明细 value 字符串，返回：
    {
        "on_time": "08:45", "off_time": "17:30",
        "on_status": "正常", "off_status": "正常",
        "leave_details": {...}, "total_leave_hours": 7.5,
        "is_business_trip": False, "trip_hours": 0.0
    }
    """
    result = {
        "on_time": "-",
        "off_time": "-",
        "on_status": "-",
        "off_status": "-",
        "leave_details": {
            "年假": 0.0, "病假": 0.0, "事假": 0.0,
            "婚假": 0.0, "产假": 0.0, "产检假": 0.0, "丧假": 0.0
        },
        "total_leave_hours": 0.0,
        "is_business_trip": False,
        "trip_hours": 0.0
    }
    if not isinstance(value_str, str) or value_str == "-":
        return result

    segments = value_str.split(';')
    punch_events = []
    leave_str = ""

    for seg in segments:
        seg = seg.strip()
        if not seg:
            continue
        if any(kw in seg for kw in ["年假", "病假", "事假", "婚假", "产假", "产检假", "丧假", "调休"]):
            leave_str = seg
        else:
            punch_events.append(seg)

    all_times = []
    for event_str in punch_events:
        times = re.findall(r'\((\d{2}:\d{2})\)', event_str)
        all_times.extend(times)
        if "外勤" in event_str:
            result["is_business_trip"] = True
            if "上班" not in result["on_status"]:
                result["on_status"] = "外勤"
                result["off_status"] = "外勤"
        elif "正常" in event_str:
            if "上班" not in result["on_status"]:
                result["on_status"] = "正常"
                result["off_status"] = "正常"
        elif "缺卡" in event_str:
            if "上班" not in result["on_status"]:
                result["on_status"] = "缺卡"
                result["off_status"] = "缺卡"

    if all_times:
        def to_min(t):
            h, m = map(int, t.split(':'))
            return h * 60 + m
        minutes = [to_min(t) for t in all_times]
        minutes.sort()
        earliest = minutes[0]
        latest = minutes[-1]
        # 找到对应原始时间字符串（简单处理，取第一个匹配）
        for t in all_times:
            if to_min(t) == earliest:
                result["on_time"] = t
                break
        for t in all_times:
            if to_min(t) == latest:
                result["off_time"] = t
                break

    if leave_str:
        leave_dict = parse_attendance_value(leave_str)
        total = 0.0
        for k, v in leave_dict.items():
            result["leave_details"][k] = v
            total += v
        result["total_leave_hours"] = total

    if result["is_business_trip"] and result["on_time"] != "-" and result["off_time"] != "-":
        is_trip, trip_hours = parse_business_trip(value_str)
        result["trip_hours"] = trip_hours

    return result

def parse_daily_data(api_data):
    records = []
    user_datas = api_data.get("user_datas", [])
    for user_data in user_datas:
        datas = user_data.get("datas", [])
        name = user_data.get("name", "")
        user_id = user_data.get("user_id", "")
        dept_raw = extract_value(datas, "50102")
        dept = simplify_dept_path(dept_raw)
        emp_no = extract_value(datas, "50103")
        group = extract_value(datas, "52108")

        scheduled_hours = extract_duration_hour(datas, "52104")
        actual_hours_api = extract_duration_hour(datas, "52105")

        for item in datas:
            code = item.get("code", "")
            if re.match(r"^\d{4}-\d{2}-\d{2}$", code):
                date_str = code
                value_str = item.get("value", "")
                workday_flag = is_workday(date_str)
                daily_info = parse_daily_value_string(value_str)

                if workday_flag:
                    overtime_hours = calculate_overtime(daily_info["off_time"]) if daily_info["off_time"] != "-" else 0.0
                else:
                    overtime_hours = calculate_non_workday_overtime(daily_info["on_time"], daily_info["off_time"])

                intra_shift_hours = round(scheduled_hours - daily_info["total_leave_hours"], 2)

                record = {
                    "姓名": name,
                    "工号": emp_no,
                    "部门": dept,
                    "日期": date_str,
                    "月份": date_str[:7],
                    "班次": "",
                    "上班打卡": daily_info["on_time"],
                    "下班打卡": daily_info["off_time"],
                    "应出勤(小时)": scheduled_hours,
                    "实际出勤(小时)": actual_hours_api,
                    "加班时长(小时)": overtime_hours,
                    "请假时长": daily_info["total_leave_hours"],
                    "班内工作时长(小时)": intra_shift_hours,
                    "上午打卡结果": daily_info["on_status"],
                    "下午打卡结果": daily_info["off_status"],
                    "考勤组": group,
                    "_原始日期": date_str,
                    "用户ID": user_id,
                    "是否出差": daily_info["is_business_trip"],
                    "出差时长(小时)": daily_info["trip_hours"],
                    "年假": daily_info["leave_details"]["年假"],
                    "病假": daily_info["leave_details"]["病假"],
                    "事假": daily_info["leave_details"]["事假"],
                    "婚假": daily_info["leave_details"]["婚假"],
                    "产假": daily_info["leave_details"]["产假"],
                    "产检假": daily_info["leave_details"]["产检假"],
                    "丧假": daily_info["leave_details"]["丧假"],
                }
                records.append(record)
    return records

def fetch_all_records(start_date_str, end_date_str, user_ids):
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
            progress_bar.progress((i + 1) / total_dates)

    progress_bar.empty()
    if failed_dates:
        st.warning(f"以下日期获取失败：{', '.join([d for d, _ in failed_dates])}")
    return all_records

# ==================== 报表生成函数 ====================
def generate_daily_report(records):
    daily_report = []
    for record in records:
        daily_report.append({
            "用户名称": record["姓名"],
            "部门": record["部门"],
            "所属中心": record.get("所属中心", ""),
            "日期": record["日期"],
            "总应出勤(小时)": record["应出勤(小时)"],
            "总实际出勤(小时)": record["实际出勤(小时)"],
            "出差时长(小时)": record["出差时长(小时)"],
            "是否出差": "是" if record["是否出差"] else "否",
            "班内工作时长(小时)": record["班内工作时长(小时)"],
            "上班打卡时间": record["上班打卡"],
            "下班打卡时间": record["下班打卡"],
            "加班时间(小时)": record["加班时长(小时)"],
            "请假时长(小时)": record["请假时长"] if record["请假时长"] != "-" else 0.0,
            "用户ID": record["用户ID"]
        })
    daily_report.sort(key=lambda x: (x["部门"], x["用户名称"], -int(x["日期"].replace("-", "")) if x["日期"] != "-" else 0))
    return daily_report

def generate_monthly_report_by_month(records, month):
    user_stats = defaultdict(lambda: {
        "总应出勤": 0.0, "总实际出勤_api": 0.0, "总班内": 0.0, "总加班": 0.0,
        "总请假": 0.0, "出差天数": 0, "出差总时长": 0.0,
        "年假": 0.0, "病假": 0.0, "事假": 0.0,
        "婚假": 0.0, "产假": 0.0, "产检假": 0.0, "丧假": 0.0,
        "部门": "", "姓名": "", "所属中心": ""
    })
    for record in records:
        if record["月份"] != month:
            continue
        user_id = record["用户ID"]
        name = record["姓名"]
        dept = record["部门"]
        center = record.get("所属中心", "")
        stats = user_stats[user_id]
        stats["姓名"] = name
        stats["部门"] = dept
        stats["所属中心"] = center
        stats["总应出勤"] += record["应出勤(小时)"]
        stats["总实际出勤_api"] += record["实际出勤(小时)"]
        stats["总班内"] += record["班内工作时长(小时)"]
        stats["总加班"] += record["加班时长(小时)"]
        leave_val = record["请假时长"]
        if leave_val != "-":
            stats["总请假"] += float(leave_val)
        if record["是否出差"]:
            stats["出差天数"] += 1
            stats["出差总时长"] += record["出差时长(小时)"]
        for leave_type in ["年假","病假","事假","婚假","产假","产检假","丧假"]:
            stats[leave_type] += record.get(leave_type, 0.0)

    monthly_report = []
    for user_id, stats in user_stats.items():
        monthly_report.append({
            "用户名称": stats["姓名"], "部门": stats["部门"], "所属中心": stats["所属中心"], "月份": month,
            "总应出勤时长(小时)": round(stats["总应出勤"], 2),
            "总实际出勤(小时)": round(stats["总实际出勤_api"], 2),
            "出差天数": stats["出差天数"], "出差总时长(小时)": round(stats["出差总时长"], 2),
            "总班内工作时长(小时)": round(stats["总班内"], 2),
            "总加班时间(小时)": round(stats["总加班"], 2),
            "总请假时长(小时)": round(stats["总请假"], 2),
            "年假(小时)": round(stats["年假"], 2), "病假(小时)": round(stats["病假"], 2),
            "事假(小时)": round(stats["事假"], 2), "婚假(小时)": round(stats["婚假"], 2),
            "产假(小时)": round(stats["产假"], 2), "产检假(小时)": round(stats["产检假"], 2),
            "丧假(小时)": round(stats["丧假"], 2), "用户ID": user_id
        })
    monthly_report.sort(key=lambda x: (x["部门"], -x["总加班时间(小时)"]))
    return monthly_report

def generate_summary_report(records):
    user_stats = defaultdict(lambda: {
        "总应出勤": 0.0, "总实际出勤_api": 0.0, "总班内": 0.0, "总加班": 0.0,
        "总请假": 0.0, "出差天数": 0, "出差总时长": 0.0,
        "年假": 0.0, "病假": 0.0, "事假": 0.0,
        "婚假": 0.0, "产假": 0.0, "产检假": 0.0, "丧假": 0.0,
        "部门": "", "姓名": "", "所属中心": ""
    })
    for record in records:
        user_id = record["用户ID"]
        name = record["姓名"]
        dept = record["部门"]
        center = record.get("所属中心", "")
        stats = user_stats[user_id]
        stats["姓名"] = name
        stats["部门"] = dept
        stats["所属中心"] = center
        stats["总应出勤"] += record["应出勤(小时)"]
        stats["总实际出勤_api"] += record["实际出勤(小时)"]
        stats["总班内"] += record["班内工作时长(小时)"]
        stats["总加班"] += record["加班时长(小时)"]
        leave_val = record["请假时长"]
        if leave_val != "-":
            stats["总请假"] += float(leave_val)
        if record["是否出差"]:
            stats["出差天数"] += 1
            stats["出差总时长"] += record["出差时长(小时)"]
        for leave_type in ["年假","病假","事假","婚假","产假","产检假","丧假"]:
            stats[leave_type] += record.get(leave_type, 0.0)

    summary_report = []
    for user_id, stats in user_stats.items():
        summary_report.append({
            "用户名称": stats["姓名"], "部门": stats["部门"], "所属中心": stats["所属中心"],
            "总应出勤时长(小时)": round(stats["总应出勤"], 2),
            "总实际出勤(小时)": round(stats["总实际出勤_api"], 2),
            "出差天数": stats["出差天数"], "出差总时长(小时)": round(stats["出差总时长"], 2),
            "总班内工作时长(小时)": round(stats["总班内"], 2),
            "总加班时间(小时)": round(stats["总加班"], 2),
            "总请假时长(小时)": round(stats["总请假"], 2),
            "年假(小时)": round(stats["年假"], 2), "病假(小时)": round(stats["病假"], 2),
            "事假(小时)": round(stats["事假"], 2), "婚假(小时)": round(stats["婚假"], 2),
            "产假(小时)": round(stats["产假"], 2), "产检假(小时)": round(stats["产检假"], 2),
            "丧假(小时)": round(stats["丧假"], 2), "用户ID": user_id
        })
    summary_report.sort(key=lambda x: (x["部门"], -x["总加班时间(小时)"]))
    return summary_report

def parse_address_book(uploaded_file):
    try:
        df = pd.read_excel(uploaded_file)
        required_cols = ["用户ID", "部门", "所属中心"]
        if not all(col in df.columns for col in required_cols):
            st.error("通讯录文件必须包含列：用户ID、部门、所属中心")
            return None, None
        df = df.dropna(subset=["用户ID"]).copy()
        df["用户ID"] = df["用户ID"].astype(str).str.strip()
        df["部门"] = df["部门"].astype(str).str.strip().apply(simplify_dept_path)
        df["所属中心"] = df["所属中心"].astype(str).str.strip()
        if "姓名" in df.columns:
            df["姓名"] = df["姓名"].astype(str).str.strip()
        else:
            df["姓名"] = ""
        df = df[df["用户ID"] != ""]
        user_ids = df["用户ID"].unique().tolist()
        return df, user_ids
    except Exception as e:
        st.error(f"解析通讯录失败: {e}")
        return None, None

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

def export_to_excel(df, filename):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    return output.getvalue()

# ==================== Streamlit 界面 ====================
st.set_page_config(page_title="考勤报表系统", layout="wide")

st.markdown("""
<style>
    :root {
        --bg-primary: #ffffff;
        --bg-card: #f8f9fa;
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
        --filter-bg: #f8f9fa;
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
        --filter-bg: #1e1e1e;
    }
    h1 {
        margin-top: -20px !important;
        font-size: 32px !important;
        font-weight: 700 !important;
        margin-bottom: 0.3rem !important;
        color: var(--text-primary);
        border-bottom: 2px solid var(--border-light);
        padding-bottom: 0.5rem;
    }
    .filter-container {
        margin-bottom: 0 !important;
        padding-top: 0 !important;
        padding-bottom: 0 !important;
    }
    .filter-container .stSelectbox, .filter-container .stMultiSelect, .filter-container .stTextInput {
        margin-bottom: 0.1rem !important;
        margin-top: 0 !important;
    }
    .filter-container .stSelectbox label, .filter-container .stMultiSelect label, .filter-container .stTextInput label {
        margin-bottom: 0 !important;
        padding-bottom: 0 !important;
        line-height: 1.2 !important;
        font-size: 0.85rem !important;
    }
    hr {
        margin-top: 0.2rem !important;
        margin-bottom: 0.2rem !important;
        border: 0;
        border-top: 1px solid var(--border-light);
    }
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
    .top3-name {
        color: var(--text-primary);
    }
    .top3-value {
        color: var(--color-blue);
        font-weight: 600;
        font-size: 16px;
    }
    div[data-testid="column"] {
        gap: 1rem;
    }
    .stDataFrame {
        font-size: 13px;
        border-radius: 8px;
        overflow: hidden;
        box-shadow: 0 2px 4px rgba(0,0,0,0.02);
    }
    .stDataFrame > div {
        overflow-x: auto !important;
        white-space: nowrap;
        max-width: 100%;
    }
    .stDataFrame table {
        border-collapse: collapse;
        width: max-content;
        min-width: 100%;
    }
    .stDataFrame thead tr th {
        background-color: var(--table-header-bg) !important;
        font-weight: 600 !important;
        border-bottom: 2px solid var(--border-table) !important;
        padding: 12px 8px !important;
        white-space: normal !important;
        word-break: break-word;
        color: var(--text-primary);
        position: sticky;
        top: 0;
        z-index: 20;
    }
    .stDataFrame th:nth-child(1), .stDataFrame td:nth-child(1) {
        position: sticky !important;
        left: 0 !important;
        z-index: 30 !important;
        background-color: inherit !important;
    }
    .stDataFrame th:nth-child(2), .stDataFrame td:nth-child(2) {
        position: sticky !important;
        left: 80px !important;
        z-index: 30 !important;
        background-color: inherit !important;
    }
    .stDataFrame th:nth-child(3), .stDataFrame td:nth-child(3) {
        position: sticky !important;
        left: 230px !important;
        z-index: 30 !important;
        background-color: inherit !important;
    }
    .stDataFrame th:nth-child(1), .stDataFrame th:nth-child(2), .stDataFrame th:nth-child(3) {
        background-color: var(--table-header-bg) !important;
    }
    .stDataFrame tbody tr:nth-child(odd) td {
        background-color: var(--table-row-odd);
    }
    .stDataFrame tbody tr:hover td {
        background-color: var(--table-row-hover) !important;
        transition: background-color 0.15s;
    }
    .stDataFrame td {
        padding: 10px 8px !important;
        border-bottom: 1px solid var(--border-light);
        color: var(--text-primary);
    }
    .stDataFrame td:nth-child(8), .stDataFrame td:nth-child(9),
    .stDataFrame td:nth-child(10), .stDataFrame td:nth-child(11),
    .stDataFrame td:nth-child(12) {
        text-align: right;
        font-family: 'Courier New', monospace;
    }
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
    .stSelectbox {
        font-size: 13px !important;
    }
    .stSelectbox > div > div {
        font-size: 13px !important;
    }
    .stTabs [data-baseweb="tab-list"] button {
        font-size: 16px !important;
    }
    .stMultiSelect[disabled] {
        background-color: var(--disabled-bg);
        border-radius: 4px;
    }
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
    .stTabs {
        margin-top: 0.5rem !important;
        margin-bottom: 0 !important;
    }
    [data-testid="stHorizontalBlock"] .stSelectbox {
        margin-top: 0 !important;
        padding-top: 0 !important;
    }
    [data-testid="stHorizontalBlock"] .sort-buttons .stRadio {
        margin-top: 0 !important;
        padding-top: 0 !important;
    }
</style>
""", unsafe_allow_html=True)

st.title("📊 北斗考勤报表")

# 初始化session_state
if "address_book_df" not in st.session_state:
    df_default = pd.DataFrame(DEFAULT_ADDRESS_BOOK)
    df_default["部门"] = df_default["部门"].astype(str).str.strip()
    df_default["所属中心"] = df_default["所属中心"].astype(str).str.strip()
    df_default = df_default[df_default["用户ID"].notna() & (df_default["用户ID"] != "")]
    st.session_state.address_book_df = df_default
    st.session_state.user_ids = df_default["用户ID"].unique().tolist()

if "raw_records" not in st.session_state:
    st.session_state.raw_records = None
if "df_daily_raw" not in st.session_state:
    st.session_state.df_daily_raw = None
if "df_daily_by_month" not in st.session_state:
    st.session_state.df_daily_by_month = {}
if "df_detail_by_month" not in st.session_state:
    st.session_state.df_detail_by_month = {}
if "df_summary_raw" not in st.session_state:
    st.session_state.df_summary_raw = None
if "monthly_data" not in st.session_state:
    st.session_state.monthly_data = {}
if "month_list" not in st.session_state:
    st.session_state.month_list = []
if "data_loaded" not in st.session_state:
    st.session_state.data_loaded = False
if "auto_load_attempted" not in st.session_state:
    st.session_state.auto_load_attempted = False
if "loading" not in st.session_state:
    st.session_state.loading = False
if "pending_reload" not in st.session_state:
    st.session_state.pending_reload = False
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
if "page_summary" not in st.session_state:
    st.session_state.page_summary = 1
if "page_size_summary" not in st.session_state:
    st.session_state.page_size_summary = 10

def get_today():
    return datetime.now().date()

def get_first_day_of_last_month():
    today = get_today()
    first_this = today.replace(day=1)
    last_month_last_day = first_this - timedelta(days=1)
    return last_month_last_day.replace(day=1)

def get_last_day_of_last_month():
    today = get_today()
    first_this = today.replace(day=1)
    return first_this - timedelta(days=1)

# 侧边栏
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
            st.session_state.center_filter = "全部中心"
            st.session_state.dept_filter = []
            st.session_state.name_filter = ""
            st.session_state.pending_reload = True
            st.session_state.loading = True
            st.rerun()
    with col_b:
        if st.button("本月", use_container_width=True):
            today = get_today()
            st.session_state.start_date = today.replace(day=1)
            st.session_state.end_date = today
            st.session_state.center_filter = "全部中心"
            st.session_state.dept_filter = []
            st.session_state.name_filter = ""
            st.session_state.pending_reload = True
            st.session_state.loading = True
            st.rerun()
    with col_c:
        if st.button("本年", use_container_width=True):
            today = get_today()
            st.session_state.start_date = today.replace(month=1, day=1)
            st.session_state.end_date = today
            st.session_state.center_filter = "全部中心"
            st.session_state.dept_filter = []
            st.session_state.name_filter = ""
            st.session_state.pending_reload = True
            st.session_state.loading = True
            st.rerun()
    today = get_today()
    default_start = today.replace(day=1)
    default_end = today
    start_date = st.date_input(
        "开始日期",
        value=st.session_state.get("start_date", default_start),
        key="start_date",
        format="YYYY/MM/DD"
    )
    end_date = st.date_input(
        "结束日期",
        value=st.session_state.get("end_date", default_end),
        key="end_date",
        format="YYYY/MM/DD"
    )
    fetch_btn = st.button(
        "获取数据",
        type="primary",
        use_container_width=True,
        disabled=st.session_state.get("loading", False)
    )
    if st.session_state.get("loading", False):
        st.info("⏳ 数据加载中，请稍候...")
    if st.session_state.data_loaded:
        workdays_sidebar = count_workdays(start_date, end_date)
        if st.session_state.df_summary_raw is not None and not st.session_state.df_summary_raw.empty:
            df_filtered = st.session_state.df_summary_raw.copy()
            current_center = st.session_state.get("center_filter", "全部中心")
            current_depts = st.session_state.get("dept_filter", [])
            name_query = st.session_state.get("name_filter", "").strip()
            if current_center != "全部中心":
                df_filtered = df_filtered[df_filtered["所属中心"] == current_center]
            if current_depts:
                mask = df_filtered["部门"].apply(
                    lambda x: any(dept.strip() in current_depts for dept in str(x).split('|')))
                df_filtered = df_filtered[mask]
            if name_query:
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

# 处理待重新加载请求
if st.session_state.get("pending_reload", False) and st.session_state.get("loading", False):
    st.session_state.pending_reload = False
    with st.spinner("🌞 考勤数据正在飞奔而来... 请稍候"):
        try:
            start_str = start_date.strftime(DATE_FORMAT)
            end_str = end_date.strftime(DATE_FORMAT)
            raw_records = fetch_all_records(start_str, end_str, st.session_state.user_ids)
            if not raw_records:
                st.warning("未获取到任何数据，请检查参数或网络")
                st.session_state.loading = False
                st.stop()
            addr_df = st.session_state.address_book_df
            user_to_center = dict(zip(addr_df["用户ID"], addr_df["所属中心"]))
            for r in raw_records:
                r["所属中心"] = user_to_center.get(r["用户ID"], "")
            summary_report = generate_summary_report(raw_records)
            st.session_state.df_summary_raw = pd.DataFrame(summary_report)
            daily_raw = generate_daily_report(raw_records)
            st.session_state.df_daily_raw = pd.DataFrame(daily_raw)
            detail_raw = sorted(raw_records, key=lambda x: (x["部门"], x["姓名"], -int(x["日期"].replace("-", "")) if x["日期"] != "-" else 0))
            st.session_state.df_detail_raw = pd.DataFrame(detail_raw)
            months = sorted(set(r["月份"] for r in raw_records if r["月份"]))
            st.session_state.month_list = months
            monthly_data = {}
            for month in months:
                monthly_report = generate_monthly_report_by_month(raw_records, month)
                monthly_data[month] = pd.DataFrame(monthly_report)
            st.session_state.monthly_data = monthly_data
            daily_by_month = {}
            detail_by_month = {}
            for month in months:
                month_records = [r for r in raw_records if r["月份"] == month]
                daily_by_month[month] = pd.DataFrame(generate_daily_report(month_records))
                detail_by_month[month] = pd.DataFrame(sorted(month_records, key=lambda x: (x["部门"], x["姓名"], -int(x["日期"].replace("-", "")) if x["日期"] != "-" else 0)))
            st.session_state.df_daily_by_month = daily_by_month
            st.session_state.df_detail_by_month = detail_by_month
            st.session_state.raw_records = raw_records
            st.session_state.data_loaded = True
        finally:
            st.session_state.loading = False
            st.rerun()

if fetch_btn and not st.session_state.loading:
    st.session_state.loading = True
    st.session_state.pending_reload = True
    st.session_state.center_filter = "全部中心"
    st.session_state.dept_filter = []
    st.session_state.name_filter = ""
    st.rerun()

if not st.session_state.data_loaded and not st.session_state.auto_load_attempted and not st.session_state.loading:
    st.session_state.auto_load_attempted = True
    st.session_state.loading = True
    with st.spinner("🌞 考勤数据正在飞奔而来... 请稍候"):
        try:
            start_str = start_date.strftime(DATE_FORMAT)
            end_str = end_date.strftime(DATE_FORMAT)
            raw_records = fetch_all_records(start_str, end_str, st.session_state.user_ids)
            if raw_records:
                addr_df = st.session_state.address_book_df
                user_to_center = dict(zip(addr_df["用户ID"], addr_df["所属中心"]))
                for r in raw_records:
                    r["所属中心"] = user_to_center.get(r["用户ID"], "")
                summary_report = generate_summary_report(raw_records)
                st.session_state.df_summary_raw = pd.DataFrame(summary_report)
                daily_raw = generate_daily_report(raw_records)
                st.session_state.df_daily_raw = pd.DataFrame(daily_raw)
                detail_raw = sorted(raw_records, key=lambda x: (x["部门"], x["姓名"], -int(x["日期"].replace("-", "")) if x["日期"] != "-" else 0))
                st.session_state.df_detail_raw = pd.DataFrame(detail_raw)
                months = sorted(set(r["月份"] for r in raw_records if r["月份"]))
                st.session_state.month_list = months
                monthly_data = {}
                for month in months:
                    monthly_report = generate_monthly_report_by_month(raw_records, month)
                    monthly_data[month] = pd.DataFrame(monthly_report)
                st.session_state.monthly_data = monthly_data
                daily_by_month = {}
                detail_by_month = {}
                for month in months:
                    month_records = [r for r in raw_records if r["月份"] == month]
                    daily_by_month[month] = pd.DataFrame(generate_daily_report(month_records))
                    detail_by_month[month] = pd.DataFrame(sorted(month_records, key=lambda x: (x["部门"], x["姓名"], -int(x["日期"].replace("-", "")) if x["日期"] != "-" else 0)))
                st.session_state.df_daily_by_month = daily_by_month
                st.session_state.df_detail_by_month = detail_by_month
                st.session_state.raw_records = raw_records
                st.session_state.data_loaded = True
            else:
                st.warning("自动加载数据失败，请稍后手动点击获取数据")
        finally:
            st.session_state.loading = False
            st.session_state.center_filter = "全部中心"
            st.session_state.dept_filter = []
            st.session_state.name_filter = ""
            st.rerun()

# 主内容区
if st.session_state.data_loaded:
    addr_df = st.session_state.address_book_df

    with st.container():
        st.markdown('<div class="filter-container">', unsafe_allow_html=True)
        col_center, col_dept, col_name = st.columns([1, 1.5, 0.8])
        with col_center:
            centers = sorted(addr_df["所属中心"].unique())
            center_options = ["全部中心"] + centers
            selected_center = st.selectbox("🏢 选择所属中心", center_options, key="center_filter")
        with col_dept:
            if selected_center == "全部中心":
                dept_options = sorted(addr_df["部门"].unique())
                disabled = True
                st.session_state.dept_filter = []
            else:
                dept_options = sorted(addr_df[addr_df["所属中心"] == selected_center]["部门"].unique())
                disabled = False
            selected_depts = st.multiselect("🏷️ 选择部门", dept_options, key="dept_filter", disabled=disabled)
        with col_name:
            st.text_input("🔍 姓名", key="name_filter", placeholder="输入姓名模糊匹配",
                          value=st.session_state.get("name_filter", ""))
        st.markdown('</div>', unsafe_allow_html=True)

    current_center = selected_center
    current_depts = selected_depts
    name_query = st.session_state.get("name_filter", "").strip()

    def filter_df(df, center_col="所属中心", dept_col="部门"):
        if df is None or df.empty:
            return df
        filtered = df.copy()
        if current_center != "全部中心":
            filtered = filtered[filtered[center_col] == current_center]
        if current_depts:
            mask = filtered[dept_col].apply(lambda x: any(dept.strip() in current_depts for dept in str(x).split('|')))
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

    workdays = count_workdays(st.session_state.start_date, st.session_state.end_date)
    df_raw = st.session_state.df_summary_raw
    df_summary_filtered = filter_df(df_raw)

    if df_summary_filtered is not None and not df_summary_filtered.empty:
        top3_list = df_summary_filtered.nlargest(3, '总加班时间(小时)')[['用户名称', '总加班时间(小时)']].values.tolist()
    else:
        top3_list = []

    if df_raw is not None and not df_raw.empty:
        avg_all = df_raw["总加班时间(小时)"].mean()
        df_raw_rd = df_raw[df_raw["所属中心"] == "产研中心"]
        avg_rd = df_raw_rd["总加班时间(小时)"].mean() if not df_raw_rd.empty else 0.0
        show_selected_center = current_center != "全部中心" and current_center != "产研中心"
        if show_selected_center and df_summary_filtered is not None and not df_summary_filtered.empty:
            df_selected = df_summary_filtered[df_summary_filtered["所属中心"] == current_center]
            avg_selected = df_selected["总加班时间(小时)"].mean() if not df_selected.empty else 0.0
        else:
            avg_selected = None
        daily_avg_all = avg_all / workdays if workdays > 0 else 0.0
        daily_avg_rd = avg_rd / workdays if workdays > 0 else 0.0
        if show_selected_center and avg_selected is not None:
            daily_avg_selected = avg_selected / workdays if workdays > 0 else 0.0
        else:
            daily_avg_selected = None
    else:
        avg_all = avg_rd = daily_avg_all = daily_avg_rd = 0.0
        show_selected_center = False
        avg_selected = daily_avg_selected = None

    def metric_card(label, value, unit="小时", color=None, warning=False, blue=False):
        color_class = "metric-value-red" if color == "red" else ("metric-value-blue" if blue else "")
        warning_class = " metric-warning" if warning else ""
        unit_display = f"<span style='font-size: 16px; font-weight: 400; color: var(--text-secondary); margin-left: 4px;'>{unit}</span>" if unit else ""
        return f"""
        <div class="metric-card{warning_class}">
            <div class="metric-label">{label}</div>
            <div class="metric-value {color_class}">{value:.2f}{unit_display}</div>
        </div>
        """

    def top3_card(top3_data):
        top3_html = '<div class="metric-card" style="height: 100%; display: flex; flex-direction: column; justify-content: center;"><div class="metric-label" style="margin-bottom: 16px; font-size: 18px;">🐝 小蜜蜂TOP3</div>'
        if top3_data and len(top3_data) > 0:
            for i, (name, hours) in enumerate(top3_data, 1):
                medal = "🥇" if i == 1 else ("🥈" if i == 2 else "🥉")
                top3_html += f'<div style="display: flex; justify-content: space-between; align-items: baseline; font-size: 16px; margin-bottom: 10px;"><span style="color: var(--text-primary);">{medal} {name}</span><span style="color: var(--color-blue); font-weight: 600; font-size: 18px;">{hours:.1f} 小时</span></div>'
        else:
            top3_html += f'<div style="color: var(--text-secondary); font-size: 16px;">暂无数据</div>'
        top3_html += '</div>'
        return top3_html

    if show_selected_center:
        col_widths = [1.2, 1, 1, 1]
    else:
        col_widths = [1.2, 1, 1]
    cols = st.columns(col_widths)
    with cols[0]:
        st.markdown(top3_card(top3_list), unsafe_allow_html=True)
    with cols[1]:
        st.markdown(metric_card("🏢 全公司人均加班", avg_all), unsafe_allow_html=True)
        st.markdown(metric_card("🏢 全公司人均日均加班", daily_avg_all, unit="小时/日"), unsafe_allow_html=True)
    with cols[2]:
        warning_rd = (avg_rd < avg_all) or (show_selected_center and avg_rd < avg_selected)
        st.markdown(metric_card("⚙️ 产研中心人均加班", avg_rd, color="red" if warning_rd else None, warning=warning_rd),
                    unsafe_allow_html=True)
        st.markdown(
            metric_card("⚙️ 产研中心人均日均加班", daily_avg_rd, unit="小时/日", color="red" if warning_rd else None,
                        warning=warning_rd), unsafe_allow_html=True)
    if show_selected_center:
        with cols[3]:
            st.markdown(metric_card(f"🏷️ {current_center}人均加班", avg_selected), unsafe_allow_html=True)
            st.markdown(metric_card(f"🏷️ {current_center}人均日均加班", daily_avg_selected, unit="小时/日"),
                        unsafe_allow_html=True)

    is_cross_month = len(st.session_state.month_list) > 1

    if is_cross_month:
        tab_titles = ["📊 总报表", "📅 月报表", "📆 日报表", "📋 明细数据"]
        tabs = st.tabs(tab_titles)
        tab_summary, tab_monthly, tab_daily, tab_detail = tabs
    else:
        tab_titles = ["📅 月报表", "📆 日报表", "📋 明细数据"]
        tabs = st.tabs(tab_titles)
        tab_monthly, tab_daily, tab_detail = tabs

    if is_cross_month:
        with tab_summary:
            col_sort, col_export = st.columns([3, 1])
            with col_sort:
                sort_option = st.selectbox("排序", options=["加班倒序", "请假倒序"], key="sort_summary",
                                           label_visibility="collapsed")
            with col_export:
                if not df_summary_filtered.empty:
                    df_temp = df_summary_filtered.copy()
                    if sort_option == "加班倒序":
                        df_temp = df_temp.sort_values(by="总加班时间(小时)", ascending=False)
                    else:
                        df_temp = df_temp.sort_values(by="总请假时长(小时)", ascending=False)
                    excel_data = export_to_excel(df_temp, "总报表.xlsx")
                    st.download_button(
                        label="📎 导出当前数据",
                        data=excel_data,
                        file_name=f"总报表_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="export_summary",
                        use_container_width=True
                    )

            df_display = df_summary_filtered.copy()
            if sort_option == "加班倒序":
                df_display = df_display.sort_values(by="总加班时间(小时)", ascending=False)
            else:
                df_display = df_display.sort_values(by="总请假时长(小时)", ascending=False)

            if not df_display.empty:
                df_display.insert(0, '序号', range(1, len(df_display) + 1))

            total_rows = len(df_display)
            start_idx = (st.session_state.page_summary - 1) * st.session_state.page_size_summary
            end_idx = min(start_idx + st.session_state.page_size_summary, total_rows)
            df_page = df_display.iloc[start_idx:end_idx] if total_rows > 0 else df_display

            column_config = {
                "序号": st.column_config.NumberColumn(width=80),
                "用户名称": st.column_config.TextColumn(width=150),
                "部门": st.column_config.TextColumn(width=150),
                "所属中心": st.column_config.TextColumn(width=150),
                "总应出勤时长(小时)": st.column_config.NumberColumn(width=120, format="%.2f"),
                "总实际出勤(小时)": st.column_config.NumberColumn(width=120, format="%.2f"),
                "出差天数": st.column_config.NumberColumn(width=80, format="%d"),
                "出差总时长(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
                "总班内工作时长(小时)": st.column_config.NumberColumn(width=120, format="%.2f"),
                "总加班时间(小时)": st.column_config.NumberColumn(width=120, format="%.2f"),
                "总请假时长(小时)": st.column_config.NumberColumn(width=120, format="%.2f"),
                "年假(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
                "病假(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
                "事假(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
                "婚假(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
                "产假(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
                "产检假(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
                "丧假(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
                "用户ID": st.column_config.TextColumn(width=100)
            }
            st.dataframe(df_page, column_config=column_config, use_container_width=True, hide_index=True)

            total_pages = (total_rows - 1) // st.session_state.page_size_summary + 1 if total_rows > 0 else 1
            if st.session_state.page_summary > total_pages:
                st.session_state.page_summary = total_pages
            if st.session_state.page_summary < 1:
                st.session_state.page_summary = 1

            col_left, col_right = st.columns([1, 2])
            with col_left:
                left_inner_cols = st.columns([1, 1])
                with left_inner_cols[0]:
                    st.markdown(f"**共 {len(df_display)} 条记录**")
                with left_inner_cols[1]:
                    page_size_options = [10, 20, 50, 100]
                    selected_size = st.selectbox("每页行数", options=page_size_options,
                                                 index=page_size_options.index(st.session_state.page_size_summary),
                                                 key="page_size_summary_select", label_visibility="collapsed")
                    if selected_size != st.session_state.page_size_summary:
                        st.session_state.page_size_summary = selected_size
                        st.session_state.page_summary = 1
                        st.rerun()
            with col_right:
                pagination_cols = st.columns([1, 1, 1, 1, 1])
                with pagination_cols[0]:
                    if st.button("⏮️ 首页", disabled=(st.session_state.page_summary == 1), key="first_summary"):
                        st.session_state.page_summary = 1
                        st.rerun()
                with pagination_cols[1]:
                    if st.button("← 上一页", disabled=(st.session_state.page_summary == 1), key="prev_summary"):
                        st.session_state.page_summary -= 1
                        st.rerun()
                with pagination_cols[2]:
                    st.markdown(
                        f"<div style='text-align: center; font-weight: 600;'>{st.session_state.page_summary} / {total_pages}</div>",
                        unsafe_allow_html=True)
                with pagination_cols[3]:
                    if st.button("下一页 →", disabled=(st.session_state.page_summary == total_pages),
                                 key="next_summary"):
                        st.session_state.page_summary += 1
                        st.rerun()
                with pagination_cols[4]:
                    if st.button("⏭️ 末页", disabled=(st.session_state.page_summary == total_pages),
                                 key="last_summary"):
                        st.session_state.page_summary = total_pages
                        st.rerun()

    with tab_monthly:
        if is_cross_month:
            months = st.session_state.month_list
            if "selected_month" not in st.session_state or st.session_state.selected_month not in months:
                st.session_state.selected_month = months[0]
            col_month, col_sort, col_export = st.columns([2, 2, 1])
            with col_month:
                selected_month = st.selectbox("选择月份", options=months, key="selected_month",
                                              label_visibility="collapsed")
            with col_sort:
                sort_option = st.selectbox("排序", options=["加班倒序", "请假倒序"], key="sort_monthly",
                                           label_visibility="collapsed")
            with col_export:
                df_monthly_raw = st.session_state.monthly_data.get(selected_month, pd.DataFrame())
                df_monthly = filter_df(df_monthly_raw)
                if not df_monthly.empty:
                    df_temp = df_monthly.copy()
                    if sort_option == "加班倒序":
                        df_temp = df_temp.sort_values(by="总加班时间(小时)", ascending=False)
                    else:
                        df_temp = df_temp.sort_values(by="总请假时长(小时)", ascending=False)
                    excel_data = export_to_excel(df_temp, f"月报表_{selected_month}.xlsx")
                    st.download_button(
                        label="📎 导出当前数据",
                        data=excel_data,
                        file_name=f"月报表_{selected_month}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="export_monthly",
                        use_container_width=True
                    )
            df_monthly_raw = st.session_state.monthly_data.get(selected_month, pd.DataFrame())
            df_monthly = filter_df(df_monthly_raw)
        else:
            col_sort, col_export = st.columns([3, 1])
            with col_sort:
                sort_option = st.selectbox("排序", options=["加班倒序", "请假倒序"], key="sort_monthly",
                                           label_visibility="collapsed")
            with col_export:
                df_monthly_raw = next(iter(st.session_state.monthly_data.values()))
                df_monthly = filter_df(df_monthly_raw)
                if not df_monthly.empty:
                    df_temp = df_monthly.copy()
                    if sort_option == "加班倒序":
                        df_temp = df_temp.sort_values(by="总加班时间(小时)", ascending=False)
                    else:
                        df_temp = df_temp.sort_values(by="总请假时长(小时)", ascending=False)
                    excel_data = export_to_excel(df_temp, "月报表_当前.xlsx")
                    st.download_button(
                        label="📎 导出当前数据",
                        data=excel_data,
                        file_name=f"月报表_当前_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="export_monthly",
                        use_container_width=True
                    )
            df_monthly_raw = next(iter(st.session_state.monthly_data.values()))
            df_monthly = filter_df(df_monthly_raw)

        df_display = df_monthly.copy()
        if sort_option == "加班倒序":
            df_display = df_display.sort_values(by="总加班时间(小时)", ascending=False)
        else:
            df_display = df_display.sort_values(by="总请假时长(小时)", ascending=False)

        if not df_display.empty:
            df_display.insert(0, '序号', range(1, len(df_display) + 1))

        total_rows = len(df_display)
        start_idx = (st.session_state.page_monthly - 1) * st.session_state.page_size_monthly
        end_idx = min(start_idx + st.session_state.page_size_monthly, total_rows)
        df_page = df_display.iloc[start_idx:end_idx] if total_rows > 0 else df_display

        column_config = {
            "序号": st.column_config.NumberColumn(width=80),
            "用户名称": st.column_config.TextColumn(width=150),
            "部门": st.column_config.TextColumn(width=150),
            "所属中心": st.column_config.TextColumn(width=150),
            "月份": st.column_config.TextColumn(width=80),
            "总应出勤时长(小时)": st.column_config.NumberColumn(width=120, format="%.2f"),
            "总实际出勤(小时)": st.column_config.NumberColumn(width=120, format="%.2f"),
            "出差天数": st.column_config.NumberColumn(width=80, format="%d"),
            "出差总时长(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
            "总班内工作时长(小时)": st.column_config.NumberColumn(width=120, format="%.2f"),
            "总加班时间(小时)": st.column_config.NumberColumn(width=120, format="%.2f"),
            "总请假时长(小时)": st.column_config.NumberColumn(width=120, format="%.2f"),
            "年假(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
            "病假(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
            "事假(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
            "婚假(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
            "产假(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
            "产检假(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
            "丧假(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
            "用户ID": st.column_config.TextColumn(width=100)
        }
        st.dataframe(df_page, column_config=column_config, use_container_width=True, hide_index=True)

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
                selected_size = st.selectbox("每页行数", options=page_size_options,
                                             index=page_size_options.index(st.session_state.page_size_monthly),
                                             key="page_size_monthly_select", label_visibility="collapsed")
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
                if st.button("下一页 →", disabled=(st.session_state.page_monthly == total_pages),
                             key="next_monthly"):
                    st.session_state.page_monthly += 1
                    st.rerun()
            with pagination_cols[4]:
                if st.button("⏭️ 末页", disabled=(st.session_state.page_monthly == total_pages),
                             key="last_monthly"):
                    st.session_state.page_monthly = total_pages
                    st.rerun()

    with tab_daily:
        if is_cross_month:
            months = st.session_state.month_list
            if "selected_daily_month" not in st.session_state or st.session_state.selected_daily_month not in months:
                st.session_state.selected_daily_month = months[0]
            col_month, col_export = st.columns([3, 1])
            with col_month:
                selected_daily_month = st.selectbox("选择月份", options=months, key="selected_daily_month",
                                                    label_visibility="collapsed")
            with col_export:
                df_daily_raw = st.session_state.df_daily_by_month.get(selected_daily_month, pd.DataFrame())
                df_daily_filtered = filter_df(df_daily_raw)
                if not df_daily_filtered.empty:
                    excel_data = export_to_excel(df_daily_filtered, f"日报表_{selected_daily_month}.xlsx")
                    st.download_button(
                        label="📎 导出当前数据",
                        data=excel_data,
                        file_name=f"日报表_{selected_daily_month}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="export_daily",
                        use_container_width=True
                    )
            df_daily_raw = st.session_state.df_daily_by_month.get(selected_daily_month, pd.DataFrame())
            df_daily_filtered = filter_df(df_daily_raw)
        else:
            col_export = st.columns([1])[0]
            with col_export:
                df_daily_filtered = filter_df(st.session_state.df_daily_raw)
                if not df_daily_filtered.empty:
                    excel_data = export_to_excel(df_daily_filtered, "日报表_当前.xlsx")
                    st.download_button(
                        label="📎 导出当前数据",
                        data=excel_data,
                        file_name=f"日报表_当前_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="export_daily",
                        use_container_width=True
                    )
            df_daily_filtered = filter_df(st.session_state.df_daily_raw)

        df_display = df_daily_filtered.copy()
        if not df_display.empty:
            df_display.insert(0, '序号', range(1, len(df_display) + 1))

        total_rows = len(df_display)
        start_idx = (st.session_state.page_daily - 1) * st.session_state.page_size_daily
        end_idx = min(start_idx + st.session_state.page_size_daily, total_rows)
        df_page = df_display.iloc[start_idx:end_idx] if total_rows > 0 else df_display

        column_config = {
            "序号": st.column_config.NumberColumn(width=80),
            "用户名称": st.column_config.TextColumn(width=150),
            "部门": st.column_config.TextColumn(width=150),
            "所属中心": st.column_config.TextColumn(width=150),
            "日期": st.column_config.TextColumn(width=100),
            "总应出勤(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
            "总实际出勤(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
            "出差时长(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
            "是否出差": st.column_config.TextColumn(width=80),
            "班内工作时长(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
            "加班时间(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
            "请假时长(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
            "用户ID": st.column_config.TextColumn(width=100)
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
                st.markdown(f"**共 {len(df_daily_filtered)} 条记录**")
            with left_inner_cols[1]:
                page_size_options = [10, 20, 50, 100]
                selected_size = st.selectbox("每页行数", options=page_size_options,
                                             index=page_size_options.index(st.session_state.page_size_daily),
                                             key="page_size_daily_select", label_visibility="collapsed")
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

    with tab_detail:
        if is_cross_month:
            months = st.session_state.month_list
            if "selected_detail_month" not in st.session_state or st.session_state.selected_detail_month not in months:
                st.session_state.selected_detail_month = months[0]
            col_month, col_export = st.columns([3, 1])
            with col_month:
                selected_detail_month = st.selectbox("选择月份", options=months, key="selected_detail_month",
                                                     label_visibility="collapsed")
            with col_export:
                df_detail_raw = st.session_state.df_detail_by_month.get(selected_detail_month, pd.DataFrame())
                df_detail_filtered = filter_df(df_detail_raw)
                if not df_detail_filtered.empty:
                    excel_data = export_to_excel(df_detail_filtered, f"明细数据_{selected_detail_month}.xlsx")
                    st.download_button(
                        label="📎 导出当前数据",
                        data=excel_data,
                        file_name=f"明细数据_{selected_detail_month}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="export_detail",
                        use_container_width=True
                    )
            df_detail_raw = st.session_state.df_detail_by_month.get(selected_detail_month, pd.DataFrame())
            df_detail_filtered = filter_df(df_detail_raw)
        else:
            col_export = st.columns([1])[0]
            with col_export:
                df_detail_filtered = filter_df(st.session_state.df_detail_raw)
                if not df_detail_filtered.empty:
                    excel_data = export_to_excel(df_detail_filtered, "明细数据_当前.xlsx")
                    st.download_button(
                        label="📎 导出当前数据",
                        data=excel_data,
                        file_name=f"明细数据_当前_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="export_detail",
                        use_container_width=True
                    )
            df_detail_filtered = filter_df(st.session_state.df_detail_raw)

        df_display = df_detail_filtered.copy()
        if not df_display.empty and '请假时长' in df_display.columns:
            df_display['请假时长'] = pd.to_numeric(df_display['请假时长'], errors='coerce').fillna(0)
        if not df_display.empty:
            df_display.insert(0, '序号', range(1, len(df_display) + 1))

        total_rows = len(df_display)
        start_idx = (st.session_state.page_detail - 1) * st.session_state.page_size_detail
        end_idx = min(start_idx + st.session_state.page_size_detail, total_rows)
        df_page = df_display.iloc[start_idx:end_idx] if total_rows > 0 else df_display

        column_config = {
            "序号": st.column_config.NumberColumn(width=80),
            "姓名": st.column_config.TextColumn(width=120),
            "部门": st.column_config.TextColumn(width=150),
            "工号": st.column_config.TextColumn(width=100),
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
            "是否出差": st.column_config.TextColumn(width=80),
            "出差时长(小时)": st.column_config.NumberColumn(width=100, format="%.2f"),
            "用户ID": st.column_config.TextColumn(width=100)
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
                st.markdown(f"**共 {len(df_detail_filtered)} 条记录**")
            with left_inner_cols[1]:
                page_size_options = [10, 20, 50, 100]
                selected_size = st.selectbox("每页行数", options=page_size_options,
                                             index=page_size_options.index(st.session_state.page_size_detail),
                                             key="page_size_detail_select", label_visibility="collapsed")
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