
# ---------------------------------------------
# 3.Variable Construction.py
#
# 本脚本实现对带货数据的变量构建，包括结果变量、前因变量、调节变量和控制变量等，将最终变量结果输出为CSV文件，便于后续分析。
# 主要流程：
#   3.1 读取数据文件。
#   3.2.1 针对带货样本，构建各类变量（结果变量、前因变量、调节变量、控制变量等）。
#   3.2.2 针对带货样本，输出变量结果到CSV文件，便于后续分析。
#   3.3.1 针对不带货样本，构建各类变量（结果变量、前因变量、调节变量、控制变量等）。
#   3.3.2 针对不带货样本，输出变量结果到CSV文件，便于后续分析。
# ---------------------------------------------


# 3.1 读取数据文件。

# 导入必要的库
import pandas as pd
import dask
import dask.dataframe as dd

# 数据文件目录
file_path = r'.\data'

# 读取带货数据，采用Dask以支持大文件处理
sales_data = dd.read_csv(
    file_path + r'\with_sales.csv',
    encoding='UTF-8',
    encoding_errors='ignore',
    assume_missing=True
)

nosales_data = dd.read_csv(
    file_path + r'\without_sales.csv',
    encoding='UTF-8',
    encoding_errors='ignore',
    assume_missing=True
)


# 3.2.1 针对带货样本，构建各类变量（结果变量、前因变量、调节变量、控制变量等）。

# 结果变量
sales_data['播放量'] = sales_data['play_count']
sales_data['销量'] = sales_data['sales']
sales_data['点赞'] = sales_data['likes']
sales_data['评论'] = sales_data['comment']
sales_data['分享'] = sales_data['share']
sales_data['销售转化率'] = sales_data['sales'] / sales_data['play_count'].replace(0, None)
sales_data['点赞转化率'] = sales_data['likes'] / sales_data['play_count'].replace(0, None)
sales_data['评论转化率'] = sales_data['comment'] / sales_data['play_count'].replace(0, None)
sales_data['分享转化率'] = sales_data['share'] / sales_data['play_count'].replace(0, None)

# 前因变量
# 确保createtime为datetime类型，便于后续时间相关变量构建
sales_data['createtime'] = dd.to_datetime(sales_data['createtime'], errors='coerce')

# 构建时间段变量（上午/下午/晚上/深夜/未知）
def get_time_period(dt):
    hour = dt.hour if not pd.isnull(dt) else -1
    if 6 <= hour < 12:
        return '上午'
    elif 12 <= hour < 18:
        return '下午'
    elif 18 <= hour < 24:
        return '晚上'
    elif 0 <= hour < 6:
        return '深夜'
    else:
        return '未知'
sales_data['时间段'] = sales_data['createtime'].map(get_time_period, meta=('createtime', 'object'))  # 新增时段变量

# 构建周末变量（0=工作日，1=周末，-1=未知）
def is_weekend(dt):
    if pd.isnull(dt):
        return -1
    return 1 if dt.weekday() >= 5 else 0
sales_data['是否周末'] = sales_data['createtime'].map(is_weekend, meta=('createtime', 'int8'))  # 新增是否为周末变量

# 构建粉丝数分组变量（0=小于1万，1=1万-5万，2=5万-100万，3=100万-500万，4=500万及以上，-1=未知）
def influencer_size(fans):
    if pd.isnull(fans):
        return -1
    try:
        fans = float(fans)
    except:
        return -1
    if fans < 10000:
        return 0
    elif fans < 50000:
        return 1
    elif fans < 1000000:
        return 2
    elif fans < 5000000:
        return 3
    else:
        return 4
sales_data['影响力规模'] = sales_data['follower_count'].map(influencer_size, meta=('follower_count', 'int8'))  # 新增粉丝分组变量

# 调节变量
sales_data['产品类型'] = sales_data['v11_category_big']

# 控制变量
sales_data['时长'] = sales_data['duration']


# 3.2.2 针对带货样本，输出变量结果到CSV文件，便于后续分析。

save_columns = [
    'category', 'author_id', 'name', 'title', 'rid', 'product', 'aweme_url',
    '播放量', '销量', '点赞', '评论', '分享',
    '销售转化率', '点赞转化率', '评论转化率', '分享转化率',
    'createtime', '时间段', '是否周末', '影响力规模', '产品类型', '时长'
]

sales_data[save_columns].compute().to_csv(file_path + r'\带货数据变量指标.csv', index=False, encoding='utf-8-sig')


# 3.3.1 针对不带货样本，构建各类变量（结果变量、前因变量、调节变量、控制变量等）。

# 结果变量
nosales_data['播放量'] = nosales_data['play_count'] #播放量
nosales_data['点赞'] = nosales_data['likes'] #点赞
nosales_data['评论'] = nosales_data['comment'] #评论
nosales_data['分享'] = nosales_data['share'] #分享
nosales_data['点赞转化率'] = nosales_data['likes'] / nosales_data['play_count'].replace(0, None) #点赞转化率
nosales_data['评论转化率'] = nosales_data['comment'] / nosales_data['play_count'].replace(0, None) #评论转化率
nosales_data['分享转化率'] = nosales_data['share'] / nosales_data['play_count'].replace(0, None) #分享转化率

# 前因变量
# 确保createtime为datetime类型
nosales_data['createtime'] = dd.to_datetime(nosales_data['createtime'], errors='coerce')

# 构建时间段变量
def get_time_period(dt):
    hour = dt.hour if not pd.isnull(dt) else -1
    if 6 <= hour < 12:
        return '上午'
    elif 12 <= hour < 18:
        return '下午'
    elif 18 <= hour < 24:
        return '晚上'
    elif 0 <= hour < 6:
        return '深夜'
    else:
        return '未知'
nosales_data['时间段'] = nosales_data['createtime'].map(get_time_period, meta=('createtime', 'object'))  # 新增时段变量

# 构建周末变量（0=工作日，1=周末）
def is_weekend(dt):
    if pd.isnull(dt):
        return -1
    return 1 if dt.weekday() >= 5 else 0
nosales_data['是否周末'] = nosales_data['createtime'].map(is_weekend, meta=('createtime', 'int8'))  # 新增是否为周末变量

# 构建粉丝数分组变量
def influencer_size(fans):
    if pd.isnull(fans):
        return -1
    try:
        fans = float(fans)
    except:
        return -1
    if fans < 10000:
        return 0
    elif fans < 50000:
        return 1
    elif fans < 1000000:
        return 2
    elif fans < 5000000:
        return 3
    else:
        return 4
nosales_data['影响力规模'] = nosales_data['follower_count'].map(influencer_size, meta=('follower_count', 'int8'))  # 新增粉丝分组变量

# 控制变量
nosales_data['时长'] = nosales_data['duration']

# 3.3.2 针对不带货样本，输出变量结果到CSV文件，便于后续分析。

save_columns = [
    'category', 'author_id', 'name', 'title', 'rid', 'aweme_url',
    '播放量', '点赞', '评论', '分享',
    '点赞转化率', '评论转化率', '分享转化率',
    'createtime', '时间段', '是否周末', '影响力规模', '时长'
]

nosales_data[save_columns].compute().to_csv(file_path + r'\不带货数据变量指标.csv', index=False, encoding='utf-8-sig')