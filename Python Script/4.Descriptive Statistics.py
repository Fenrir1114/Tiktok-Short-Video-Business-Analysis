
# ---------------------------------------------
# 4.Descriptive Statistics.py
#
# 本脚本实现对带货与不带货变量数据的描述性统计分析与可视化对比，并输出Excel报告。
# 主要流程：
#   4.1 读取变量数据文件。
#   4.2 进行描述性统计分析，计算均值、中位数、方差、最大值、最小值、样本数量等。
#   4.3 生成对比图片（按小时、星期分布、各变量分布、产品类型频次等）。
#   4.4 整合统计表与图片，写入Excel文件，便于后续查看和分析。
# ---------------------------------------------


# 4.1 读取变量数据文件。

# 导入必要的库
import pandas as pd
import dask
import dask.dataframe as dd
import numpy as np

# 数据文件目录
file_path = r'.\data'

# 读取带货与不带货变量数据
sales_df = pd.read_csv(file_path + r'\带货数据变量指标.csv', encoding='utf-8-sig')
nosales_df = pd.read_csv(file_path + r'\不带货数据变量指标.csv', encoding='utf-8-sig')


# 4.2 进行描述性统计分析，计算均值、中位数、方差、最大值、最小值、样本数量等。

# 需要进行描述性统计的可数变量（去除分类变量）
sales_numeric_cols = ['播放量', '销量', '点赞', '评论', '分享', '销售转化率', '点赞转化率', '评论转化率', '分享转化率', '时长']
nosales_numeric_cols = ['播放量', '点赞', '评论', '分享', '点赞转化率', '评论转化率', '分享转化率', '时长']

# 针对可数变量进行统计分析
def describe_numeric(df, cols):
    desc = df[cols].agg(['mean', 'median', 'var', 'max', 'min', 'count']).T
    desc = desc.rename(columns={'mean': '均值', 'median': '中位数', 'var': '方差', 'max': '最大值', 'min': '最小值', 'count': '样本数量'})
    return desc


# 4.3 生成对比图片（按小时、星期分布、各变量分布、产品类型频次等）。

# 导入可视化相关库
import matplotlib.pyplot as plt
import seaborn as sns

# 设置matplotlib支持中文，防止中文乱码
import matplotlib
matplotlib.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']  # 优先使用黑体、雅黑等支持中文的字体
matplotlib.rcParams['axes.unicode_minus'] = False  # 正常显示负号

# 工具函数：去除极端5%
def remove_outliers(df, cols, lower=0.05, upper=0.95):
    df_clip = df.copy()
    for col in cols:
        if col in df_clip.columns:
            q_low = df_clip[col].quantile(lower)
            q_high = df_clip[col].quantile(upper)
            df_clip = df_clip[(df_clip[col] >= q_low) & (df_clip[col] <= q_high)]
    return df_clip

# 合并统计表并并排插入对比图片到Excel
import io
from PIL import Image
import xlsxwriter

# Excel输出路径
excel_path = file_path + r'\描述性统计对比结果.xlsx'

# 合并统计表
sales_desc = describe_numeric(sales_df, sales_numeric_cols)
sales_desc['样本类型'] = '带货样本'
nosales_desc = describe_numeric(nosales_df, nosales_numeric_cols)
nosales_desc['样本类型'] = '不带货样本'
desc_all = pd.concat([sales_desc, nosales_desc], axis=0)

# 生成所有对比图片（小时/星期用histplot，其余变量用去极端值小提琴图，部分变量取对数）
def gen_compare_imgs():
    img_dict = {}  # key: 图名, value: img_bytes
    # 1）按小时分布（类别型，histplot）
    for var, label in [('hour', '按小时分布'), ('weekday_cn', '按星期分布')]:
        for df in [sales_df, nosales_df]:
            df['createtime'] = pd.to_datetime(df['createtime'], errors='coerce')
            if var == 'hour':
                df['hour'] = df['createtime'].dt.hour
            elif var == 'weekday_cn':
                weekday_map = {0: '周一', 1: '周二', 2: '周三', 3: '周四', 4: '周五', 5: '周六', 6: '周日'}
                df['weekday'] = df['createtime'].dt.weekday
                df['weekday_cn'] = df['weekday'].map(weekday_map)
        plot_df = pd.concat([sales_df[[var]].assign(样本类型='带货样本'), nosales_df[[var]].assign(样本类型='不带货样本')], axis=0)
        fig, ax = plt.subplots(figsize=(7,4))
        if var == 'hour':
            sns.histplot(data=plot_df, x=var, hue='样本类型', multiple='dodge', shrink=0.8, bins=24, discrete=True, ax=ax)
        else:
            order = ['周一','周二','周三','周四','周五','周六','周日']
            sns.histplot(data=plot_df, x=var, hue='样本类型', multiple='dodge', shrink=0.8, discrete=True, ax=ax, stat='count', binrange=None, bins=len(order))
            ax.set_xticks(range(len(order)))
            ax.set_xticklabels(order)
        ax.set_title(label)
        ax.set_xlabel(var)
        ax.set_ylabel('频次')
        ax.legend()
        plt.tight_layout()
        buf = io.BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight')
        buf.seek(0)
        img_dict[label] = buf.read()
        plt.close(fig)

    # 2）可数变量分布对比（每个变量单独一个框，横向对比，去极端值小提琴图，部分变量取对数）
    log_vars = ['销量','播放量','分享','点赞','评论']
    all_vars = list(set(sales_numeric_cols + nosales_numeric_cols))
    for var in all_vars:
        # 合并数据，添加样本类型
        df1 = sales_df[[var]].copy() if var in sales_df.columns else None
        df2 = nosales_df[[var]].copy() if var in nosales_df.columns else None
        if df1 is not None:
            q_low = df1[var].quantile(0.05)
            q_high = df1[var].quantile(0.95)
            df1 = df1[(df1[var] >= q_low) & (df1[var] <= q_high)]
            df1['样本类型'] = '带货样本'
        if df2 is not None:
            q_low = df2[var].quantile(0.05)
            q_high = df2[var].quantile(0.95)
            df2 = df2[(df2[var] >= q_low) & (df2[var] <= q_high)]
            df2['样本类型'] = '不带货样本'
        plot_df = pd.concat([df1, df2], axis=0) if (df1 is not None or df2 is not None) else None
        if plot_df is not None and not plot_df.empty:
            fig, ax = plt.subplots(figsize=(7,4))
            if var in log_vars:
                plot_df = plot_df[plot_df[var] > 0]
                plot_df[var] = np.log(plot_df[var])
                y_label = f'log({var})'
                log_str = ',取对数'
            else:
                y_label = var
                log_str = ''
            # 同一把提琴split，左带货右不带货，颜色一致
            sns.violinplot(x=None, y=var, hue='样本类型', data=plot_df, split=True, inner='box', palette=['royalblue','orange'], ax=ax, order=None, hue_order=['带货样本','不带货样本'])
            ax.set_title(f'{var} 分布对比（去极端5%{log_str}）')
            ax.set_xlabel('')
            ax.set_ylabel(y_label)
            ax.legend(title='样本类型')
            plt.tight_layout()
            buf = io.BytesIO()
            fig.savefig(buf, format='png', bbox_inches='tight')
            buf.seek(0)
            img_dict[f'{var}分布对比'] = buf.read()
            plt.close(fig)

    # 3）产品类型频次图（仅带货有）
    if '产品类型' in sales_df.columns:
        fig, ax = plt.subplots(figsize=(10,4))
        sns.countplot(x='产品类型', data=sales_df, color='royalblue', order=sales_df['产品类型'].value_counts().index, ax=ax)
        ax.set_title('带货样本各产品类型频次分布')
        ax.set_xlabel('产品类型')
        ax.set_ylabel('频次')
        plt.xticks(rotation=45)
        plt.tight_layout()
        buf = io.BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight')
        buf.seek(0)
        img_dict['带货样本产品类型频次'] = buf.read()
        plt.close(fig)
    return img_dict

img_dict = gen_compare_imgs()


# 4.4 整合统计表与图片，写入Excel文件，便于后续查看和分析。

with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
    # 写入统计表
    desc_all.to_excel(writer, sheet_name='描述性统计')
    workbook = writer.book
    worksheet = workbook.add_worksheet('对比图片')
    row = 0
    for title, img_bytes in img_dict.items():
        worksheet.write(row, 0, title)
        worksheet.insert_image(row+1, 0, '', {'image_data': io.BytesIO(img_bytes), 'x_scale': 0.8, 'y_scale': 0.8})
        row += 22  # 每张图片间隔
print(f'已保存为Excel：{excel_path}')