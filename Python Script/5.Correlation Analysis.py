
# ---------------------------------------------
# 5.Correlation Analysis.py
#
# 本脚本实现带货与不带货样本的相关系数分析与分布对比，并输出Excel报告。
# 主要流程：
#   5.1 读取变量数据文件。
#   5.2 计算相关系数表（带货/不带货分开，显著性标注）。
#   5.3 生成分组变量与结果变量的分布对比图。
#   5.4 整合相关系数表与分布图片，写入Excel文件，便于后续查看和分析。
# ---------------------------------------------


# 5.1 读取变量数据文件。

# 导入必要的库
import pandas as pd
from scipy.stats import pearsonr
import numpy as np

# 数据文件目录
file_path = r'.\data'

# 读取带货与不带货变量数据
sales = pd.read_csv(file_path + r'\带货数据变量指标.csv', encoding='utf-8-sig')
nosales = pd.read_csv(file_path + r'\不带货数据变量指标.csv', encoding='utf-8-sig')


# 5.2 计算相关系数表（带货/不带货分开，显著性标注）。

# 定义变量
result_vars_sales = ['播放量', '销量', '点赞', '评论', '分享', '销售转化率', '点赞转化率', '评论转化率', '分享转化率']
cause_vars_sales = ['category', '时间段', '是否周末', '影响力规模', '产品类型', '时长']

result_vars_nosales = ['播放量', '点赞', '评论', '分享', '点赞转化率', '评论转化率', '分享转化率']
cause_vars_nosales = ['category', '时间段', '是否周末', '影响力规模', '时长']

# 相关系数计算函数
def calc_corr_table(df, cause_vars, result_vars):
    # 只对实际存在的分类变量做one-hot编码
    cat_vars = [v for v in ['category', '时间段', '影响力规模', '产品类型'] if v in cause_vars and v in df.columns]
    if cat_vars:
        df_encoded = pd.get_dummies(df[cause_vars], columns=cat_vars, dummy_na=True, drop_first=True)
    else:
        df_encoded = df[cause_vars].copy()
    # 合并结果变量
    df_all = pd.concat([df_encoded, df[result_vars]], axis=1)
    # 只保留数值型
    df_all = df_all.apply(pd.to_numeric, errors='coerce')
    # 相关系数和p值
    corr_table = pd.DataFrame(index=df_encoded.columns, columns=result_vars)
    pval_table = pd.DataFrame(index=df_encoded.columns, columns=result_vars)
    for c in df_encoded.columns:
        for r in result_vars:
            x = df_all[c]
            y = df_all[r]
            mask = x.notnull() & y.notnull()
            if mask.sum() > 2:
                corr, pval = pearsonr(x[mask], y[mask])
                corr_table.loc[c, r] = corr
                pval_table.loc[c, r] = pval
            else:
                corr_table.loc[c, r] = np.nan
                pval_table.loc[c, r] = np.nan
    # 标注显著性
    def mark_sig(c, p):
        if pd.isnull(c): return ''
        if p < 0.01: return f'{c:.2f}**'
        elif p < 0.05: return f'{c:.2f}*'
        else: return f'{c:.2f}'
    sig_table = corr_table.copy()
    for c in corr_table.index:
        for r in corr_table.columns:
            sig_table.loc[c, r] = mark_sig(corr_table.loc[c, r], pval_table.loc[c, r])
    return sig_table

# 带货样本相关系数表
corr_sales = calc_corr_table(sales, cause_vars_sales, result_vars_sales)
print('带货样本相关系数表（* p<0.05, ** p<0.01）')

# 不带货样本相关系数表
corr_nosales = calc_corr_table(nosales, cause_vars_nosales, result_vars_nosales)
print('不带货样本相关系数表（* p<0.05, ** p<0.01）')


# 5.3 生成分组变量与结果变量的概率分布对比图。

# 导入可视化相关库
import matplotlib.pyplot as plt
import seaborn as sns

# 设置matplotlib支持中文，防止中文乱码
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

# 合并相关系数表并分行插入对比分布图到Excel
import io
import xlsxwriter

# Excel输出路径
excel_path =  file_path + r'\变量相关性与分布分析对比结果.xlsx'

# 合并相关系数表
corr_sales_cp = corr_sales.copy()
corr_sales_cp['样本类型'] = '带货样本'
corr_nosales_cp = corr_nosales.copy()
corr_nosales_cp['样本类型'] = '不带货样本'
corr_all = pd.concat([corr_sales_cp, corr_nosales_cp], axis=0)

# 生成所有对比分布图（同类分组变量-结果变量合并到同一图，hue区分样本类型）
def gen_corr_compare_imgs():
    img_dict = {}  # key: 图名, value: img_bytes
    group_vars = ['category', '时间段', '是否周末', '影响力规模']
    result_vars_sales = ['播放量', '销量', '点赞', '评论', '分享', '销售转化率', '点赞转化率', '评论转化率', '分享转化率']
    result_vars_nosales = ['播放量', '点赞', '评论', '分享', '点赞转化率', '评论转化率', '分享转化率']
    trans_vars = ['销售转化率', '点赞转化率', '评论转化率', '分享转化率']
    for group in group_vars:
        all_result_vars = list(set(result_vars_sales + result_vars_nosales))
        for res in all_result_vars:
            # 合并数据，添加样本类型
            df1 = sales[[group, res]].copy() if (group in sales.columns and res in sales.columns) else None
            df2 = nosales[[group, res]].copy() if (group in nosales.columns and res in nosales.columns) else None
            if df1 is not None: df1['样本类型'] = '带货样本'
            if df2 is not None: df2['样本类型'] = '不带货样本'
            plot_df = pd.concat([df1, df2], axis=0) if (df1 is not None or df2 is not None) else None
            if plot_df is None or plot_df.empty: continue
            # 针对category分类多，增大图像尺寸
            if group == 'category':
                fig, ax = plt.subplots(figsize=(max(12, 0.7*plot_df['category'].nunique()), 7))
            else:
                fig, ax = plt.subplots(figsize=(9,5))
            if res not in trans_vars:
                # 去极端值+取对数+小提琴图
                plot_df = plot_df[plot_df[res].notnull() & (plot_df[res] > 0)]
                q_low = plot_df[res].quantile(0.05)
                q_high = plot_df[res].quantile(0.95)
                plot_df = plot_df[(plot_df[res] >= q_low) & (plot_df[res] <= q_high)]
                plot_df[res] = np.log(plot_df[res])
                sns.violinplot(x=group, y=res, hue='样本类型', data=plot_df, split=True, inner='box', palette=['royalblue','orange'], ax=ax)
                ax.set_title(f'{group} - {res}（去极端5%取对数）小提琴图')
                ax.set_ylabel(f'log({res})')
            else:
                # 转化率：去极端值后小提琴图（split=True），不取对数
                plot_df = plot_df[plot_df[res].notnull()]
                q_low = plot_df[res].quantile(0.05)
                q_high = plot_df[res].quantile(0.95)
                plot_df = plot_df[(plot_df[res] >= q_low) & (plot_df[res] <= q_high)]
                sns.violinplot(x=group, y=res, hue='样本类型', data=plot_df, split=True, inner='box', palette=['royalblue','orange'], ax=ax)
                ax.set_title(f'{group} - {res}（去极端5%）小提琴图')
                ax.set_ylabel(res)
                ax.legend(title='样本类型')
            ax.set_xlabel(group)
            plt.tight_layout()
            buf = io.BytesIO()
            fig.savefig(buf, format='png', bbox_inches='tight')
            buf.seek(0)
            img_dict[f'{group}-{res}'] = buf.read()
            plt.close(fig)
    return img_dict

img_dict = gen_corr_compare_imgs()


# 5.4 整合相关系数表与分布图片，写入Excel文件，便于后续查看和分析。

with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
    # 写入相关系数表
    corr_all.to_excel(writer, sheet_name='相关系数表')
    workbook = writer.book
    worksheet = workbook.add_worksheet('分布对比图')
    row = 0
    for idx, (title, img_bytes) in enumerate(img_dict.items()):
        worksheet.write(row, 0, title)
        worksheet.insert_image(row+1, 2, '', {'image_data': io.BytesIO(img_bytes), 'x_scale': 0.8, 'y_scale': 0.8})
        # 估算图片高度（行数），大图多占点行
        img_height = 22 if 'category' not in title else 32
        row += img_height + 4
    print(f'已保存为Excel：{excel_path}')