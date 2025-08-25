
# ---------------------------------------------
# 1.Data Processing, Active Blogger Filtering and Report Association.py
#
# 本脚本实现数据主表的高效读取、活跃博主筛选、数据清洗与多表关联，并最终输出整合结果。
# 可通过参数选择处理“带货”或“不带货”数据。
# 主要流程：
#   1.1 读取主表，进行恰当的解码以及格式初步处理，确保不出错且不占用过多内存。
#   1.2 去除无效数据，构建目标时长，筛选出主表中的活跃博主，进一步提高数据的有效性和可用性。
#   1.3 进行数据清洗与处理，选出带货/不带货数据，去除数据中的各色错误，着重修正时间格式，围绕主表按照各指标进行数据关联。
#   1.4 将多分区整合为单一文件并保存文件，方便后续的进一步处理与操作。
# ---------------------------------------------

import pandas as pd
import dask
import dask.dataframe as dd
from datetime import timedelta

# 选择处理带货或不带货数据
requirements = input('请选择处理类型（带货/不带货）: ').strip()

# 数据文件目录
file_path = r'.\data'

# 1.1 读取主表，进行恰当的解码以及格式初步处理，确保不出错且不占用过多内存。
video_data = dd.read_csv(
	file_path + r'\video_data.csv',
	encoding='GB18030',
	encoding_errors='ignore',
	usecols=[
		'category', 'author_id', 'name', 'title',
		'rid', 'comment', 'likes', 'share', 'duration', 'createtime',
		'product', 'sales', 'volume', 'aweme_url', 'collect_count',
		'download_count', 'forward_count', 'play_count', 'product_count',
	],
	dtype={
		'category': 'string', 'author_id': 'string', 'name': 'string', 'title': 'string',
		'rid': 'string', 'comment': 'string', 'likes': 'float32', 'share': 'float32', 'duration': 'float32',
		'product': 'string', 'sales': 'float32', 'volume': 'float32', 'aweme_url': 'string', 'collect_count': 'float32',
		'download_count': 'float32', 'forward_count': 'float32', 'play_count': 'float32', 'product_count': 'float32'
	}
)
print(video_data.head())

# 1.2 去除无效数据，构建目标时长，筛选出主表中的活跃博主，进一步提高数据的有效性和可用性。
video_data = video_data[~video_data['rid'].isnull()]
print(f"清除rid为空后的数据量: {len(video_data)}")

start_week_begin = pd.Timestamp('2023-06-05')
start_week_end = start_week_begin + timedelta(days=7)
end_week_begin = pd.Timestamp('2024-06-24')
end_week_end = end_week_begin + timedelta(days=7)

authors_info = dd.read_csv(file_path + r'\authors_info.csv', encoding='UTF-8')
print(authors_info['video_earliest_createtime'].head())

authors_info['video_earliest_createtime'] = dd.to_datetime(
	authors_info['video_earliest_createtime'],
	format='%Y-%m-%d %H:%M:%S',
	errors='coerce')
authors_info['video_latest_createtime'] = dd.to_datetime(
	authors_info['video_latest_createtime'],
	format='%Y-%m-%d %H:%M:%S',
	errors='coerce')

start_week_mask = (
	(authors_info['video_earliest_createtime'] >= start_week_begin) &
	(authors_info['video_earliest_createtime'] <= start_week_end)
)
end_week_mask = (
	(authors_info['video_latest_createtime'] >= end_week_begin) &
	(authors_info['video_latest_createtime'] <= end_week_end)
)

start_week_authors = authors_info.loc[start_week_mask, 'author_id'].unique()
end_week_authors = authors_info.loc[end_week_mask, 'author_id'].unique()
active_authors = set(start_week_authors) & set(end_week_authors)

print(f'活跃博主数量: {len(active_authors)}')
print('活跃博主ID示例:', list(active_authors)[:5])

active_mask = video_data['author_id'].isin(active_authors)
video_data = video_data[active_mask]

print('活跃博主数据示例:')
print(video_data.head())

# 1.3 进行数据清洗与处理，去除数据中的各色错误，着重修正时间格式，围绕主表按照各指标进行数据关联。
if requirements == '带货':
	# 仅保留带货视频
	video_data = video_data[video_data.sales != -2147483648]

	# 商品类目相关字段
	category_columns = [
		'v11_category_big', 'v11_category_big_id',
		'v11_category_first', 'v11_category_first_id',
		'v11_category_second', 'v11_category_second_id',
		'v11_category_third', 'v11_category_third_id',
		'v11_category_fourth', 'v11_category_fourth_id'
	]

	# 读取商品信息表，包含类目信息
	product_data = dd.read_csv(
		file_path + r'\product_data.csv',
		usecols=['volume_text', 'brand_name', 'product_id', 'product_title'] + category_columns,
		dtype={
			'volume_text': 'string', 'brand_name': 'string',
			'product_id': 'string', 'product_title': 'string',
			'v11_category_big': 'category',
			'v11_category_big_id': 'int16',
			'v11_category_first': 'category',
			'v11_category_first_id': 'int16',
			'v11_category_second': 'category',
			'v11_category_second_id': 'int16',
			'v11_category_third': 'category',
			'v11_category_third_id': 'int16',
			'v11_category_fourth': 'category',
			'v11_category_fourth_id': 'int16'
		}
	)

	# 清洗销量文本，去除异常字符并转为数值
	def clean_volume_text(text):
		if pd.isna(text):
			return 0
		text = str(text).replace(''', '').replace(''', '')
		text = text.replace(',', '')
		try:
			return float(text)
		except ValueError:
			return 0

	product_data['volume_numeric'] = product_data['volume_text'].map(clean_volume_text, meta=('volume_text', 'float32'))

	# 对每个商品标题，仅保留销量最大的记录，去重
	max_volume_per_title = product_data.groupby('product_title')['volume_numeric'].max().reset_index()
	product_data_deduplicated = product_data.merge(
		max_volume_per_title,
		on=['product_title', 'volume_numeric'],
		how='inner')

	# 主表与商品表关联，补充商品类目信息
	merged_dask = video_data.merge(
		product_data_deduplicated[['brand_name', 'product_id', 'product_title'] + category_columns],
		left_on='product',
		right_on='product_title',
		how='left'
	).drop(columns=['product_title'])

	print(merged_dask.head())

	# 读取粉丝趋势表
	fan_trend = dd.read_csv(file_path + r'\fan_trend.csv', encoding='UTF-8')

	# 转换时间格式并清洗异常，便于后续关联
	merged_dask['createtime'] = dd.to_datetime(
		merged_dask['createtime'],
		format='%Y/%m/%d %H:%M',
		errors='coerce')
	fan_trend['time_node'] = dd.to_datetime(
		fan_trend['time_node'],
		format='%Y-%m-%d %H:%M:%S',
		errors='coerce')
	merged_dask = merged_dask.assign(
		createtime=merged_dask['createtime'].astype('datetime64[ns]'))
	fan_trend = fan_trend.assign(
		time_node=fan_trend['time_node'].astype('datetime64[ns]'))

	# 提取日期部分，便于按天关联
	merged_dask['date_only'] = dd.to_datetime(merged_dask['createtime']).dt.date
	fan_trend['date_only'] = dd.to_datetime(fan_trend['time_node']).dt.date
	merged_dask = merged_dask.assign(
		date_only=merged_dask['date_only'].astype('datetime64[ns]'))
	fan_trend = fan_trend.assign(
		date_only=fan_trend['date_only'].astype('datetime64[ns]'))

	# 主表与粉丝趋势表按作者和日期关联，补充粉丝数
	merged_dask1 = merged_dask.merge(
		fan_trend[['cid', 'date_only', 'follower_count']],
		left_on=['author_id', 'date_only'],
		right_on=['cid', 'date_only'],
		how='left'
	).drop(columns=['cid', 'date_only'])

	print(merged_dask1.head())

	# 删除中间表，释放内存
	del merged_dask
	del video_data
	del product_data
	del fan_trend


	# 1.4 输出最终结果为单一CSV文件，便于后续分析
	merged_dask1.to_csv(file_path + '\\with_sales.csv', index=False, single_file=True)

else:
	# 仅保留不带货视频
	video_data = video_data[video_data.sales == -2147483648]

	# 读取粉丝趋势表
	fan_trend = dd.read_csv(file_path + r'\fan_trend.csv', encoding='UTF-8')

	# 转换时间格式并清洗异常，便于后续关联
	video_data['createtime'] = dd.to_datetime(
		video_data['createtime'],
		format='%Y/%m/%d %H:%M',
		errors='coerce')
	fan_trend['time_node'] = dd.to_datetime(
		fan_trend['time_node'],
		format='%Y-%m-%d %H:%M:%S',
		errors='coerce')
	video_data = video_data.assign(
		createtime=video_data['createtime'].astype('datetime64[ns]'))
	fan_trend = fan_trend.assign(
		time_node=fan_trend['time_node'].astype('datetime64[ns]'))

	# 提取日期部分，便于按天关联
	video_data['date_only'] = dd.to_datetime(video_data['createtime']).dt.date
	fan_trend['date_only'] = dd.to_datetime(fan_trend['time_node']).dt.date
	video_data = video_data.assign(
		date_only=video_data['date_only'].astype('datetime64[ns]'))
	fan_trend = fan_trend.assign(
		date_only=fan_trend['date_only'].astype('datetime64[ns]'))

	# 主表与粉丝趋势表按作者和日期关联，补充粉丝数
	merged_dask1 = video_data.merge(
		fan_trend[['cid', 'date_only', 'follower_count']],
		left_on=['author_id', 'date_only'],
		right_on=['cid', 'date_only'],
		how='left'
	).drop(columns=['cid', 'date_only'])

	print(merged_dask1.head())

	# 删除中间表，释放内存
	del video_data
	del fan_trend


	# 1.4 输出最终结果为单一CSV文件，便于后续分析
	merged_dask1.to_csv(file_path + '\\without_sales.csv', index=False, single_file=True)

print('数据处理完成。')