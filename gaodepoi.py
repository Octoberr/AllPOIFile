#coding:utf-8

import requests
import socket
import json
import sys
import pymysql

CHROME = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.115 Safari/537.36'
HEADER = {'Connection': 'keep-alive', 'User-Agent': CHROME}
AK = 'cda5abf0cdd13100706277aaee3c71b6'
overlap_ratio = 0

# 定义了一个点
class Location:
	def __init__(self, lat, lng):
		self.lat = lat
		self.lng = lng

#全成都的范围
sh_lower_left = Location(lat = 30.602641, lng = 103.984227)
sh_upper_right = Location(lat = 30.721436, lng = 104.167379)
#测试点的范围
# sh_lower_left = Location(lat = 30.641456, lng = 104.041042)
# sh_upper_right = Location(lat = 30.654194, lng = 104.067349)


# 定义了地图上的一个矩形块
class Block:
	def __init__(self, left_lower, right_upper, width, height):
		self.left_lower = left_lower
		self.right_upper = right_upper
		self.width = width
		self.height = height

class BaiduPOICrawler:

	def _get_block_poi_result(self, block, poi_name, page):
		# ak = AK[random.randint(0, len(AK)-1)]
		# lat,lng(左下角坐标),lat,lng(右上角坐标)
		global AK
		url = 'http://restapi.amap.com/v3/place/polygon?key={}&polygon={},{}|{},{}&types={}&offset=20&page={}&extensions=all&output=json'.format(AK, block.left_lower.lng, block.left_lower.lat, block.right_upper.lng, block.right_upper.lat,poi_name, page)
		json_str = self.query_json(url)
		json_dict = json.loads(json_str)
		return json_dict

	def query_json(self, url):
		while True:
			try:
				r = requests.get(url, headers= HEADER).text
			except (socket.timeout, requests.exceptions.Timeout):  # socket.timeout
				print "timeout", url
			except requests.exceptions.ConnectionError:
				print "connection error", url
			else:
				try:
					json.loads(r)
				except ValueError:
					print "no json return, retry."
				except:
					print "unknown error, retry."
				else:
					break
		return r

	# def writeTheEmpty(self,res):
	# 	if res['pois']==[]:


	# def load_poi_type(self, filename):
	# 	self.poi_types = {}
	# 	with open(filename, 'r') as fin:
	# 		for line in fin:
	# 			data = line.strip().decode('utf-8').split('#')
	# 			first_calss_cat = data[0]
	# 			second_class_cat_list = data[1].split(',')
	# 			self.poi_types[first_calss_cat] = second_class_cat_list

	def get_block_scope(self, origin, width, height, row_idx, col_idx):
		# 返回值是一个Block对象！！！
		# 例如0,0也就是整个上海市最左下角的第一个格子的左下角和右上角坐标
		# origin: Location 当前区域内的原点
		# width: 当前区域内预计的格子宽度
		# height: 当前区域内预计的格子高度
		# row_idx, col_idx: 当前格子在当前区域的位置
		left_lower = Location(lat = origin.lat + row_idx*width, lng = origin.lng + col_idx*height)
		right_upper = Location(lat = left_lower.lat + (row_idx+1)*width, lng = left_lower.lng + (col_idx+1)*height)
		return Block(left_lower, right_upper, width, height)

	def block_is_proper(self, block, poi_name):
		# 查看当前分块的width和height, 对于某个poi, 请求下来的内容是不是小于900，小于900就是全的，=900就要继续缩小格子
		# block: Block
		result = self._get_block_poi_result(block, poi_name, 0)
		# 这里可能出现网络返回值问题
		if (result['status'] == 1) and (result['count'] < 500):
			return (True, result) # result['results']是一个list,每一个元素是一个dict
		else:
			return (False, [])

	def insert(self,sql):
		db = pymysql.connect("localhost", "root", "bwc123", "bw", charset="utf8")
		cur = db.cursor()
		try:
			cur.execute(sql)
			db.commit()
		except:
			fp = open('sqlerror.txt', 'w+')
			fp.write(sql.encode('utf-8') + '\n')
			print 'this data has a special string'
			fp.close()
			db.rollback()
		db.close()

	def write_res(self, a_list):
		# a_list是一页请求回来的，有20个数据
		for i in range(len(a_list)):
			try:
				name=a_list[i]['name'].encode('utf-8')
				typecode=a_list[i]['typecode'].encode('utf-8')
				address=a_list[i]['address'].encode('utf-8')
				location=a_list[i]['location']
				loc=location.split(',')
				lat=float(loc[1])
				lng=float(loc[0])
				pcode=a_list[i]['pcode'].encode('utf-8')
				citycode=a_list[i]['citycode'].encode('utf-8')
				adcode=a_list[i]['adcode'].encode('utf-8')
				sql="INSERT INTO gdsupplementpoi(name,typecode,address,amaplat,amaplng,pcode,citycode,adcode) VALUES ('%s','%s','%s','%lf','%lf','%s','%s','%s')" % (name,typecode,address,lat,lng,pcode,citycode,adcode)
				self.insert(sql)
				print "insert success", i
			except:
				print('Conversion character failed')
				continue
		# with open(filename, 'a+') as fout:
		# 	fout.write(json.dumps(a_list, ensure_ascii=False, separators=(',', ':')).encode('utf-8')+'\n')

	def block_is_good_to_write(self, res, block, poi_name):
		# fname = 'gaode.json'.format(poi_name)
#		print(res['pois'])
		self.write_res(a_list = res['pois'])
		page_sum = int(res['count'])/20
#		print("{}: success, total:{}条poi, page 0, page_sum is {}".format(poi_name, res['count'], page_sum))
		if page_sum == 0:
			return
		last_page = page_sum if (int(res['count'])%20 == 0) else (page_sum+1)
		for i in range(1, last_page):
			res_dict = self._get_block_poi_result(block, poi_name, i)
			if (res_dict['status'] == '1') and (int(res_dict['count']) != 0):
				self.write_res(res_dict['pois'])
			#print("{}: success, total:{}条poi, page {}".format(poi_name, res_dict['total'], i))
			elif res_dict['status'] != 1:
				self.write_error_log(block, poi_name, i)



	def split_block_to_half(self, block, poi_name):
		# 将block划分成更小的block，对半分
		print("poi:{}, lat:{}, lng:{}, current width:{}, current height:{}".format(poi_name, block.left_lower.lat, block.left_lower.lng, block.width,  block.height))
		for i in range(2):
			for j in range(2):
				# 生成一个subblock：Block类型
				current_block_width = block.width
				current_block_height = block.height
				sub_block = self.get_block_scope(block.left_lower, 0.5*current_block_width, 0.5*current_block_height, i, j)
				self.get_block_all_poi(sub_block, poi_name)

	def exit_prog(self):
		# 换ak
		print("日配额已经用完")
		sys.exit(0)

	def write_error_log(self, block, poi_name, page):
		import datetime, os
		logfile = datetime.datetime.now().strftime("%Y%m%d") + '.log'
		if os.path.isfile(logfile):
			f = open(logfile, 'a+')
		else:
			f = open(logfile, 'w')
		f.write('left_lower_lat:{}, left_lower_lng:{}, right_upper_lat:{}, \
			right_upper_lng:{}, poiType:{}, page:{}, fail\n'.format(block.left_lower.lat,
			block.left_lower.lng, block.right_upper.lat, block.right_upper.lng, poi_name, page))
		f.close()


	def get_block_all_poi(self, block, poi_name):
		# 完整的请求一个block中某一poi的全部信息
		# status, res = self.block_is_proper(block, poi_name)

		# 先请求第一页,根据第一页的结果指定相应的对策
		result = self._get_block_poi_result(block, poi_name, 0)
		if (int(result['status']) == 1) and (result['info'] == 'OK') and (int(result['count']) == 0):
			return
		elif (int(result['status']) == 1) and (int(result['count']) != 0) and (int(result['count']) < 500):
			self.block_is_good_to_write(result, block, poi_name)
		elif (int(result['status']) == 1) and (int(result['count']) > 500):
			self.split_block_to_half(block, poi_name)
		elif int(result['status']) == 10010:
			self.exit_prog()
		else: # 其他意外写入log文件
			self.write_error_log(block, poi_name, 0)



	def get_pois(self):
		# l = [u'交叉口',u'临街院门',u'公交站台',u'建筑物门']
		# l = [u'0703',u'0704',u'0706',u'0709',u'0710',u'09',u'10',u'150105',u'150202',u'150203',u'1504',u'1505',u'1506',u'1507',u'1508',u'150907',u'150908',u'150909',u'1511',u'1601',u'1602',u'1604',u'1605',u'180103',u'190302',u'190600']
		l=[u'0101',u'02',u'060101',u'060102',u'060103',u'060302',u'0604',u'0706',u'0709',u'0710',u'1601',u'160400']
		return l

	def start(self):
		global sh_lower_left, sh_upper_right
		#Pi = 3.1415926
		latScope = sh_upper_right.lat - sh_lower_left.lat
		lngScope = sh_upper_right.lng - sh_lower_left.lng
		block = self.get_block_scope(sh_lower_left, latScope, lngScope, 0, 0)
		print("lat:{}, lng:{}, current width:{}, current height:{}, too big and cut half".format(block.left_lower.lat, block.left_lower.lng ,block.width, block.height))
		for poi in self.get_pois():
			self.get_block_all_poi(block, poi.encode('utf-8'))
def main():
	crawler = BaiduPOICrawler()
	# crawler.load_poi_type(filename = 'poi_type.txt')
	crawler.start()

if __name__ == '__main__':
	main()
