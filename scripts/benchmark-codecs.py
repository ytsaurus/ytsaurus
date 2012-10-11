import yt.wrapper as yt

import datetime as dt
import sys

table_name = '//home/redwood/reqans_log/"20121008"'
size = 1 * 2**20
output_dir = '//home/panin/codec_test'

read_size = 0
rows = []
for row in yt.read_table(table_name, yt.YsonFormat()):
	read_size += len(row)
	rows.append(row)
	if read_size > size: break

print 'extracted ', len(rows), ' rows'

codecs = ["none", "gzip_normal", "snappy", "gzip_best_compression", "lz4", "lz4_high_compression", "quick_lz"]

def count_speed(size, t):
	return 1. * size / t  / 1024 / 1024

def pretty_speed(speed):
	return str(round(speed, 4)) + ' Mb/s'

res = {}
yt.set_attribute(output_dir, '_result', {})


for codec in codecs:
	print 'Testing ', codec + '...'

	output =  output_dir + '/' + codec
	start = dt.datetime.now()
	yt.write_table(output, rows, yt.YsonFormat(), {"codec_id": codec})
	finish = dt.datetime.now()
	write_time = (finish - start).total_seconds()

	actual_size = yt.get(output + '/@uncompressed_data_size')
	print '  actual_size = ', actual_size
	print '  write_time = ', write_time

	write_speed = count_speed(actual_size, write_time)

	value = yt.get(output + '/@compression_ratio')
	ratio = round(100 * value, 2)

	start = dt.datetime.now()
	total_size = 0
	for tmp in yt.read_table(output, yt.YsonFormat()):
		total_size += len(tmp)

	print '  total read size = ', total_size
	finish = dt.datetime.now()
	read_time = (finish - start).total_seconds()
	read_speed = count_speed(actual_size, read_time)
	print '  read_time = ', read_time

	local_result =  {}
	local_result['ratio'] = str(ratio) + '%'
	local_result['read_speed'] = pretty_speed(read_speed)
	local_result['write_speed'] = pretty_speed(write_speed)
	print '  ', local_result

	res[codec] = local_result
	yt.set_attribute(output_dir, '_result/' + codec, local_result)

