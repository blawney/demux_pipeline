import pandas as pd
import glob
import re
import os

class ParseException(Exception):
	pass

sections = ['Per base sequence quality',
'Per sequence quality scores',
'Per base sequence content',
'Per base GC content',
'Per sequence GC content',
'Sequence Length Distribution',
'Overrepresented sequences']

basic_stats_section = 'Basic Statistics'
def parse(project_dir):
	all_files = glob.glob('%s/*/*/fastqc_data.txt' % project_dir)
	df = pd.DataFrame()

	for f in all_files:
		fq_contents = open(f).read()

		pattern = '>>%s.*?>>END_MODULE' % basic_stats_section
		m = re.search(pattern, fq_contents, re.DOTALL)
		if m:
			text = m.group()
			d = dict([tuple(x.strip().split('\t')) for x in text.split('\n')[2:-1]])
		else:
			raise ParseException('Expected section not found')

		sample = d['Filename']

		d2 = {}
		for s in sections:
			pattern = '>>%s.*?>>END_MODULE' % s
			m = re.search(pattern, fq_contents, re.DOTALL)
			if m:
				text = m.group()
				status = text.split('\n')[0].split('\t')[1]
				d2[s] = status
			else:
				raise ParseException('Expected section not found')
		d.update(d2)
		pds = pd.Series(d, name=sample)
		df = pd.concat([df,pds], axis=1)

	df.T.to_csv(os.path.join(project_dir, 'fastqc_output.tsv'), sep='\t', index_label='File')

