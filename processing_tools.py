import pandas as pd
import string
import os
from fuzzywuzzy import fuzz
import glob

def clean_name(filename, col_names=[]):
	"""
	clean the contents of the col_names in filename.
	Input:
	filename(string): target file.
	col_names(a list): the name of columnes needed to be cleaned.
	"""

	punctuation_table = str.maketrans(dict.fromkeys(string.punctuation))
	suffix = set()
	suffix_file = open('campany_suffix.txt','r')
	for line in suffix_file:
		suffix.add(line.strip())
	suffix_file.close()

	print("Loading...")
	file = pd.read_csv(filename, sep = '|', dtype=object)

	for col_name in col_names:
		print("Cleaning %s..." % col_name)
		print("Cutting...")
		file[col_name] = file[col_name].str.split(",",expand=True)[0]
		# file.iloc[0][col_name] = tuple(str(file.iloc[0][col_name]))
		# file.iloc[0][col_name] = tuple(str(file.iloc[1][col_name]))
		file[col_name] = file[col_name].str.partition("A CORP")[0]
		file[col_name] = file[col_name].str.partition("CORPORATION")[0]
		file[col_name] = file[col_name].str.partition("LIMITED")[0]

		print("Removing puctuation...")
		file[col_name] = file[col_name].str.translate(punctuation_table)

		for word in suffix:
			print("Removing %s..." % word)
			file[col_name] = file[col_name].str.replace(r'\b%s\b' % word, ' ')

		print("Removing redundant spaces...")
		file[col_name] = file[col_name].str.strip()
		file[col_name] = file[col_name].str.replace(r'  +', ' ')

	print("Saving file...")
	save_name = filename.split('/')[1].split('.')[0] + '-cleaned.csv'
	save_dir = filename.split('/')[0] + '-cleaned'
	if not os.path.exists(save_dir):
		os.makedirs(save_dir)
	file.to_csv('/'.join([save_dir, save_name]), sep='|', index = False)
	return file

def split_file(filename, col_name):
	"""
	Split the target file with the first letter of contents in the given column.
	Input:
	filename(string): target file.
	col_name(string): name of given column.
	"""

	print("Loading...")
	file = pd.read_csv(filename, sep = '|', dtype=object)
	file['start_with'] = file[col_name].str[0]

	print("Grouping...")
	start_key = file.groupby('start_with').groups.keys()

	save_name = filename.split('.')[0]
	for key in start_key:
		print("Saving file %s..." % key)
		tmp = file[file['start_with'] == key]
		tmp.drop(columns=['start_with'],inplace = True)
		tmp.to_csv("%s/%s-%s-%d.csv" % (save_name, save_name, key, len(tmp)), sep='|', index = False)

def find_inventor(patent, assignments, helper, col_assignid='assignment_id'):
	"""
	Return:

	Assignment_list: a list of dictionary, 
	in each dictionary, there three pairs: PublicationID, assignment id and STATUS

	result_status:
	0: no helper
	1: not match
	2: match
	"""

	# assignment_id = original_assignments.iloc[0][col_assignid]
	# first_assignee = original_assignments[original_assignments[col_assignid] == assignment_id]
	# first_assignee['assignee_name'] = first_assignee['assignor_name']
	# first_assignee.drop(columns=['assignor_name'], inplace = True)
	# first_assignee.drop_duplicates(subset='assignee_name')

	# patent_assignments = pd.concat([first_assignee, original_assignments], ignore_index=True)
	#row_ = helpers.itertuples()

	assignments_list = [{'PublicationID':patent, 'assignment_id': assignments.iloc[0][col_assignid], 'STATUS': 'CREATE'}]
	if len(helper) == 0:
		#print("Use default...")
		return assignments_list, 0

	# TODO:
	# 1. use all the helpers bu for loop
	# 2. record the details for not match cases
	# 3. use table manipulation
	rows = assignments.iterrows()
	helper_rows = helper.iterrows()
	counter = 0
	# target_name = helper.iloc[0]['standard_name']

	for _, helper_row in helper_rows:
		target_name = str(helper_row['standard_name'])
		assignments_dic = {}
		for counter, row in rows:
			match_ratio = fuzz.ratio(str(row['assignee_name']), str(target_name))
			patent_assignment_id = str(row[col_assignid])
			if match_ratio > 0.7:
				# patent_assignments = patent_assignments.iloc[counter:]
				#print("Found!")
				assignments_dic[patent_assignment_id] = "CREATE"
				assignments_list =[{'PublicationID':patent, 'assignment_id':key,'STATUS':assignments_dic[key]} for key in assignments_dic]
				return assignments_list, 2
			else:
				assignments_dic[patent_assignment_id] = "DROP"

	#print("Not match, Use default...")
	return assignments_list, 1


def uspto_with_inventor(uspto, helpers):
	"""
	Find and mark inventors in the uspto.
	Input:
	uspto(string or pd.DataFrame)
	helpers(string or pd.DataFrame)
	"""
	log_static_name = 'log-statics/' + uspto.split('/')[1].split('.')[0] + '-statics.txt'
	log_name = 'log-marks/' + uspto.split('/')[1].split('.')[0] + '-marks.txt'
	result_name = 'uspto-with-inventor/' + uspto.split('/')[1].split('.')[0] + '-inventor.csv'
	log_static = open(log_static_name, 'a')
	print("Loading upsto...")
	if type(uspto) == str:
		log_static.write(uspto)
		uspto = pd.read_csv(uspto, sep = '|', dtype=object)
	print("Loading helpers...")
	if type(helpers) == str:
		helpers = pd.read_csv(helpers, sep = '|', dtype=object)
	print("Grouping upsto...")
	patents = list(uspto.groupby('PublicationID').groups.keys())
	patent_inventors = []

	counter = 0
	result_counter = {'No helper':0, 'Not match':0, 'Match':0}

	
	log = open(log_name, 'a')
	for patent in patents:
		original_assignments = uspto[uspto['PublicationID'] == patent]
		helper = helpers[helpers['patent'] == patent]
		print("Finding inventor for %s..." % patent)
		log.write("Finding inventor for %s...\n" % patent)
		patent_inventor, result = find_inventor(patent, original_assignments, helper)
		if result == 0:
			result_counter['No helper'] += 1
			log.writelines('No helper\n')
		elif result == 1:
			result_counter['Not match'] += 1
			log.write('Not match\n')
		else:
			result_counter['Match'] += 1
			log.write('Match\n')

		for patent in patent_inventor:
				for key in patent:
					log.write('%s : %s,\n' % (key, patent[key]))

		patent_inventors += patent_inventor
		# counter += 1
		# if counter == 1000:
		# 	break
	log.close()

	print("Find inventors result:")
	log_static.write("Find inventors result:\n")
	for key in result_counter:
		print('%s : %d' % (key, result_counter[key]))
		log_static.write('%s : %d\n' % (key, result_counter[key]))

	log_static.close()

	print("Marking inventors in uspto...")
	mark = pd.DataFrame(patent_inventors)
	mark.drop_duplicates(inplace=True)
	mark.to_csv("tmp-inventor-helper.csv", sep='|', index = False)

	result = uspto.merge(mark, 
						 on=['PublicationID', "assignment_id"],
						 how="left", 
						 validate='many_to_one')
	result.drop(result[result['STATUS'] == 'DROP'].index, inplace=True)

	print("Saving file...")

	if type(uspto) == str:
		save_name = uspto.split('.')[0]
	else:
		save_name = 'uspto'
	result.to_csv(result_name, sep='|', index = False)
	return result


def uspto_to_transaction(uspto):
	"""
	Transform uspto formed file to transaction file.
	"""

	if type(uspto) == str:
		uspto = pd.read_csv(usoto, sep = '|', dtype=object)

	uspto['Patent_Number'] = uspto['PublicationID']
	uspto['Transaction_Date'] = uspto['assignor_assignment_exe_date']
	uspto['SELL'] = uspto['assignor_name']
	uspto['BUY'] = uspto['assignee_name']
	uspto.drop(columns=['assignee_name', 'assignor_name', 'assignor_assignment_exe_date', 'PublicationID'], inplace = True)
	transaction = uspto.melt(id_vars=['Patent_Number', 'Transaction_Date', 'colFromIndex','assignment_id', 'STATUS'], var_name='Transaction', value_name='Firm')
	
	# Drop the assignor before the inventor
	transaction.drop(transaction[(transaction.Transaction == 'SELL') & (transaction.STATUS == 'CREATE')].index, inplace = True)
	transaction.loc[transaction.STATUS == 'CREATE','Transaction'] = 'CREATE'
	transaction.drop(columns=['STATUS'],inplace = True)

	# Sort the table by patent number
	transaction.sort_values(['Patent_Number', 'Transaction_Date', 'assignment_id', 'Transaction', 'colFromIndex'], ascending = [True, True, True, False, True], inplace = True)

	transaction.drop(columns=['colFromIndex'],inplace = True)
	transaction.drop_duplicates(inplace=True)
	transaction.drop(columns=['assignment_id'], inplace=True)

	transaction = transaction[['Firm', 'Transaction_Date','Transaction','Patent_Number']]

	transaction.to_csv("transaction_for_test.csv", sep='|', index = False)
	return transaction

def get_paired_files(uspto_dir, helper_dir):
	"""
	Get a paired files of uspto and helper.
	Input:
	uspto_dir
	uspto_prefix
	helper_dir
	helper_prefix
	"""
	
	uspto_files = glob.glob('/'.join([uspto_dir,'*']))
	helper_files = glob.glob('/'.join([helper_dir,'*']))

	# TODO:
	# there is no corresponding file existing
	paired_files = []
	for uspto_file in uspto_files:
		uspto_file_no = uspto_file.split('/')[1].split('-')[1]
		for helper_file in helper_files:
			helper_file_no = helper_file.split('/')[1].split('-')[1]
			if uspto_file_no == helper_file_no:
				paired_files.append([uspto_file, helper_file])
				break

	return paired_files


def output_dir(output_folder, output_prefix):
	return 0

def main():
	# uspto = clean_name('new_USPTO/new_USPTO-3-149774.csv', ['assignee_name','assignor_name'])
	# helpers = clean_name('helper/helper-3-70367.csv', ['standard_name'])
	# uspto_with_inventor('new_USPTO/new_USPTO-3-149774-cleaned.csv', 'helper/helper-3-70367-cleaned.csv')
	uspto = pd.read_csv('../new_USPTO-3-149774-cleaned.csv', sep = '|', dtype=object)
	helpers = pd.read_csv('../helper-3-70367-cleaned.csv', sep = '|', dtype=object)
	result = uspto_with_inventor(uspto, helpers)
	transaction = uspto_to_transaction(result)

if __name__ == '__main__':
	main()
	# uspto = pd.read_csv('new_USPTO/new_USPTO-3-149774-cleaned.csv', sep = '|', dtype=object)
	# uspto = uspto[3000:5000]
	# helpers = pd.read_csv('helper/helper-3-70367-cleaned.csv', sep = '|', dtype=object)
	# result = uspto_with_inventor(uspto, helpers)
	