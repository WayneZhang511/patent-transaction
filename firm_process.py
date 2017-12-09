import pandas as pd
import argparse
import json
import os
import numpy as np
import time


from rosette.api import API, NameDeduplicationParameters, RosetteException

def run(data, key="b68ed921660dca2fbb79a4a481e931ff", alt_url='https://api.rosette.com/rest/v1/'):
    """ Run the example """
    # Create an API instance
    api = API(user_key=key, service_url=alt_url)

    #name_dedupe_data = "AMERICAN HEYER-SCHULTE CORPORATION,AMERICAN HEYER- SCHULTE CORPORATION,AMERICAN HOECHST CORPORATION,Betty Gable,Norma Shearer,Norm Shearer,Brigitte Helm,Bridget Helem,Judy Holliday,Julie Halliday"
    #name_dedupe_data = data
    threshold = 0.8
    params = NameDeduplicationParameters()
    params["names"] = data
    params["threshold"] = threshold
    try:
        return api.name_deduplication(params)
    except RosetteException as exception:
        print(exception)

def group_firm(transaction):
	print("Read csv.")
	result_name = 'grouped-transaction/' + transaction.split('/')[1].split('.')[0] + '-grouped.csv'
	transaction = pd.read_csv(transaction, sep = '|', dtype=object)
	#transaction = pd.read_csv("test_group_firm.csv", sep = '|', dtype=object)

	transaction.sort_values(["Firm"], ascending=[True], inplace = True)
	print("Group by names.")
	transaction_firm = transaction.groupby("Firm")
	firm_list = list(transaction_firm.groups.keys())

	a = pd.Series(data=firm_list)
	# print("Split by ','.")
	# b = a.str.split(",",expand=True)[0]

	# print("Split by 'A CORP'.")
	# c =b.str.partition("A CORP")[0]

	# print("Split by 'CORPORATION'.")
	# d = c.str.partition("CORPORATION")
	# d = d[0]+d[1]

	# print("Split by '  '.")
	# e = d.str.split("  ",expand=True)[0]
	e = a.str.strip()
	f = e.groupby(e)
	f_names = list(f.groups.keys())
	f_counts = f.count().values

	print("Cluster names.")
	total_groups = len(f_names)
	print("total groups: %d" % total_groups)
	back1_sets=[]

	i = 0
	while i < total_groups:
		if (i+2000) > total_groups:
			print("Cluster names between {:d} to {:d}".format(i, total_groups))
			names=f_names[i:total_groups]
			counts = f_counts[i:total_groups]
		else:
			print("Cluster names between {:d} to {:d}".format(i, i+2000))
			names=f_names[i:i+2000]
			counts = f_counts[i:i+2000]
		i += 2000

		start_time = time.time()
		RESULT = run(names)
		labels = RESULT['results']
		print("--- %s seconds ---" % (time.time() - start_time))

		firm_table = pd.DataFrame(data = {'name':names, 'label':labels, 'count':counts}, dtype=np.int64)
		firm_table.sort_values(["label","count"], ascending=[True, False], inplace = True)
		firm_table.drop(columns=["count"],inplace=True)
		max_firm_table = firm_table.drop_duplicates(subset='label')

		back1_sub = firm_table.merge(max_firm_table, left_on="label",right_on="label",how="left")
		back1_sub.drop(columns=["label"],inplace=True)
		back1_sets.append(back1_sub)

	print("Back 1.")
	back1 = pd.concat(back1_sets, ignore_index=True)
	back1 = back1.drop_duplicates(subset='name_x')

	print("Back 2.")
	e1 = pd.DataFrame(data = {'ori_name':e})
	back2 = e1.merge(back1, left_on="ori_name",right_on="name_x",how="left")
	back2["ori_name"] = a
	back2.drop(columns=["name_x"],inplace=True)
	back2.drop_duplicates(inplace=True)

	print("Back 3.")
	back3 = transaction.merge(back2,left_on="Firm",right_on="ori_name",how="left")
	back3["Firm"] = back3["name_y"]
	back3.drop(columns=["ori_name", "name_y"],inplace=True)

	back3.sort_values(["Firm", 'Transaction_Date', 'Patent_Number', 'Transaction'], ascending=[True, True, True, False], inplace = True)
	print("Write to %s." % result_name)
	back3.to_csv(result_name, sep='|', index = False)
