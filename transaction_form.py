import pandas as pd

helpers = pd.read_csv("USPTO.csv", sep = '|', dtype=object)

result = pd.DataFrame(columns=['Firm','Transaction_Year','Transaction','Patent_number'])
counter = 0
PublicationID = ""
assignor_assignment_exe_date = ""
assignment_rec_date = ""
assignment_id = ""#11
row_iterator = helpers.itertuples()
print ("Establishing: Table of results.")
for row in row_iterator:
	# a new patent
	if row[1] != PublicationID:
		PublicationID = row[1]
		assignment_id = row[11]
		result.loc[counter] = [row[10], row[4], "CREATE", PublicationID]
		counter += 1
	elif row[11] == assignment_id:
		result.loc[counter] = [row[10], row[4], "CREATE", PublicationID]
		counter += 1
	# other assignments
	else:
		result.loc[counter] = [row[12], row[4], "SELL", PublicationID]
		counter += 1
		result.loc[counter] = [row[10], row[4], "BUY", PublicationID]
		counter += 1

result = result.drop_duplicates()

print ("Finished: Table of Result.")
# slice year and sort by name of firm
result.Transaction_Year = result.Transaction_Year.str[:4]
result.sort_values(["Firm","Transaction_Year","Patent_number"], ascending=[True, True, True], inplace = True)
result = result.reset_index(drop=True)
#result.sort_values(["PublicationID"], ascending=[True], inplace=True)
result.to_csv("result_transaction.csv", sep='|', index = False)
print ("Write to 'result.csv': Table of Result.")