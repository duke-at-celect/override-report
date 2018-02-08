import pandas as pd
import numpy as np
import argparse


def read_data():
	rawData = file
	df = pd.read_csv(rawData)

	# Create and fill new type columns to track store type
	df['celect_fulfill_store_type'] = 'store'
	df['oms_fulfill_store_type'] = 'store'

	# Compare celectFulfilledStore to OMSFulfilledStore - if values don't match Celect has been overridden.
	or_df = df[df.celectFulfilledStore != df.OMSFulfilledStore].sort_values(['orderCreateDate', 'celectFulfilledStore'])
	or_df.dropna(subset=['OMSFulfilledStore'], inplace=True)

	return or_df


def truncate (or_df):
	# Assign store_type
	# print(x.head(2))  # debug
	if region == 'us':
		# set DCs and MHs to united states values
		dc = [9920, 9923]
		mh = [2283]
	else:
		# set DCs and MHs to Canadian values
		dc = [9910, 9911]
		mh = [1488]

	or_df.loc[or_df['celectFulfilledStore'].isin(dc), 'celect_fulfill_store_type'] = 'distribution center ({})'.format(region)
	or_df.loc[or_df['celectFulfilledStore'].isin(mh), 'celect_fulfill_store_type'] = 'mini-hub ({})'.format(region)

	new_or_df = or_df[['brandName', 'orderId', 'orderCreateDate', 'orderCreateDateTime', 'sku', 'inventoryVal',
	                   'celectFulfilledStore', 'celect_fulfill_store_type', 'OMSFulfilledStore',
	                   'oms_fulfill_store_type']]

	# new_or_df.drop_duplicates(subset=['orderId', 'sku'], keep='last')
	# print(new_or_df.head(5))

	new_or_df.to_csv('~/Downloads/aldo/prod/override_reports/{}_truncated_override.csv'.format(region),  sep=',')


parser = argparse.ArgumentParser(description = "Use only the 'complete_store_reponse_w_details..' override report file with this script." )
parser.add_argument('--region',  choices=['ca', 'us'], help="Must be either 'ca' for Canada or 'us' for United States")
parser.add_argument("--file",  help="Used to pass the file to process - include path if necessary")
args = parser.parse_args()
region = args.region
file = args.file

if file.endswith('.csv'):
	print("\nProcessing file...")
else:
	print("\nIncorrect file format. Exiting.")
	exit()

if region == 'us':
	or_df=read_data()
	brand = or_df.iloc[0] [ 'brandName']
	if brand != 'aldo_us':
		print('ERROR: Data in file is for brand:', brand, 'and --region was set to:', region)
		exit()
	else:
		truncate(or_df)

if region == 'ca':
	or_df=read_data()
	brand = or_df.iloc[0] [ 'brandName']
	if brand != 'aldo_ca':
		print('ERROR: Data in file is for region: Unites States but the --region flag was set to: {}\n'.format(region))
		exit()
	else:
		truncate(or_df)
