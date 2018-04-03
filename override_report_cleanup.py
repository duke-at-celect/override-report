import pandas as pd
import glob


def path():
    files_path = input("\nFull path to the override files (.csv) that need to be merged: ")

    return files_path


def merge_csv(files_path):
    all_files = glob.glob(files_path + '*.csv')
    list_ = []
    for file_ in all_files:
        df = pd.read_csv(file_, index_col=None, header=0)
        list_.append(df)
    merged_data = pd.concat(list_)
    merged_data.drop_duplicates(inplace=True)
    brand = merged_data.iloc[0]['brandName']
    output = files_path + '{}_cat_result.csv'.format(brand)
    merged_data = merged_data[['brandName', 'orderId', 'orderCreateDate',
                           'orderCreateDateTime', 'orderPostDate', 'orderPostTimeStamp', 'orderShipmentId',
                           'shipmentShippingCost', 'sku', 'zipcode', 'celectFulfilledStore', 'shipToStoreFlag',
                           'shipToStoreId', 'orderedQuantity', 'inventoryVal', 'quantitySentStore',
                           'pickDecline', 'weeksSupply', 'isOnesie', 'storeResponseShipmentId',
                           'OMSFulfilledStore', 'respondDateTime', 'responseTimeMins', 'quantityDeclined',
                           'quantityShipped']]
    # Check if the file already exists? Prompt the user if it does?
    merged_data.to_csv(output)
    return merged_data


def read_data(merged_data):
    # Pass 'merged_data' to this function from merge_csv()
    df = merged_data
    # Create and fill new type columns to track store type
    df['celect_fulfill_store_type'] = 'store'
    df['oms_fulfill_store_type'] = 'store'
    # Compare celectFulfilledStore to OMSFulfilledStore - if values don't match Celect has been overridden.
    all_data = df[df.celectFulfilledStore != df.OMSFulfilledStore].sort_values(['orderCreateDate', 'celectFulfilledStore'])
    all_data.dropna(subset=['OMSFulfilledStore'], inplace=True)
    return all_data


def truncate(all_data):
    # Pass read_data() output to this function
    # path = or_files
    brand = all_data.iloc[0]['brandName']
    if brand == 'aldo_us':
        # set DCs and MHs to United States values
        dc = [9920, 9923]
        mh = [2283]
    else:
        # set DCs and MHs to Canadian values
        dc = [9910, 9911]
        mh = [1488]

    all_data.loc[all_data['celectFulfilledStore'].isin(dc),
              'celect_fulfill_store_type'] = 'distribution center ({})'.format(brand)
    all_data.loc[all_data['celectFulfilledStore'].isin(mh),
              'celect_fulfill_store_type'] = 'mini-hub ({})'.format(brand)
    new_all_data = all_data[['brandName', 'orderId', 'orderCreateDate', 'orderCreateDateTime', 'sku', 'inventoryVal',
                       'celectFulfilledStore', 'celect_fulfill_store_type', 'OMSFulfilledStore',
                       'oms_fulfill_store_type']]
    new_all_data.to_csv(files_path + '{}_truncated_override.csv'.format(brand), sep=',')


files_path = path()
print('Starting Merge...')
merged_data = merge_csv(files_path)
print('Reading Merged Data...')
all_data = read_data(merged_data)
print('Truncating Data...')
truncate(all_data)
print('Processing Complete.\n')
