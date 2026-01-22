import pandas as pd
import os
import warnings
from user_input.parallel_get import main as update_pipedrive_data

warnings.simplefilter(action='ignore', category=pd.errors.SettingWithCopyWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)

def assign_deal_id(df: pd.DataFrame, phone_to_deal_dict: dict, file_name: str) -> pd.DataFrame:
    mask = df['Deal ID'].isna()
    df.loc[mask, 'Deal ID'] = df.loc[mask, 'phone_number'].map(phone_to_deal_dict)
    df.loc[mask & df['phone_number'].isin(phone_to_deal_dict), 'Resolved By'] = 'Joyce Marie Gempesaw'
    df.drop(columns=['phone_number'], axis=1, inplace=True)
    df.to_excel(f'output/new_deals_deal_id/(Lookup Output) {file_name}', index=False)

def format_pipedrive_data(pipedrive_df: pd.DataFrame) -> dict:
    print("Formatting Pipedrive data")

    pipedrive_df['phone_number'] = pipedrive_df['phone_number'].fillna('').astype(str)
    pipedrive_df = pipedrive_df[pipedrive_df['phone_number'].str.strip() != '']
    pipedrive_df['phone_number'] = pipedrive_df['phone_number'].str.split(',')
    pipedrive_df['phone_number'] = pipedrive_df['phone_number'].apply(
        lambda x: sorted(set(x), key=x.index)
    )

    pipedrive_final_data = pipedrive_df.explode('phone_number').reset_index(drop=True)
    pipedrive_final_data['phone_number'] = pipedrive_final_data['phone_number'].str.replace(r'\D', '', regex=True)
    pipedrive_final_data = pipedrive_final_data[pipedrive_final_data['phone_number'] != '']

    grouped_df = (
        pipedrive_final_data.groupby('phone_number')['Deal - ID']
        .agg(lambda row: " | ".join(row.astype(str).unique()))
        .reset_index()
    )
    phone_to_deal = grouped_df.set_index('phone_number')['Deal - ID'].to_dict()
    return phone_to_deal

def read_pipedrive(path):

    pipedrive_file = os.listdir(path)
    pipedrive_df = pd.read_csv(os.path.join(path, pipedrive_file[0]), low_memory=False)
    
    return pipedrive_df

def read_rc_data(path, file):

    if file.endswith('.csv'):
        df = pd.read_csv(os.path.join(path, file), low_memory=False)
        return df
    elif file.endswith('.xlsx'):
        df = pd.read_excel(os.path.join(path, file))
        return df
    else:
        return None
    
def main():

    update_pipedrive_data()

    warnings.filterwarnings("ignore", category=FutureWarning)


    abandoned_calls_path = 'data/abandoned_calls'
    abandoned_calls_files = os.listdir(abandoned_calls_path)

    # If empty RC Files folder
    if len(abandoned_calls_files) == 0:
        return 'rc_empty_grab'

    # Iterate through RC Input Files
    for file in abandoned_calls_files:
        pipedrive_df = read_pipedrive('data/pipedrive')
        deal_id_dict = format_pipedrive_data(pipedrive_df)
        rc_df = read_rc_data(abandoned_calls_path, file)
        print("Looking up Deal IDs")
        rc_df.loc[:, 'phone_number'] = rc_df['ANI'].astype(str)
        mask_phone = rc_df['phone_number'].astype(str).str.len() == 11
        rc_df.loc[mask_phone, 'phone_number'] = rc_df.loc[mask_phone, 'phone_number'].astype(str).str[1:].str.strip()
        if not rc_df.empty:
            assign_deal_id(rc_df, deal_id_dict, file)

    print("Process Complete")

if __name__ == "__main__":
    main()
