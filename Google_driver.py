import os.path
from os import listdir
import io
import logging
import pandas as pd
from google.cloud import storage
from apiclient.http import MediaIoBaseDownload
from googleapiclient.discovery import build
from google.oauth2 import service_account
import psycopg2 as pg
from settings import *
from sql import *
import time
import re

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M')
                    # handlers=[logging.FileHandler('my.log', 'w', 'utf-8'), ])

def Download_Finish_File(BUCKET):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(BUCKET)
    blob = bucket.blob(file_name)
    blob.download_to_filename(os.path.join(file_path, file_name))
    blob = bucket.blob(SERVICE_ACCOUNT_FILE)
    blob.download_to_filename(os.path.join(file_path, SERVICE_ACCOUNT_FILE))
    with open(os.path.join(file_path, file_name), 'r', encoding='utf-8') as f:
        finish_file = f.read()
        f.close()
    return  finish_file

def main(file_path,finish_file):
    SCOPES = ['https://www.googleapis.com/auth/drive']
    SERVICE_ACCOUNT = os.path.join(SERVICE_ACCOUNT_PATH, SERVICE_ACCOUNT_FILE)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT
    credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT, scopes=SCOPES)

    service = build('drive', 'v3', credentials=credentials)
    param = {}
    results = service.files().list(**param).execute()
    items = results.get('files', [])
    file_attr_list = []
    if not items:
        logging.info('No files found.')
    else:
        logging.info('Files:')
        for item in items:
            if item['mimeType'] not in 'application/vnd.google-apps.folder':
                logging.info(u'{0} ({1})'.format(item['name'], item['id']))
                file_attr_list.append(tuple([item['id'], 'text/csv', item['name'], service, file_path]))

    download_file(file_attr_list,finish_file)

def download_file(file_attr_list,finish_file):
    logging.info(file_attr_list)
    pattern = re.compile(r'[\u4e00-\u9fa5]')
    for file_attr in file_attr_list:
        if re.search('[a-zA-Z]', file_attr[2]):
            if re.sub(pattern,"",file_attr[2].split('.')[0]) in finish_file:
                logging.info('{} : already finished'.format(file_attr[2]))
                print(file_attr[2],': already finished')
                continue
        try:
            request = file_attr[3].files().get_media(fileId=file_attr[0])
            fp = os.path.join(file_attr[4], file_attr[2])
            fh = io.FileIO(fp, 'wb')
            downloader = MediaIoBaseDownload(fh, request)
            status, done = downloader.next_chunk()
            logging.info("FileName:%s Download %d%%." % (file_attr[2], int(status.progress() * 100)))
        except Exception as Ex:
            print(Ex)
            logging.ERROR(Ex)
            continue

def Log_File(finish_file_name):
    logging.info('Writing Files with Process Finished')
    with open(os.path.join(file_path, file_name), 'a', encoding='utf-8') as f:
        for write in finish_file_name:
            f.write('\n' + write)
        f.close()

def Upload_file_to_Cloud_Storage(file_path, bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob('final_doc_list.txt')
    blob.upload_from_filename(file_path + '/final_doc_list.txt', content_type='text/plain')

def ETL_CSV(file_path):
    logging.info('Running the ETL Process...')
    insert_data = []
    finish_file_name = []
    selected_col = ['您的姓名','性別','連絡電話','電子郵件','出發日期','旅遊地點','業務人員','餐食安排','飯店安排','交通安排','行程安排','出團領隊','出團導遊','是否有收到行前說明資料','下次旅遊會優先選擇旅遊的原因為何','若您尚非會員，請填寫手機號碼(或「同上」連絡電話)，以利我們邀請您加入會員','本意見函資料，僅提供旅遊於意見收集及行銷方案使用','歡迎提供您的寶貴意見讓我們知道','填答時間','填答秒數','IP紀錄','額滿結束註記','使用者紀錄','會員時間','會員編號','自訂ID','備註']
    select_settour_cause_col = ['業務人員','餐食安排','飯店安排','交通安排','行程安排','出團領隊','出團導遊']
    Pending_File_List = listdir(file_path)
    absPending_File_List = [os.path.join(file_path, filename) for filename in Pending_File_List if 'csv' in filename]
    if absPending_File_List:
        for absPending_File in absPending_File_List:
            df = pd.read_csv(absPending_File, names = selected_col, header=0).fillna('')
            settour_cause = []
            for index, i in enumerate(df[['您的姓名', '下次旅遊會優先選擇旅遊的原因為何']].iloc[0:, 1]):
                dict_ = dict({"您的姓名": df['您的姓名'][index]})
                dict_.update({'item_list': i.split('\n')})
                settour_cause.append(dict_)
            settour_cause_df = pd.DataFrame(settour_cause)
            settour_cause_df = settour_cause_df.explode('item_list').reset_index(drop=True)
            settour_cause_df = settour_cause_df[settour_cause_df['item_list'] != '']
            settour_cause_score_df = pd.get_dummies(settour_cause_df, columns=['item_list']).groupby(['您的姓名'], as_index=False).sum()
            item_header = ['餐食', '住宿', '交通', '業務人員', '領隊', '公司品牌', '門市便利性', '行銷活動', '保證成團', '導遊']
            for i in item_header:
                if not re.search(i, str(settour_cause_score_df.columns.to_list())):
                    print(i)
                    settour_cause_score_df[i] = 0
                else:
                    settour_cause_score_df = settour_cause_score_df.rename(columns={"item_list_{}".format(i): i})
            settour_cause_score_df = settour_cause_score_df[['您的姓名', '交通', '住宿', '保證成團', '公司品牌', '導遊', '業務人員', '行銷活動', '門市便利性', '領隊', '餐食']]
            settour_cause_score_df = settour_cause_score_df.rename(columns={"業務人員": '業務'})
            settour_cause = df.merge(settour_cause_score_df, how='left',left_on='您的姓名', right_on='您的姓名').fillna('')
            settour_cause[select_settour_cause_col] = settour_cause[select_settour_cause_col].replace('非常不滿意', 1).replace('不滿意',2).replace('普通', 3).replace('滿意', 4).replace('非常滿意', 5)
            settour_cause.pop('下次旅遊會優先選擇旅遊的原因為何')
            settour_cause[['是否有收到行前說明資料','本意見函資料，僅提供旅遊於意見收集及行銷方案使用']] = settour_cause[['是否有收到行前說明資料','本意見函資料，僅提供旅遊於意見收集及行銷方案使用']].replace('否', 'N').replace('是','Y').replace('同意', 'Y').replace('不同意', 'N')
            settour_cause['group_no'] = absPending_File.split('/')[len(absPending_File.split('/')) - 1].replace('國內團體旅遊意見函', '').replace('.csv', '')
            logging.info(settour_cause)
            finish_file_name.append(absPending_File.split('/')[len(absPending_File.split('/')) - 1].replace('國內團體旅遊意見函', '').replace('.csv', ''))
            [insert_data.append(tuple(i)) for i in settour_cause.values]

        return insert_data, finish_file_name
    else:
        logging.info('No Pending File found.')
        raise('No Pending File found.')

def InsertToPostgres(insert_data):
    conn = pg.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        dbname=DBNAME,
        port=PORT
    )
    cur = conn.cursor()
    print('======INSERT SQL====', cus_insert_sql % insert_data[0])
    logging.info('======INSERT SQL====\n %s' % (cus_insert_sql % insert_data[0]))
    cur.executemany(cus_insert_sql, insert_data)
    conn.commit()
    conn.close()

if __name__ == '__main__':
    finish_file = Download_Finish_File(BUCKET)
    main(file_path,finish_file)
    insert_data, finish_file_name = ETL_CSV(file_path)
    Log_File(finish_file_name)
    InsertToPostgres(insert_data)
    Upload_file_to_Cloud_Storage(file_path, BUCKET)
