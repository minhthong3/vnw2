import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from oauth2client.service_account import ServiceAccountCredentials
from vnstock3 import Vnstock
from datasets import Dataset, DatasetDict
import concurrent.futures

# Xác thực truy cập vào Google Sheets
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials_path = "C:\\Users\\thong\\Downloads\\vnw.data\\datavnwealth-25a353ea3781.json"
credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
client = gspread.authorize(credentials)

# Mở bảng tính 
sheet = client.open('Stock_list_VNW').worksheet('VNW_list')

# Lấy danh sách mã cổ phiếu từ cột A, bao gồm cả hàng đầu tiên
stock_symbols = sheet.col_values(1)

# Tạo đối tượng Vnstock
vnstock = Vnstock()

# Lấy dữ liệu cổ phiếu từ sàn HSX với nguồn dữ liệu là VCI
stock = vnstock.stock(symbol='HSX', source='VCI')

# Lấy lịch sử giá cổ phiếu của từng mã trong danh sách từ '2022-01-01' đến '2024-12-31'
stock_data = {}

def fetch_and_store(symbol):
    data = stock.quote.history(symbol, start='2022-01-01', end='2024-12-31')
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, f"{symbol}_historical_data.parquet")
    return symbol, df

# Sử dụng concurrent.futures để lưu dữ liệu đồng thời
with concurrent.futures.ThreadPoolExecutor() as executor:
    results = list(executor.map(fetch_and_store, stock_symbols))

# Chuyển đổi stock_data thành DataFrame và lưu trữ trên Hugging Face Dataset
dataset_dict = {symbol: Dataset.from_pandas(df) for symbol, df in results}

datasets = DatasetDict(dataset_dict)

# Đẩy DatasetDict lên Hugging Face Hub
datasets.push_to_hub("rawdatapool", token="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")

print("Historical Data đã được lưu thành công trên Hugging Face Dataset")
