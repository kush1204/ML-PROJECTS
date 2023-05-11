import pandas as pd
import io
from minio import Minio


def handle(req):
    """handle a request to the function
    Args:
        req (str): request body
    """

    sio = io.StringIO(req)
    data = pd.read_csv(sio,header=None,sep='"]',engine='python')

    col_1 = data[1].to_list()
    Temp1 = []
    Temp2 = []
    for i in col_1:
        value = str(i)
        value_list = value.split(': ',maxsplit=1)
        Temp1.append(value_list[0])
        Temp2.append(value_list[-1])
    data[1] = Temp1
    data['Description'] = Temp2
    data[[1,'Hostname','Source',2,'Application']] = data[1].str.split(' ', expand=True)
    data = data.drop(data.columns[[1,5]],axis = 1)

    data[[0,1,2]] = data[0].str.split('00:00', expand=True)
    temp1_list = []
    timestamp = []
    col_0 = data[0].to_list()
    for i in col_0:
        value = str(i)
        value_split = value.split(' ',maxsplit=2)
        temp1 = value_split[0]
        temp2 = value_split[-1]+"00:00"
        temp1_list.append(temp1)
        timestamp.append(temp2)
    data['Timestamp'] = timestamp
    data['Temp'] = temp1_list
    data[['Priority','Version']] = data['Temp'].str.split('>', expand=True)
    data['Priority'] = data['Priority'].replace(regex='<',value='')
    data = data.drop(data.columns[[0,5,6,8]],axis = 1)
    data = data[['Priority','Version','Timestamp','Hostname','Source','Application','Description']]

    return data.to_csv(sep=',',escapechar='#',index=False)
