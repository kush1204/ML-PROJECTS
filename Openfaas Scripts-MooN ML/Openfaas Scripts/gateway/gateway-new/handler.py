import pandas as pd
import io
from minio import Minio

def handle(req):
    """handle a request to the function
    Args:
        req (str): request body
    """
    sio = io.StringIO(req)
    data = pd.read_csv(sio,delimiter='\t',header=None)
    data = data[0].str.split('|', expand=True)
    data[6] = data[1].map(str) + " " +data[2].map(str) + " " +data[3].map(str) +" " + data[4].map(str) +" " + data[5].map(str)
    data['Description'] = data[6]
    data = data.drop(data.columns[[1,2,3,4,5,6]],axis = 1)
    data[0] = data[0].replace(regex='00:00',value='00:00@')
    data[[0,1]] = data[0].str.split('@', expand=True)
    data[[0,2]] = data[0].str.split(' ', expand=True)
    data['Timestamp'] = data[2]
    data[['Priority','Version']] = data[0].str.split('>',expand=True)
    data = data.drop(data.columns[[0,3]],axis = 1)
    data['Priority'] = data['Priority'].replace(regex='<',value='')
    data[[0,1]] = data[1].str.split('"]',expand=True)
    Temp = []
    Temp1 = []
    Temp2 = []
    hostname=[]
    source=[]
    application=[]
    col_1 = data[1].to_list()
    for i in col_1:
        value = str(i)
        value_list = value.split(' ',maxsplit=1)
        Temp.append(value_list[-1])
    for j in Temp:
        value_list1 = str(j).split(' ',maxsplit=1)
        hostname.append(value_list1[0])
        Temp1.append(value_list[-1])
    for k in Temp1:
        value_list2 = str(k).split(' ',maxsplit=1)
        source.append(value_list2[0])
        Temp2.append(value_list2[-1])
    for p in Temp2:
        value_list3 = str(p).split(' ',maxsplit=1)
        application.append(value_list3[0])
    data['Hostname'] = hostname
    data['Source'] = source
    data['Application'] = application
    data['Description'] = data[1] + " " + data['Description']
    data = data.drop(data.columns[[1,5]],axis = 1)
    data = data[['Priority','Version','Timestamp','Hostname','Source','Application','Description']]
    index_num = data[data['Timestamp'].isnull() == True].index
    data.drop(index_num, inplace=True)

    return data.to_csv(sep=',',index=False)
