import pandas as pd
import io
from minio import Minio

def handle(req):
    """handle a request to the function
    Args:
        req (str): request body
    """
    sio = io.StringIO(req)
    data = pd.read_csv(sio,sep='|',header=None)
    data[6] = data[1].map(str) + " " +data[2].map(str) + " " +data[3].map(str) +" " + data[4].map(str) +" " + data[5].map(str)
    data['Description'] = data[6]
    data = data.drop(data.columns[[1,2,3,4,5,6]],axis = 1)
    data[[0,1,2,3,4,5,6,7,8,9,10,11,12]] = data[0].str.split(' ', expand=True)
    data['Timestamp'] = data[1]
    data['Hostname'] = data[8]
    data['Source'] = data[9]
    data['Application'] = data[10]
    data['Description'] = data[11] + " " + data['Description']
    data[['Priority','Version']] = data[0].str.split('>',expand=True)
    data['Priority'] = data['Priority'].replace(regex='<',value='')
    data = data.drop(data.columns[[0,2,3,4,5,6,7,8,9,10,11,12,13]],axis = 1)
    data = data[['Priority','Version','Timestamp','Hostname','Source','Application','Description']]

    return data.to_csv(sep=',',index=False)
