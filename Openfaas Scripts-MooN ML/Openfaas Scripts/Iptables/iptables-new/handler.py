import pandas as pd
import io

def handle(req):
    """handle a request to the function
    Args:
        req (str): request body
    """
    sio = io.StringIO(req)
    data = pd.read_csv(sio,header=None,sep=' - -',engine='python')
    data[[0,'Timestamp',2,3]] = data[0].str.split(' ', expand=True)
    data[['Priority','Version']] = data[0].str.split('>', expand=True)
    data['Priority'] = data['Priority'].replace(regex='<',value='')
    data = data.drop(data.columns[[0,3,4]],axis = 1)
    data[1] = data[1].replace(regex='\[meta sequenceId="\d"\]',value='')
    data[1] = data[1].replace(regex='\[meta sequenceId="\d\d"\]',value='')
    data[1] = data[1].replace(regex='\[meta sequenceId="\d\d\d"\]',value='')
    data[1] = data[1].replace(regex='\[meta sequenceId="\d\d\d\d"\]',value='')
    data[1] = data[1].replace(regex='- ',value=' ')
    data[[0,1]] = data[1].str.split('] ', expand=True)
    data[[0,2]] = data[0].str.split('[', expand=True)
    data['Description'] = data[2] + " " + data[1]
    data[0].str.split(' ', expand=True)
    data[[0,2,'Hostname','Source',4,'Application',5]] = data[0].str.split(' ', expand=True)
    data = data.drop(data.columns[[0,4,5,9,11]],axis = 1)
    data = data[['Priority','Version','Timestamp','Hostname','Source','Application','Description']]


    return data.to_csv(sep=',',index=False)
