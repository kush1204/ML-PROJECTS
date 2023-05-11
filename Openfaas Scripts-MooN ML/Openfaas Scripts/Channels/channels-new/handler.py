import pandas as pd
import numpy as np
import io
import warnings
warnings.filterwarnings("ignore")

def handle(req):
    """handle a request to the function
    Args:
        req (str): request body
    """
    sio = io.StringIO(req)
    data = pd.read_csv(sio,sep='|',low_memory=False,header=None,error_bad_lines=False, warn_bad_lines=False)
    data[6] = data[1].map(str) + " " +data[2].map(str) + " " +data[3].map(str) +" " + data[4].map(str) +" " + data[5].map(str)
    data['Description'] = data[6]
    data = data.drop(data.columns[[1,2,3,4,5,6]],axis = 1)
    data[[0,1,2,3,4,5,6,7,8,9,10,11]] = data[0].str.split(' ', expand=True)
    data['Timestamp'] = data[1]
    data['Hostname'] = data[7]
    data['Source'] = data[8]
    data[['Application',1]] = data[9].str.split('[', expand=True)
    #data['Severity'] = data[10]
    data['Description'] = data[10] + " |" + data['Description']
    data[['Priority','Version']] = data[0].str.split('>',expand=True)
    data['Priority'] = data['Priority'].replace(regex='<',value='')
    data = data.drop(data.columns[[0,2,3,4,5,6,7,8,9,10,11,12]],axis = 1)
    #facility = []
    #for i,j in zip(data.Priority,data.Severity):
     #   if j == 'D':
      #      value = round((int(i) - 7)/8)
       # elif j == 'I':
        #    value = round((int(i)-6)/8)
        #elif j == 'N':
         #   value = round((int(i)-5)/8)
        #elif j == 'W':
         #   value = round((int(i)-4)/8)
        #elif j == 'F':
         #   value = round((int(i)-3)/8)
        #else:
          #  value = round(100)
        #facility.append(value)
    #data['Facility'] = facility
    #data['Facility'] = data['Facility'].replace(100,'')
    data = data[['Priority','Version','Timestamp','Hostname','Source','Application', 'Description']]

    return data.to_csv(sep=',',index=False)
