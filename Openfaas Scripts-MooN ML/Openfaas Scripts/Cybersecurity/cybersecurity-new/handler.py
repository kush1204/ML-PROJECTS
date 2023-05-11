import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import io

def handle(req):
    """handle a request to the function
    Args:
        req (str): request body
    """
    sio = io.StringIO(req)
    data = pd.read_csv(sio,header=None,sep='\t',error_bad_lines=False,warn_bad_lines=False)
    data_list = data[0].to_list()
    priority = []
    version = []
    timestamp = []
    temperary = []
    description = []
    for i in data_list:
        value = str(i)
        Temp1 = value[value.find('<')+len('<'):value.rfind('>',0,10)]
        Temp2 = value[value.find('>')+len('>'):value.rfind(' ',0,10)]
        Temp3 = value[value.find(' ')+len(' '):value.rfind(':00',0,50)]
        Temp4 = value[value.find('- ')+len('- '):value.rfind(':',0,110)]
        Temp5 = value[value.find('- ')+len('- '):value.rfind('')]
        priority.append(Temp1)
        version.append(Temp2)
        timestamp.append(Temp3+":00")
        if Temp4.__contains__('meta sequence'):
            Temp4 = Temp4.strip()
        else:
            Temp4 = '[meta sequenceId=]' + Temp4.strip()
        temperary.append(Temp4.strip())
        description.append(Temp5)
    data['Priority'] = priority
    data['Version'] = version
    data['Timestamp'] = timestamp
    data['Temperary'] = temperary
    data['Description'] = description
    data['Temperary'] = data['Temperary'].replace(regex='-',value='')
    data['Temperary'] = data['Temperary'].str.strip()
    data[[0,1,2]] = data['Temperary'].str.split(']', expand=True)
    data[1] = data[1].str.strip()
    temperary = []
    temperary_1 = []
    hostname = []
    source = []
    application = []
    col_1 = data[1].to_list()
    for i in col_1:
        value = str(i)
        temp = value.split(' ',maxsplit=0)
        temperary.append(temp[-1])
    for j in temperary:
        value_1 = str(j)
        value_list = value_1.split(' ',maxsplit=1)
        hostname.append(value_list[0])
        temperary_1.append(value_list[-1])
    for k in temperary_1:
        value_2 = str(k)
        value_list1 = value_2.split(' ',maxsplit=1)
        source.append(value_list1[0])
        application.append(value_list1[-1])
    data['Hostname'] = hostname
    data['Source'] = source
    data['Application'] = application
    data = data.drop(data.columns[[0,4,6,7]],axis = 1)
    data = data[['Priority','Version','Timestamp','Hostname','Source','Application','Description']]

    return data.to_csv(sep=',',index=False)
