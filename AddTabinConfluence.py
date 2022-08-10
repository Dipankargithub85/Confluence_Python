import json
from msilib.schema import _Validation_records
from ssl import VERIFY_DEFAULT
from tabnanny import filename_only
import urllib3
from atlassian import Confluence
import pandas as pd
import numpy as np
import sys
import math

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def readJsonfile(filename):
    fl = open (filename, "r")
    j=1
    data = json.loads(fl.read())
    if len(data['properties'].items()) > 0 :
        #print (data['properties'])
        return (data)
    else:
       val= data['allOf'][0]['$ref']
       #index = val.rfind('/')
       index = val.rsplit('/')
       if ".json" in index[-1]:
           print(index[-1])
           return readJsonfile(index[-1])
       else:
           jfilename= index[-2]

def connectwithConfluence(filename,ctype):
    #pd.options.mode.chained_assignment = None  # default='warn'
    baseurl='https://confluence.site'
    conf = Confluence(url=baseurl, username='###', password='$###@',verify_ssl=False) # nosec
    filenameforprint = filename.split('.')
    datafram = pd.DataFrame(columns=['#','Data Item','Description','Data Type','Cardinality','Default Value','Business Rules and Data Validations','Comments']) 
    dfindex=0
    tabindex=1
    if ctype == 'validate':
        #print('hi')
        pageid='207094419'
        pagetitle='/validate'
        #page = conf.get_page_by_id(pageid, expand='body.storage')
        #print(page)
       # datafram= getdataframe(page)
        valili =[("error_records_limit","Integer","Number of records failing per validator included in the response",'0..1','50'),
                ("denormalised","Boolean","Whether the data is deformalized",'0..1','False'),
                ("data","Array of "+ filenameforprint[0] +" object","List of data rows",'1..n','NA')]
        
        for val in valili:
            datafram.loc[dfindex,'#'] = tabindex
            datafram.loc[dfindex,'Data Item']= val[0]
            datafram.loc[dfindex,'Description'] = val[2]
            datafram.loc[dfindex,'Data Type'] = val[1]
            datafram.loc[dfindex,'Cardinality'] = val[3]
            datafram.loc[dfindex,'Default Value'] = val[4]
            datafram.loc[dfindex,'Business Rules and Data Validations']=''
            datafram.loc[dfindex,'Comments'] =''
            dfindex = dfindex+1
            tabindex = tabindex+1
   # df.loc[index,'F'] = '

        fileprop = readJsonfile(filename)
        #print(fileprop)
        
        updateconf(conf,datafram,fileprop,pageid,pagetitle,ctype,dfindex,tabindex,filename)
    elif ctype == 'upload':
        pageid='207094419'
        pagetitle='/validate'
       # page = conf.get_page_by_id(pageid, expand='body.storage')
        #datafram= getdataframe(page)
        fileprop = readJsonfile(filename)
        #print()
        
        
        valili =[("data","Array of "+filenameforprint[0] +" object","List of data rows",'1..n','NA')]
        for val in valili:
            datafram.loc[dfindex,'#'] = tabindex
            datafram.loc[dfindex,'Data Item']= val[0]
            datafram.loc[dfindex,'Description'] = val[2]
            datafram.loc[dfindex,'Data Type'] = val[1]
            datafram.loc[dfindex,'Cardinality'] = val[3]
            datafram.loc[dfindex,'Default Value'] = val[4]
            datafram.loc[dfindex,'Business Rules and Data Validations']=''
            datafram.loc[dfindex,'Comments'] =''
            #index = index+1
            dfindex = dfindex+1
            tabindex = tabindex+1

        updateconf(conf,datafram,fileprop,pageid,pagetitle,ctype,dfindex,tabindex,filename)

   
def getdataframe(page):
    page_content = page['body']['storage']['value']
    table = pd.read_html(page_content)
    df = pd.DataFrame(table[0])
    #print(df)
    return df


def updateconf(conf,datafram,fileprop,pageid,pagetitle,ctype,dfindex,tabindex,filename):
    #datafrmflg=False
    #concheckflg=True
    subind=1
    tabindexmod = tabindex - 1
    for indexval in fileprop['properties'].items():
        dataitem=''
        des=''
        enumli=list()
        typeval=''
        tupval=tuple()
        dataitem= indexval[0] 
        newdict = indexval[1]
        subindstr=''
        comments=''
        if '$ref' in newdict:
            refval= newdict['$ref']
            comments=refval
            tupmod = tuple()
            tupval = getvalforJsonEntity(refval)
            subindstr = str(tabindexmod) + '.'+str(subind)
            tupmod = (dataitem,tupval[1],tupval[2],tupval[3])
            datafram= updateconftab(tupmod,datafram,dfindex,subindstr,comments)
            #index = index + 1
            dfindex = dfindex + 1
            subind = subind + 1
        else:
            if 'description' in newdict:
                des= newdict['description']

            if 'enum' in newdict:
                enumli= newdict['enum']

            if 'type' in newdict:
                if newdict['type'] == 'array':
                    typeval= 'Array of ' + dataitem + ' Object'
                else:
                    typeval=newdict['type']

            tupval =(dataitem,des,typeval,enumli)
            subindstr = str(tabindexmod) + '.'+str(subind)
            datafram= updateconftab(tupval,datafram,dfindex,subindstr,comments)
            dfindex = dfindex + 1
            subind = subind + 1

            if 'array' in newdict['type']:
                if 'properties' in newdict['items']:
                    listvalue=list()
                    tupval= tuple()
                    propertyname='properties'
                    #print('NEW-DICT ITEAM ',newdict['items'])
                    listvalue= getalltupval(newdict['items'],propertyname)
                    arrsubind=1
                    subindmod= subind -1
                    for lval in listvalue:
                        arrstr=''
                        arrstr = str(tabindexmod) + '.'+str(subindmod) + '.' + str(arrsubind)
                        datafram= updateconftab(lval,datafram,dfindex,arrstr,comments)
                        dfindex = dfindex + 1
                        arrsubind = arrsubind + 1

    #if concheckflg:
    if ctype == 'upload':
        cosntactli =[("name","String","Number of records failing per validator included in the response",''),
        ("date","DateTime","Date of collection","2022-03-20T12:44:41.086Z"),
        ("count","Integer","The expected Number of entries in the data field.",'0'),
        ("fail_tolerance","Integer","A value between 0-1 that expresses how many entries may fail schema validation and still ingest the valid data.","0"),
        ("object_response","Boolean","Whether the response should be a json object.","true"),
        ("denormalised","Boolean","Use denormalised schemas (only available on some systems)","false")]
        for lival in cosntactli:
            datafram= updatedataframeforUpload(datafram,lival,dfindex,str(tabindex),'')
            #pass
            dfindex= dfindex + 1
            tabindex = tabindex + 1

 
    df1 = datafram.replace(np.nan, ' ', regex=True)
    page_content=df1.to_html(index=False)
    print("Table data from Top")
    print(datafram.head())
    print("=========================================")
    print("Table data from Bottom")
    print(datafram.tail())
    try:
        conf.update_page(pageid, pagetitle, page_content)
        print("=========================================================================")
        print("Confluence Table for schema "+ filename + " has updated with "+ctype + " format" )
        print("=========================================================================")
    except:
        print("Internal service error in confluence side")


        
def updateconftab(tupval,df,dfindex,indexvalue,com):
    
    df.loc[dfindex,"#"] = indexvalue
    df.loc[dfindex,"Data Item"]= tupval[0]
    df.loc[dfindex,"Description"] = tupval[1]
    df.loc[dfindex,"Data Type"] = tupval[2]
    if tupval[0] in ['id','date']:
        df.loc[dfindex,"Cardinality"] = '1..1'
    elif tupval[2] =='array':
        df.loc[dfindex,"Cardinality"] = '1..n'
    else:
        df.loc[dfindex,"Cardinality"] = '0..1'
    ##if len(tupval[3]) > 1:
    df.loc[dfindex,"Default Value"] = ''

    if len(tupval[3]) > 1:
        df.loc[dfindex,"Business Rules and Data Validations"] = ('Enum Values:-' + ','.join(tupval[3])).split(',')
    else:
        df.loc[dfindex,"Business Rules and Data Validations"] = ''

    df.loc[dfindex,"Comments"] =com
    
    return df

    

def updatedataframeforUpload(df,lival,index,indexval,com):
    tup=tuple()
    tup=lival
    #print(index)
    #print("hiS"
    df.loc[index,"#"] = indexval
    df.loc[index,"Data Item"]= tup[0]
    df.loc[index,"Description"] = tup[2]
    df.loc[index,"Data Type"] = tup[1]
    if tup[0] == 'name':
        df.loc[index,"Cardinality"] = '1..1'
    else:
        df.loc[index,"Cardinality"] = '0..1'
    df.loc[index,"Default Value"] = tup[3]
    df.loc[index,"Business Rules and Data Validations"] = ''
    df.loc[index,"Comments"] = com

    return df




def getvalforJsonEntity(refval):

    rettup=tuple()
    lival=list()
    index = refval.rsplit('/')
    if "common" in index[-2]:
        propertyname=index[-1]
        #print(propertyname)
        cfl = open ('common.json', "r")
        data = json.loads(cfl.read())
        #print(data['mic_code'])
        lival= getalltupval(data,propertyname)
        #print(lival)
        if len(lival) > 0:
            rettup= lival[0]
        else :
            idval=''
            des=''
            typ=''
            enum=list()
            rettup=(idval,des,typ,enum)
        
        #print(rettup)      
    return rettup


def getalltupval(data,propertyname):
    listfordata=list()
    idval=''
    des=''
    typ=''
    enum=list()
    typflg=False
    enumflg=False
    
    anotherdict=dict()
    if propertyname == 'properties':
        for val  in data[propertyname].items():
            rettup= tuple()
            idval=val[0]
            anotherdict = val[1]
            #print(anotherdict)
            if 'description' in anotherdict:
                des= anotherdict['description']

            if 'type' in anotherdict:
                typ= anotherdict['type']
            if 'enum' in anotherdict:
                enum= anotherdict['enum']

            rettup=(idval,des,typ,enum)
            listfordata.append(rettup)
        #print(listfordata)

    else:
        idval=propertyname
        i=0
        datavallen = len(data[propertyname])
        for key,val  in data[propertyname].items():
                
            if key =='description':
                des=val
            if key == 'type':
                typ = val
                #typflg= True
                #print('key val',typ)
            if key == 'enum':
                enum=val

            i=i+1
            if datavallen == i :
                rettup=(idval,des,typ,enum)
                listfordata.append(rettup)
            
    
    return listfordata

if __name__ == "__main__":
    
    schemalist=['json file name']
    
    tabtypelist = ['validate','upload']
    
    schemaname = input("Enter the table schema name:")
   
    if schemaname not in schemalist:
        print("Schema name should be one of the below")
        print(schemalist)
        sys.exit(0)
    tabtype= input("Enter the table type (validate/upload):")
    if tabtype not in tabtypelist:
        print("Table type should be one of the below")
        print(tabtypelist)
        sys.exit(0)
    connectwithConfluence(schemaname,tabtype)
