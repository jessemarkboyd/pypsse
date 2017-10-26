# -*- coding: utf-8 -*-
"""
Created on Tue Feb 21 10:17:38 2017

Module interfaces with the PSS/E software through the standard PSS/E APIs. 

Creates and deletes additional files needed for certain PSS/E features (e.g. 
.dfx, .con, .mon, .sub). Redirects PSS/E output to a string variable. Use 
import_psse_results.py module to translate the text of the results into a
dataframe.

@author: Jesse Boyd
"""

PSSE_LOCATION = input('Enter location of psse executable (typical location is {})'.format('C:\\Program Files (x86)\\PTI\\PSSE34\\PSSPY27\\'))
import re
import os,sys
sys.path.append(PSSE_LOCATION)
os.environ['PATH'] = (PSSE_LOCATION + os.environ['PATH'])   
import pandas as pd
import numpy as np
import psse34
import psspy
import pssarrays
import pssexcel
import redirect
import import_psse_results
import StringIO

# create constants that PSS/E requires as default inputs
_i = 100000000
_f = 1.00000002004e+20
_s = 'ÿ'


class pypsse(object):
    """Creates an interface for PSS/E through Python and contains modules 
    for functionality. Note: PSS/E does not appear to support threading,
    so only a single instance of PSS/E can be run at a time. If another
    instance is created, it will replace the previous."""
        
    def __init__(self):
        """Initialize by opening the PSS/E case and attempting to solve. Output is 
        sent to a file with the same path as the case."""
        self.dfx_file = ''
        self.con_df = pd.DataFrame()
        self.error_message = ''
        self.outputfilepath = casepath[:-4]
        self.casepath = casepath
        self.old_stdout = sys.stdout
        self.out = StringIO.StringIO()
        sys.stdout = self.out
        self.created_files = list()
        psspy.psseinit(80000)
        self.opencase(casepath)
        sys.stdout = self.old_stdout

    def __del__(self):
        sys.stdout = self.old_stdout
        if self.error_message:
            self.__delete_created_files__()
        else:
            self.__delete_created_files__()

    def __reset__(self,delete_files=True):
        """Resets the instance variables so that the object may be reused without
        reinstanciation"""
        sys.stdout = self.old_stdout
        self.__delete_created_files__()
        dic = vars(self)
        for i in dic.keys():
            dic[i] = None
        self.old_stdout = sys.stdout
        self.out = StringIO.StringIO()
        self.outputfilepath = ''
        self.error_message = ''
        self.created_files = list()
                
    def opencase(self,casepath):
        """Initialize PSSE and open case"""
        sys.stdout = self.out
        ierr = psspy.case(casepath)
        if ierr:
            self.error_message += ('Error opening case. API \'case\' error code %d.' %ierr)
        else:
            psspy.fdns([0,0,0,0,0,0,0,0])
            ival = psspy.solved()
            if ival:
                psspy.fnsl([0,0,0,0,0,0,0,0])
                ival = psspy.solved()
                if ival:
                    self.error_message += ('Case did not solve using FNSL. API \'solved\' error code %d.' %ival)
        sys.stdout = self.old_stdout       
        return ierr
   
                    
    def __buses_exist__(self,busnumlist):
        """Returns a boolean list corresponding to a bus number list,
        True if the bus exists in the case and False if it does not"""
        return [(psspy.busexs(b)==0) for b in busnumlist]
        
    def __get_branch_len__(self,ibus,jbus,ckt):
        """Returns integer branch length for the specified branch"""
        if jbus:
            ierr, rval = psspy.brndat(ibus,jbus,ckt,'LENGTH')
            if not ierr:
                return rval
        return np.nan

    def __loads_exist__(self,busnumlist):
        """Returns a boolean list corresponding to a bus number list,
        True if the bus exists and has a generator and false otherwise"""
        fields = ['NUMBER','STATUS']
        ierr, arr = psspy.alodbusint(sid=-1,flag=4,string=fields)
        if not ierr:
            return [x in arr[0] for x in busnumlist]
        else:
            self.error_message += 'Error retrieving load buses. API \'alodbusint\' code {}'.format(ierr)
            return list()

    def __get_load_data__(self,busnumlist=[],areanumlist=[]):
        """Returns the real power load of the specified load"""
        df = pd.DataFrame()
        if busnumlist:
            sid = 11
            ierr = psspy.bsys(sid=sid, numbus=len(busnumlist), buses=busnumlist)
        elif areanumlist:
            sid = 11
            ierr = psspy.asys(sid=sid, num=len(areanumlist), areas=areanumlist)
        else:
            sid = -1
        fields = ['NUMBER','STATUS']
        ierr, arr = psspy.aloadint(sid=11,flag=4,string=fields)
        mask = [x == 1 for x in arr[1]]
        if not ierr:
            data = arr
            columns = fields
            fields = ['MVANOM','TOTALNOM']
            ierr, arr = psspy.aloadreal(sid=11, string=fields)
            for l in arr:
                for i in xrange(len(mask)):
                    if not mask[i]:
                        l.insert(i, np.nan)
        if not ierr:
            for l in arr:
                data.append(l)
            columns.extend(fields)
            fields = ['ID','NAME','EXNAME']
            ierr, arr = psspy.aloadchar(11,flag=4,string=fields)
        if not ierr:
            for l in arr:
                data.append(l)
            columns.extend(fields)
            df = pd.DataFrame(data=data,index=columns).transpose()
        return df
    
    def __machines_exist__(self,busnumlist):
        """Returns a boolean list corresponding to a bus number list,
        True if the bus exists and has a generator and false otherwise"""
        fields = ['NUMBER','STATUS']
        ierr, arr = psspy.amachint(sid=-1,flag=4,string=fields)
        if not ierr:
            return [x in arr[0] for x in busnumlist]
        else:
            self.error_message += 'Error retrieving machine buses. API \'amachint\' code {}'.format(ierr)
            return list()
        
    def __get_machine_data__(self,busnumlist=[],areanumlist=[]):
        """Returns the real power output of the specified machine"""
        df = pd.DataFrame()
        if busnumlist:
            sid = 11
            ierr = psspy.bsys(sid=sid, numbus=len(busnumlist), buses=busnumlist)
        elif areanumlist:
            sid = 11
            ierr = psspy.asys(sid=sid, num=len(areanumlist), areas=areanumlist)
        else:
            sid = -1
        fields = ['NUMBER','STATUS','OWN1']
        ierr, arr = psspy.amachint(sid=11,flag=4,string=fields)
        mask = [x == 1 for x in arr[1]]
        if not ierr:
            data = arr
            columns = fields
            fields = ['PGEN','QGEN','PMAX','PMIN','QMAX','QMIN']
            ierr, arr = psspy.amachreal(sid=11, string=fields)
            for l in arr:
                for i in xrange(len(mask)):
                    if not mask[i]:
                        l.insert(i, np.nan)
        if not ierr:
            for l in arr:
                data.append(l)
            columns.extend(fields)
            fields = ['ID','NAME','EXNAME']
            ierr, arr = psspy.amachchar(11,flag=4,string=fields)
        if not ierr:
            for l in arr:
                data.append(l)
            columns.extend(fields)
            df = pd.DataFrame(data=data,index=columns).transpose()
        return df
        
    def __get_bus_names__(self,busnumlist):
        """Returns a list of bus names for the list of bus numbers"""
        ierr = psspy.bsys(sid=11, numbus=len(busnumlist), buses=busnumlist)
        l = list()
        if ierr:
            self.error_message += ('Bus system not created for {}. API \'bsys\' error code {}.'.format(busnumlist,ierr))
            return l
        else:
            ierr, busnamelist = psspy.abuschar(sid=11, string="NAME")
        if not ierr:
            for busname in busnamelist[0]:
                if not busname.strip() in l:
                    l.append(busname.strip())
            return l
        else:
            print('Error retrieving bus names for {}: \nAPI \'abuschar\' error code {}'.format(busnumlist,ierr))
            l = [str(x) for x in busnumlist]
            return l 

    def __get_bus_owners__(self,busnumlist):
        """Returns a list of bus names for the list of bus numbers"""
        ierr = psspy.bsys(sid=11, numbus=len(busnumlist), buses=busnumlist)
        l = list()
        if ierr:
            self.error_message += ('Bus system not created for {}. API \'bsys\' error code {}.'.format(busnumlist,ierr))
            return l
        else:
            ierr, ownernumlist = psspy.abusint(sid=11, string="OWNER")
        if ierr:
            self.error_message += ('Error retrieving bus owners for {}. API \'abusint\' error code {}.'.format(busnumlist,ierr))
            return l
        else:
            for i in ownernumlist[0]:
                ierr, cval = psspy.ownnam(i)
                if not ierr:
                    l.append(cval)
        return l 
            
    def __get_bus_areas__(self,busnumlist):
        """Returns a list of areas to which the specified buses belong"""
        ierr = psspy.bsys(sid=11, numbus=len(busnumlist), buses=busnumlist)
        ierr, busarealist = psspy.abusint(sid=11, string="AREA")
        if not ierr:
            return list(set(busarealist[0]))
        else:
            print('Error retrieving areas for {}. abusint error code {}'.format(busnumlist,ierr))
            return []

    def __get_bus_pu__(self,busnumlist):
        """Returns a list of areas to which the specified buses belong"""
        ierr = psspy.bsys(sid=11, numbus=len(busnumlist), buses=np.asarray(busnumlist))
        ierr, busarealist = psspy.abusreal(sid=11, flag=2, string="PU")
        if not ierr:
            return busarealist[0]
        else:
            return None
        
    def __get_bus_kv__(self,busnum):
        """Returns the nominal voltage of the specified bus"""
        return psspy.busdat(busnum,'BASE')[1]
        
    def __get_bus_zones__(self,busnumlist):
        """Returns a list of zones to which the specified buses belong"""
        ierr = psspy.bsys(sid=11, numbus=len(busnumlist), buses=busnumlist)
        ierr, buszonelist = psspy.abusint(sid=11, string="ZONE")
        return buszonelist[0]
       
    def __get_3wnd_tx__(self,busnum,cktid):
        """Returns a dataframe of three winding transformers in the specified areas"""
        areanumlist = self.__get_bus_areas__([busnum])
        ierr = psspy.bsys(sid=1, numarea=len(areanumlist), areas=np.asarray(areanumlist))
        string = ['WIND1NUMBER','WIND2NUMBER','WIND3NUMBER']
        ierr, iarray = psspy.atr3int(sid=1,string=string)
        if ierr:
            print('Error getting 3W transformer data: {}'.format(ierr))
            return None
        ierr, carray = psspy.atr3char(sid=1,string=['ID'])
        if ierr:
            print('Error getting 3W transformer ID: {}'.format(ierr))
            return None
        string.append('ID')
        iarray.append(carray[0])
        tx_data = pd.DataFrame(columns=string,data=np.transpose(iarray))
        tx_data['WIND1NUMBER'] = tx_data['WIND1NUMBER'].astype(int)
        tx_data['WIND2NUMBER'] = tx_data['WIND2NUMBER'].astype(int)
        tx_data['WIND3NUMBER'] = tx_data['WIND3NUMBER'].astype(int)
        mask = tx_data.isin([busnum])
        tx_data = tx_data[mask.any(axis=1)]
        tx_data = tx_data[tx_data['ID'].str.strip()==cktid.strip()]
        return tx_data

    def __get_transformers__(self,areanumlist=None):
        """Returns a dataframe of transformers in the specified areas"""
        if areanumlist:
            sid = 11
            ierr = psspy.bsys(sid=sid, numarea=len(areanumlist), areas=[int(x) for x in areanumlist])
            if ierr:
                sid = -1
        else:
            sid = -1
        fields = ['FROMNUMBER','TONUMBER']
        ierr, tx_int_list = psspy.atrnint(sid=sid, string=fields)
        if not ierr:
            df = pd.DataFrame(tx_int_list,fields).transpose()
            fields = ['FROMNAME','TONAME','ID']
            ierr, tx_chr_list = psspy.atrnchar(sid=sid, string=fields)
        if not ierr:
            df = df.join(pd.DataFrame(tx_chr_list,fields).transpose())
        else:
            print('Error retrieving branch data for {}: \nAPI error code {}'.format(areanumlist,ierr))
            df = pd.DataFrame()
        return df
    
    def __get_branches__(self,areanumlist=None):
        """Returns a dataframe of branches in the specified areas"""
        if areanumlist:
            sid = 11
            ierr = psspy.bsys(sid=sid, numarea=len(areanumlist), areas=[int(x) for x in areanumlist])
            if ierr:
                sid = -1
        else:
            sid = -1
        fields = ['FROMNUMBER','TONUMBER']
        ierr, br_int_list = psspy.abrnint(sid=sid, string=fields)
        if not ierr:
            df = pd.DataFrame(br_int_list,fields).transpose()
            fields = ['FROMNAME','TONAME','ID']
            ierr, br_chr_list = psspy.abrnchar(sid=sid, string=fields)
        if not ierr:
            df = df.join(pd.DataFrame(br_chr_list,fields).transpose())
        else:
            self.error_message += 'Error retrieving branch data for {}: \nAPI error code {}'.format(areanumlist,ierr)
        return df
    
    def __get_branches_within_n_nodes__(self,busnum,n):
        """Returns dataframe of branches n nodes away from specified bus number"""
        br_df = self.__get_branches__()
        br_df = br_df.append(self.__get_transformers__())
        if not br_df.empty:
            df = br_df[(br_df['FROMNUMBER'] == busnum) | (br_df['TONUMBER'] == busnum)]
            i = 0
            while i < n:
                iter_df = df.drop_duplicates()
                for key, item in iter_df.iterrows():
                    df = df.append(br_df[(br_df['FROMNUMBER']==item['FROMNUMBER']) | 
                            (br_df['TONUMBER']==item['FROMNUMBER']) | 
                            (br_df['TONUMBER']==item['TONUMBER']) |
                            (br_df['FROMNUMBER']==item['TONUMBER'])])
                i += 1
        return df.drop_duplicates()
   
    def __saveas__(self,path):
        sys.stdout = self.out
        psspy.save(path)
        sys.stdout = self.old_stdout
        
    def __insert_tap__(self,new_busnum,new_busname,tap_busnum,tap_busnum1,tap_ckt):
        """Creates a new bus along the specified branch"""
        self.out = StringIO.StringIO()
        sys.stdout = self.out
        if '.' in str(tap_ckt):
            tap_ckt = str(tap_ckt).split('.')[0]
        ierr = psspy.ltap(tap_busnum,tap_busnum1,str(tap_ckt),0.50,new_busnum,str(new_busname))
        if not ierr:
            psspy.fdns([0,0,0,0,0,0,0,0])
            ierr = psspy.solved()
            if ierr:
                psspy.fnsl([0,0,0,0,0,0,0,0])
                ierr = psspy.solved()
                if ierr:
                    self.error_message += 'Case did not solve after inserting tap.\n'
        elif ierr == 2:
            self.error_message += 'Branch {} - {} {} was not found in the case.\n'.format(tap_busnum,tap_busnum1,tap_ckt)
        sys.stdout = self.old_stdout
        return ierr
        
    def __insert_gen__(self,name,poi_busnum,capacity,new_poi_busnum,new_gen_busnum):
        """Splits the specified bus and inserts a generator at the new bus"""
        self.out = StringIO.StringIO()
        sys.stdout = self.out
        ierr = psspy.splt(poi_busnum,new_poi_busnum,'POI ' + str(name))
        if not ierr:
            try:
                areanum = self.__get_bus_areas__([poi_busnum])[0]
                zonenum = self.__get_bus_zones__([poi_busnum])[0]
                vreg = self.__get_bus_pu__([poi_busnum])[0]
            except Exception as e:
                self.error_message += 'Error retrieving bus area: {}'.format(str(e))
                sys.stdout = self.old_stdout
                return 1
            ierr = psspy.bus_data_3(new_gen_busnum,intgar1=2,intgar2=areanum,intgar3=zonenum,realar1=34.5,name=str(name))        
            if not ierr:
                ierr = psspy.plant_data(new_gen_busnum,poi_busnum,[vreg,100.0])
                if not ierr:
                    ierr, realaro = psspy.two_winding_data(new_gen_busnum,new_poi_busnum,'1',intgar=1)
                    if not ierr:
                        ierr = psspy.machine_data_2(new_gen_busnum,r"""1""",realar3=capacity/3,realar4=-capacity/3,realar5=capacity, realar6=0.0,realar8=1000000000,realar9=1000000000)
                        if not ierr:
                            psspy.fnsl([0,0,0,0,0,0,0,0])
                            ierr = psspy.solved()
                            count = 1
                            while ierr == 1 and count < 10:
                                psspy.fnsl([0,0,0,0,0,0,0,0])
                                ierr = psspy.solved()
                                count += 1
                            if ierr:
                                self.error_message += 'API \'solved\' error code {}'.format(ierr)
                    else:
                        self.error_message += 'API \'machine_data_2\' error code {}'.format(ierr)
                else:
                    self.error_message += 'API \'plant_data\' error code {}'.format(ierr)
            else:
                self.error_message += 'API \'bus_data_3\' error code {}'.format(ierr)
        else:
            self.error_message += 'API \'splt\' error code {}'.format(ierr)
        sys.stdout = self.old_stdout
        return ierr

    def __dispatch_gen__(self,busnum,uid='1',pgen=_f):
        """redispatches the assigned generator to the specified power output"""
        self.out = StringIO.StringIO()
        sys.stdout = self.out
        intgar = [_i,_i,_i,_i,_i,_i]
        realar = [pgen,_f,_f,_f,_f,_f,_f,_f,_f,_f,_f,_f,_f,_f,_f,_f,_f]
        ierr = psspy.machine_chng_2(busnum,uid,intgar,realar)
        if not ierr:
            psspy.fnsl([0,0,0,0,0,0,0,0])
            ierr = psspy.solved()
            if ierr:
                self.error_message += 'API \'solved\' error code {} for {}\n'.format(ierr,busnum)
        else:
            self.error_message += 'API \'machine_chng_2\' error code {} for {}\n'.format(ierr,busnum)
        sys.stdout = self.old_stdout
        return ierr

    
    def __create_load__(self,busnum,uid='1',pload=_f,qload=_f):
        """redispatches the assigned generator to the specified power output"""
        self.out = StringIO.StringIO()
        sys.stdout = self.out
        intgar = [_i,_i,_i,_i,_i,_i,_i]
        realar = [pload,qload,_f,_f,_f,_f,_f,_f]
        ierr = psspy.load_data_5(busnum,uid,intgar,realar)
        if not ierr:
            psspy.fnsl([0,0,0,0,0,0,0,0])
            ierr = psspy.solved()
            if ierr:
                self.error_message += 'API \'solved\' error code {} for {}\n'.format(ierr,busnum)
        else:
            self.error_message += 'API \'load_data_5\' error code {} for {}\n'.format(ierr,busnum)
        sys.stdout = self.old_stdout
        return ierr
    
    def __change_load__(self,busnum,uid='1',pload=_f,qload=_f,status=None):
        """redispatches the assigned generator to the specified power output"""
        self.out = StringIO.StringIO()
        sys.stdout = self.out
        intgar = [_i if not status else status,_i,_i,_i,_i,_i,_i]
        realar = [pload,qload,_f,_f,_f,_f,_f,_f]
        ierr = psspy.load_chng_5(busnum,uid,intgar,realar)
        if not ierr:
            psspy.fnsl([0,0,0,0,0,0,0,0])
            ierr = psspy.solved()
            if ierr:
                self.error_message += 'API \'solved\' error code {} for {}\n'.format(ierr,busnum)
        else:
            self.error_message += 'API \'load_chng_5\' error code {} for {}\n'.format(ierr,busnum)
        sys.stdout = self.old_stdout
        return ierr

