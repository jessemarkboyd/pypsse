# -*- coding: utf-8 -*-
"""
Created on Tue Feb 21 10:17:38 2017

Module interfaces with the PSS/E software through the standard PSS/E APIs. 

Creates and deletes additional files needed for certain PSS/E features (e.g. 
.dfx, .con, .mon, .sub). Redirects PSS/E output to a string variable. Transfers
the text of the results into a dataframe.

@author: Jesse Boyd
"""
import Tkinter
import tkFileDialog
import StringIO
import re
import os, sys
import pandas as pd
import numpy as np

# import PSSE APIs
PSSE_LOCATION = 'C:\\Program Files (x86)\\PTI\\PSSE34\\PSSPY27\\'
sys.path.append(PSSE_LOCATION)
os.environ['PATH'] = (PSSE_LOCATION + os.environ['PATH'])   
try:
    import psse34
except ImportError:
    inputfilepath = ''
    if not inputfilepath:
        root = Tkinter.Tk()
        PSSE_LOCATION = tkFileDialog.askdirectory(title='Select PSSE Python library directory (usually PSSE34//PSSPY27)',filetypes=[('application','*.exe')])
        root.destroy()
    sys.path.append(PSSE_LOCATION)
    os.environ['PATH'] = (PSSE_LOCATION + os.environ['PATH'])
    try:
        import psse34
    except ImportError as e:
        raise ImportError('An issue was encountered importing the PSSE library: /n{}'.format(str(e)))
import psspy
import pssarrays
import pssexcel
import redirect

# create constants that PSS/E requires as default inputs
_i = 100000000
_f = 1.00000002004e+20
_s = 'ÿ'


class pypsse(object):
    """Creates an wrapper for PSS/E APIs. Note: although PSS/E supports 
    multiple instances of its program, if another object of the pypsse class 
    is created, it will change all pypsse object reference cases to the most
    recently opened case. This is unfortunate. Note: reserve sid number 11 for
    use in the class methods (it will overwrite other subsystems with sid 11)."""
        
    
    def __init__(self):
        """Initialization prepares the error message and redirects the PSSE
        output from sys.out to a StringIO for processing."""
        self.error_message = ''
        self.__internally_created_files__ = []
        if isinstance(sys.stdout,StringIO.StringIO):
            pass
        else:
            self.__old_stdout__ = sys.stdout
        self.out = StringIO.StringIO()
        sys.stdout = self.out
        self.support_file_path = ''
        self.support_files = list()
        self.psspy = psspy
        self.pssarrays = pssarrays
        self.pssexcel = pssexcel
        self.data = None
        self.BUS_FIELDS = {}
        self.BUS_FIELDS['Real'] = ['BASE','PU','KV','ANGLE','ANGLED','NVLMHI','NVLMLO','EVLMHI','EVLMLO','MISMATCH','O_MISMATCH']
        self.BUS_FIELDS['Integer'] = ['STATION','NUMBER','TYPE','AREA','ZONE','OWNER','DUMMY']
        self.BUS_FIELDS['Complex'] = ['MVA','IL','YL','TOTAL','LDDGN','SC_MVA','SC_IL','SC_YL','SC_TOTAL','FX_MVA','FX_IL','FX_YL','FX_TOTAL','YS','YSZERO','YSZ','YSW','YSWZ','SHUNTN','SHUNTZ']
        self.A_BUS_FIELDS = {}
        self.A_BUS_FIELDS['Real'] = ['BASE','PU','KV','ANGLE','ANGLED','NVLMHI','NVLMLO','EVLMHI','EVLMLO','MISMATCH','O_MISMATCH']
        self.A_BUS_FIELDS['Integer'] = ['NUMBER','TYPE','AREA','ZONE','OWNER','DUMMY']
        self.A_BUS_FIELDS['Complex'] = ['VOLTAGE','SHUNTACT','O_SHUNTACT','SHUNTNOM','O_SHUNTNOM','SHUNTN','SHUNTZ','MISMATCH','O_MISMATCH']
        self.A_BUS_FIELDS['Character'] = ['NAME','EXNAME']
        self.BRN_FIELDS = {}
        self.BRN_FIELDS['Real'] = ['RATEA','RATEB','RATEC','RATE','LENGTH','CHARG','CHARGZ','MOVIPR','FRACT1','FRACT2','FRACT3','FRACT4']
        self.BRN_FIELDS['Integer'] = ['STATUS','METER','NMETR','OWNERS','OWN1','OWN2','OWN3','OWN4','SCTYP']
        self.BRN_FIELDS['Complex'] = ['RX','ISHNT','JSHNT','RXZ','ISHNTZ','JSHNTZ','LOSSES','O_LOSSES']
        self.BRN_FIELDS['Miscellaneous'] = ['MVA','O_MVA','AMPS','PUCUR','CURANG','PCTRTA','PCTRTB','PCTRTC','PCTMVA','PCTMVB','PCTMVC','PCTCPA','PCTCPB','PCTCPC','P','O_P','Q','O_Q','PLOS','O_PLOS','QLOS','O_QLOS']   
        self.A_BRN_FIELDS = {}
        self.A_BRN_FIELDS['Real'] = ['AMPS','PUCUR','PCTRATE','PCTRATEA','PCTRATEB','PCTRATEC','PCTMVARATE','PCTMVARATEA','PCTMVARATEB','PCTMVARATEC','PCTCORPRATE','PCTCORPRATEA','PCTCORPRATEB','PCTCORPRATEC','MAXPCTRATE','MAXPCTRATEA','MAXPCTRATEB','MAXPCTRATEC','MAXPCTCRPRATE','MAXPCTCRPRATEA','MAXPCTCRPRATEB','MAXPCTCRPRATEC','FRACT1','FRACT2','FRACT3','FRACT4','RATE','RATEA','RATEB','RATEC','LENGTH','CHARGING','CHARGINGZERO','MOVIRATED','P','Q','MVA','MAXMVA','PLOSS','QLOSS','O_P','O_Q','O_MVA','O_MAXMVA','O_PLOSS','O_QLOSS']
        self.A_BRN_FIELDS['Integer'] = ['FROMNUMBER','TONUMBER','STATUS','METERNUMBER','NMETERNUMBER','OWNERS','OWN1','OWN2','OWN3','OWN4','MOVTYPE']
        self.A_BRN_FIELDS['Complex'] = ['RX','FROMSHNT','TOSHNT','RXZERO','FRONSHNTZERO','TOSHNTZERO','PQ','PQLOSS','O_PQ','O_PQLOSS']
        self.A_BRN_FIELDS['Character'] = ['ID','FROMNAME','FROMEXNAME','TONAME','TOEXNAME','METERNAME','METEREXNAME','NMETERNAME','NMETEREXNAME']   
        self.A_TR3_FIELDS = {}
        self.A_TR3_FIELDS['Real'] = ['FRACT1','FRACT2','FRACT3','FRACT4','VMSTAR','ANSTAR','PLOSS','QLOSS','O_PLOSS','O_QLOSS']
        self.A_TR3_FIELDS['Integer'] = ['WIND1NUMBER','WIND2NUMBER','WIND3NUMBER','STATUS','NMETERNUMBER','OWNERS','OWN1','OWN2','OWN3','OWN4','CW','CZ','CM','CZ0','CZG','CNXCOD','ZADCOD']
        self.A_TR3_FIELDS['Complex'] = ['RX1-2ACT','RX1-2ACTCZ','RX1-2NOM','RX1-2NOMCZ','RX2-3ACT','RX2-3ACTCZ','RX2-3NOM','RX2-3NOMCZ','RX3-1ACT','RX3-1ACTCZ','RX3-1NOM','RX3-1NOMCZ','YMAG','YMAGCM','ZG1','ZGRND','ZG1CZG','ZG2','ZGRND2','ZG2CZG','ZG3','ZGRND3','ZG3CZG','Z01','Z02','Z03','Z01CZ0','Z02CZ0','Z03CZ0','ZNUTRL','ZNUTRLCZG','PQLOSS','O_PQLOSS']
        self.A_TR3_FIELDS['Character'] = ['ID','WIND1NAME','WIND1EXNAME','WIND2NAME','WIND2EXNAME','WIND3NAME','WIND3EXNAME','NMETERNAME','NMETEREXNAME','XFRNAME','VECTORGROUP']   
        self.A_TRN_FIELDS = {}
        self.A_TRN_FIELDS['Real'] = ['AMPS','PUCUR','PCTRATE','PCTRATEA','PCTRATEB','PCTRATEC','PCTMVARATE','PCTMVARATEA','PCTMVARATEB','PCTMVARATEC','PCTCORPRATE','PCTCORPRATEA','PCTCORPRATEB','PCTCORPRATEC','MAXPCTRATE','MAXPCTRATEA','MAXPCTRATEB','MAXPCTRATEC','MAXPCTCRPRATE','MAXPCTCRPRATEA','MAXPCTCRPRATEB','MAXPCTCRPRATEC','MXPCTMVARAT','MXPCTMVARATA','MXPCTMVARATB','MXPCTMVARATC','MXPCTCRPRAT','MXPCTCRPRATA','MXPCTCRPRATB','MXPCTCRPRATC','FRACT1','FRACT2','FRACT3','FRACT4','RATE','RATEA','RATEB','RATEC','RATIO','RATIOCW','RATIO2','RATIO2CW','ANGLE','RMAX','RMAXCW','RMIN','RMINCW','VMAX','VMAXKV','VMIN','VMINKV','STEP','STEPCW','CNXANG','NOMV1','NOMV2','SBASE1','P','Q','MVA','MAXMVA','PLOSS','QLOSS','O_P','O_Q','O_MVA','O_MAXMVA','O_PLOSS','O_QLOSS']
        self.A_TRN_FIELDS['Integer'] = ['FROMNUMBER','TONUMBER','STATUS','METERNUMBER','NMETERNUMBER','OWNERS','OWN1','OWN2','OWN3','OWN4','ICONTNUMBER','SIDCOD','WIND1NUMBER','WIND2NUMBER','TABLE','CODE','NTPOSN','CW','CZ','CM','CZ0','CZG','CNXCOD','TPSTT','ANSTT']
        self.A_TRN_FIELDS['Complex'] = ['RXACT','RXACTCZ','RXNOM','RXNOMCZ','YMAG','YMAGCM','COMPRX','RXZERO','ZG1','ZGRND','ZG1CZG','ZG2','ZGRND2','ZG2CZG','Z01','Z02','Z01CZ0','Z02CZ0','ZNUTRL','ZNUTRLCZG','PQ','PQLOSS','O_PQ','O_PQLOSS']
        self.A_TRN_FIELDS['Character'] = ['ID','FROMNAME','FROMEXNAME','TONAME','TOEXNAME','METERNAME','METEREXNAME','NMETERNAME','NMETEREXNAME','ICONTNAME','ICONTEXNAME','WIND1NAME','WIND1EXNAME','WIND2NAME','WIND2EXNAME','XFRNAME','VECTORGROUP']
        self.A_MACH_FIELDS = {}
        self.A_MACH_FIELDS['Integer'] = ['NUMBER','STATUS','WMOD','OWNERS','OWN1','OWN2','OWN3','OWN4','CZG']
        self.A_MACH_FIELDS['Real'] = ['FRACT1','FRACT2','FRACT3','FRACT4','PERCENT','MBASE','GENTAP','WPF','RPOS','XSUBTR','XTRANS','XSYNCH','PGEN','QGEN','MVA','PMAX','PMIN','QMAX','QMIN','O_PGEN','O_QGEN','O_MVA','O_PMAX','O_PMIN','O_QMAX','O_QMIN']
        self.A_MACH_FIELDS['Complex'] = ['ZSORCE','XTRAN','ZPOS','ZNEG','ZZERO','ZGRND','ZGRNDPU','PQGEN','O_PQGEN']
        self.A_MACH_FIELDS['Character'] = ['ID','NAME','EXNAME']
        self.A_LOAD_FIELDS = {}
        self.A_LOAD_FIELDS['Integer'] = ['NUMBER','TYPE','AREA','ZONE','OWNER','DUMMY','STATUS']
        self.A_LOAD_FIELDS['Real'] = ['BASE','PU','KV','ANGLE','ANGLED','MVAACT','MVANOM','ILACT','ILNOM','YLACT','YLNOM','TOTALACT','TOTALNOM','MISMATCH','O_MVAACT','O_MVANOM','O_ILACT','O_ILNOM','O_YLACT','O_YLNOM','O_TOTALACT','O_TOTALNOM','O_MISMATCH']
        self.A_LOAD_FIELDS['Complex'] = ['MVAACT','MVANOM','ILACT','ILNOM','YLACT','YLNOM','TOTALACT','TOTALNOM','MISMATCH','LDGNACT','LDGNNOM','O_MVAACT','O_MVANOM','O_ILACT','O_ILNOM','O_YLACT','O_YLNOM','O_TOTALACT','O_TOTALNOM','O_MISMATCH','O_LDGNACT','O_LDGNNOM']
        self.A_LOAD_FIELDS['Character'] = ['NAME','EXNAME']
        
        
    def __del__(self):
        """method automatically called when an instance of pypsse is deleted."""
        sys.stdout = self.__old_stdout__
        self.__delete_created_files__()


    def __reset__(self,delete_files=True):
        """Resets the class attributes so that the object may be reused without
        reinstantiation. This is especially helpful to get new results in the 
        pypsse.out attribute."""
        self.__delete_created_files__()
        dic = vars(self)
        for i in dic.keys():
            dic[i] = None
        self.__init__()


    def redirectoutput(self):
        if isinstance(sys.stdout,StringIO.StringIO):
            sys.stdout = self.__old_stdout__
            return True
        else:
            self.out = StringIO.StringIO()
            sys.stdout = self.out
            return True


    def __delete_created_files__(self,exclude_ext=''):
        """Deletes the files which PSS/E annoyingly requires the user to create"""
        def delete_file(path):
            try:
                os.remove(path)
            except OSError:
                pass
        if self.__internally_created_files__ is not None:
            for path in self.__internally_created_files__:
                if exclude_ext:
                    if not os.path.splitext(path)[1] == exclude_ext:
                        delete_file(path)
                else:
                    delete_file(path)   

        
    def record(self,filepath=''):
        if filepath == '':
            root = Tkinter.Tk()
            filepath = tkFileDialog.askopenfilename(title='Select output file',filetypes=[('Text','*.txt')])
            root.destroy()
        with open(filepath,'w') as f:
            f.write(self.out.getvalue())
        return True
    
    
    def opencase(self,casepath=''):
        """Opens PSS/E case"""
        if casepath == '':
            root = Tkinter.Tk()
            casepath = tkFileDialog.askopenfilename(title='Select PSS/E case',filetypes=[('PSS/E',('*.sav','*.raw'))])
            root.destroy()
        self.support_file_path = os.path.splitext(casepath)[0]
        psspy.psseinit(80000)
        if os.path.splitext(casepath)[1] == '.raw':
            version = input('PSS/E version (number 15-34) > ')
            if version not in range(15,35):
                raise ValueError('Incorrect number entered for PSS/E version: {}'.format(version))
            ierr = psspy.readrawversion(0,str(version),casepath)
            if ierr:
                self.error_message += ('Error opening case. API \'readrawversion\' error code %d/n' %ierr)    
                return False
            self.data = pypsseresultconverter.readrawversion(self.out.getvalue())
        else:
            ierr = psspy.case(casepath)
            if ierr:
                self.error_message += ('Error opening case. API \'case\' error code %d/n' %ierr)    
                return False
        return True
    
    
    def solvecase(self, method='FNSL',options=[0,0,0,0,0,0,0,0]):
        """Solves the case with the specified method and options. Returns
        False if the case does not solve, True if the case solves."""
        methods = ['FNSL','FDNS']
        if method not in methods:
            raise ValueError('Invalid method \'{}\'. Expected one of: {}/n'.format(method,methods))
            return False
        if method == 'FNSL':
            app = psspy.fnsl
        elif method == 'FDNS':
            app = psspy.fdns
        app(options)
        ierr = psspy.solved()
        if ierr:
            self.error_message += ('Error solving case. API \'{}\' code {}/n'.format(method.lower(),ierr))
            return False
        return True
    
    
    def savecaseas(self,path):
        ierr = psspy.save(path)
        if ierr:
            raise Warning('Case not saved. API \'save\' error code {}/n'.format(ierr))
        else:
            return True
        
                
###############################################################################
#### create, append, and complete file methods create external files which ####
#### are required by PSS/E to perform certain functions (e.g. dfax, tltg). ####
## The files are tracked in self.__internally_created_files__ and deleted on ##
############################## instance deletion ##############################
###############################################################################
    def create_sid(self,sid=0,buslist=[],arealist=[],filepath=''):
        """Creates a subsystem from buses and/or areas. From what I can tell, 
        this subsystem definition cannot be used like a *.sub file (e.g. to 
        create a DFAX), but can be used as an input to other APIs."""
        if sid not in xrange(11):
            raise ValueError('{} is not a valid subsystem ID. SIDs must be an integer in the range 0-10 (leaving 11 for internal method use).')
        ierr = psspy.bsys(sid=sid,numbus=len(buslist),buses=buslist,numarea=len(arealist),areas=arealist)
        if ierr:
            self.error_message += 'Error defining subsystem {}. API \'bsys\' error code {}.'.format(sid,ierr)
            return False
        if filepath:
            ierr = psspy.bsysmem(sid=sid,sfile=filepath)
            if ierr:
                self.error_message += 'Error recording subsystem {}. API \'bsysmem\' error code {}'.format(sid,ierr)
                return False
            self.__internally_created_files__ += filepath
        return True
    

    def create_subfile(self,filepath):
        """Creates a subsystem file from buses and/or areas."""
        text = 'COM\nCOM  Subystem file created through pypsse \nCOM\n'
        with open(filepath, 'w') as f:
            f.write(text)
        self.__internally_created_files__ += filepath
        return True
    
    
    def append_subfile(self,filepath,subname,buslist=[],arealist=[],partlist=[]):
        """Appends a subsystem to an existing subsystem file from buses and/or areas."""
        text = 'SUBSYSTEM \'{}\'\n'.format(subname)
        for b in buslist:
            text += '\tBUS {}\n'.format(b)
        if partlist:
            if len(partlist) == len(buslist):
                text += '\tPARTICIPATE\n'
                for n in xrange(len(partlist)):
                    text += '\t\tBUS {} {}\n'.format(buslist[n],partlist[n])
                text += '\tEND\n'
            else:
                raise ValueError('Participation factor list is length {} and bus list is length {}. These must be the same length.'.format(len(partlist),len(buslist)))
        for a in arealist:
            text += '\tAREA {}\n'.format(a)
        text += 'END\n'
        with open(filepath, 'a') as f:
            f.write(text)
        return True
    
    
    def create_confile(self,filepath,subname,desc=None,element=None):
        text = 'COM\nCOM  Contingency file created through pypsse \nCOM\n'
        if desc is not None and element is not None:
            text += desc + ' ' + element + ' IN SUBSYSTEM \'' + subname + '\'\n'
        with open(filepath, 'w') as f:
            f.write(text)
        self.__internally_created_files__ += filepath
        return True


    def append_confile(self,filepath,subname,desc='SINGLE',element='BRANCH'):
        text = desc + ' ' + element + ' IN SUBSYSTEM ' + subname + '\n'
        with open(filepath, 'a') as f:
            f.write(text)
        return True
    
    
    def create_monfile(self,filepath):
        text = 'COM\nCOM  Monitored element file created through pypsse \nCOM\n'
        with open(filepath, 'w') as f:
            f.write(text)
        self.__internally_created_files__ += filepath
        return True
    
    
    def append_monfile(self,filepath,sublist,midtext='BRANCHES IN',fintext=''):
        text = '\n'
        for s in sublist:
            text += 'MONITOR {} SUBSYSTEM \'{}\' {}\n'.format(midtext,s,fintext)
        with open(filepath, 'a') as f:
            f.write(text)
        return True
    
    
    def complete_file(self,filepath):
        """Completes the file by adding END"""
        text = '\nEND\n'
        with open(filepath, 'a') as f:
            f.write(text)
        return True
    
        
    def create_dfax(self,filepath,subfilepath,monfilepath,confilepath,options=[_i,_i]):
        ierr = psspy.dfax(options,subfilepath,monfilepath,confilepath,filepath)
        if ierr:
            self.error_message += 'Error creating Distribution Factor file. API \'dfax\' error code {}\n'.format(ierr)
            return False
        self.__internally_created_files__ += filepath
        return True
    
###############################################################################
#### exists methods return boolean True if specified member exists in that ####
################################### data family ###############################
###############################################################################
    def bus_exists(self,ibus):
        """Returns boolean value."""
        return psspy.busexs(ibus)==0


    def branch_exists(self,ibus=0,jbus=0):
        """Returns boolean value."""
        if self.bus_exists(ibus) and self.bus_exists(jbus):
            ierr = psspy.bsys(sid=11, numbus=2, buses=[ibus,jbus])
            if not ierr:
                df = self.get_multiple_branch_data(sid=11,datafields=['STATUS'])
                if not df.dropna().empty:
                    return True
                else:
                    return False
            else:
                self.error_message += 'Error creating subsystem for buses {} and {}. API \'bsys\' code {}/n'.format(ibus,jbus,ierr)
        return None
    
    
    def load_exists(self,loadbusnum):
        """Returns boolean value."""
        fields = ['NUMBER','STATUS']
        ierr, arr = psspy.alodbusint(sid=-1,flag=4,string=fields)
        if not ierr:
            return loadbusnum in arr[0]
        else:
            self.error_message += 'Error retrieving load bus data. API \'alodbusint\' code {}/n'.format(ierr)
        return None


    def machine_exists(self,machbusnum):
        """Returns boolean value."""
        ierr, arr = psspy.amachint(sid=-1,flag=4,string=['NUMBER','STATUS'])
        if not ierr:
            return machbusnum in arr[0]
        else:
            self.error_message += 'Error retrieving machine buses. API \'amachint\' code {}/n'.format(ierr)
        return None

###############################################################################
###### get_single methods pull data for a single element for the bus or #######
############################ branch data family ###############################
###############################################################################
    def get_single_bus_data(self,ibus=0,datafields=[],other=None):
        """Returns a series of bus data with field names as indices. If the 
        field is invalid, returns NaN for that index."""
        if not datafields:
            for k in self.BUS_FIELDS.keys():
                datafields += self.BUS_FIELDS[k]
        s = pd.Series(np.empty(len(datafields)).fill(np.nan),datafields)
        if not ibus:
            return s
        for fld in datafields:
            dtype = None
            app = None
            if fld in self.BUS_FIELDS['Real']:
                app = psspy.busdat
                dtype = float
            elif fld in self.BUS_FIELDS['Integer']:
                app = psspy.busint
                dtype = int
            elif fld in self.BUS_FIELDS['Complex']:
                app = psspy.busdt1
                dtype = float
                if not other:
                    other = 'NOM'
            else:
                continue
            if not other:
                ierr, val = app(ibus,fld)
            else:
                ierr, val = app(ibus,fld,other)
            if not ierr:
                try:
                    s[fld] = dtype(val)
                except Exception as e:
                    self.error_message += 'Error adding bus {} {} data series to dataframe: {}'.format(ibus,fld,str(e))
            else:
                self.error_message += 'Error retrieving bus {} {} data. API \'{}\' code {}/n'.format(ibus,fld,str(app).split(' ')[1],ierr)
        return s
    
    
    def get_single_branch_data(self,ibus=0,jbus=0,ckt='1',datafields=[]):
        """Returns branch data as a series with field names as indexes. If the 
        field is invalid, returns NaN for that index."""
        if not datafields:
            for k in self.BRN_FIELDS.keys():
                datafields += self.BRN_FIELDS[k]
        s = pd.Series(np.nan,datafields)
        for fld in datafields:
            dtype = None
            app = None
            if fld in self.BRN_FIELDS['Real']:
                app = psspy.brndat
                dtype = float
            elif fld in self.BRN_FIELDS['Complex']:
                app = psspy.brndt2
                dtype = float
            elif fld in self.BRN_FIELDS['Integer']:
                app = psspy.brnint
                dtype = int
            elif fld in self.BRN_FIELDS['Miscellaneous']:
                app = psspy.brnmsc
                dtype = str
            else:
                continue
            ierr, val = app(ibus,jbus,ckt,fld)
            if not ierr:
                try:
                    s[fld] = dtype(val)
                except Exception as e:
                    self.error_message += 'Error adding branch {}-{} {} data series to dataframe: {}'.format(ibus,jbus,fld,str(e))
            else:
                self.error_message += 'Error retrieving data for branch {}-{} {}. API \'{}\' code {}/n'.format(ibus,jbus,fld.lower(),str(app).split(' ')[1],ierr)
            return s


###############################################################################
##### get_multiple methods are the prefered methods for pulling case data #####
###############################################################################
    def __dtype_map__(self,psse_dtype='C'):
        """Internal function maps between PSSE indicators of datatypes and 
        Python data types"""
        if psse_dtype == 'I':
            return int
        elif psse_dtype == 'R':
            return float
        elif psse_dtype == 'X':
            return str
        elif psse_dtype == 'C':
            return str
        else:
            return None
        
        
    def __add_arr__(self,df,arrapp,colname,dtype=str,*args,**kwargs):
        """Internal function combines psse array API (arrapp) results with existing dataframe.
        The kwargs are for the arrapp."""
        ierr, arr = arrapp(*args,**kwargs)
        data = arr[0]
        if dtype == str and data is not None:
            data = [str(x).strip() for x in data]
        if ierr:
            self.error_message += 'Error retrieving {} data: \nAPI \'{}\' error code {}'.format(colname,str(arrapp),ierr)
            return df.copy()
        if len(df.index) != len(data):
            raise ValueError('Dataframe must be the same length as the array results. Dataframe length: {}. Array length: {}.'.format(len(df.index),len(arr[0])))
        try:
            df[colname] = pd.Series(index=df.index,data=data,dtype=dtype)
        except Exception as e:
            self.error_message += 'Error adding {} data series to dataframe: {}'.format(colname,str(e))
        print(data)
        return df.copy()
        

    def get_multiple_bus_data(self,sid=None,ibuslist=[],datafields=[],flag=2):
        """Returns a dataframe of bus data for buses in the specified SID or bus list. If the field is invalid, returns NaN for that column."""
        if not datafields:
            for k in self.A_BUS_FIELDS.keys():
                datafields += self.A_BUS_FIELDS[k]
        if sid and ibuslist:
            print('Both sid and busnumlist were provided. Only using sid.')
        if not sid:
            sid = 11
            ierr = psspy.bsys(sid=sid, numbus=len(ibuslist), buses=ibuslist)
            if ierr:
                self.error_message += 'Bus system not created for {}. API \'bsys\' error code {}.'.format(ibuslist,ierr)
                return pd.DataFrame()
            df_sid = self.get_multiple_bus_data(sid=sid,datafields=datafields,flag=flag)
            df = pd.DataFrame(index=ibuslist).join(df_sid,how='left')
        elif sid:
            ierr, ibuslist = psspy.abusint(sid=sid,flag=flag,string='NUMBER')
            ibuslist = ibuslist[0]
            df = pd.DataFrame(index=ibuslist,data=np.empty((len(ibuslist),len(datafields))).fill(np.nan),columns=datafields)
            dtypes = psspy.abustypes(datafields)[1]
            self.debug = dtypes, datafields
            for n in xrange(len(datafields)):
                if dtypes[n] == 'I':
                    app = psspy.abusint
                elif dtypes[n] == 'R':
                    app = psspy.abusreal
                elif dtypes[n] == 'X':
                    app = psspy.abuscplx
                elif dtypes[n] == 'C':
                    app = psspy.abuschar
                else:
                    continue
                d = self.__dtype_map__(dtypes[n])
                fld = datafields[n]
                df = self.__add_arr__(df,app,fld,d,sid=sid,flag=flag,string=fld)    
        else:
            df = pd.DataFrame(columns=datafields)
        df.index.name = 'NUMBER'
        return df
    

    def get_multiple_branch_data(self,sid=None,ibuslist=[],jbuslist=[],cktlist=[],datafields=[],flag=4):
        """Returns a dataframe of branch data for branches in the specified SID or bus lists. If the field is invalid, returns NaN for that column."""
        # data check
        if len(ibuslist) != len(jbuslist) or len(jbuslist) != len(cktlist):
            raise ValueError('Dimensions of ibuslist, jbuslist, and cktlist must be the same')
        if not datafields:
            for k in self.A_BRN_FIELDS.keys():
                datafields += self.A_BRN_FIELDS[k]
        for c in ['FROMNUMBER','TONUMBER','ID']:
            if c not in datafields:
                datafields.append(c)
        if sid and ibuslist:
            print('Both sid and busnumlist were provided. Only using sid.')
        # results for specified branches
        if not sid and ibuslist:
            sid = 11
            sysbuslist = ibuslist
            sysbuslist = sysbuslist + [x for x in jbuslist if x not in sysbuslist]
            ierr = psspy.bsys(sid=sid, numbus=len(sysbuslist), buses=sysbuslist)
            if ierr:
                self.error_message += 'Bus system not created. API \'bsys\' error code {}.'.format(ierr)
                return pd.DataFrame()
            # get entire sid branch info and include to as from and from as to
            df_sid = self.get_multiple_branch_data(sid=sid,datafields=datafields,flag=flag)
            df_sid_2 = df_sid.copy()
            df_sid_2['FROMNUMBER'] = df_sid['TONUMBER']
            df_sid_2['TONUMBER'] = df_sid['FROMNUMBER']
            df_sid = df_sid.append(df_sid_2)
            df = pd.DataFrame(index=xrange(len(ibuslist)))
            df['FROMNUMBER'] = pd.Series(index=df.index,data=ibuslist,dtype=int)
            df['TONUMBER'] = pd.Series(index=df.index,data=jbuslist,dtype=int)
            df['ID'] = pd.Series(index=df.index,data=cktlist,dtype=str)            
            df = df.merge(df_sid,how='left',on=['FROMNUMBER','TONUMBER','ID'])
        # results for specified sid
        elif sid:
            ierr, ibuslist = psspy.abrnint(sid=sid,flag=flag,string='FROMNUMBER')
            ibuslist = ibuslist[0]
            if ibuslist:
                df = pd.DataFrame(index=xrange(len(ibuslist)),data=np.empty((len(ibuslist),len(datafields))).fill(np.nan),columns=datafields)
                dtypes = psspy.abrntypes(datafields)[1]
                for n in xrange(len(datafields)):
                    if dtypes[n] == 'I':
                        app = psspy.abrnint
                    elif dtypes[n] == 'R':
                        app = psspy.abrnreal
                    elif dtypes[n] == 'X':
                        app = psspy.abrncplx
                    elif dtypes[n] == 'C':
                        app = psspy.abrnchar
                    else:
                        continue
                    d = self.__dtype_map__(dtypes[n])
                    fld = datafields[n]
                    df = self.__add_arr__(df,app,fld,d,sid=sid,flag=flag,string=fld)                
            else:
                df = pd.DataFrame(columns=datafields)   
        else:
            df = pd.DataFrame(columns=datafields)
        return df 
       

    def get_multiple_trn_data(self,sid=None,ibuslist=[],jbuslist=[],cktlist=[],datafields=[],flag=4):
        """Returns a dataframe of branch data for branches in the specified SID or bus lists. If the field is invalid, returns NaN for that column."""
        # data check
        if len(ibuslist) != len(jbuslist) or len(jbuslist) != len(cktlist):
            raise ValueError('Dimensions of ibuslist, jbuslist, and cktlist must be the same')
        if not datafields:
            for k in self.A_BRN_FIELDS.keys():
                datafields += self.A_BRN_FIELDS[k]
        for c in ['FROMNUMBER','TONUMBER','ID']:
            if c not in datafields:
                datafields.append(c)
        if sid and ibuslist:
            print('Both sid and busnumlist were provided. Only using sid.')
        # results for specified branches
        if not sid and ibuslist:
            sid = 11
            sysbuslist = ibuslist
            sysbuslist = sysbuslist + [x for x in jbuslist if x not in sysbuslist]
            ierr = psspy.bsys(sid=sid, numbus=len(sysbuslist), buses=sysbuslist)
            if ierr:
                self.error_message += 'Bus system not created. API \'bsys\' error code {}.'.format(ierr)
                return pd.DataFrame()
            # get entire sid branch info and include to as from and from as to
            df_sid = self.get_multiple_trn_data(sid=sid,datafields=datafields,flag=flag)
            df_sid_2 = df_sid.copy()
            df_sid_2['FROMNUMBER'] = df_sid['TONUMBER']
            df_sid_2['TONUMBER'] = df_sid['FROMNUMBER']
            df_sid = df_sid.append(df_sid_2)
            df = pd.DataFrame(index=xrange(len(ibuslist)))
            df['FROMNUMBER'] = pd.Series(index=df.index,data=ibuslist,dtype=int)
            df['TONUMBER'] = pd.Series(index=df.index,data=jbuslist,dtype=int)
            df['ID'] = pd.Series(index=df.index,data=cktlist,dtype=str)            
            df = df.merge(df_sid,how='left',on=['FROMNUMBER','TONUMBER','ID'])
        # results for specified sid
        elif sid:
            ierr, ibuslist = psspy.atrnint(sid=sid,flag=flag,string='FROMNUMBER')[1]
            ibuslist = ibuslist[0]
            if ibuslist:
                df = pd.DataFrame(index=xrange(len(ibuslist)),data=np.empty((len(ibuslist),len(datafields))).fill(np.nan),columns=datafields)
                dtypes = psspy.atrntypes(datafields)
                for n in xrange(len(datafields)):
                    if dtypes[n] == 'I':
                        app = psspy.atrnint
                    elif dtypes[n] == 'R':
                        app = psspy.atrnreal
                    elif dtypes[n] == 'X':
                        app = psspy.atrncplx
                    elif dtypes[n] == 'C':
                        app = psspy.atrnchar
                    else:
                        continue
                    d = self.__dtype_map__(dtypes[n])
                    fld = datafields[n]
                    df = self.__add_arr__(df,app,fld,d,sid=sid,flag=flag,string=fld) 
            else:
                df = pd.DataFrame(columns=datafields)        
        else:
            df = pd.DataFrame(columns=datafields)        
        return df 
    
    
    def get_multiple_tr3_data(self,sid=None,ibuslist=[],jbuslist=[],kbuslist=[],datafields=[],flag=4):
        """Returns a dataframe of three winding transformers as specified in the buslist or in the SID"""
        # data check
        if len(ibuslist) != len(jbuslist) or len(jbuslist) != len(kbuslist):
            raise ValueError('Dimensions of ibuslist, jbuslist, and cktlist must be the same')
        if not datafields:
            for k in self.A_TR3_FIELDS.keys():
                datafields += self.A_TR3_FIELDS[k]
        for c in ['WIND1NUMBER','WIND2NUMBER','WIND3NUMBER']:
            if c not in datafields:
                datafields.append(c)
        if sid and ibuslist:
            print('Both sid and busnumlist were provided. Only using sid.')
        # results for specified branches
        if not sid and (ibuslist and jbuslist and kbuslist):
            sid = 11
            sysbuslist = ibuslist
            sysbuslist = sysbuslist + [x for x in jbuslist if x not in sysbuslist]
            sysbuslist = sysbuslist + [x for x in kbuslist if x not in sysbuslist]
            ierr = psspy.bsys(sid=sid, numbus=len(sysbuslist), buses=sysbuslist)
            if ierr:
                self.error_message += 'Bus system not created. API \'bsys\' error code {}.'.format(ierr)
                return pd.DataFrame()
            # get entire sid branch info and include to as from and from as to
            df = self.get_multiple_tr3_data(sid=sid,datafields=datafields)
        # results for specified sid
        elif sid:
            ierr, ibuslist = psspy.abrnint(sid=sid,flag=flag,string='FROMNUMBER')
            ibuslist = ibuslist[0]
            if ibuslist:
                df = pd.DataFrame(index=xrange(len(ibuslist)),data=np.empty((len(ibuslist),len(datafields))).fill(np.nan),columns=datafields)
                dtypes = psspy.atr3types(datafields)[1]
                for n in xrange(len(datafields)):
                    if dtypes[n] == 'I':
                        app = psspy.atr3int
                    elif dtypes[n] == 'R':
                        app = psspy.atr3real
                    elif dtypes[n] == 'X':
                        app = psspy.atr3cplx
                    elif dtypes[n] == 'C':
                        app = psspy.atr3char
                    else:
                        continue
                    d = self.__dtype_map__(dtypes[n])
                    fld = datafields[n]
                    df = self.__add_arr__(df,app,fld,d,sid=sid,flag=flag,string=fld) 
            else:
                df = pd.DataFrame(columns=datafields)           
        else:
            df = pd.DataFrame(columns=datafields)
        return df 


    def get_multiple_machine_data(self,sid=None,buslist=[],datafields=[],flag=4):
        """Returns a dataframe of machine data for machines in the specified SID or bus list. If the field is invalid, returns NaN for that column."""
        if not datafields:
            for k in self.A_MACH_FIELDS.keys():
                datafields += self.A_MACH_FIELDS[k]
        if sid and buslist:
            print('Both sid and buslist were provided. Only using sid.')
        if not sid and buslist:
            sid = 11
            ierr = psspy.bsys(sid=sid, numbus=len(buslist), buses=buslist)
            if ierr:
                self.error_message += 'Bus system not created for {}. API \'bsys\' error code {}.'.format(buslist,ierr)
                return pd.DataFrame()
            df_sid = self.get_multiple_machine_data(sid=sid,datafields=datafields)
            df = pd.DataFrame(index=buslist).join(df_sid,how='left')
        elif sid:
            ierr, buslist = psspy.amachint(sid=sid,flag=flag,string='NUMBER')
            buslist = buslist[0]
            df = pd.DataFrame(index=buslist,data=np.empty((len(buslist),len(datafields))).fill(np.nan),columns=datafields)
            dtypes = psspy.amachtypes(datafields)[1]
            for n in xrange(len(datafields)):
                if dtypes[n] == 'I':
                    app = psspy.amachint
                elif dtypes[n] == 'R':
                    app = psspy.amachreal
                elif dtypes[n] == 'X':
                    app = psspy.amachcplx
                elif dtypes[n] == 'C':
                    app = psspy.amachchar
                else:
                    continue
                d = self.__dtype_map__(dtypes[n])
                fld = datafields[n]
                df = self.__add_arr__(df,app,fld,d,sid=sid,flag=flag,string=fld) 
        else:
            df = pd.DataFrame(datafields)
        df.index.name = 'NUMBER'
        return df
       
    
    def get_multiple_load_data(self,sid=None,buslist=[],datafields=[],flag=4):
        """Returns a dataframe of load data for loads in the specified SID or bus list. If the field is invalid, returns NaN for that column."""
        if not datafields:
            for k in self.A_LOAD_FIELDS.keys():
                datafields += self.A_LOAD_FIELDS[k]
        if sid and buslist:
            print('Both sid and buslist were provided. Only using sid.')
        if not sid and buslist:
            sid = 11
            ierr = psspy.bsys(sid=sid, numbus=len(buslist), buses=buslist)
            if ierr:
                self.error_message += 'Bus system not created for {}. API \'bsys\' error code {}.'.format(buslist,ierr)
                return pd.DataFrame()
            df_sid = self.get_multiple_load_data(sid=sid,datafields=datafields)
            df = pd.DataFrame(index=buslist).join(df_sid,how='left')
        elif sid:
            ierr, buslist = psspy.amachint(sid=sid,flag=flag,string='NUMBER')
            buslist = buslist[0]
            df = pd.DataFrame(index=buslist,data=np.empty((len(buslist),len(datafields))).fill(np.nan),columns=datafields)
            dtypes = psspy.aloadtypes(datafields)[1]
            for n in xrange(len(datafields)):
                if dtypes[n] == 'I':
                    app = psspy.aloadint
                elif dtypes[n] == 'R':
                    app = psspy.aloadreal
                elif dtypes[n] == 'X':
                    app = psspy.aloadcplx
                elif dtypes[n] == 'C':
                    app = psspy.aloadchar
                else:
                    continue
                d = self.__dtype_map__(dtypes[n])
                fld = datafields[n]
                df = self.__add_arr__(df,app,fld,d,sid=sid,flag=flag,string=fld) 
        else:
            df = pd.DataFrame(datafields)
        df.index.name = 'NUMBER'
        return df
    
    
###############################################################################
###### Specialized get functions do not have corresponding PSSE APIs and ######
### so achieve their pull using available APIs and some data manipulations ####
###############################################################################
    def __get_next_node__(self,br_df,trn_df,tr3_df,busnum):
        df = br_df[(br_df['FROMNUMBER'] == busnum) | (br_df['TONUMBER'] == busnum)]
        buses = df['FROMNUMBER'].tolist()
        buses.extend(df['TONUMBER'].tolist())
        df = trn_df[(trn_df['FROMNUMBER'] == busnum) | (trn_df['TONUMBER'] == busnum)]
        buses.extend(df['FROMNUMBER'].tolist())
        buses.extend(df['TONUMBER'].tolist())
        df = tr3_df[(tr3_df['WIND1NUMBER'] == busnum) | (tr3_df['WIND2NUMBER'] == busnum) | (tr3_df['WIND3NUMBER'] == busnum)]
        buses.extend(df['WIND1NUMBER'].tolist())
        buses.extend(df['WIND2NUMBER'].tolist())
        buses.extend(df['WIND3NUMBER'].tolist())
        self.debug = buses
        return list(set(buses))


    def get_xnode_buses(self,busnum,x,datafields=[]):
        """Returns a dataframe of all the buses within x nodes of the given bus."""
        br_df = self.get_multiple_branch_data(sid=-1,datafields=['FROMNUMBER','TONUMBER','ID'])
        trn_df = self.get_multiple_trn_data(sid=-1,datafields=['FROMNUMBER','TONUMBER','ID'])
        tr3_df = self.get_multiple_tr3_data(sid=-1,datafields=['WIND1NUMBER','WIND2NUMBER','WIND3NUMBER'])
        i = 0
        buslist = []
        nextbuslist = [busnum]
        while i < x:
            thisbuslist = nextbuslist
            nextbuslist = []
            for b in thisbuslist:
                nextbuslist.extend(self.__get_next_node__(br_df=br_df,trn_df=trn_df,tr3_df=tr3_df,busnum=b))
            i += 1
            buslist.extend(nextbuslist)
        buslist = list(set(buslist))
        return self.get_multiple_bus_data(ibuslist=buslist,datafields=datafields)

    
    
###############################################################################
####### create functions add members to the specified PSSE data family ########
###############################################################################
    def create_bus_from_tap(self,frmbus,tobus,ckt='1',fraction=0.50,newnum=None,newnam=None,newkv=''):
        """Creates a new bus along the specified existing branch.
        By default, searches for the next bus number available above 90000 and
        names the bus 'NEWBUS9XXXX. Return a dataframe of the new bus."""
        if newnum is None:
            newnum = 999997
            while newnum > 0:
                if not self.bus_exists(newnum):
                    break
                newnum -= 1
        if newnam is None:
            newnam = 'NEWBUS{}'.format(newnum)
        if newkv is None:
            newkv = ''
        ierr = psspy.ltap(frmbus=frmbus,tobus=tobus,ckt=ckt,fraction=fraction,newnum=newnum,newnam=newnam,newkv=newkv)
        if ierr:
            self.error_message += 'Error splitting bus {} - {}. API \'ltap\' code {}'.format(frmbus,tobus,ierr)
        return self.get_multiple_bus_data(ibuslist=[newnum])
        
    
    def create_bus_from_split(self,bus,newnum=None,newnam=None,newkv=None):
        """Creates a new bus by splitting the specified existing bus. 
        By default, searches for the next bus number available below 99999 and
        names the bus 'NEWBUS9XXXX. Return a dataframe of the new bus."""
        if newnum is None:
            newnum = 999997
            while newnum > 0:
                if not self.bus_exists(newnum):
                    break
                newnum -= 1
        if newnam is None:
            newnam = 'NEWBUS{}'.format(newnum)
        if newkv is None:
            newkv = _f
        ierr = psspy.splt(bus=bus,newnum=newnum,newnam=newnam,newkv=newkv)
        if ierr:
            self.error_message += 'Error splitting bus {}. API \'splt\' code {}'.format(bus,ierr)
        return self.get_multiple_bus_data(ibuslist=[newnum])
        
    
    def create_gen_from_split(self,bus,capacity,*args,**kwargs):
        """Creates a plant at the specified bus"""
        if 'newkv' not in kwargs:
            kwargs['newkv'] = 34.5
        df = self.create_bus_from_split(bus,*args,**kwargs)
        newbus = df['NUMBER'].max()
        ierr = psspy.bus_data_3(newbus,intgar1=2,name='NEWGEN{}'.format(newbus))
        if ierr:
            self.error_message += 'Error modifying bus data. API \'bus_data_3\' code {}'.format(ierr)
            return None
        vreg = self.get_multiple_bus_data(ibuslist=[bus],datafields=['PU'])['PU'].max()
        ierr = psspy.plant_data(newbus,bus,[vreg,100.0])
        if ierr:
            self.error_message += 'Error creating plant. API \'plant_data\' code {}'.format(ierr)
        ierr = psspy.machine_data_2(newbus,r'1',realar3=capacity/3,realar4=-capacity/3,realar5=capacity,realar6=0.0,realar8=_i,realar9=_i)
        if ierr:
            self.error_message += 'Error creating machine. API \'machine_data_2\' code {}'.format(ierr)
        return self.get_multiple_machine_data(buslist=[newbus])   
        
        
    def create_gen(self,name,busnum,capacity,newbusnum,newgenbusnum,newgenbuskv):
        """Splits the specified bus and inserts a generator at the new bus"""
        ierr = psspy.splt(poi_busnum,new_poi_busnum,'POI ' + str(name))
        if not ierr:
            try:
                areanum = self.__get_bus_areas__([poi_busnum])[0]
                zonenum = self.__get_bus_zones__([poi_busnum])[0]
                vreg = self.__get_bus_pu__([poi_busnum])[0]
            except Exception as e:
                self.error_message += 'Error retrieving bus area: {}'.format(str(e))
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
        return ierr

    def __dispatch_gen__(self,busnum,uid='1',pgen=_f):
        """redispatches the assigned generator to the specified power output"""
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
        return ierr

    
    def __create_load__(self,busnum,uid='1',pload=_f,qload=_f):
        """redispatches the assigned generator to the specified power output"""
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
        return ierr
    
    def __change_load__(self,busnum,uid='1',pload=_f,qload=_f,status=None):
        """redispatches the assigned generator to the specified power output"""
        intgar = [_i if not status else status,_i,_i,_i,_i,_i,_i]
        realar = [pload,qload,_f,_f,_f,_f,_f,_f]
        ierr = psspy.load_chng_5(busnum,uid,intgar,realar)
        if ierr:
            self.error_message += 'API \'load_chng_5\' error code {} for {}\n'.format(ierr,busnum)
        return ierr

if __name__ == '__main__':
    psse = pypsse()
    psse.opencase()
    psse.solvecase()
    



