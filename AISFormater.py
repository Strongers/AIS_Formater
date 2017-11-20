# -*- coding: utf-8 -*-
"""
Created on Sat Nov 18 11:07:45 2017

@author: Stronger
@title: Preprocess AIS data 
"""
import os
import pandas as pd
import numpy as np
import time

# ---- Function -----

class AISFormater:
    '''
    Toolkit of AIS data processing
    @param folder_path a folder contains all ais record which in csv format 
    '''
    record_folder = None
    output_folder = None
    
    def __init__(self, record_folder, output_folder):
        self.record_folder = record_folder
        self.output_folder = output_folder
    
    def readCSV(self, n_file=0):
        '''
        Read csv file of a folder and preprocessing.
            1. Drop columns no use
            2. Drop record not ship
            3. Transform timestamp to time tuple
        @param n_file if n_file is defined, then will return top n files.
        '''
        # set work space
        os.chdir(self.record_folder)
        # get all file_name
        file_name = os.listdir()
        # get the number of file we want
        if(n_file == 0): 
            n_file = len(file_name)
        else:
            pass
        data_raw = pd.read_csv(file_name[0])
        if(n_file > 1):
            for i in np.arange(1, n_file):
                data_temp = pd.read_csv(file_name[i])
                data_raw = pd.concat([data_raw, data_temp], ignore_index=True)
        else:
            pass
        '''
        Drop columns no use. Left attributes 'unique_ID',
        'acquisition_time', 'target_type', 'status', 'longitude', 
        'latitude', 'speed'
        '''
        data_clean = data_raw[['unique_ID', 'acquisition_time', 
                               'target_type', 'status', 'longitude', 
                               'latitude', 'speed']]
        # select data of ship
        n_data_raw = data_clean.shape[0]
        data_clean = data_clean[data_clean['target_type'] == 0]
        n_data_ship = data_clean.shape[0]
        print('---------------------------------------')
        print('All record: %s' % n_data_raw)
        print('Ship record: %s (%s %% of all record)' % (n_data_ship, 
              round(n_data_ship*100/n_data_raw, 2)))
        print('---------------------------------------')
        # time attribute processing
        data_clean['date'] = [time.gmtime(date_i) 
        for date_i in data_clean['acquisition_time']]
        return data_clean
    
    def get_mmsi(self, data):
        '''
        Get a list contains all MMSI.
        Drop MMSI which length is not equal to 9 
        '''
        mmsi = data['unique_ID'].drop_duplicates()
        print('---------------------------------------')
        print('Count of ship: %s' % mmsi.shape[0])
        mmsi_len = [len(str(x)) for x in mmsi]
        mmsi = pd.DataFrame({'mmsi': mmsi,
                             'length': mmsi_len})
        mmsi = mmsi[mmsi['length'] == 9]['mmsi']
        print('Count of ship with right format mmsi: %s (%s %% of all ship)' %
              (len(mmsi), round(len(mmsi)*100/len(mmsi_len), 2)))
        print('---------------------------------------')
        return mmsi
    
    def random_select(self, mmsi, n_row):
        '''
        Select ship randomly. Return the mmsi list of the selected ship
        '''
        mmsi_selected = mmsi.sample(n_row)
        print('Select %s ships (%s %% of all)' %
              (n_row, round(n_row*100/len(mmsi), 2)))
        return mmsi_selected
    
    def get_day(self, data, dateStr):
        '''
        Get record of target day.
        @param dateStr must be string like '2017-10-12'
        '''
        day = dateStr.split('-')
        data['select_day'] = [((day_i[0]==int(day[0])) & (day_i[1]==int(day[1])) & (day_i[2]==int(day[2]))) 
        for day_i in data['date']]
        data_selected = data[data.select_day]
        del data_selected['select_day']
        print('---------------------------------------')
        print('%s has %s records' % (dateStr, data_selected.shape[0]))
        print('---------------------------------------')
        return data_selected
        
    def get_ships(self, data, mmsi):
        '''
        Get record of target ships
        @param mmsi a list contains all mmsi of target ships
        '''
        data_selected = data[data['unique_ID'].isin(mmsi)]
        print('---------------------------------------')
        print('Select %s records (%s %% of all)'
              % (data_selected.shape[0], round(data_selected.shape[0]*100/data.shape[0], 2)))
        print('---------------------------------------')
        return data_selected
     
    def csv_writer(self, data):
        '''
        Write target data to csv file with filename '20171012sample.csv'
        '''
        os.chdir(self.output_folder)
        index1 = data.index[0]
        day = ("%s-%s-%s" % (data['date'][index1][0], data['date'][index1][1], data['date'][index1][2]))
        file_name = ('%ssample.csv' % day)
        # add new date attribute
        data['date'] = [time.strftime('%Y-%m-%d %H:%M:%S', date_i) for date_i in data['date']]
        data.to_csv(file_name, index=False, sep=',')
        print('%s write to %s' % (day, file_name))
        return None
    
    def sample_day(self, data, day, n_sample):
        '''
        Sampling from the dataset and write to csv file.
        Note: One ship only have one record.
        @param day in '2017-10-12'
        @param n_sample number of sample
        '''
        data_day = self.get_day(data, day)
        mmsi = self.get_mmsi(data_day)
        ship_selected = self.random_select(mmsi, n_sample)
        data_sample = self.get_ships(data_day, ship_selected)
        data_sample = data_sample.drop_duplicates('unique_ID')
        self.csv_writer(data_sample)
        return None
    

# ---- Main ----
# test
#formater = AISFormater('/home/stronger/文档/ais/record', '/home/stronger/文档/ais/sample')
#data_test = formater.readCSV()
#formater.sample_day(data_test, '2016-07-20', 500)
#formater.sample_day(data_test, '2016-08-20', 500)
#formater.sample_day(data_test, '2016-09-20', 500)