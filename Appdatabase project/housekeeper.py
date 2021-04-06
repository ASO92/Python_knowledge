import os
from os import path 
import json
import csv
import pandas as pd
import shutil

class data_housekeeper():
    
    def __init__(self):
        pass

        
    @staticmethod
    def create_dir(myDir):
        try:
            if not path.exists(myDir):
                print('Creating dir: ', myDir)
                os.mkdir(myDir)
        except AccessDeniedError as err:
            print('No dir created: ', err)

    @staticmethod
    def delete_dir(myDir):
        try:
            if path.exists(myDir):
                print('Deleting dir: ', myDir)
                os.rmdir(myDir)
            else:
                print('Inexistent folder')
        except AccessDeniedError as err:
            print('No dir deleted: ', err)

    @staticmethod
    def delete_non_empty_dir(myDir):
        try:
            if path.exists(myDir):
                print('Deleting dir: ', myDir)
                shutil.rmtree(myDir)
        except:
            print('No dir deleted: ')        

    @staticmethod
    def create_nested_dir_path(dir_list):
        nested_path = '/' + '/'.join(dir_list)
        return nested_path
    
    
    @staticmethod
    def create_file_path_in_nested_dir(dir_list, file_name):
        nested_dir_path_ = data_housekeeper.create_nested_dir_path(dir_list)
        print('nested_dir_path: ', nested_dir_path_)
        file_path_in_nested_dir_ = nested_dir_path_ + '/' + file_name
        return file_path_in_nested_dir_
    

    @staticmethod
    def create_nested_dir(dir_path):
        try:
            myPath = ''
            for directory in dir_path:
                myPath += './' + directory
                data_housekeeper.create_dir(myPath)
            #print(myPath)
        except:
            print('No nested dir created')
            #print(myPath)

    @staticmethod
    def create_nested_dir_in_parent_dir(dir_path , n):
        try:
            origin_wd = os.getcwd()
            for n in range(n):
                path_parent = os.path.dirname(os.getcwd())
                os.chdir(path_parent)
            root_dir = os.getcwd()
            print('Root dir: ', root_dir)
            data_housekeeper.create_nested_dir(dir_path)
            os.chdir(origin_wd)
            return root_dir
        except:
            os.chdir(origin_wd)
            print('Error: No directory created. Posible causes: \nWrong type imput arguments \nUnable to access to nested dir')
            return os.getcwds()
                
    @staticmethod
    def list_dict_to_json(dir_list,upper_stages,file_name, dictionary_list):
        parent_dir_path = data_housekeeper.create_nested_dir_in_parent_dir(dir_list,upper_stages)
        print('parent_dir_path: ', parent_dir_path)
        print('dir_list: ', dir_list)
        file_path = parent_dir_path + data_housekeeper.create_file_path_in_nested_dir(dir_list, file_name)
        print('file path: ', file_path)
        with open(file_path, 'w') as json_file:
            json.dump(dictionary_list, json_file, indent=4)

    @staticmethod
    def load_json_to_list(dir_list, file_name):
        relative_file_path_ = '.' + data_housekeeper.create_file_path_in_nested_dir(dir_list, file_name)
        print(relative_file_path_)
        with open(relative_file_path_) as json_file:
            data = json.load(json_file)
        return data
    
    @staticmethod
    def list_to_csv(dir_list, upper_stages, file_name, data):
        relative_file_path_ = '.' + data_housekeeper.create_file_path_in_nested_dir(dir_list, file_name)
        print(relative_file_path_)
        with open(relative_file_path_, 'w', newline='') as file:
            write = csv.writer(file)
            write.writerows(data)
    
    @staticmethod
    def df_to_csv(dir_list, upper_stages, file_name, data):
        relative_file_path_ = '.' + data_housekeeper.create_file_path_in_nested_dir(dir_list, file_name) + '.csv'
        print(relative_file_path_)
        data.to_csv(relative_file_path_)
    
    @staticmethod
    def csv_to_df(dir_list, file_name):
        relative_file_path_ = '.' + data_housekeeper.create_file_path_in_nested_dir(dir_list, file_name) + '.csv'
        print(relative_file_path_)
        return pd.read_csv(relative_file_path_)

def instance_class():
    myHousekeeper = data_housekeeper()
    return myHousekeeper
