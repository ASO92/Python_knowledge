import os
from os import path 
import json
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
    def create_my_path(dir_path):
        myPath = '.'
        for directory in dir_path:
                myPath += '/' +  directory
        return myPath
            
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
        dir_path = data_housekeeper.create_nested_dir_in_parent_dir(dir_list,upper_stages)
        print('dir_path: ', dir_path)
        print('dir_list: ', dir_list)
        nested_path = '/' + '/'.join(dir_list) +'/'
        print('nested_path: ', nested_path)
        file_path = dir_path + nested_path+ file_name
        print('file path: ', file_path)
        with open(file_path, 'w') as json_file:
            json.dump(dictionary_list, json_file, indent=4)

    @staticmethod
    def load_json_to_list(dir_list, file_name):
        dir_path = data_housekeeper.create_my_path(dir_list)
        print(dir_path)
        file_path = dir_path + '/' +file_name
        print(file_path)
        with open(file_path) as json_file:
            data = json.load(json_file)
        return data

def instance_class():
    myHousekeeper = data_housekeeper()
    return myHousekeeper