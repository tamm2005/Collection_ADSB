import pandas as pd
import numpy as np
import os
os.environ['PROJ_LIB'] = r'C:\\Users\\s2010\\anaconda3\\Library\\share\\proj'
import matplotlib.pyplot as plt
import geopandas as gpd
from geopandas import GeoDataFrame
from geopandas import points_from_xy
import folium

path = os.getcwd() + '\\FlightTrajectory\\Flight_tra\\IATA_HKG' 

class complete_process():

    def __init__(self,path):
        self.path = path

    def complete(self):
        long_list = []
        lat_list = []
        name_list = []
        att_list = []
        course_list = []
        for i in os.listdir(self.path):
            path_number = path + f'\\{i}'
            for j in os.listdir(path_number):
                if j == 'arrivals':
                    path_number_mode = path_number + f'\\{j}'
                    for k in os.listdir(path_number_mode):
                        if k == 'complete.csv':
                            break
                        elif "VHHH" in k:
                            path_number_mode_files = path_number + f'\\{j}\\{k}'
                            df = pd.read_csv(path_number_mode_files)
                            name = i
                            y = df['Latitude'].to_list()
                            x = df['Longitude'].to_list()
                            z = df['meters'].to_list()
                            lat_list.append(y)
                            long_list.append(x)
                            name_list.append(name)
                            att_list.append(z)
                            course_list.append(df['Course'].to_list())
                            #array = array.append(df)
                        else:
                            path_number_mode_files = path_number + f'\\{j}\\{k}'
                            #print(path_number_mode_files)
                            continue
        geometry = points_from_xy(df['Latitude'],df['Longitude'])
        df = GeoDataFrame(df, geometry=geometry)   
        zippath = gpd.datasets.get_path('nybb')
        polydf = gpd.read_file(zippath)
        print(polydf)
        '''world_data = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
        axis = world_data[world_data.continent == 'Africa'].plot(color = 'lightblue',edgecolor = 'black')
        df.plot("geometry",ax=axis, color = 'black', edgecolor='0.8')
        plt.title('Testing')

        plt.show()

        plt.figure()
        for i in range(len(long_list)):
            plt.plot(long_list[i], lat_list[i], '-',label='${k}$'.format(k=name_list[i]), alpha=0.7)
        #plt.legend(loc='best')
        plt.title("HKG arrival flight trajectory")
        plt.xlabel('Long') 
        plt.ylabel('Lat') 
        plt.xlim([110, 121])
        plt.ylim([18,25])
        plt.savefig('HKG_arrival_flighttraj.png', dpi = 1000)
        plt.title("HKG arrival flight trajectory")
        plt.xlabel('Long') 
        plt.ylabel('Lat') 
        #plt.xlim([113, 118])
        #plt.ylim([21,23])
        #plt.savefig('HKG_arrival_flighttraj_ALL.png', dpi = 1000)
        #plt.show()'''

        
def main():
    # check callsign per day
    complete = complete_process(path)
    complete.complete()

if __name__ == "__main__":
    # Run mainloop if the programme is called directly
    main()