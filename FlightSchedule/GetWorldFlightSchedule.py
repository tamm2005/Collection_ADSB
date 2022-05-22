# The useful library
from pyflightdata import FlightData
import pandas as pd
import numpy as np
import csv
from datetime import date
import schedule
import time
from pathlib import Path
import os
from datetime import datetime


# The original directory in this program
root = os.path.dirname(os.path.realpath(__file__))


class FlightDataCreate():
    def __init__(self, iata, df, mode, path):
        self.iata = iata
        self.df = df
        self.mode = mode
        self.path = path

    # It will update the new data in the old dataset or it will create the new file if the file doesn't not exist
    def duplicate(self): 

        if not os.path.exists(f'{self.path}\ADSB_{self.mode}_{date.today().day}_{self.iata}.csv'):
            print(self.df)
            self.df.to_csv(
                f'{self.path}\ADSB_{self.mode}_{date.today().day}_{self.iata}.csv', index=False)
            #self.df.to_pickle(
            #    f'{self.path}\ADSB_{self.mode}_{date.today().day}_{self.iata}.pkl')
        else:
            read = pd.read_csv(
                f'{self.path}\ADSB_{self.mode}_{date.today().day}_{self.iata}.csv')
            df = pd.concat([read, self.df], axis=0).drop_duplicates(
                subset=['default'], keep = 'last')
            print('Updated\n', df)
            df.to_csv(
                f'{self.path}\ADSB_{self.mode}_{date.today().day}_{self.iata}.csv', index=False)
            #df.to_pickle(
            #    f'{self.path}\ADSB_{self.mode}_{date.today().day}_{self.iata}.pkl')


class FlightDataFormatChange():

    def __init__(self, data):
        self.data = data

    # It will change the format which in the flightradar24 since the file is used the nested dict to save it (This parts may have some imporvement)
    def data_collection(self):

        for i_1 in range(len(self.data)):
            content = []

            # It will remove the first unuseful dict in the dataset
            data_layer1 = self.data[i_1]['flight']
            content.append("total_json")
            content.append(str(self.data[i_1]))

            # It will break down the data with different layer and it will collect it into the variable content
            for i_2 in data_layer1:
                try:
                    if len(i_2) == 1:
                        data_layer1[i_2] = str(data_layer1[i_2])
                    if type(data_layer1[i_2]) == int or type(data_layer1[i_2]) == bool or type(data_layer1[i_2]) == float:
                        data_layer1[i_2] = str(data_layer1[i_2])
                        data_layer2 = data_layer1[i_2]
                        # print(i_2,type(data_layer2))
                    else:
                        data_layer2 = data_layer1[i_2]
                        # print(i_2,type(data_layer2))
                    if type(data_layer2) == str:
                        content.append(i_2)
                        content.append(data_layer2)
                except:
                    continue

                for i_3 in data_layer2:
                    try:
                        if len(i_3) == 1:
                            data_layer2[i_3] = str(data_layer2[i_3])
                        if type(data_layer2[i_3]) == int or type(data_layer2[i_3]) == bool or type(data_layer2[i_3]) == float:
                            data_layer2[i_3] = str(data_layer2[i_3])
                            data_layer3 = data_layer2[i_3]
                            # print(i_3,type(data_layer3))
                        else:
                            data_layer3 = data_layer2[i_3]
                            # print(i_3,type(data_layer3))
                        if type(data_layer3) == str:
                            content.append(i_3)
                            content.append(data_layer3)
                    except:
                        continue

                    for i_4 in data_layer3:
                        try:
                            if len(i_4) == 1:
                                data_layer3[i_4] = str(data_layer3[i_4])
                            if type(data_layer3[i_4]) == int or type(data_layer3[i_4]) == bool or type(data_layer3[i_4]) == float:
                                data_layer3[i_4] = str(data_layer3[i_4])
                                data_layer4 = str(data_layer3[i_4])
                                # print(i_4,type(data_layer4))
                            else:
                                data_layer4 = data_layer3[i_4]
                                # print(i_4,type(data_layer4))
                            if type(data_layer4) == str:
                                content.append(i_4)
                                content.append(data_layer4)
                        except:
                            continue

                        for i_5 in data_layer4:
                            try:
                                if len(i_5) == 1:
                                    data_layer4[i_5] = str(data_layer4[i_5])
                                if type(data_layer4[i_5]) == int or type(data_layer4[i_5]) == bool or type(data_layer4[i_5]) == float:
                                    data_layer4[i_5] = str(data_layer4[i_5])
                                    data_layer5 = str(data_layer4[i_5])
                                    # print(i_5,type(data_layer5))
                                else:
                                    data_layer5 = data_layer4[i_5]
                                    # print(i_5,type(data_layer5))
                                if type(data_layer5) == str:
                                    content.append(i_5)
                                    content.append(data_layer5)
                            except:
                                continue

                            for i_6 in data_layer5:
                                try:
                                    if len(i_6) == 1:
                                        data_layer5[i_6] = str(
                                            data_layer5[i_6])
                                    if type(data_layer5[i_6]) == int or type(data_layer5[i_6]) == bool or type(data_layer5[i_6]) == float:
                                        data_layer5[i_6] = str(
                                            data_layer5[i_6])
                                        data_layer6 = str(data_layer5[i_5])
                                        # print(i_6,type(data_layer6))
                                    else:
                                        data_layer6 = data_layer5[i_5]
                                        # print(i_6,type(data_layer6))
                                    if type(data_layer3) == str:
                                        content.append(i_6)
                                        content.append(data_layer6)
                                except:
                                    continue

            # It will change from content (np.array) to res_dict (dict)
            res_dct = {content[f]: content[f + 1]
                       for f in range(0, len(content), 2)}

            # It will change res_dict (dict) to df (pandas)
            if i_1 == 0:
                df = pd.DataFrame(res_dct, index=[i_1])
            else:
                df2 = pd.DataFrame(res_dct, index=[i_1])
                df = pd.concat([df, df2], axis=0)
        columns = [col for col in df.columns if 'millis' in col]
        for column in columns:
            df.loc[df[column].notnull(), column] = df.loc[df[column].notnull(), column].apply(np.int64)
            df[column] = pd.to_datetime(df[column], unit='ms').dt.strftime('%Y/%m/%s, %H:%M:%S')
        return df


class WorldFlightSchedule():

    def FlightTrader24ScheduleCall(self):
        print("Flight schedule data capture start:", date.today())
        f = FlightData()
        f.login('aae.asm.lab@polyu.edu.hk', 'PolyUASM8232')
        iatas = pd.read_csv(f'{root}\iata.csv', header=None).T.to_numpy()[0]

        # It will have all iata code in the world and it will collect the arrivals and departures data (Max:100)
        for iata in iatas:
            print(iata)
            data = f.get_airport_arrivals(iata)

            if data == []:
                continue
            else:
                try:
                    IATAFolder = root + '\\IATA_' + str(iata)
                    if not os.path.exists(IATAFolder):
                        os.mkdir(IATAFolder)
                    IATADir = root + '\\IATA_' + \
                        str(iata)+"\\" + str(date.today().year) + \
                        "-" + str(date.today().month)
                    if not os.path.exists(IATADir):
                        os.mkdir(IATADir)

                    # It will collect the arrival data
                    FDC_A = FlightDataFormatChange(data)
                    df = FDC_A.data_collection()
                    FDCupdated = FlightDataCreate(
                        f"{iata}", df, "arrivals", IATADir)
                    FDCupdated.duplicate()

                    #Auto = FDC(f'{iata}', df, 'arrivals', IATADir)
                    # Auto.duplicate()
                    time.sleep(1)

                    # It will collect the departure data
                    data = f.get_airport_departures(iata)
                    FDC_D = FlightDataFormatChange(data)
                    df = FDC_D.data_collection()
                    FDC_Dupdated = FlightDataCreate(
                        f"{iata}", df, "departures", IATADir)
                    FDC_Dupdated.duplicate()

                    time.sleep(1)
                except:
                    time.sleep(1)
                    continue


def main():
    # It will form the auto run procedure
    WFS = WorldFlightSchedule()
    WFS.FlightTrader24ScheduleCall()


# It will create the starting point of the file: main()
if __name__ == '__main__':
    main()
