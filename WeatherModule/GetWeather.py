# The useful library
from jsonpath_rw.jsonpath import Index
from pyflightdata import FlightData
import pandas as pd
import numpy as np
import csv
from datetime import date
import schedule
import time
from pathlib import Path
import os

root = os.path.dirname(os.path.realpath(__file__))


class WeatherDataCollection():
    def __init__(self, df, iata, IATADir):
        self.df = df
        self.iata = iata
        self.IATADir = IATADir

    # It will update the new data in the old dataset or it will create the new file if the file doesn't not exist
    def duplicate(self):
        if not os.path.exists(f'{self.IATADir}\weather_{date.today().day}_{self.iata}.csv'):
            print(self.df)
            self.df.to_csv(
                f'{self.IATADir}\weather_{date.today().day}_{self.iata}.csv', index=False)
        else:
            read = pd.read_csv(
                f'{self.IATADir}\weather_{date.today().day}_{self.iata}.csv')
            df = pd.concat([read, self.df], axis=0).drop_duplicates(
                subset=['time'])
            print('Updated:\n', df)
            df.to_csv(
                f'{self.IATADir}\weather_{date.today().day}_{self.iata}.csv', index=False)


class WorldAirportWeather():
    def Flightraader24WeatherCall(self):
        print("Meteorological data cature start:", date.today())
        f = FlightData()
        #Login in Flightradar24
        f.login('aae.asm.lab@polyu.edu.hk', 'PolyUASM8232')
        IATAcodeDir = root + "/iata.csv"
        iatas = pd.read_csv(IATAcodeDir, header=None).T.to_numpy()[0]

        # It will have all iata code in the world and it will collect the weather data (Last 72 hours)
        for iata in iatas:
            try:
                # print(iata)
                IATAFolder = root + '\\IATA_' + str(iata)
                if not os.path.exists(IATAFolder):
                    os.mkdir(IATAFolder)
                IATADir = root + '\\IATA_' + \
                    str(iata)+"\\" + str(date.today().year) + \
                    "-" + str(date.today().month)
                # print(IATADir)
                if not os.path.exists(IATADir):
                    os.mkdir(IATADir)

                # It will collect the weather data
                data = f.get_airport_metars_hist(iata)
                time.sleep(10)
                keys_values = data.items()
                data = [[key, value] for key, value in keys_values]
                df = pd.DataFrame(data, columns=['time', 'weather'])
                Auto = WeatherDataCollection(df, iata, IATADir)
                if df.empty:
                    continue
                else:
                    if not os.path.exists(IATADir):
                        os.mkdir(IATADir)
                    Auto.duplicate()
                    continue
            except:
                time.sleep(10)
                continue


def main():
    WM = WorldAirportWeather()
    WM.Flightraader24WeatherCall()


# It will create the starting point of the file: main()
if __name__ == '__main__':
    main()
