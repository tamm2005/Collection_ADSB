
# The useful library
from pandas.io.parsers import read_csv
import requests
from bs4 import BeautifulSoup
import numpy as np
import json
import pandas as pd
import os
from datetime import date
import re
from faker import Faker
import time
import schedule

# The original directory in this program
root = os.path.dirname(os.path.realpath(__file__))

# Class definition flightra()


class flighttra():

    def __init__(self):
        self.folder = self.folder()
        self.loop = self.loop()
        self.content_flighttra = self.content_flighttra()

    # Class definition folder()
    class folder():

        def __init__(self, iata, flight, mode):
            self.iata = iata
            self.flight = flight
            self.mode = mode

        # Normal operation
        def folder(self):
            flighttra.folder(self.iata, self.flight,
                             self.mode).folder_countires()
            flighttra.folder(self.iata, self.flight, self.mode).folder_iata()
            flighttra.folder(self.iata, self.flight, self.mode).folder_flight()
            path = flighttra.folder(
                self.iata, self.flight, self.mode).folder_mode()
            return path

        # It will open folder for storing the file
        def folder_countires(self):
            path = f'{root}\\Flight_tra'
            if not os.path.exists(path):
                os.mkdir(path)

        # It will open folder for storing the file
        def folder_iata(self):
            path = f'{root}\\Flight_tra\\IATA_{self.iata}'
            if not os.path.exists(path):
                os.mkdir(path)

        # It will open folder for storing the file
        def folder_flight(self):
            path = f'{root}\\Flight_tra\\IATA_{self.iata}\\{self.flight}'
            if not os.path.exists(path):
                os.mkdir(path)

        # It will open folder for storing the file
        def folder_mode(self):
            path = f'{root}\\Flight_tra\\IATA_{self.iata}\\{self.flight}\\{self.mode}'
            if not os.path.exists(path):
                os.mkdir(path)
            return path

    # Class definition loop()
    class loop():

        def __init__(self, new):
            self.new = new

        # Dataset: Time, Lat, Long, Course, kts, mph, meters, Rate, Reporting Facility (Total:9)
        # Therefore, we can use mod (%) to classify the type of the data
        def mod_loop(self):
            time_t, lat, long, course, kts, mph, meters, rate, report = np.array([]), np.array([]), np.array(
                []), np.array([]), np.array([]), np.array([]), np.array([]), np.array([]), np.array([])
            for j in range(len(self.new)):
                if j % 9 == 0:
                    time_t = np.append(time_t, str(self.new[j])[:-7])
                elif j % 9 == 1:
                    self.new[j] = str(self.new[j]).split(
                        '.')[0] + '.' + str(self.new[j]).split('.')[1][:4]
                    lat = np.append(lat, self.new[j])
                elif j % 9 == 2:
                    self.new[j] = str(self.new[j]).split(
                        '.')[0] + '.' + str(self.new[j]).split('.')[1][:4]
                    long = np.append(long, self.new[j])
                elif j % 9 == 3:
                    self.new[j] = str(self.new[j]).split(' ')[1].split('°')[0]
                    course = np.append(course, self.new[j])
                elif j % 9 == 4:
                    kts = np.append(kts, self.new[j])
                elif j % 9 == 5:
                    mph = np.append(mph, self.new[j])
                elif j % 9 == 6:
                    self.new[j] = str(self.new[j]).replace(',', '')
                    num = int(len(str(self.new[j]))/2)
                    meters = np.append(meters, str(self.new[j])[:num])
                elif j % 9 == 7:
                    rate = np.append(rate, re.sub('\xa0', '', self.new[j]))
                elif j % 9 == 8:
                    report = np.append(report, self.new[j])
            return time_t, lat, long, course, kts, mph, meters, rate, report

    # Class definition content_flighttra()
    class content_flighttra():
        def __init__(self, flight, iata, mode):
            self.flight = flight
            self.iata = iata
            self.mode = mode

        # It is the normal operation of grabbing the flight trajectory data in different callsign
        def content(self):
            url = f'https://uk.flightaware.com/live/flight/{self.flight}/history/160'
            fake = Faker()
            headers = {'user-agent': fake.user_agent()}
            soup = BeautifulSoup(requests.get(
                url, headers).content, 'html.parser')
            #a_tags = soup.find_all('a')

            # find all flight traj link
            hreflink = []
            for tag in soup.find_all("a"):
                hreflink.append(str(tag.get("href")))

            FlightTrajLink = []
            for item in hreflink:
                if str(self.flight) in item and "history" in item and "buy" not in item and "refer" not in item:
                    FlightTrajLink.append(item)

            print("Flight trajectory data")
            print(FlightTrajLink)

            if FlightTrajLink != []:
                for flightlink in FlightTrajLink:

                    url_info = f'https://uk.flightaware.com/{flightlink}/tracklog'
                    place2 = str(flightlink).split('/')[-1]
                    place1 = str(flightlink).split('/')[-2]
                    file_time = str(flightlink).split('/')[-3]
                    file_date = str(flightlink).split('/')[-4][0:4] + "-" + str(flightlink).split(
                        '/')[-4][4:6] + "-" + str(flightlink).split('/')[-4][6:8]

                    # print(place2)
                    # print(place1)
                    # print(file_time)
                    # print(file_date)
                    # print(date.today())
                    # print(url_info)

                    # Since the website will show the scheduled flight, we need to prevent this situation to collect the wrong data
                    # if (date.today() - file_date).days <= 0:

                    if str(file_date) == str(date.today()):
                        continue
                    else:

                        try:
                            soup_info = BeautifulSoup(requests.get(
                                url_info, headers).content, 'lxml')
                            tables = soup_info.find_all(
                                "table", attrs={"class": "prettyTable fullWidth"})

                            pks = tables[0].find_all(
                                "tr", attrs={"class": [re.compile('smallrow1'), re.compile('smallrow2')]})
                            new = np.array([])

                            for pk in pks:
                                if not ("flight_event_facility" in pk.attrs.get("class") or "flight_event" in pk.attrs.get("class") or "flight_event_taxi" in pk.attrs.get("class")):
                                    ko = str(pk.get_text(
                                        strip=False)).splitlines()
                                    del ko[0]
                                    del ko[4]
                                    new = np.append(new, ko)
                            dict = np.array([])
                            header_value = ['Time (EDT)', 'Latitude', 'Longitude', 'Course',
                                            'kts', 'mph', 'meters', 'Rate', 'Reporting Facility']

                            time_t, lat, long, course, kts, mph, meters, rate, report = flighttra.loop(
                                new).mod_loop()

                            # The data should form the nested array to make the further operation
                            rev = [time_t, lat, long, course,
                                   kts, mph, meters, rate, report]

                            # It will form dict(dict) from the header_value(array) and rev(nested array)
                            for k in range(len(header_value)):
                                if len(dict) == 0:
                                    dict = {header_value[k]: rev[k]}
                                    sumdict = dict
                                else:
                                    dict = {header_value[k]: rev[k]}
                                    sumdict[header_value[k]] = rev[k]

                            try:
                                path = flighttra.folder(
                                    self.iata, self.flight, self.mode).folder()
                                path = f'{path}\\{self.flight}_{place1}_{place2}_{file_date}_{self.mode}_flightraj.csv'

                                if not os.path.exists(path):
                                    # print(pd.DataFrame(sumdict))
                                    pd.DataFrame(sumdict).to_csv(
                                        path, index=False)
                                time.sleep(10)
                            except:
                                continue
                        except:
                            continue


class AirportArrDepFlightTraj():

    def AirportList(self):
        # one weeks data
        # iatas = pd.read_csv(f'{root}\iata.csv', header=None).T.to_numpy()[0]
        iatas = ['HKG', 'MFM', 'ZUH', 'CAN', 'SZX',"FUO","HUZ"]
        modes = np.array(['arrivals', 'departures'])
        for iata in iatas:
            for mode in modes:
                # iatas = pd.read_csv(f'{root}\Flight_{mode}_{iata}.csv').T.to_numpy()[0]
                try:
                    # final_path = f'{root}\\Countries\\{iata}\\Flight_{mode}_{iata}.csv'
                    final_path = f'{root}\\Flight_{mode}_{iata}.csv'
                    flights = pd.read_csv(final_path).T.to_numpy()[0]
                    for flight in flights:
                        print(iata, flight)
                        flighttra.content_flighttra(
                            flight, iata, mode).content()
                        time.sleep(5)
                except:
                    continue

# call sign


class sign():

    def __init__(self):
        self.find = self.find()
        self.folder_create = self.folder_create()
        self.iata_document_callsign = self.iata_document_callsign()

    # Class definition iata_document_callsign()
    class iata_document_callsign():
        def __init__(self, dict, path, iata, mode):
            self.dict = dict
            self.path = path
            self.iata = iata
            self.mode = mode

        # It will update the new data in the old dataset or it will create the new file if the file doesn't not exist
        def save_file(self):
            final_path = f'{self.path}\\Flight_{self.mode}_{self.iata}.csv'
            if not os.path.exists(final_path):
                print(pd.DataFrame(self.dict).drop_duplicates())
                pd.DataFrame(self.dict).drop_duplicates().to_csv(
                    final_path, index=False)
            else:
                read = pd.read_csv(final_path)
                df = pd.concat([read, pd.DataFrame(self.dict)],
                               axis=0).drop_duplicates()
                for correct in (df[self.mode].to_list()):
                    if len(str(correct).split('-')) > 1:
                        df[self.mode] = df[self.mode].replace([correct], str(
                            str(str(correct).split('-')[0]) + str(str(correct).split('-')[1])))
                #pd.set_option("display.max_rows", None, "display.max_columns", None)
                print('Updated\n', df)
                pd.DataFrame(df).to_csv(final_path, index=False)

    # Class definition folder_create()
    class folder_create():
        def __init__(self, iata):
            self.iata = iata

        # Normal operation
        def folder(self):
            sign.folder_create(self.iata).folder_countires()
            sign.folder_create(self.iata).folder_iata()

        # It will open folder for storing the file
        def folder_countires(self):
            path = f'{root}\\Countries'
            if not os.path.exists(path):
                os.mkdir(path)

        # It will open folder for storing the file
        def folder_iata(self):
            path = f'{root}\\Countries\\IATA_{self.iata}'
            if not os.path.exists(path):
                os.mkdir(path)
            return path

    # Class definition find()
    class find():
        def __init__(self, iata, mode, soup, headers):
            self.iata = iata
            self.mode = mode
            self.soup = soup
            self.headers = headers

        # It will find the callsign in different iata code
        def find(self):
            try:
                if self.mode == 'arrivals':
                    modes_html = self.soup.find_all(
                        'th', attrs={'class': 'mainHeader'})[0]
                else:
                    modes_html = self.soup.find_all(
                        'th', attrs={'class': 'mainHeader'})[1]
                modes_html = modes_html.find_all('a')[0].get('href')
                url_mode = f'https://uk.flightaware.com{modes_html}'
                try:
                    soup_mode = BeautifulSoup(requests.get(
                        url_mode, self.headers).content, 'lxml')
                except requests.exceptions.ConnectionError:
                    print(
                        f"Connection refused in the {self.iata}:{self.mode} page")
                datas = soup_mode.find_all('table', attrs={'class': 'fullWidth'})[
                    0].find_all('a', href=re.compile('/live/flight/id/'))
                data_store = np.array([])
                for data in datas:
                    data_store = np.append(data_store, data.text)
            except:
                data_store = np.array([])
            return data_store


class CallsignAtAirport():

    def GenCallsign(self):
        iatas = ['HKG', 'MFM', 'ZUH', 'CAN', 'SZX',"FUO","HUZ"]
        for iata in iatas:
            #iata = 'HKG'
            print(iata)
            url = f'https://flightaware.com/live/airport/{iata}'
            fake = Faker()
            headers = {'user-agent': fake.user_agent()}
            try:
                soup = BeautifulSoup(requests.get(
                    url, headers).content, 'lxml')
            except requests.exceptions.ConnectionError:
                print("Connection refused in the first page")
            modes = np.array(['arrivals', 'departures'])
            for mode in modes:
                dict = np.array([])
                data_store = sign.find(iata, mode, soup, headers).find()
                if not list(data_store):
                    continue
                else:
                    #a = np.append(a,iata)
                    dict = {mode: data_store}
                    # sign.folder_create(iata).folder()
                    #path = sign.folder_create(iata).folder_iata()
                    path = root
                    sign.iata_document_callsign(
                        dict, path, iata, mode).save_file()
                    time.sleep(5)


def main():
    # check callsign per day
    AADcallsign = CallsignAtAirport()
    AADcallsign.GenCallsign()

    # gen flight traj per three days
    AADTraj = AirportArrDepFlightTraj()
    AADTraj.AirportList()


# It will create the starting point of the file: main()
if __name__ == '__main__':
    main()
