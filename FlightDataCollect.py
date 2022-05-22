import pandas as pd
import numpy as np
import csv
import time
import os
# scheduler
from apscheduler.schedulers.blocking import BlockingScheduler
# email
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from datetime import date, datetime

from WeatherModule import WorldAirportWeather
from FlightSchedule import WorldFlightSchedule
from FlightTrajectory import CallsignAtAirport
from FlightTrajectory import AirportArrDepFlightTraj


sender_email = "aaeasmlab@polyu.edu.hk"
receiver_email = "aaeasmlab@polyu.edu.hk"
password = "PolyUASM8232"

MainRoot = os.path.dirname(os.path.realpath(__file__))

WMFileSize = 0
FSFileSize = 0
FTFileSize = 0

def timer():
    print("The current time is ",datetime.now())

def mailing(delta_WMFileSize, delta_FSFileSize, delta_FTFileSize):
    print("Sending email ...")
    # email code
    # https://realpython.com/python-send-email/
    message = MIMEMultipart("alternative")
    message["Subject"] = "Summary of flight data record on" + str(date.today())
    message["From"] = sender_email
    message["To"] = receiver_email

    # Create the plain-text and HTML version of your message

    text = f"""\
    Summary of flight data
    The number of files in weather module: {delta_WMFileSize}
    The number of files in flight schedule module: {delta_FSFileSize}
    The number of files in flight trajectory module: {delta_FTFileSize}
    """

    # Turn these into plain/html MIMEText objects
    part1 = MIMEText(text, "plain")

    # Add HTML/plain-text parts to MIMEMultipart message
    # The email client will try to render the last part first
    message.attach(part1)

    # Create secure connection with server and send email
    context = ssl.create_default_context()
    # with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
    #    server.login(sender_email, password)
    #    server.sendmail(sender_email, receiver_email, message.as_string())

    with smtplib.SMTP("smtp.office365.com", 587) as server:
        server.ehlo()
        server.starttls(context=context)
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message.as_string())


def FileSizeCheck():
    current_WMFileSize = sum(len(files)
                             for r, d, files in os.walk(MainRoot + "//WeatherModule"))
    current_FSFileSize = sum(len(files)
                             for r, d, files in os.walk(MainRoot + "//FlightSchedule"))
    current_FTFileSize = sum(len(files)
                             for r, d, files in os.walk(MainRoot + "//FlightTrajectory"))

    print("The number of files in weather module:", current_WMFileSize)
    print("The number of files in flight schedule module:", current_FSFileSize)
    print("The number of files in flight trajectory module:", current_FTFileSize)
    global WMFileSize, FSFileSize, FTFileSize
    delta_WMFileSize = current_WMFileSize - WMFileSize
    delta_FSFileSize = current_FSFileSize - FSFileSize
    delta_FTFileSize = current_FTFileSize - FTFileSize

    WMFileSize = current_WMFileSize
    FSFileSize = current_FSFileSize
    FTFileSize = current_FTFileSize

    mailing(delta_WMFileSize, delta_FSFileSize, delta_FTFileSize)


def WeatherModule():
    # per day
    WM = WorldAirportWeather()
    WM.Flightraader24WeatherCall()


def FlightScheduleModule():
    # 100 records
    # per minutes
    FSR = WorldFlightSchedule()
    FSR.FlightTrader24ScheduleCall()


def FlightTrajectoryModule():
    # callsign per day
    # flight traj per three days
    AADcallsign = CallsignAtAirport()
    AADcallsign.GenCallsign()

    # gen flight traj per three days
    AADTraj = AirportArrDepFlightTraj()
    AADTraj.AirportList()


def main():
    print("programme start")
    # initialies number of file
    global WMFileSize, FSFileSize, FTFileSize
    current_WMFileSize = sum(len(files)
                             for r, d, files in os.walk(MainRoot + "//WeatherModule"))
    current_FSFileSize = sum(len(files)
                             for r, d, files in os.walk(MainRoot + "//FlightSchedule"))
    current_FTFileSize = sum(len(files)
                             for r, d, files in os.walk(MainRoot + "//FlightTrajectory"))
    WMFileSize = current_WMFileSize
    FSFileSize = current_FSFileSize
    FTFileSize = current_FTFileSize

    # scheduler code
    # http://www.hechunbo.com/index.php/archives/181.html
    # https://www.cnblogs.com/leffss/p/11912364.html
    scheduler = BlockingScheduler()

    scheduler.add_job(WeatherModule, "interval", hours=24, next_run_time=datetime.now())
    scheduler.add_job(FlightScheduleModule, "interval", minutes=30, next_run_time=datetime.now())
    scheduler.add_job(FlightTrajectoryModule, "interval", hours=24, next_run_time=datetime.now())
    scheduler.add_job(FileSizeCheck, trigger="cron", hour = "10")
    scheduler.add_job(FileSizeCheck, trigger="cron", hour = "15")
    scheduler.add_job(FileSizeCheck, trigger="cron", hour = "20")
    #scheduler.add_job(FileSizeCheck, "interval", hours=24, next_run_time=datetime.now())
    scheduler.add_job(timer, "interval", hours=1, next_run_time=datetime.now())
    scheduler.start()


if __name__ == "__main__":
    # Run mainloop if the programme is called directly
    main()
