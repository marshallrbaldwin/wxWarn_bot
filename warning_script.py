# -*- coding: utf-8 -*-
"""
Created on Thu May 18 17:47:51 2023

@author: nimzo
"""

#nice system libraries
import os
from io import BytesIO
import requests, zipfile
from datetime import datetime as dt
from datetime import timedelta as td
from datetime import timezone as tz
from pathlib import Path
import json

#for reading shapefile information
import shapefile as shp
from shapely.geometry import Point, Polygon

#for sending the warning email
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

download_path = 'C:\\Users\\nimzo\\Desktop\\Python Experimentation\\Severe Weather Emailer\\Outlook Files'
def download_conv_outlook():
    """
    Downloads the most recent set of spc convective outlook files
    """
    
    #clear the folder
    [f.unlink() for f in Path(download_path).glob("*") if f.is_file()] 
    
    #construct download url
    current_time = dt.now(tz = tz(td(hours=-1)))
    url_1 = "https://www.spc.noaa.gov/products/outlook/archive/"
    url_2 = dt.strftime(current_time, format = "%Y/day1otlk_%Y%m%d_")
    #hope this doesn't break
    issue_times = [0, 11, 12, 16, 19]
    issue_codes = ["0100-shp.zip","1200-shp.zip","1300-shp.zip","1630-shp.zip","2000-shp.zip"]
    for time, code in zip(reversed(issue_times), reversed(issue_codes)):
        if time <= current_time.hour:
            url_3 = code
            break
    zip_url = f"{url_1}{url_2}{url_3}"
    
    #download zip file and extract to folder
    r = requests.get(zip_url, stream=True)
    z = zipfile.ZipFile(BytesIO(r.content))
    z.extractall(path = download_path)

def parse_convective_outlook(shapefile):
    """
    Using a shapefile Reader for a convective outlook,
    returns dictionary of polygons for each severe risk type
    """
    conv_polygons = shapefile.__geo_interface__['features']  #nested dicts with nice info :)
    
    risk_geometry = {'MRGL': None,
                     'SLGT': None,
                     'ENH' : None,
                     'MDT' : None,
                     'HIGH': None
                     }
    
    for poly in conv_polygons:
        label = poly['properties']['LABEL']
        geometry_type = poly['geometry']['type']
        geometry_coords = poly['geometry']['coordinates']
        if label in risk_geometry.keys():
            
            risk_geometry[label] = {'coords' : geometry_coords,
                                    'type'   : geometry_type}

    return risk_geometry

def parse_hazard_outlook(shapefile):
       conv_polygons = shapefile.__geo_interface__['features']
       risk_geometry = {}
       SIGN_idx = 0

       for category in conv_polygons:
           if category["properties"]["LABEL"] == "SIGN":
               label = f"SIGN_{SIGN_idx}"
           else:
               label = category["properties"]["LABEL"]
               
           geometry_type = category["geometry"]["type"]
           geometry_coords = category["geometry"]["coordinates"]
           risk_geometry[label] = {"coords": geometry_coords,
                                    "type": geometry_type}
               
       return risk_geometry

def get_location_cat_risk(location, risk_geometry):
    """
    Given a location tuple (longitude, latitude), return location catagorical risk code
    """
    location_point = Point(location[0], location[1])
    location_cat_risk = None
    
    for risk in reversed(risk_geometry.keys()):
        
        if risk_geometry[risk] is None:
            continue
        
        if risk_geometry[risk]['type'] == 'Polygon':
            poly = risk_geometry[risk]['coords']
        else:
            poly = risk_geometry[risk]['coords'][0]
            for component in risk_geometry[risk]['coords'][1:]:
                poly.extend(component)
                
        for sub_poly_coords in poly:
            risk_poly = Polygon(sub_poly_coords)
            contains_location = not location_point.intersection(risk_poly).is_empty
            
            if contains_location:
                break
        
        if contains_location:
            location_cat_risk = risk
            break
                
    return location_cat_risk   

def get_location_hazard_risk(location, risk_geometry):
    """
    Given a location tuple (longitude, latitude), return location catagorical risk code
    """
    location_point = Point(location[0], location[1])
    location_haz_risk = None
    risk_is_sig = False
    
    for risk in reversed(risk_geometry.keys()):
        
        if risk_geometry[risk] is None:
            continue
        
        if risk_geometry[risk]['type'] == 'Polygon':
            poly = risk_geometry[risk]['coords']
        else:
            poly = risk_geometry[risk]['coords'][0]
            for component in risk_geometry[risk]['coords'][1:]:
                poly.extend(component)
        
        if "SIGN" in risk:
            for sub_poly_coords in poly:
                risk_poly = Polygon(sub_poly_coords)
                contains_location = not location_point.intersection(risk_poly).is_empty
                if contains_location:
                    risk_is_sig = True
        else:                 
            for sub_poly_coords in poly:
                risk_poly = Polygon(sub_poly_coords)
                contains_location = not location_point.intersection(risk_poly).is_empty
                if contains_location:
                    break
            if contains_location:
                location_haz_risk = risk
                break
    
    if location_haz_risk is None:
        hazard_prob_msg = "Not a worry today."
    else: 
       hazard_prob_msg = str(int(float(location_haz_risk) * 100.)) + "%"
    if risk_is_sig:
        hazard_prob_msg += " (Significant too!)"
        
    return hazard_prob_msg

def get_location_hazards(shp_files, shp_types, location_coords):
    
    location_risks = {}
    
    for filename, hazard in zip(shp_files, shp_types):
        with shp.Reader(f"{download_path}\\{filename}") as sf:
            if "cat" in filename:
                risk_geometry = parse_convective_outlook(sf)
                location_risks[hazard] = get_location_cat_risk(location_coords, risk_geometry)
            else:
                risk_geometry = parse_hazard_outlook(sf)
                location_risks[hazard] = get_location_hazard_risk(location_coords, risk_geometry)

    return location_risks

def construct_email_body(recipient_info, location_risks):
    
    custom_message = recipient_info["custom_msg"]
    
    cat_code_decoder = {'MRGL': "a marginal</strong>",
                        'SLGT': "a <strong>slight</strong>",
                        'ENH' : "an <strong>enhanced</strong>",
                        'MDT' : "a <strong>moderate</strong>",
                        'HIGH': "a <strong>high</strong>"
                        }
    risk_level = cat_code_decoder[location_risks["catagorical"]]
    wind_risk = location_risks['wind']
    hail_risk = location_risks['hail']
    tornado_risk = location_risks['torn']
    
    email_body = f"""\
<html>
  <body>
    <p>{custom_message}</p>
    <p>There is {risk_level} risk of severe weather in your area. Hazard probabilities are:</p>
    <ul>
      <li><strong>Wind Gusts</strong>: {wind_risk}</li>
      <li><strong>Hail</strong>: {hail_risk}</li>
      <li><strong>Tornados</strong>: {tornado_risk}</li>
    </ul>
    <p>Go to the <a href="https://www.spc.noaa.gov/products/outlook/day1otlk.html">spc webpage</a> for more information.</p>
    <p>-Marshall's Warning Bot</p>
  </body>
</html>
"""
    return email_body

def send_warning(recipient_info, location_risks):
    
    #if there's no threat, don't cry wolf
    if location_risks['catagorical'] is None:
        return
    
    #access login info
    with open("login_credentials.txt", "r") as f:
        USER = f.readline()
        PASS = f.readline()
        
    #construct the email
    msg = MIMEMultipart()
    msg['Subject'] = f"Convection Alert! {location_risks['catagorical']} Risk"
    msg['From'] = USER
    if recipient_info['email'] is not None:
        msg['Cc'] = recipient_info['email'] #can be a list of emails!
    msg['To'] = USER
    
    # Construct the email body
    email_body = construct_email_body(recipient_info, location_risks)
    
    # Attach the email body as HTML
    msg.attach(MIMEText(email_body, 'html'))
    
    #gmail SMTP host and TLS port (secure)
    with smtplib.SMTP('smtp.gmail.com', 587) as server:

        server.starttls() #begins encryption
        server.login(USER, PASS)
        server.send_message(msg)

def main():
   
   #update convective outlook
   download_conv_outlook()
   
   #a couple nifty lists for accessing the convective outlook files
   shp_files = [f for f in os.listdir(download_path) if ".shp" in f and "sig" not in f]
   shp_types = ["catagorical", "hail", "torn", "wind"]
   
   #get info dictionary of all recipients of the warnings
   with open("recipient_info_database.json", "r") as file:
       recipient_info_database = json.load(file)
       
   #send warning to each recipient that is at risk
   for recipient in recipient_info_database:
       recipient_info = recipient_info_database[recipient]
       location = recipient_info["location"]
       location_risks = get_location_hazards(shp_files, shp_types, location)
       send_warning(recipient_info, location_risks)
        
if __name__ == "__main__":
   main()
        