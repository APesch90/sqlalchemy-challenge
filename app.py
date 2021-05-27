import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from flask import Flask, jsonify, request

import numpy as np
import pandas as pd
import datetime as dt

# create engine to hawaii.sqlite
# Note - using fully qualified file path 
engine = create_engine("sqlite:////Users/amandapesch/Documents/Bootcamp_HW/sqlalchemy-challenge/Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# View all of the classes that automap found
Base.classes.keys()

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)
conn = engine.connect()


#################################################
# Flask Setup
#################################################
app = Flask(__name__)
#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():

    return (
        f"Welcome to the Hawaii Climate Analysis API!<br/>"
        f"Available Routes:<br/>"
        f"<a href='/api/v1.0/precipitation' target=_blank> /api/v1.0/precipitation </a><br/>"
        f"<a href='/api/v1.0/stations' target=_blank> /api/v1.0/stations </a><br/>"
        f"<a href='/api/v1.0/temperatureObservation' target=_blank> /api/v1.0/temperatureObservation </a><br/>"
        f"<a href='/api/v1.0/temp/start' target=_blank> /api/v1.0/temp/start </a><br/>"
        f"<a href='/api/v1.0/temp/start/end' target=_blank> /api/v1.0/temp/start/end </a><br/>"
        
    )

@app.route("/api/v1.0/precipitation")
def precip():
    """Return the precipitation data for the last year"""
    # Start session, query, close session, data extraction
    
    session = Session(engine)
    
    # Calculate the date 1 year ago from last date in database
    recent_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    
    # Calculate the date one year from the last date in data set.
    last_year = dt.date(2017, 8, 23) - dt.timedelta(days=365)

    # Perform a query to retrieve the data and precipitation scores
    precip_date_query = session.query(Measurement.date, Measurement.prcp)\
    .filter(Measurement.date <= dt.date(2017, 8, 23), Measurement.date >= last_year)\
    .order_by(Measurement.date.desc()).all()
    
    session.close()  
    
    # Dict with date as the key and prcp as the value
    # Convert the query results to a dictionary using `date` as the key and `prcp` as the value.
    precip_dict = {}

    for item in precip_date_query:
        precip_dict[item[0]] = item[1]  
    
    return jsonify(precip_dict)

@app.route("/api/v1.0/stations")
def stations():
    """Return a list of stations."""
    
    session = Session(engine)

    stations_list = session.query(Station.station).all()
    
    session.close()
    
    # Unravel results into a 1D array and convert to a list
    final_stations_list = list(np.ravel(stations_list)) 
    
    return jsonify(final_stations_list)

@app.route("/api/v1.0/temperatureObservation")
def temp_monthly():
    """Return the temperature observations (tobs) for previous year."""
    # Query the dates and temperature observations of the most active station for the last year of data.
    # Return a JSON list of temperature observations (TOBS) for the previous year.
    
    session = Session(engine)
    
    recent_station_date = session.query(Measurement.date)\
    .filter(Measurement.station == 'USC00519281').order_by(Measurement.date.desc()).first()
    recent_station_date

    # Calculate the date one year from the last date in data set.
    one_year_timeframe = dt.date(2017, 8, 18) - dt.timedelta(days=365)

    # Perform a query to retrieve the data
    tobs_data = session.query(Measurement.date, Measurement.tobs)\
    .filter(Measurement.date <= dt.date(2017, 8, 18), Measurement.date >= one_year_timeframe)\
    .order_by(Measurement.date.desc()).all()

    session.close()
    
    return jsonify(tobs_data)

@app.route("/api/v1.0/temp/start")
def stats_1():
    #"""Return TMIN, TAVG, TMAX."""
    # Return a JSON list of the minimum temperature, the average temperature, 
    # and the max temperature for a given start.
    
    # When given the start only, calculate TMIN, TAVG, and TMAX for all dates greater than 
    # and equal to the start date.

    session = Session(engine)    
    
    query_temp = session.query(Measurement.tobs).filter(Measurement.date >= dt.date(2017, 4, 27)).all()
    query_temp_unravel = list(np.ravel(query_temp)) 
   
    low_temp_1 = min(query_temp_unravel)
    high_temp_1 = max(query_temp_unravel)
    avg_temp_1 = (round(sum(query_temp_unravel)/len(query_temp_unravel),2))

    session.close()
   
    final_dict = {
        "MIN: ": low_temp_1,
        "MAX: ": high_temp_1,
        "AVG: ": avg_temp_1
    }
    
    return jsonify(final_dict)

@app.route("/api/v1.0/temp/start/end")
def stats_2():
    """Return TMIN, TAVG, TMAX."""
    # Return a JSON list of the minimum temperature, the average temperature, 
    # and the max temperature for a given start-end range.
    
    # When given the start and the end date, calculate the TMIN, TAVG, and TMAX 
    # for dates between the start and end date inclusive.
    
    session = Session(engine)    
    
    query_temp_2 = session.query(Measurement.tobs).filter(Measurement.date >= dt.date(2016, 7, 8), 
                                                          Measurement.date <= dt.date(2016, 9, 30)).all()
    query_temp_2_unravel = list(np.ravel(query_temp_2)) 
   
    low_temp_2 = min(query_temp_2_unravel)
    high_temp_2 = max(query_temp_2_unravel)
    avg_temp_2 = (round(sum(query_temp_2_unravel)/len(query_temp_2_unravel),2))

    session.close()
   
    final_dict_2 = {
        "MIN: ": low_temp_2,
        "MAX: ": high_temp_2,
        "AVG: ": avg_temp_2
    }
    
    return jsonify(final_dict_2)

if __name__ == '__main__':
    app.run()