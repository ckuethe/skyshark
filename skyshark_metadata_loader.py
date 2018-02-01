#!/usr/bin/env python
# vim: tabstop=4:softtabstop=4:shiftwidth=4:expandtab:

import requests
import re
import csv
import pymongo
import logging
import config


def main():
    dbh = pymongo.MongoClient(config.mongo_url)['skyshark']
    load_airlines(dbh)
    load_airports(dbh)

def load_airports(dbh, force=False):
    coll = 'airport_info'

    if int(dbh[coll].count()) > 0 and force is False:
        logging.info("skipped loading airports")
        return True

    for column in ['country', 'callsign', 'airline']:
        dbh[coll].create_index(column)

    for k in ['continent', 'elevation_ft', 'gps_code', 'iata_code', 'ident', 'iso_region', 'iso_country', 'local_code', 'municipality', 'name', 'type']:
        dbh[coll].create_index(k)
    dbh[coll].create_index([('coordinates', pymongo.GEOSPHERE)])

    url = 'https://raw.githubusercontent.com/datasets/airport-codes/master/data/airport-codes.csv'

    resp = requests.get(url, timeout=300, stream=True)
    if resp.ok is False:
        return False

    airports = csv.DictReader(resp.iter_lines())
    for apt in airports:
        apt['_id'] = apt.pop('ident', None)
        apt['lon'], apt['lat'] = map(float, apt['coordinates'].split(','))
        apt['coordinates'] = {'type':'Point', 'coordinates': [ apt['lon'], apt['lat'] ] }
        
        for k in list(apt.keys()):
            if apt[k] == '':
                apt.pop(k, None)
        try:
            apt['elevation_ft'] = int(apt['elevation_ft'])
        except (TypeError, KeyError):
            pass
        dbh[coll].update_one({'_id': apt['_id']}, {'$set': apt}, upsert=True)


def load_airlines(dbh, force=False):
    '''Load airline (IATA, ICAO, Country, Callsign, Name) metadata'''
    coll = 'airline_info'

    if int(dbh[coll].count()) > 0 and force is False:
        logging.info("skipped loading airlines")
        return True

    dbh[coll].create_index([('iata', 1), ('icao', 1), ('callsign', 1)], unique=True)
    for column in ['country', 'callsign', 'airline', 'iata', 'icao']:
        dbh[coll].create_index(column)

    url = 'https://raw.githubusercontent.com/BroadcastEngineer/Airlines-ICAO-IATA-Database/master/airlines.sql'
    rgx = re.compile(r"[(]\d+, '(?P<iata>.+?)', '(?P<icao>.+?)', '(?P<airline>.+?)', '(?P<callsign>.+?)', '(?P<country>.+?)'[)]")

    resp = requests.get(url, timeout=300, stream=True)
    if resp.ok is False:
        return False

    for line in resp.iter_lines():
        match = re.match(rgx, line)
        if match:
            data = match.groupdict()
            selector = {'iata': data['iata'], 'icao': data['icao'], 'callsign':data['callsign']}
            dbh[coll].update_one(selector, {'$set': data}, upsert=True)

    # observed deviations from the documentation
    fixes = [
        {'iata': 'WW', 'icao': 'WOW', 'callsign': 'WOW AIR', 'country': 'Iceland', 'airline':'Wow Air'},
        {'iata': 'Y4', 'icao': 'VOI', 'callsign': 'VOLARIS', 'country': 'Mexico', 'airline': 'Volaris'},
        {'iata': 'XA', 'icao': 'XAA', 'callsign': 'ROCKFISH', 'country': 'USA', 'airline': 'ARINC (Aeronautical Radio, Inc.)'},

        {'iata': 'UV', 'icao': 'UVA', 'callsign': 'UNIVERSAL', 'country': 'USA', 'airline': 'Universal Airways Inc'},
        # Babcock Mission Critical Services (INAIER HELICOPTEROS) is unlikely near me
        {'iata': 'UV', 'icao': 'INR', 'callsign': 'INAIER HELICOPTEROS', 'country': 'Spain', 'airline':'Babcock Mission Critical Services', 'unlikely':True},

        # PSA Airlines (BLUE STREAK) is unlikely near me
        {'icao': 'JIA', 'iata': 'US', 'unlikely':True},

        # Isles of Scilly Skybus (SCILLONIA) is unlikely near me
        {'icao': 'IOS', 'iata': '5Y', 'unlikely':True},

        # This is a lie, but it reflects reality. UPS should be using their assigned 
        # '5X' iata prefix and I have no evidence that 'UP' is a controlled duplicate.
        {'iata': 'UP', 'icao': 'UPS', 'callsign': 'UPS', 'country': 'USA', 'airline': 'United Parcel Service'},
        # Bahamasair (BAHAMAS) is unlikely near me
        {'iata': 'UP', 'icao': 'BHS', 'unlikely': True},
    ]

    for data in fixes:
        selector = {'iata': data['iata'], 'icao': data['icao']}
        dbh[coll].update_one(selector, {'$set': data}, upsert=True)

    return True


if __name__ == '__main__':
    main()
