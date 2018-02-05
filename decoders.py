#!/usr/bin/env python
# vim: tabstop=4:softtabstop=4:shiftwidth=4:expandtab:

import re
import arrow

from expn import *

def fix_coord(x, scale=1e-4):
    hemi = x[0]
    degrees = int(x[1:]) * scale
    if hemi in ['S', 'W']:
        degrees *= -1.0
    return degrees

# decoders take a message (dict) as input, and modify it
# Returns True on success or False otherwise

def decode_default(message):
    print "[{}-{}] {}\n{}\n".format(message['label'],
                                    arinc620.get(message[label], 'unknown'),
                                    message['date'],
                                    message['text'])

def decode_SA(x):
    mtype = {'V': 'VHF-ACARS',
             'S': 'Default Satcom',
             'H': 'HF',
             'G': 'Global Star Satcom',
             'C': 'ICO Satcom',
             '2': 'VDL Mod 2',
             'X': 'Inmarsat Aero H/H+/I/L',
             'I': 'Iridium Satcom',
             }
    m = re.search(r'(?P<version>\d)(?P<est_los>.)(?P<media_type>.)(?P<utctime>\d{6})(?P<cur_media>.)((?P<text>.*))?', x, flags=re.I|re.S|re.M)
    d ={}
    if m:
        d.update( m.groupdict() )
        d['media_type'] = mtype[ d['media_type'] ]
        d['cur_media'] = mtype[ d['cur_media'] ]
    return d

def decode_colonsemi(x):
    try:
        new_freq = int(x['text'].strip())/1000.0
        x['new_freq'] = new_freq
        return True
    except ValueError:
        return False
    

def decode_SQ(x):
    m = re.search(r'(?P<something>.)(?P<ver>\d)(?P<lat>\d{4})(?P<lat_hemi>[NS])(?P<lon>\d{5})(?P<lon_hemi>[EW])(?P<acars_mode>.)(?P<vdl2freq>\d+)(?P<text>.+)?', x)
    if m is None:
        return {}
    d = m.groupdict()
    d['lon'] = int(d['lon'])/100.0
    d['lat'] = int(d['lat'])/100.0
    if d.pop('lon_hemi', 'E') == 'W':
        d['lon'] *= -1.0
    if d.pop('lat_hemi', 'N') == 'S':
        d['lat'] *= -1.0
    d['vdl2freq'] = int(d['vdl2freq'])/1000.0
    return d

def decode_5Z(x):
    d = {'text': x, 'united_type': 'not decoded'}
    
    mtype = x.split()[0]
    d['united_type'] = united_5z.get(mtype, 'not decoded')
    d['mtype'] = mtype
    if mtype == '/B6':
        m = re.search('K?(?P<dest>[A-Z]{3,4}) R(?P<runway>\d+[RCL]?)', x)
        if m:
            d.update(m.groupdict())
    return d

def decode_15(x):
    rgx = r'[(]2(?P<lat>[NS]\d{5})(?P<lon>[EW]\d{6})(OFF(?P<d>\d{2})(?P<m>\d{2})(?P<y>\d{2})(?P<H>\d{2})(?P<M>\d{2}))?(?P<unknown>.*)[(]Z'
    d = re.search(rgx, x['text']).groupdict()
    d['lat'] = fix_coord(d['lat'], 1e-3)
    d['lon'] = fix_coord(d['lon'], 1e-3)
    try:
        offtm = "20{}-{}-{} {}:{}:00".format(d.pop('y', None), d.pop('m', None), d.pop('d', None), d.pop('H', None), d.pop('M', None))
        d['offtm'] = arrow.get(offtm).datetime
    except arrow.parser.ParserError:
        pass
    return d

def decode_16(x):
    '''Decoder for either "Fedex Position Report-AUTPOS" or "General Aviation Weather Request"'''
    d = {}
    m = re.search(r'(?P<something>\d+)/AUTPOS/LLD (?P<lat>[NS]\d+) (?P<lon>[WE]\d+)\s+/ALT (?P<altitude>\d+)/SAT (?P<sat>\S+)\s+/WND (?P<wind_dir>\d{3})(?P<wind_spd>\d{3})/TAT (?P<tat>\S+)/TAS (?P<tas>\d+)/CRZ (?P<crz>\d+)\s+/FOB (?P<fuel>\d+)\r\n/DAT (?P<mdate>\d+)/TIM (?P<mtime>\d+)', x)
    if m:
        d.update(m.groupdict())
        d['datetime'] = arrow.get("{mdate} {mtime}".format(**d), "YYMMDD HHmmss").datetime
        d['lat'] = fix_coord(d['lat'])
        d['lon'] = fix_coord(d['lon'])
        d['fuel'] = int(d['fuel'])
        d['sat'] = int(d['sat'])
        d['tas'] = int(d['tas'])
        d['crz'] = int(d['crz'])
        d['wind_spd'] = int(d['wind_spd'])
        d['wind_dir'] = int(d['wind_dir'])
        d.pop('mdate', '')
        d.pop('mtime', '')
        return d
    m = re.search(r'(?P<x>[NS])\s*(?P<lat>[0-9.]+)[/,](?P<y>[EW])\s*(?P<lon>[0-9.]+)(,(?P<altitude>\d+))?', x)
    if m:
        d.update(m.groupdict())
        x = 1.0 if d['x'] == 'N' else -1.0
        y = 1.0 if d['y'] == 'E' else -1.0
        d['lat'] = x * float(d['lat'])
        d['lon'] = y * float(d['lon'])
        if d['altitude'] is None:
            d.pop('altitude', '')
        d.pop('x', '')
        d.pop('y', '')
        return d
    m = re.search(r'(?P<lat>[NS]\d+)(?P<lon>[EW]\d+)(?P<dep>[A-Z]{4})?(?P<arr>[A-Z]{4})?', x)
    if m:
        d.update(m.groupdict())
        d['lat'] = fix_coord(d['lat'], 1e-3)
        d['lon'] = fix_coord(d['lon'], 1e-3)
        if d['arr'] is None:
            d.pop('arr', '')
        if d['dep'] is None:
            d.pop('dep', '')
        return d
    return d

