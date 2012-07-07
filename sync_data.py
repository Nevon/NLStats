#!/usr/bin/env python
# -*- coding: utf-8 -*- 

import re, urllib, urllib2, getpass, csv, MySQLdb
from argparse import ArgumentParser
try:
    import configparser
except:
    import ConfigParser as configparser

class Spreadsheet:
    def __init__(self, key):
        self.key = key
        self.file = None

class Client:
    def __init__(self, email, password):
        self.email = email
        self.password = password

    def _get_auth_token(self, email, password, source, service):
        url = "https://www.google.com/accounts/ClientLogin"
        params = {
            "Email": email, "Passwd": password,
            "service": service,
            "accountType": "HOSTED_OR_GOOGLE",
            "source": source
        }
        req = urllib2.Request(url, urllib.urlencode(params))
        return re.findall(r"Auth=(.*)", urllib2.urlopen(req).read())[0]

    def get_auth_token(self):
        source = type(self).__name__
        return self._get_auth_token(self.email, self.password, source, service="wise")

    def download(self, spreadsheet, gid=0, format="csv"):
        url_format = "https://spreadsheets.google.com/feeds/download/spreadsheets/Export?key=%s&exportFormat=%s&gid=%i"
        headers = {
            "Authorization": "GoogleLogin auth=" + self.get_auth_token(),
            "GData-Version": "3.0"
        }
        req = urllib2.Request(url_format % (spreadsheet.key, format, gid), headers=headers)
        return urllib2.urlopen(req)

def csvToDict(infile, delimiter=","):
    result = []
    reader = csv.reader(infile, delimiter=delimiter)

    #First row contains the headings
    headings = reader.next()

    #Remove whitespace and make lower-case
    for i, v in enumerate(headings):
        headings[i] = re.sub(r'\s', '', v.lower())
        headings[i] = headings[i].replace('#', '')

    #Create dictionaries for each row
    reader = csv.DictReader(infile, headings)

    #Turn the DictReader object into a list
    for row in reader:
        result.append(row)

    #Do some data wrangling to use more correct data types
    for i, v in enumerate(result):
        #Change win or lose to True or False
        try:
            if v['result'] == 'W':
                result[i]['result'] = True
            else:
                result[i]['result'] = False
        except KeyError, e:
            pass

        #Change episode number to int
        try:
            result[i]['ep'] = int(v['ep'])
        except ValueError, TypeError:
            result[i]['ep'] = 0

        #Change any empty strings to None
        for key, value in result[i].iteritems():
            if value == '':
                result[i][key] = None

    return result



if __name__ == "__main__":
    #Get the spreadsheet from the command line argument
    parser = ArgumentParser("Download a .cvs file from Google Docs")
    parser.add_argument("spreadsheet")
    options = parser.parse_args()

    #Now get user credentials from the config file
    config = configparser.ConfigParser()
    config.read("credentials.ini")

    
    email = config.get('GoogleAccount', 'Email')
    password = config.get('GoogleAccount', 'Password')
    dbuser = config.get('Database', 'User')
    dbpass = config.get('Database', 'Password')
    dbname = config.get('Database', 'DatabaseName')
    dbhost = config.get('Database', 'Host')

    spreadsheet_id = options.spreadsheet # (spreadsheet id here)

    print 'Authenticating...'
    # Create client and spreadsheet objects
    gs = Client(email, password)
    ss = Spreadsheet(spreadsheet_id)

    print 'Authentication OK'

    print 'Downloading file...'
    # Request a file-like object containing the spreadsheet's contents
    ss.file = gs.download(ss)

    print 'Parsing file'
    games = csvToDict(ss.file)

    print 'Saving to database...'
    try:
        db = MySQLdb.connect(host = dbhost,
                            user = dbuser,
                            passwd = dbpass,
                            db = dbname)
        cursor = db.cursor()

        for index, value in enumerate(games):
            print 'Run {} out of {}'.format(index, len(games))
            try:
                cursor.execute("INSERT INTO runs VALUES (%s, %s, %s, %s, %s, %s)", (index+1, value['ep'], value['character'], value['result'], value['killedon'], value['killedby']))
                db.commit()
            except MySQLdb.IntegrityError:
                print 'Run {} already present in database. Skipping.'.format((index+1))
                pass

        db.close()
        print 'Done!'
    except MySQLdb.Error, e:
        print "Error {}: {}".format(e.args[0], e.args[1])
