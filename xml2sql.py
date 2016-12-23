#!/usr/bin/python

#------------------------------------------------------------------------------
#           Name: xml2sql.py
#         Author: Ra Inta, 20150707
#  Last Modified: 20150707
# This reads an XML file and converts it to a SQL database (in this case
# SQLite).
#------------------------------------------------------------------------------

import xml.etree.ElementTree as ET
import subprocess
import bz2
import sqlite3
from string import lstrip, rstrip, split

search_bands_file = bz2.BZ2File("search_bands.xml.bz2", 'rb')
#fscan_H1_file =    bz2.BZ2File( "fscan_sfts_H1.txt.bz2", 'rb')
#fscan_L1_file =    bz2.BZ2File( "fscan_sfts_L1.txt.bz2", 'rb')
fscan_H1_file = "fscan_sfts_H1.txt.bz2"
fscan_L1_file = "fscan_sfts_L1.txt.bz2"

db_name = "search_db"

search_bands_table = "search_bands"
fscan_H1_table = "fscan_H1"
fscan_L1_table = "fscan_L1"

# Set fscan_threshold manurally here
# TODO: read from setup XML
fscan_threshold = 6

############################################################
#1) Read setup, upper limit and veto bands
############################################################

tree = ET.parse( search_bands_file )
root = tree.getroot()

#############################################################
####  Cut this out for testing ##############################
#twoF = []
#search_freq = []
#jobId = []
#num_templates = []
#
#
##Walk through the XML tree for 2F and f0; don't pick up the vetoed bands. Have
## to keep jobId label to produce topJobs list.
#for jobNumber in root.iter('job'):
#    nodeInfo = root[int(jobNumber.text)].find('loudest_nonvetoed_template')
#    if nodeInfo is not None:
#        twoF.append( float( nodeInfo.find('twoF').text ) )
#        search_freq.append( float( nodeInfo.find('search_freq').text ) )
#        jobId.append( jobNumber.text )
#
## Get number of templates in each band
#for nTempl in root.iter('num_templates'):
#   num_templates.append( float( nTempl.text ) )
#
#
#############################################################

# For now, let's just keep these as vectors for clarity
jobId = []
search_freq = []
search_band = []

#Walk through the XML tree for search bands info
for jobNumber in root.iter('job'):
    jobId.append( int(jobNumber.text) )
    search_freq.append( float( root[int(jobNumber.text)].find('freq').text ) )
    search_band.append( float( root[int(jobNumber.text)].find('band').text ) )


#Walk through the fscans text files


# 1) H1
fscan_H1_freq = []
fscan_H1_power = []
fscan_H1_SNR = []
fscan_H1 = []

for lines in bz2.BZ2File( fscan_H1_file, 'rb').readlines():
    eachLine = split(lines.lstrip().rstrip())
    fscan_H1_freq.append(eachLine[0])
    fscan_H1_power.append(eachLine[1])
    fscan_H1_SNR.append(eachLine[2])

# 2) L1
fscan_L1_freq = []
fscan_L1_power = []
fscan_L1_SNR = []
fscan_L1 = []

for lines in bz2.BZ2File( fscan_L1_file, 'rb').readlines():
    eachLine = split(lines.lstrip().rstrip())
    fscan_L1_freq.append(eachLine[0])
    fscan_L1_power.append(eachLine[1])
    fscan_L1_SNR.append(eachLine[2])



###########################################
# Create the database
###########################################

#try:
db = sqlite3.connect( db_name )
cursor = db.cursor()

# Clean-up operation while debugging:
cursor.execute('''DROP TABLE IF EXISTS '''+ search_bands_table)
cursor.execute('''DROP TABLE IF EXISTS '''+ fscan_H1_table)
cursor.execute('''DROP TABLE IF EXISTS '''+ fscan_L1_table)
db.commit()


#### WARNING!!! THIS SECTION SEEMS TO #####
##### CAUSE VIM TO FREEZE WHEN UNCOMMENTED#
# Add these right after creating the db for
# See:
# http://stackoverflow.com/questions/784173/what-are-the-performance-characteristics-of-sqlite-with-very-large-database-file
# Might want to change synchronous mode to ' FULL ', as per:
# http://www.sqlite.org/pragma.html#pragma_synchronous
#
#db_pre_script = '''PRAGMA main.page_size = 4096;
#                    PRAGMA main.cache_size=10000;
#                    PRAGMA main.locking_mode=EXCLUSIVE;
#                    PRAGMA main.synchronous=NORMAL;
#                    PRAGMA main.journal_mode=WAL;
#                    PRAGMA main.cache_size=5000;'''
#
#cursor.executescript( db_pre_script )
###########################################

# Create search_bands table

cursor.execute('''CREATE TABLE IF NOT EXISTS ''' +
               search_bands_table +
               ''' (job INTEGER PRIMARY KEY, search_freq REAL, search_band REAL)''')

cursor.executemany('''INSERT INTO search_bands(job, search_freq, search_band) VALUES(?,?,?)''', zip( jobId, search_freq, search_band) )

#### Let's have a look (testing only): ####
#cursor.execute('''SELECT * FROM search_bands ''')
#for row in cursor:
#    print(row)
############################################
db.commit()

#except sqlite3.IntegrityError as IntegrityError:
#    print("Table already created!")
#    db.rollback()
#    raise IntegrityError
#except:
#    db.rollback()
#finally:
#db.close()


cursor.execute(
    '''CREATE TABLE IF NOT EXISTS '''+
    fscan_H1_table +
    ''' (fscan_H1_id INTEGER PRIMARY KEY, fscan_H1_freq REAL, fscan_H1_power REAL, fscan_H1_SNR  REAL)'''
)

cursor.executemany(
    '''INSERT INTO ''' +
    fscan_H1_table +
    '''(fscan_H1_freq, fscan_H1_power, fscan_H1_SNR) VALUES(?,?,?)''',
    zip(fscan_H1_freq, fscan_H1_power, fscan_H1_SNR)
)

db.commit()
#cursor.execute('''SELECT * FROM ''' + fscan_H1_table)
#cursor.execute('''SELECT * FROM ''' + fscan_H1_table + ''' WHERE SNR > 6 ''')
#for row in cursor:
#    print(row)

## For debugging only
#cursor.execute('''DROP TABLE ''' + fscan_H1_table)


cursor.execute(
    '''CREATE TABLE IF NOT EXISTS '''+
    fscan_L1_table +
    ''' (fscan_L1_id INTEGER PRIMARY KEY, fscan_L1_freq REAL, fscan_L1_power REAL, fscan_L1_SNR REAL)'''
)

cursor.executemany(
    '''INSERT INTO ''' +
    fscan_L1_table +
    '''(fscan_L1_freq, fscan_L1_power, fscan_L1_SNR) VALUES(?,?,?)''',
    zip(fscan_L1_freq, fscan_L1_power, fscan_L1_SNR)
)

db.commit()

#cursor.execute('''SELECT * FROM ''' + fscan_L1_table)
#cursor.execute('''SELECT * FROM ''' + fscan_L1_table + ''' WHERE SNR > 6 ''')
#for row in cursor:
#    print(row)

## For debugging only
#cursor.execute('''DROP TABLE ''' + fscan_L1_table)


# Now for the real meaty filtering part:
cursor.execute(
    '''SELECT fscan_H1_freq, fscan_H1_SNR, fscan_L1_SNR FROM fscan_H1 INNER JOIN fscan_L1 ON fscan_H1_freq=fscan_L1_freq
    WHERE fscan_L1_SNR > ''' +
    str(fscan_threshold) +
    ''' or fscan_H1_SNR > ''' +
    str(fscan_threshold)
)

db.commit()


# Print out what's in the current cursor
for row in cursor:
    print "Stuff in current DB cursor:"
    print(row)


cursor.execute(
    '''CREATE TABLE IF NOT EXISTS search_results_table (freq REAL, alpha REAL, delta REAL, f1dot REAL, f2dot REAL,
    f3dot REAL, twoF REAL, log10BSGL REAL, twoF_H1 REAL, twoF_L1 REAL)'''
)
db.commit()

def parseSearchResults( searchResultsFile ):
    """Reads in textfile and puts into its own database table"""
    searchResultText = []
    for lines in open( searchResultsFile, 'r').readlines():
        if not lines[0] == '%':
            searchResultText.append(lines.rstrip())
    return searchResultText


def getLoudestNonvetoedTemplate( searchResultsFile, db, db_cursor ):
    """This takes in the search_results filename, database and cursor object, creates its own table and
    gets the maximum template (that isn't IFO vetoed). It then shoves it into the search_results_table """
    job = lstrip(searchResultsFile, 'search_results.txt.')
    searchResultText = parseSearchResults( searchResultsFile )
    jobTableName = "search_results_" +  str(job)
    db_cursor.execute( '''CREATE TABLE ''' +
    jobTableName +
    ''' (freq REAL, alpha REAL, delta REAL, f1dot REAL, f2dot REAL, f3dot REAL, twoF REAL, log10BSGL REAL, twoF_H1 REAL, twoF_L1 REAL)'''
    )
    for resultsLine in searchResultText:
        db_cursor.executemany(
        '''INSERT INTO ''' +
        jobTableName +
        ''' (freq, alpha, delta, f1dot, f2dot, f3dot, twoF, log10BSGL, twoF_H1, twoF_L1) VALUES (?,?,?,?,?,?,?,?,?,?)''',
        (split( resultsLine ),)
        )
    db.commit()
    db_cursor.execute(
    '''INSERT INTO search_results_table SELECT freq, alpha, delta, f1dot, f2dot, f3dot, MAX(twoF), log10BSGL, twoF_H1, twoF_L1
    FROM search_results_0 WHERE (twoF_H1 < twoF and twoF_L1 < twoF);''')
    db.commit()
    return db_cursor


# For debugging only; see above
db.close()

###########################################

###### End of xml2sql.py  ##################################
