import csv
import argparse
import glob
import json
import os
import hashlib

def rawDataDir( ):
	return "./data/raw"
	
def outputDataDir( ):
	ret = "./data/preprocess"
	if not os.path.exists( ret ):
		os.mkdir( ret )
	return ret
	
def encodeKey( key ):
	md5hash = hashlib.md5( )
	md5hash.update( key )
	return md5hash.hexdigest( )

def outputDataDirYear( year, subdir ):
	year_dir = "/".join( [ outputDataDir( ), year, subdir ] )
	if not os.path.exists( year_dir ):
		os.mkdir( year_dir )
	return year_dir

def writeGlobalFrag( fragObj, filename ):
	outputFilename = "/".join( [ outputDataDir( ), filename ] ) 
	with open( outputFilename, 'w+' ) as f:
		f.write( json.dumps( fragObj, sort_keys=True, indent=5 ) )
		
def writeYearFrag( fragObj, year, subdir, filename ):
	outputFilename = "/".join( [ outputDataDirYear( year, subdir ), filename ] ) 
	with open( outputFilename, 'w+' ) as f:
		f.write( json.dumps( fragObj, sort_keys=True, indent=5 ) )

def genFrag_years( ):
	rawCSVFiles = glob.glob( "/".join( [ rawDataDir( ), "*.csv" ] ) )
	ret = [ ]
	for csvFile in rawCSVFiles:
		csvFile = csvFile.replace( rawDataDir( ), "" )
		csvFile = csvFile.replace( "/", "" )
		csvFile = csvFile.replace( ".csv", "" )
		ret.append( csvFile )
	return ret

def getFrag_headers( year ):
	filename = "/".join( [ rawDataDir( ), ".".join( [ year, "csv" ] ) ] )
	with open( filename, 'r' ) as datafile:
		datareader = csv.reader( datafile, delimiter=',' )
		header = datareader.next( )
		ret = { }
		for idx, val in enumerate(header):
			ret.update( { encodeKey( val ):{ "label":val, "index":idx } } )
		return ret

def genFrag_taxon( year, header_item_index, key, label ):
	filename = "/".join( [ rawDataDir( ), ".".join( [ year, "csv" ] ) ] )
	with open( filename, 'r' ) as datafile:
		datareader = csv.reader( datafile, delimiter=',' )
		is_header = True
		values = set( )
		for row in datareader:
			if is_header:
				is_header = False
				continue
			values.add( row[ header_item_index ] )
		return { "key":key, "label":label, "values":list( values ) }


def main( ):
	parser = argparse.ArgumentParser( description='A utiltiy to preprocess budget CSV data into JSON fragments' )
	args = parser.parse_args( )
	
	years = genFrag_years( )
	writeGlobalFrag( years, "years.json" )
	for year in years:
		header = getFrag_headers( year )
		writeYearFrag( header, year, "", "header.json" )
		for key, value in header.iteritems( ):
			if value[ "label" ] == "AMT":
				continue
			values = genFrag_taxon( year, value[ "index" ], key, value[ "label" ] )
			values_filename = "{0}.json".format( key )
			writeYearFrag( values, year, "taxonomy", values_filename )
			
		
	

if __name__ == "__main__":
	main( )