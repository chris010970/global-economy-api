import os
import yaml
import requests
import pycountry
import pandas as pd

import xmltodict
import xml.etree.ElementTree as elementTree

from enum import Enum
from time import sleep
from munch import munchify
from datetime import date, datetime


"""
From Neven @ Global Economy

URL: https://www.theglobaleconomy.com/data_feed_api.php
username: earthintelligence
password: XXXXX

1. The ind and cnt parameters in the XML identify the indicators and the countries. The annual indicators use the
country code and the monthly/quarterly indicators use the country IDs.

2. The uid and uidc parameters are used to define the account uniquely and securely. The parameter tp equals 1 for
annual indicators and 2 for monthly and quarterly indicators.

3. One of the XML formats gives only the latest numbers and the other gives the entire series. The default period is
1960 to 2030 and one can adjust the period values in the XML for a specific time period.

4. The selection of periods uses only years to keep things simple. For example, if one selects a year for which there is
only data until mid-year, the XML will give only the data up to the month for which they are available. Hence one can
use only the NOT NULL cells.

Example URI:  https://www.theglobaleconomy.com/data_export_api.php?tp=1&ind=2&cnt=CN&prd=1960:2022&uid=90902&uidc=0318f527ddfd08c361f1df372fdfd2e4

"""


class Client:

    """
    api frequency options
    """    
    class Frequency( Enum ):
        annual = 1
        monthly = 2


    def __init__( self, **kwargs ):

        """
        constructor
        """
        
        # copy arguments
        self._root = kwargs.get( 'root', 'https://www.theglobaleconomy.com/data_export_api.php' )

        # parse cfg path if exists
        cfg_path = kwargs.get ( 'cfg_path' )
        if cfg_path is None:

            # setup default cfg path            
            repo = 'global-economy-api'
            root_path = os.getcwd()[ 0 : os.getcwd().find( 'GitHub' ) + len ( 'GitHub' )]
            cfg_path = os.path.join( os.path.join( root_path, repo ), 'cfg' )

        # load files from cfg path
        self._uid, self._uidc = self.getCredentials( cfg_path )
        self._lut = dict()

        # load api-specific country code lut 
        pathname = os.path.join( cfg_path, 'country-code.csv' )
        self._lut[ 'api' ] = pd.read_csv( pathname )

        # generate indicator lut file
        for frequency in Client.Frequency:

            pathname = os.path.join( cfg_path, 'indicator-{name}.csv'.format(name=frequency.name) )
            if not os.path.exists( pathname ):

                lut = pd.DataFrame( self.getIndicatorLookup( frequency ) )
                lut.to_csv( pathname )

            # load lut file into dataframe
            self._lut[ frequency.name ] = pd.read_csv( pathname )

        return


    def getIndicatorLookup( self, frequency, max_index=2000 ):

        """
        generate name to index lookup table for indicators
        """

        lut = list()

        # iterate through indicator index uris
        for idx in range( 1, max_index ):

            uri = self.getUri( frequency, indexes=[idx], countries=[ 'India' ], period='latest' )
            data = self.postRequest( uri )

            # xml response
            if data is not None:

                try:

                    # sleep a short while
                    sleep(0.1)

                    # parse name of indicator
                    name = data[ 'ge:data' ][ 'ge:country' ][ 'ge:element' ][ 'ge:indicator' ]
                    lut.append( { 'index' : idx, 'name' : name } )

                except BaseException as error:
                    print ( 'Error: {} -> {} '.format ( data, error ) )            

        return lut


    def postRequest( self, uri, timeout=5 ):
        
        """
        post request to global economy server and handle response
        """

        def isXml(value):
            
            # check string is valid xml
            try:
                # attempt to parse string
                elementTree.fromstring(value)

            except elementTree.ParseError:
                # invalid xml
                return False
            
            return True

        # default null return value        
        data = None
        try:                
            # post request and handle exceptions
            response = requests.get( uri, timeout=timeout )
            response.raise_for_status()

            # validate response and parse into dict
            if isXml( response.content ):
                data = xmltodict.parse(response.content)

        # exception handling
        except requests.exceptions.HTTPError as error:
            print ( 'Http Error: {} '.format ( error ) )
        except requests.exceptions.ConnectionError as error:
            print ( 'Connection Error: {} '.format ( error ) )
        except requests.exceptions.Timeout as error:
            print ( 'Timeout Error: {} '.format ( error ) )
        except requests.exceptions.RequestException as error:
            print ( 'Request Error: {} '.format ( error ) )
        except BaseException as error:
            print ( 'General Error: {} '.format ( error ) )

        return data


    def convertToDataFrame( self, data ):

        """
        convert xml response to dataframe
        """

        df = None
        records = []

        # iterate through country schemas
        schemas = data[ 'ge:data' ][ 'ge:country' ] if isinstance( data[ 'ge:data' ][ 'ge:country' ], list ) else [ data[ 'ge:data' ][ 'ge:country' ] ]
        for schema in schemas:

            if 'ge:element' in schema:

                # iterate through elements
                elements = schema[ 'ge:element'] if isinstance( schema[ 'ge:element'], list ) else [ schema[ 'ge:element'] ]
                for element in elements:

                    try:

                        # parse element values into record
                        records.append ( {  'id' : schema[ '@id' ],
                                            'date' : date( int( element.get( 'ge:year' ) ), int( element.get( 'ge:month', 1 ) ), 1 ),
                                            'indicator' : element[ 'ge:indicator' ],
                                            'value' : element[ 'ge:value' ] } )

                    except BaseException as error:
                        print ( 'XML Read Error: {} '.format ( error ) )

        # create dataframe if records available
        if len( records ) > 0:
            df = pd.DataFrame( records )

        return df


    def getUri( self, iso_codes, frequency, **kwargs ):

        """
        build and post uri request
        """

        uri = None

        # create period string        
        period = kwargs.get( 'period' )        
        if period is None:
            start_year = kwargs.get( 'start_year', 1960 )
            end_year = kwargs.get( 'end_year', datetime.now().year )
            period = f'{start_year}:{end_year}'

        # use alpha_2 codes for annual uris
        if frequency is Client.Frequency.annual:
            codes = self.getAlpha2Codes( iso_codes )

        # use api-specific (*ugh*) codes for monthly uris
        if frequency is Client.Frequency.monthly:

            alpha3_codes = self.getAlpha3Codes( iso_codes )
            codes = self._lut[ 'api' ][ self._lut[ 'api' ][ 'code' ].str.contains( '|'.join( alpha3_codes ) ) ]
            codes = [ code for code in codes[ 'id' ] ]

        # parse indicator args
        indicators = kwargs.get( 'indicators' )
        indexes = kwargs.get( 'indexes' )
        
        # convert indicator names to indexes
        if indicators is not None:
            indexes = self.getIndicatorIndexes( frequency, indicators )

        # check valid args
        if len( codes ) > 0 and len( indexes ) > 0:
        
            # format uri string
            uri = '{root}?tp={tp}&ind={indexes}&cnt={codes}&prd={period}&uid={uid}&uidc={uidc}'.format ( root=self._root,
                                                                                                            tp=frequency.value,
                                                                                                            indexes=','.join(str(idx) for idx in indexes),
                                                                                                            codes=','.join(str(code) for code in codes),
                                                                                                            period=period,
                                                                                                            uid=self._uid,
                                                                                                            uidc=self._uidc )
        else:

            # invalid arguments
            print ( 'Invalid indicator arguments' )

        return uri


    def getCredentials( self, path ):

        """
        load credentials from file        
        """

        uid = uidc = None

        # load config parameters from file
        with open( os.path.join( path, 'credentials.yml' ), 'r' ) as f:
            credentials = munchify( yaml.safe_load( f )[ 'credentials' ] )

            uid = credentials.uid
            uidc = credentials.uidc

        return uid, uidc


    def getAlpha2Codes( self, iso_codes ):

        """
        convert country names into alpha 2 or alpha 3 codes
        """

        # iterate through country names
        codes = set()
        for iso_code in iso_codes:
            try:

                if len( iso_code ) == 2:
                    codes.add( iso_code )
                    continue

                if len( iso_code ) == 3:
                    codes.add( pycountry.countries.get(alpha_3=iso_code).alpha_2 )
                            
            except BaseException as error:
                print ( 'Error: {} '.format ( error ) )

        return codes


    def getAlpha3Codes( self, iso_codes ):

        """
        convert country names into alpha 2 or alpha 3 codes
        """

        # iterate through country names
        codes = set()
        for iso_code in iso_codes:
            try:

                if len( iso_code ) == 3:
                    codes.add( iso_code )
                    continue

                if len( iso_code ) == 2:
                    codes.add( pycountry.countries.get(alpha_2=iso_code).alpha_3 )
                            
            except BaseException as error:
                print ( 'Error: {} '.format ( error ) )

        return codes


    def getIndicatorIndexes( self, frequency, names ):

        """
        convert indicator names to indexes
        """

        # iterate through names
        indexes = []
        for name in names:

            rows = self._lut[ frequency.name ][ self._lut[ frequency.name ][ 'name' ] == name ]
            if len( rows ) == 1:
                indexes.append( rows[ 'index' ].iloc[ 0 ] )
                continue

            # indicator name not found
            print ( 'Indicator name not found / duplicated: {}'.format( name ) )

        return indexes


    @staticmethod
    def runTests():

        """
        execute tests
        """

        def test1():

            # test countries + monthly indicator names
            print ( 'test 1')
            uri = obj.getUri( ['IN', 'CN'], Client.Frequency.monthly, indicators=['Debt service ratios for private non-financial sector'], period='latest' )
            print ( uri )

            # convert response to dataframe
            df = obj.convertToDataFrame( obj.postRequest( uri ) )
            print ( df )
            return

        def test2():

            # test countries + annual indicator names
            print ( 'test 2')
            indicators = [ 'Exports, percent of GDP', 'Access to electricity' ]
            uri = obj.getUri( [ 'IND', 'CHN'], Client.Frequency.annual, indicators=indicators, start_year=1960, end_year=2020 )
            print ( uri )

            # convert response to dataframe
            df = obj.convertToDataFrame( obj.postRequest( uri ) )
            print ( df )
            return


        def test3():

            # test countries + annual indicator names
            print ( 'test 3')
            indicators = [ 'Exports, percent of GDP', 'Access to electricity' ]
            uri = obj.getUri( [ 'IN', 'CN'], Client.Frequency.annual, indicators=indicators, start_year=1960, end_year=2020 )
            print ( uri )

            # convert response to dataframe
            df = obj.convertToDataFrame( obj.postRequest( uri ) )
            print ( df )
            return


        # create request object
        obj = Client()

        # run tests
        test1()
        test2()
        test3()

        return


#Client.runTests()

"""
To build URIs to access monthly / quaterly indicator values, API bizarrely utilises its own unique set of country codes (!?)

Code snippet generates country code lookup table from JSON file created from country select HTML taken from GE website !
"""

"""
import json

# get repo root path 
repo = 'global-economy'
root_path = os.getcwd()[ 0 : os.getcwd().find( repo ) + len ( repo )]

# load json from file
cfg_path = os.path.join( root_path, 'cfg' )
with open( os.path.join( cfg_path, 'html.json' ), 'r' ) as f:
    data = json.load(f)

    records = list()
    for element in data[ 'root' ][ 'li' ]:
        records.append( {   'code' : element[ 'input' ][ '@value'].upper(),
                            'name' : element[ '#text'],
                            'id' : element [ 'input'][ '@country_id']
         })

df = pd.DataFrame( records )
df.to_csv( 'api-country-ids.csv')
"""

