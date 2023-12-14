from ofxtools.models import *
from ofxtools.Types import *
from ofxtools.utils import UTC
from decimal import Decimal
from datetime import datetime
from pprint import pprint
from ofxtools.header import make_header

import xml.etree.ElementTree as ET

import csv
import uuid
import os
import re

PATH   = "data/History_for_Account_9554419996.csv"
OUT_PATH = "output/transactions.qfx"
HEADER = ['Run Date', 'Action', 'Symbol', 'Security Description', 'Security Type', 'Quantity', 'Price ($)', 'Commission ($)', 'Fees ($)', 'Accrued Interest ($)', 'Amount ($)', 'Settlement Date' ]

filename = os.path.basename( PATH )
filename = re.search( ".*_(.*)\.csv", filename )
acctid   = filename.group(1)

def validate_file( lines ):
    if lines[3] != ['Brokerage']:
        print( "[!] Forth line does not contain Brokerage" )
        print( lines[3] )
        return False
    
    fileHeader = lines[5]
    
    if len( HEADER ) != len( fileHeader ):
        print( "[!] Header Length Mismatch" )
        return False
    
    for column in HEADER:
        if column not in fileHeader:
            print( f"[!] Header Column Not Found: {column}" )
            return False

    return True


def extract_unique_securities( lines ):
    lines = lines[1:]
    uniqueSecurities = set()
    identifier = 100000000
    
    for line in lines:
        if line[3].strip() != 'No Description':
            uniqueSecurities.add( ( line[2].strip(), line[3].strip(), identifier ) )
            identifier = identifier + 1
    
    uniqueSecurities = list( uniqueSecurities )
    
    securityMap = {}
    
    for security in uniqueSecurities:
        securityMap[ security[0] ] = security
    
    # pprint( securityMap )
    
    return securityMap


def make_security_list_message_set_response_messages( securityMap ): # SECLISTMSGSRSV1
    
    messages = []
    securityList = []
    
    for security in securityMap.values():
        
        secid   = SECID( uniqueid = str( security[2] ), uniqueidtype = 'OTHER' )
        secname = security[1]
        ticker  = security[0]
        secinfo = SECINFO( secid = secid, secname = secname, ticker = ticker )
        mfinfo  = MFINFO( secinfo = secinfo )
        
        securityList.append( mfinfo )
        
    seclist = SECLIST( *securityList )
    messages = SECLISTMSGSRSV1( seclist )
    
    return messages



def make_investment_statement_message_set_response_messages( securityMap, transactions ):
    
    transactionList = []
    response        = None
    
    trnuid = "0"
    status = STATUS( code = 0, severity = 'INFO' )
    
    startDate = datetime( 3000, 1, 1, tzinfo = UTC )
    endDate = datetime( 1970, 1, 1, tzinfo = UTC )
    
    for transaction in transactions[1:]:
        transactionDate = datetime.strptime( transaction[0].strip().rstrip(" ET"), "%m/%d/%Y %I:%M:%S %p" ).replace( tzinfo = UTC )
        
        if startDate > transactionDate:
            startDate = transactionDate
            
        if endDate < transactionDate:
            endDate = transactionDate

        description    = transaction[1].strip()
        symbol         = transaction[2].strip() if len( transaction[2].strip() ) > 0 else None
        securityType   = transaction[4].strip() if len( transaction[4].strip() ) > 0 else None
        quantity       = float( transaction[5].strip() ) if len( transaction[5].strip() ) > 0 else None
        price          = float( transaction[6].strip() ) if len( transaction[6].strip() ) > 0 else None
        fee            = float( transaction[8].strip() ) if len( transaction[8].strip() ) > 0 else None
        amount         = float( transaction[10].strip() ) if len( transaction[10].strip() ) > 0 else None
        settlementDate = datetime.strptime( transaction[11].strip(), "%m/%d/%Y" ).replace( tzinfo = UTC ) if len( transaction[11].strip() ) > 0 else None
        
        # print( f"{transactionDate} {symbol} {quantity} {price} {fee} {amount} {settlementDate}" )
        
        if description.startswith("You bought"):
            
            if amount < 0:
                
                invtran     = INVTRAN( fitid = str( uuid.uuid4() ), dttrade = transactionDate, dtsettle = settlementDate )
                secid       = SECID( uniqueid = str( securityMap[ symbol ][2] ), uniqueidtype = 'OTHER' )
                units       = quantity
                unitprice   = price
                fees        = fee
                total       = amount * -1
                subacctsec  = securityType
                subacctfund = 'CASH'
                
                invbuy      = INVBUY( invtran = invtran, secid = secid, units = units, unitprice = unitprice, fees = fees, total = total, subacctsec = subacctsec, subacctfund = subacctfund )
                buyother    = BUYOTHER( invbuy = invbuy )
                
                transactionList.append( buyother )
        elif description.startswith("Transfer in from brokerage a/c"):
            # deal with transfer in from brokerage
            pass       
        else:
            
            print( f"[?] Not Handled {transaction}" )
                
            
        
    invtranlist = INVTRANLIST( dtstart = startDate, dtend = endDate, *transactionList )
    
    currentDate         = datetime.now().replace( tzinfo = UTC )
    invacctfrom         = INVACCTFROM( brokerid = "fidelity.com", acctid = acctid )
    invstmtrs           = INVSTMTRS( dtasof = currentDate, curdef = 'USD', invacctfrom = invacctfrom, invtranlist = invtranlist  )
    transactionResponse = INVSTMTTRNRS( trnuid = trnuid, status = status, invstmtrs = invstmtrs )
    messages            = INVSTMTMSGSRSV1( transactionResponse )
    
    return messages



def process_file( lines ):
    transactions = list( filter( lambda line: len( line ) > 1, lines ) )
    
    currentDate = datetime.now().replace( tzinfo = UTC )
    
    securityMap         = extract_unique_securities( transactions )
    securityMessages    = make_security_list_message_set_response_messages( securityMap )
    transactionMessages = make_investment_statement_message_set_response_messages( securityMap, transactions )
    
    status     = STATUS( code = 0, severity = 'INFO' )
    fi         = FI( org = 'Fidelity Investments', fid = '07776' )
    sonrs      = SONRS( status = status, dtserver = currentDate, language='ENG', fi = fi )
    signonmsgs = SIGNONMSGSRSV1( sonrs = sonrs )

    ofx  = OFX( signonmsgsrsv1 = signonmsgs, seclistmsgsrsv1 = securityMessages, invstmtmsgsrsv1 = transactionMessages )
    root = ofx.to_etree()
    
    ET.indent( root )

    fileData = ET.tostring( root ).decode()
    
    header = str( make_header( version = 102 ) )
    
    # print( header ) 
    # print( fileData )
    
    with open( OUT_PATH, "w" ) as fp:
        fp.write( header )
        fp.write( fileData )

    
    
with open(PATH, newline='') as f:
    fileLines = list( csv.reader( f ) )
  
if validate_file( fileLines ):
    process_file( fileLines[5:] )