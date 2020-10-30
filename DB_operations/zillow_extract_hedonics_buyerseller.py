import psycopg2
import sys
import os

## Run in format: python3 zillow_extract_hedonics.py ST_num (delete)

# Declare gloabal variables, see zillow_txt_to_database.py for details
path = '/net/storage-01/econ/research/zillow/raw/current_20200407/decompressed/'
dbname = 'zillow_2017_nov'
st_num = sys.argv[1]
completionfilename = '/net/storage-01/econ/research/zillow/misc/store_records_new/Data_Stored_%s.txt' % st_num
zasmschema = 'newzasmt' + st_num
ztransschema = 'newztrans' + st_num
# delete option, indicating whether to delete the zasmt & ztrans schemas
delete = False



# Check input format
if len(sys.argv) == 1 or len(sys.argv) > 3:
	print("Please enter a state number.")
	sys.exit(-1)

# Check state number exists
if not os.path.exists(path + st_num):
	print("Please enter a valid state number (i.e. two digits).")
	sys.exit(-2)

# Check delete option
if len(sys.argv) == 3 and sys.argv[2] == 'delete':
	delete = True

# Connect to database
conn_string = "host=db1.econ.duke.edu dbname='{}' user='zrw' password='thezillowdance'".format(dbname)
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()
print("Connected to database: host = db1.econ.duke.edu dbname = {} user = 'zrw'".format(dbname))

# deleting temporary schemas if necessary
print("Only final hedonic database will be kept. All temporary tables are being deleted.")
cursor.execute(""" DROP TABLE IF EXISTS hedonics_new.hedonics_aug_%s """ % (st_num))
cursor.execute(""" DROP TABLE IF EXISTS %s.BASE """ % (zasmschema))
cursor.execute(""" DROP TABLE IF EXISTS %s.BLDG """ % (zasmschema))
cursor.execute(""" DROP TABLE IF EXISTS %s.HEDONICS """ % (zasmschema))
cursor.execute(""" DROP TABLE IF EXISTS %s.PROPTRANS """ % (ztransschema))
cursor.execute(""" DROP TABLE IF EXISTS %s.TRANS """ % (ztransschema))
cursor.execute(""" DROP TABLE IF EXISTS %s.HEDONICS """ % (ztransschema))
cursor.execute(""" DROP TABLE IF EXISTS %s.BUYERN """ % (ztransschema))
cursor.execute(""" DROP TABLE IF EXISTS %s.SELLERN""" % (ztransschema))
cursor.execute(""" DROP TABLE IF EXISTS %s.BUYERSELLER""" % (ztransschema))
conn.commit()

# Start constructing hedonics
print("Start Constructing hedonics...")
# ZAsmt hedonics
cursor.execute(""" SELECT RowID, ImportParcelID, LoadID,
                          FIPS, State, County,
                          PropertyFullStreetAddress,
                          PropertyHouseNumber, PropertyHouseNumberExt,
						  PropertyStreetPreDirectional, PropertyStreetName,
						  PropertyStreetSuffix, PropertyStreetPostDirectional,
                          PropertyCity, PropertyState, PropertyZip,
                          PropertyBuildingNumber, PropertyAddressUnitDesignator,
						  PropertyAddressUnitNumber, PropertyAddressLatitude,
						  PropertyAddressLongitude,
						  PropertyAddressCensusTractAndBlock,
                          NoOfBuildings,
                          LotSizeAcres, LotSizeSquareFeet,
                          TaxAmount, TaxYear
  				  INTO %s.BASE FROM %s.utmain
			   """ % (zasmschema, zasmschema)) 
# from newzamst01.utmain to newzamst01.BASE
               

# clear duplicates, keep the only importparcelid with the largest loadid
cursor.execute(""" WITH DATA AS (
						SELECT ImportParcelID, MAX(LoadID) AS LoadID, COUNT(*)
						FROM %s.BASE
						GROUP BY ImportParcelID
						HAVING COUNT(*) > 1
					)
				   DELETE FROM %s.BASE AS BA
				   USING DATA
				   WHERE DATA.ImportParcelID = BA.ImportParcelID
				   AND DATA.LoadID > BA.LoadID
			   """ % (zasmschema, zasmschema))

cursor.execute(""" SELECT RowID, NoOfUnits, BuildingOrImprovementNumber,
                       	  YearBuilt, EffectiveYearBuilt, YearRemodeled,
                       	  NoOfStories, StoryTypeStndCode, TotalRooms,
						  TotalBedrooms, FullBath, ThreeQuarterBath, HalfBath,
						  QuarterBath, HeatingTypeorSystemStndCode,
                          PropertyLandUseStndCode, WaterStndCode
					INTO %s.BLDG
					FROM %s.utbuilding
					WHERE PropertyLandUseStndCode
					IN (	'RR101',  /* SFR */
                        	'RR999',  /* Inferred SFR */
                        	'RR104',  /* Townhouse */
                        	'RR105',  /* Cluster Home */
	                        'RR106',  /* Condominium */
	                        'RR107',  /* Cooperative */
	                        'RR108',  /* Row House */
	                        'RR109',  /* Planned Unit Development */
	                        'RR113',  /* Bungalow */
	                        'RR116',  /* Patio Home */
	                        'RR119',  /* Garden Home */
	                        'RR120')
			   """ % (zasmschema, zasmschema))

# collect zasmt hedonics
cursor.execute(""" WITH ATTR AS (
						SELECT *
					    FROM %s.BASE
						INNER JOIN %s.BLDG
					    USING (RowID)
					),
					SQFT AS (
						SELECT RowID,
							   BuildingOrImprovementNumber,
							   MAX(BuildingAreaSqFt) AS SqFeet
						FROM %s.utbuildingareas
						WHERE BuildingAreaStndCode
						IN( 'BAL',  /* Building Area Living */
							'BAF',  /* Building Area Finished */
							'BAE',  /* Effective Building Area */
							'BAG',  /* Gross Building Area */
							'BAJ',  /* Building Area Adjusted */
							'BAT',  /* Building Area Total */
							'BLF')
						GROUP BY RowID, BuildingOrImprovementNumber
					)
					SELECT ATTR.*,
						   SQFT.SqFeet
					INTO %s.HEDONICS
					FROM ATTR
					LEFT JOIN SQFT
					ON ATTR.RowID = SQFT.RowID
					AND ATTR.BuildingOrImprovementNumber = SQFT.BuildingOrImprovementNumber
			   """ % (zasmschema, zasmschema, zasmschema, zasmschema))
conn.commit()
print("ZAsmt hedonics finished")
# The HEDONICS above is newzamstXX.HEDONICS

# ZTrans hedonics
cursor.execute(""" SELECT *
					INTO %s.PROPTRANS
					FROM %s.utpropertyinfo
			   """ % (ztransschema, ztransschema))
# from newztrans01.utpropertyindo to newztrans01.PROPTRANS
# delete duplicates
cursor.execute(""" WITH DATA AS (
						  SELECT PropertySequenceNumber,
						  		 TransId,
						  		 MAX(LoadID) AS LoadID,
								 Count(*)
						  FROM %s.PROPTRANS
						  GROUP BY PropertySequenceNumber, TransId
						  HAVING COUNT(*) > 1
					 )
					DELETE FROM %s.PROPTRANS AS PROP
					USING DATA
					WHERE DATA.TransId = PROP.TransId
					AND DATA.PropertySequenceNumber = PROP.PropertySequenceNumber
					AND DATA.LoadID > PROP.LoadID;
			   """ % (ztransschema, ztransschema))
cursor.execute(""" SELECT TransId, LoadID,
                          RecordingDate, DocumentDate,
                          SignatureDate, EffectiveDate,
                          SalesPriceAmount, LoanAmount,
                          SalesPriceAmountStndCode, LoanAmountStndCode,
                          DataClassStndCode, DocumentTypeStndCode,
                          PartialInterestTransferStndCode,
                          IntraFamilyTransferFlag,
                          TransferTaxExemptFlag, PropertyUseStndCode,
                          AssessmentLandUseStndCode, OccupancyStatusStndCode
					INTO %s.TRANS
					FROM %s.utmain
					WHERE DataClassStndCode IN ('D', 'H', 'F', 'M')
		   	   """ % (ztransschema, ztransschema))
# delete duplicates
cursor.execute(""" WITH DUP AS (
						SELECT TransId, MAX(LoadID) AS LoadID, COUNT(*)
						FROM %s.TRANS
						GROUP BY TransId
						HAVING COUNT(*) > 1
					)
					DELETE FROM %s.TRANS as TR
					USING DUP
					WHERE DUP.TransId = TR.TransId
					AND DUP.LoadID > TR.LoadID;
			   """ % (ztransschema, ztransschema))
               
#add buyer seller tables: 
# ZTrans buyer: 
cursor.execute(""" SELECT  TransId, 
                           BuyerFirstMiddleName, 
                           BuyerLastName, 
                           BuyerIndividualFullName, 
                           BuyerNonIndividualName, 
                           BuyerNameSequenceNumber, 
                           BuyerMailSequenceNumber, 
                           LoadID
					INTO %s.BUYERN
					FROM %s.utbuyername
			   """ % (ztransschema, ztransschema))
# ZTrans seller: 
cursor.execute(""" SELECT  TransId, 
                           SellerFirstMiddleName, 
                           SellerLastName, 
                           SellerIndividualFullName, 
                           SellerNonIndividualName ,
                           SellerNameSequenceNumber, 
                           SellerMailSequenceNumber,
                           LoadID
					INTO %s.SELLERN
					FROM %s.utsellername
			   """ % (ztransschema, ztransschema))

# delete duplicates
cursor.execute(""" WITH DATA AS (
						  SELECT TransId,
						  		 MAX(LoadID) AS LoadID,
								 Count(*)
						  FROM %s.BUYERN
						  GROUP BY TransId
						  HAVING COUNT(*) > 1
					 )
					DELETE FROM %s.BUYERN AS BUYN
					USING DATA
					WHERE DATA.TransId = BUYN.TransId
					AND DATA.LoadID > BUYN.LoadID;
			   """ % (ztransschema, ztransschema))
cursor.execute(""" WITH DATA AS (
						  SELECT TransId,
						  		 MAX(LoadID) AS LoadID,
								 Count(*)
						  FROM %s.SELLERN
						  GROUP BY TransId
						  HAVING COUNT(*) > 1
					 )
					DELETE FROM %s.SELLERN AS SELLN
					USING DATA
					WHERE DATA.TransId = SELLN.TransId
					AND DATA.LoadID > SELLN.LoadID;
			   """ % (ztransschema, ztransschema))

# transform data type of TransId from PROPTRANS because the next merge is not working: 
cursor.execute(""" ALTER TABLE %s.PROPTRANS ALTER COLUMN TransId TYPE VARCHAR USING TransId::VARCHAR """ % (ztransschema))

cursor.execute(""" SELECT BUYN.TransId,
                          BUYN.BuyerFirstMiddleName,
                          BUYN.BuyerLastName, 
                          BUYN.BuyerIndividualFullName, 
                          BUYN.BuyerNonIndividualName, 
                          BUYN.BuyerNameSequenceNumber, 
                          BUYN.BuyerMailSequenceNumber,
                          SELLN.SellerFirstMiddleName,
                          SELLN.SellerLastName, 
                          SELLN.SellerIndividualFullName, 
                          SELLN.SellerNonIndividualName, 
                          SELLN.SellerNameSequenceNumber, 
                          SELLN.SellerMailSequenceNumber
					INTO %s.BUYERSELLER
					FROM %s.BUYERN AS BUYN
					INNER JOIN %s.SELLERN AS SELLN
					USING (TransId)
			   """ % (ztransschema, ztransschema, ztransschema))
cursor.execute(""" ALTER TABLE %s.BUYERSELLER ALTER COLUMN TransId TYPE VARCHAR USING TransId::VARCHAR """ % (ztransschema))

# collect ztrans hedonics
cursor.execute(""" SELECT BUYSELL.BuyerFirstMiddleName,
                          BUYSELL.BuyerLastName, 
                          BUYSELL.BuyerIndividualFullName, 
                          BUYSELL.BuyerNonIndividualName, 
                          BUYSELL.BuyerNameSequenceNumber, 
                          BUYSELL.BuyerMailSequenceNumber,
                          BUYSELL.SellerFirstMiddleName,
                          BUYSELL.SellerLastName, 
                          BUYSELL.SellerIndividualFullName, 
                          BUYSELL.SellerNonIndividualName, 
                          BUYSELL.SellerNameSequenceNumber, 
                          BUYSELL.SellerMailSequenceNumber,
                          PROP.*,
						  TR.RecordingDate,
						  TR.DocumentDate,
						  TR.SignatureDate,
						  TR.EffectiveDate,
						  TR.SalesPriceAmount,
					      TR.SalesPriceAmountStndCode,
						  TR.LoanAmountStndCode,
						  TR.DataClassStndCode,
						  TR.DocumentTypeStndCode,
						  TR.PartialInterestTransferStndCode,
						  TR.IntraFamilyTransferFlag,
						  TR.TransferTaxExemptFlag,
						  TR.PropertyUseStndCode,
						  TR.AssessmentLandUseStndCode,
						  TR.OccupancyStatusStndCode
					INTO %s.HEDONICS
					FROM %s.PROPTRANS AS PROP
					INNER JOIN %s.TRANS AS TR
					USING (TransId)
                    			FULL JOIN %s.BUYERSELLER AS BUYSELL
					USING (TransId)
			   """ % (ztransschema, ztransschema, ztransschema, ztransschema))
conn.commit()
print("ZTrans hedonics finished")
# HEDONICS above is on newztransXX.HEDONICS
# Final hedonics
# Create final hedonic SCHEMA if not exists: hedonics_new
cursor.execute(""" CREATE SCHEMA IF NOT EXISTS %s """ % ('hedonics_new'))
cursor.execute(""" SELECT ZASMTHED.*,
					      ZTRANSHED.transid,
						  ZTRANSHED.assessorparcelnumber,
						  ZTRANSHED.unformattedassessorparcelnumber,
						  ZTRANSHED.legallotsize,
						  ZTRANSHED.propertysequencenumber,
						  ZTRANSHED.propertyaddressmatchcode,
						  ZTRANSHED.propertyaddressgeocodematchcode,
						  ZTRANSHED.legalsectwnrngmer,
						  ZTRANSHED.legalcity,
						  ZTRANSHED.bkfspid,
						  ZTRANSHED.assessmentrecordmatchflag,
						  ZTRANSHED.recordingdate,
						  ZTRANSHED.documentdate,
						  ZTRANSHED.signaturedate,
						  ZTRANSHED.salespriceamountstndcode,
						  ZTRANSHED.loanamountstndcode,
						  ZTRANSHED.dataclassstndcode,
						  ZTRANSHED.partialinteresttransferstndcode,
						  ZTRANSHED.salespriceamount,
						  ZTRANSHED.IntraFamilyTransferFlag,
						  ZTRANSHED.TransferTaxExemptFlag, 
                          ZTRANSHED.BuyerFirstMiddleName,
                          ZTRANSHED.BuyerLastName, 
                          ZTRANSHED.BuyerIndividualFullName, 
                          ZTRANSHED.BuyerNonIndividualName, 
                          ZTRANSHED.BuyerNameSequenceNumber, 
                          ZTRANSHED.BuyerMailSequenceNumber,
                          ZTRANSHED.SellerFirstMiddleName,
                          ZTRANSHED.SellerLastName, 
                          ZTRANSHED.SellerIndividualFullName, 
                          ZTRANSHED.SellerNonIndividualName, 
                          ZTRANSHED.SellerNameSequenceNumber, 
                          ZTRANSHED.SellerMailSequenceNumber
					INTO %s
					FROM %s.HEDONICS AS ZASMTHED
					LEFT JOIN %s.HEDONICS AS ZTRANSHED
					ON ZASMTHED.importparcelid = ZTRANSHED.importparcelid
			   """ % ('hedonics_new.hedonics_aug_'+st_num, zasmschema, ztransschema))
# Final commit
conn.commit()
print("Final hedonics finished")

# Deleting temporary schemas if necessary
print("Only final hedonic database will be kept. All temporary tables are being deleted.")
cursor.execute(""" DROP TABLE IF EXISTS %s.BASE """ % (zasmschema))
cursor.execute(""" DROP TABLE IF EXISTS %s.BLDG """ % (zasmschema))
cursor.execute(""" DROP TABLE IF EXISTS %s.HEDONICS """ % (zasmschema))
cursor.execute(""" DROP TABLE IF EXISTS %s.PROPTRANS """ % (ztransschema))
cursor.execute(""" DROP TABLE IF EXISTS %s.TRANS """ % (ztransschema))
cursor.execute(""" DROP TABLE IF EXISTS %s.HEDONICS """ % (ztransschema))
cursor.execute(""" DROP TABLE IF EXISTS %s.BUYERN """ % (ztransschema))
cursor.execute(""" DROP TABLE IF EXISTS %s.SELLERN""" % (ztransschema))
cursor.execute(""" DROP TABLE IF EXISTS %s.BUYERSELLER""" % (ztransschema))
conn.commit()

# Finished!
print("Extraction of hedonics for State %s finished" % st_num)
