## Header
## ---------------------------------------------------

# Title: REC Registry data extraction script
# Purpose: To extract all required fields from the RecRegistry database. 
# Status: <in development>
# (Input/Components):
# (Output):
# (How to execute code):
# 
# Author: Rodrigo Lopez
# rodrigo.lopez@cer.gov.au
# CER5049
# Data Science team / Data and Innovation
# 
# # Date created: 5/10/2021
# (Date of last major modification):git

# Copyright (c) Clean Energy Regulator

## ---------------------------------------------------

###############
## Libraries ##
###############

library(httr) #needed for content()
library(odbc) # needed for open database connectivity
library(magrittr)
library(readr)
library(stringr) #needed for manipulating strings
library(dplyr)
library(tidyr)
# library(tidyverse)
library(lubridate) #needed for manipulation of date and times
library(RODBC)
library(gtools) # Needed for permutations
library(odbc) # needed for Odbc


####################
## DB connections ##
####################

conRECREG <- dbConnect(odbc::odbc(), "RecRegistry", timeout = 10)

###############
## Variables ##
###############

options(scipen=999, stringsAsFactors = FALSE)

DevOpsNum <- "Work Item 35079" # Used in log file, and relates to the Feature/PBI number in DevOps



# Range for "installation dates" whose records are extracted
DateStart <- as.Date("2021-10-01") # example 2019-10-21
SQLDateStart <- gsub("-","",DateStart) # Used in SQL script
DateEnd <- as.Date("2021-10-08") # example 2019-10-21
SQLDateEnd <- gsub("-","",DateEnd) # Used in SQL script

###############
## Functions ##
############### 

#None


##########
## Main ##
########## 







#######################
## Data input/ingest ##
####################### 

# SQL script as input string to an R function, which extracts the required data.
# Un/comment the required fields, as needed.
# Ensure the last uncommented data field before FROM has no comma at end of line.
RecReg_Raw_SQL <- paste0("SELECT 
      sgu.ACCREDITATION_CODE as Small_Unit_Accreditation_Code,
      sgu.ID, -- Needed for Serial Numbers matching below
     
      --Installation Address
      sgu_addr.POSTCODE as Small_Unit_Installation_Postcode,
      sgu_addr.STATE as Small_Unit_Installation_State,
      sgu_addr.SUBURB as Small_Unit_Installation_City,
      CONCAT_WS(', ',CONCAT_WS(' ',sgu_addr.UNIT_TYPE,sgu_addr.UNIT_NUMBER,sgu_addr.STREET_NUMBER,sgu_addr.STREET_NAME,sgu_addr.STREET_TYPE),sgu_addr.SUBURB,sgu_addr.STATE,sgu_addr.POSTCODE) AS 'Small_Unit_Installation_Street_Address_Full',
      sgu_addr.SITE_NAME as Small_Unit_Installation_Property_Name,
      sgu_addr.UNIT_TYPE as Small_Unit_Installation_Address_Type,
      sgu_addr.UNIT_NUMBER as Small_Unit_Installation_Address_Type_Number,
      sgu_addr.STREET_NUMBER as Small_Unit_Installation_Street_Number,
      sgu_addr.STREET_NAME as Small_Unit_Installation_Street_Name,
      sgu_addr.STREET_TYPE as Small_Unit_Installation_Street_Type,
      sgu_addr.SPECIAL_ADDRESS as Small_Unit_Installation_Additional_Address_Information,
      
      --Dates
      sgu.INSTALLATION_DATE as Small_Unit_Installed_Date, 
      convert(DATE, cert_reg.CREATED_DATE AT TIME ZONE 'UTC' AT TIME ZONE 'AUS Eastern Standard Time') as REC_Creation_Date,
      
      --Account
      acct.[Name] as Account_Name,
      vacct.[RPE_ID] as Registered_Person_ID,
      sgu.FUEL_SOURCE, 
      
      -- --Installer details
      installer.INSTALLER_ACCREDITED_NUMBER as Small_Unit_Installer_CEC_Accreditation_Code,
      installer.SURNAME as Small_Unit_Installer_Surname,
      installer.FIRST_NAME as Small_Unit_Installer_Firstname,
      installer.MOBILE as Small_Unit_Installer_Mobile_Number,
      installer.PHONE as Small_Unit_Installer_Phone_Number,
      installer.FAX as Small_Unit_Installer_Fax_Number,
      installer.EMAIL as Small_Unit_Installer_Email_Address,
      
      installer_address.POSTCODE as Small_Unit_Installer_Postcode,
      installer_address.STATE as Small_Unit_Installer_State,
      installer_address.SUBURB as Small_Unit_Installer_City,
      CONCAT_WS(', ',CONCAT_WS(' ',installer_address.UNIT_TYPE,installer_address.UNIT_NUMBER,installer_address.STREET_NUMBER,installer_address.STREET_NAME,installer_address.STREET_TYPE),installer_address.SUBURB,installer_address.STATE,installer_address.POSTCODE) AS 'Small_Unit_Installer_Street_Address_Full',
      installer_address.SITE_NAME as Small_Unit_Installer_Property_Name,
      installer_address.UNIT_TYPE as Small_Unit_Installer_Address_Type,
      installer_address.UNIT_NUMBER as Small_Unit_Installer_Address_Type_Number,
      installer_address.STREET_NUMBER as Small_Unit_Installer_Street_Number,
      installer_address.STREET_NAME as Small_Unit_Installer_Street_Name,
      installer_address.STREET_TYPE as Small_Unit_Installer_Street_Type,
      installer_address.SPECIAL_ADDRESS as Small_Unit_Installer_Additional_Address_Information,

      -- --Designer details
      designer.INSTALLER_ACCREDITED_NUMBER as Small_Unit_Designer_CEC_Accreditation_Code,
      designer.SURNAME as Small_Unit_Designer_Surname,
      designer.FIRST_NAME as Small_Unit_Designer_Firstname,
      designer.MOBILE as Small_Unit_Designer_Mobile_Number,
      designer.PHONE as Small_Unit_Designer_Phone_Number,
      designer.FAX as Small_Unit_Designer_Fax_Number,
      designer.EMAIL as Small_Unit_Designer_Email_Address,
      
      designer_address.POSTCODE as SGU_Designer_Postcode,
      designer_address.STATE as SGU_Designer_State,
      designer_address.SUBURB as SGU_Designer_City,
      CONCAT_WS(', ',CONCAT_WS(' ',designer_address.UNIT_TYPE,designer_address.UNIT_NUMBER,designer_address.STREET_NUMBER,designer_address.STREET_NAME,designer_address.STREET_TYPE),designer_address.SUBURB,designer_address.STATE,designer_address.POSTCODE) AS 'Small_Unit_Designer_Street_Address_Full',
      designer_address.SITE_NAME as SGU_Designer_Property_Name,
      designer_address.UNIT_TYPE as SGU_Designer_Address_Type,
      designer_address.UNIT_NUMBER as SGU_Designer_Address_Type_Number,
      designer_address.STREET_NUMBER as SGU_Designer_Address_Street_Number,
      designer_address.STREET_NAME as SGU_Designer_Address_Street_Name,
      designer_address.STREET_TYPE as SGU_Designer_Address_Street_Type,
      designer_address.SPECIAL_ADDRESS as SGU_Designer_Additional_Address_Information,
     
      -- --Electrician
      electrician.[ELECTRICIAN_NUMBER] as SGU_Electrician_License_Number,
      electrician.[SURNAME] as SGU_Electrician_Surname,
      electrician.[FIRST_NAME] as SGU_Electrician_Firstname,
      electrician.[MOBILE] as SGU_Electrician_Mobile_Number,
      electrician.[PHONE] as SGU_Electrician_Phone_Number,
      electrician.[FAX] as SGU_Electrician_Fax_Number,
      electrician.[EMAIL] as SGU_Electrician_Email_Address,
      
      electrician_address.[POSTCODE] as SGU_Electrician_Postcode,
      electrician_address.[STATE] as SGU_Electrician_State,
      electrician_address.[SUBURB] as SGU_Electrician_City,
      CONCAT_WS(', ',CONCAT_WS(' ',electrician_address.UNIT_TYPE,electrician_address.UNIT_NUMBER,electrician_address.STREET_NUMBER,electrician_address.STREET_NAME,electrician_address.STREET_TYPE),electrician_address.SUBURB,electrician_address.STATE,electrician_address.POSTCODE) AS 'SGU_Electrician_Street_Address_Full',
      electrician_address.SITE_NAME as SGU_Electrician_Property_Name,
      electrician_address.[UNIT_TYPE] as SGU_Electrician_Address_Type,
      electrician_address.[UNIT_NUMBER] as SGU_Electrician_Address_Type_Number,
      electrician_address.[STREET_NUMBER] as SGU_Electrician_Street_Number,
      electrician_address.[STREET_NAME] as SGU_Electrician_Street_Name,
      electrician_address.[STREET_TYPE] as SGU_Electrician_Street_Type,
      electrician_address.[SPECIAL_ADDRESS] as SGU_Electrician_Additional_Address_Information,
     
      --Small Unit Owner
      owner.[Small Unit Owner Surname] as Small_Unit_Owner_Surname,
      owner.[Small Unit Owner Firstname] as Small_Unit_Owner_Firstname,
      --owner.[Small Unit Owner Initials] as Small_Unit_Owner_Initials,
      --owner.[Small Unit Owner Title] as Small_Unit_Owner_Title,
      owner.[Small Unit Owner Mobile Number] as Small_Unit_Owner_Mobile_Number,
      owner.[Small Unit Owner Phone Number] as Small_Unit_Owner_Phone_Number,
      owner.[Small Unit Owner Fax Number] as Small_Unit_Owner_Fax_Number,
      owner.[Small Unit Owner Email Address] as Small_Unit_Owner_Email_Address,
      owner.[Small Unit Owner Postcode] as Small_Unit_Owner_Postcode,
      owner.[Small Unit Owner State] as Small_Unit_Owner_State,
      owner.[Small Unit Owner City] as Small_Unit_Owner_City,
      owner.[Small Unit Owner Street Address Full],
      CONCAT_WS(', ',owner.[Small Unit Owner Street Address Full],owner.[Small Unit Owner City],owner.[Small Unit Owner State],owner.[Small Unit Owner Postcode]) AS 'Small_Unit_Owner_Street_Address_Full',
      owner.[Small Unit Owner Address Type] as Small_Unit_Owner_Address_Type,
      owner.[Small Unit Owner Address Type Number] as Small_Unit_Owner_Address_Type_Number,
      owner.[Small Unit Owner Street Number] as Small_Unit_Owner_Street_Number,
      owner.[Small Unit Owner Street Name] as Small_Unit_Owner_Street_Name,
      owner.[Small Unit Owner Street Type] as Small_Unit_Owner_Street_Type,
     
      ----- Facts -----
     
      --validation audit status
      vas.Any_RECs_Passed_Validation_Audit_Flag as Any_RECs_Passed_Validation_Audit_Flag,
     
      --RECs
      fsur.[RECs Created Quantity] as RECs_Created_Quantity,
      fsur.[RECs Pending Audit Quantity] as RECs_Pending_Audit_Quantity,
      fsur.[RECs Passed Audit Quantity] as RECs_Passed_Audit_Quantity,
      fsur.[RECs Failed Audit Quantity] as RECs_Failed_Audit_Quantity,
      fsur.[RECs Registered Quantity] as RECs_Registered_Quantity,
     
      --common
      fsur.[Deeming Period in Years] as Deeming_Period_in_Years,
      dsur.[Small Unit Zone] as Small_Unit_Zone,
      dsur.[Small Unit Customer Reference] as Small_Unit_Customer_Reference,
      
      --SGU facts
      dsur.[RECs Multiplier Used] as RECs_Multiplier_Used,
      fsur.[SGU Rated Output in kW] as SGU_Rated_Output_in_kW,
      dsur.[SGU Brand] as SGU_Brand,
      dsur.[SGU Model] as SGU_Model,
      
      --SGU statements etc
      --dsur.[SGU Installation Type] as SGU_Installation_Type,
      dsur.[SGU Inverter Manufacturer] as SGU_Inverter_Manufacturer,
      dsur.[SGU Inverter Series] as SGU_Inverter_Series,
      dsur.[SGU Inverter Model Number] as SGU_Inverter_Model_Number,
      dsur.[RECs Multiplier Used Previously Flag] as RECs_Multiplier_Used_Previously_Flag,
      dsur.[SGU Premises Eligible for Solar Credits Flag] as SGU_Premises_Eligible_for_Solar_Credits_Flag,
      dsur.[SGU Complete Unit Flag] as SGU_Complete_Unit_Flag,
      dsur.[SGU Transitional Multiplier Flag] as SGU_Transitional_Multiplier_Flag,
      dsur.[Site Specific Audit Report Available Flag] as Site_Specific_Audit_Report_Available_Flag,
      dsur.[Received Statement for Installer and Designer CEC Accreditation Flag] as Received_Statement_for_Installer_and_Designer_CEC_Accreditation_Flag,
      dsur.[Received Statement for Adherence to State Requirements Flag] as Received_Statement_for_Adherence_to_State_Requirements_Flag,
      dsur.[Received Certificate of Electrical Safety or Compliance Flag] as Received_Certificate_of_Electrical_Safety_or_Compliance_Flag,
      dsur.[Received Statement that System is Off Grid Flag] as Received_Statement_that_System_is_Off_Grid_Flag,
      dsur.[All Electrical Work Undertaken by Electrician Flag] as All_Electrical_Work_Undertaken_by_Electrician_Flag,
      dsur.[Received Statement Confirming Liability Insurance Flag] as Received_Statement_Confirming_Liability_Insurance_Flag,
      dsur.[Received Statement for Adherence to CEC Code of Conduct Flag] as Received_Statement_for_Adherence_to_CEC_Code_of_Conduct_Flag,
      dsur.[Received Statement for Adherence to ANZ Standards Flag] as Received_Statement_for_Adherence_to_ANZ_Standards_Flag,
      dsur.[SGU Number of Panels] as SGU_Number_of_Panels,
      dsur.[SGU Default Availability Used Flag] as SGU_Default_Availability_Used_Flag,
      dsur.[SGU Availability] as SGU_Availability,
      dsur.[More than One SGU at Address Flag] as More_than_One_SGU_at_Address_Flag,
      sgu.MORE_THAN_ONE_SGU_SAME_ADDRESS,
      --sgu.HAS_FAILED_PREVIOUSLY,
      sgu.INSTALLATION_TYPE,
      sgu.RECREATION_EXPLANATION_NOTE,
      sgu.ADDITIONAL_CAPACITY_DETAILS,
      sgu.VERSION,
      sgu.ADDITIONAL_SYSTEM_INFORMATION,
      --sgu.PREVIOUS_RECS_MULTIPLIER_FLAG,
      
      dsur.[SGU Rebate Approved Flag] as SGU_Rebate_Approved_Flag,
      fsur.[SGU Out of Pocket Expense] as SGU_Out_of_Pocket_Expense,
      
      dsur.[SWH Brand] as SWH_Brand,
      dsur.[SWH Model] as SWH_Model,
      dsur.[SWH Installation Type] as SWH_Installation_Type,
      dsur.[SWH Technology Type] as SWH_Technology_Type,
      dsur.[SWH Number of Panels] as SWH_Number_of_Panels,
      dsur.[SWH Capacity over 700L Flag] as SWH_Capacity_over_700L_Flag,
      dsur.[Stat Declaration for SWH Capacity Supplied Flag] as Stat_Declaration_for_SWH_Capacity_Supplied_Flag,
      dsur.[SWH Second Hand Flag] as SWH_Second_Hand_Flag,
      dsur.[More than One SWH at Address Flag] as More_than_One_SWH_at_Address_Flag,
      dsur.[RETAILER NAME] as RETAILER_NAME,
      dsur.[RETAILER ABN] as RETAILER_ABN
      --dsur.[NATIONAL METERING NUMBER] as NATIONAL_METERING_NUMBER,
      --dsur.[BATTERY MANUFACTURER] as BATTERY_MANUFACTURER,
      --dsur.[BATTERY MODEL] as BATTERY_MODEL,
      --dsur.[BATTERY PART OF AGG CONTROL] as BATTERY_PART_OF_AGG_CONTROL,
      --dsur.[BATTERY SETTINGS CHANGED] as BATTERY_SETTINGS_CHANGED,
      --dsur.[SGU ELECTRICITY GRID CONNECTIVITY] as SGU_ELECTRICITY_GRID_CONNECTIVITY

      
      FROM [RECREG_PROD].CERREGISTRY.SGU sgu
      INNER JOIN CERREGISTRY.CERTIFICATE_REGISTRATION cert_reg
       ON sgu.ACCREDITATION_CODE = cert_reg.ACCREDITATION_CODE 
       AND sgu.INSTALLATION_DATE >= convert(date,'",SQLDateStart,"',112)
       AND sgu.INSTALLATION_DATE <= convert(date,'",SQLDateEnd,"',112)
      
      --Installation Address 
      LEFT JOIN CERREGISTRY.ADDRESS sgu_addr
       ON sgu.INSTALLATION_ADDRESS_ID = sgu_addr.ID 
      
      --Installer
      LEFT JOIN CERREGISTRY.SGU_TECHNICAL_PERSON installer
       ON installer.ID = sgu.INSTALLER_ID
      LEFT JOIN CERREGISTRY.ADDRESS installer_address
       ON installer_address.ID = installer.ADDRESS_ID
      
      --Designer
      LEFT JOIN CERREGISTRY.SGU_TECHNICAL_PERSON designer
       ON designer.ID = sgu.DESIGNER_ID
      LEFT JOIN CERREGISTRY.ADDRESS designer_address
       ON designer_address.ID = designer.ADDRESS_ID 
      
      --Electrician 
      LEFT JOIN CERREGISTRY.SGU_TECHNICAL_PERSON electrician
       ON electrician.ID = sgu.ELECTRICIAN_ID
      LEFT JOIN CERREGISTRY.ADDRESS electrician_address
       ON electrician_address.ID = electrician.ADDRESS_ID
       
      --Agent
      LEFT JOIN CERREGISTRY.ACCOUNT acct
       ON cert_reg.REGISTERED_PERSON_ACCOUNT_ID = acct.ID
      
      --registered person - vacct
      inner join [RECREG_PROD].RETDim.vAccount vacct
        on	acct.ID = vacct.ACC_ID
      
      --Owner    
      inner join [RECREG_PROD].RETDim.vw_Small_Unit_Owner owner
        on	cert_reg.OWNER_ID = owner.[Small Unit Owner ID]  

      --Small Unit Registrations - dsur
      inner join [RECREG_PROD].RETDim.SmallUnitRegistrations dsur
        on sgu.ACCREDITATION_CODE = dsur.[Small Unit Accreditation Code]
      
      --Fact Small Unit Registrations - fsur
      inner join [RECREG_PROD].RETFact.SmallUnitRegistration fsur
        on	fsur.[Dim_Small_Unit_Registration_ID] = dsur.[Dim_Small_Unit_Registration_ID] 
      
      --Validation Audit Status - vas                     
      inner join [RECREG_PROD].RETDim.Dim_Validation_Audit_Status vas
        on	vas.[Dim_Validation_Audit_Status_ID] = fsur.[Dim_Validation_Audit_Status_ID] 

")

start_time <- Sys.time()


RecReg_data_YTD <- dbGetQuery(conRECREG, RecReg_Raw_SQL) %>% unique() #all columns in the right order; however there is an issue with line breaks

RecReg_data_test2210 <- dbGetQuery(conRECREG, RecReg_Raw_SQL) %>% unique()

## Cleaning 
## ---------------------------------------------------

#Found issues in ADDITONAL_SYSTEM_INFORMATION (likely a free text field) that 
# contains line breaks hence messing up the output CSV file


# isolating the issue 
RecReg_data_YTD %>% filter(Small_Unit_Accreditation_Code == "PVD4268573") %>% 
  select(ADDITIONAL_SYSTEM_INFORMATION) %>% 
# substituting the \n pattern with a space 
  gsub("[\r\n]", " ", .)

# To clean 
RecReg_data_YTD$ADDITIONAL_SYSTEM_INFORMATION <- RecReg_data_YTD$ADDITIONAL_SYSTEM_INFORMATION %>% gsub("[\r\n]", " ", .) 

sum(grepl('[\n]', RecReg_data_YTD$Small_Unit_Installation_Additional_Address_Information)) # works; sums all occurences


# apply(RecReg_data_YTD, 2, sum(grepl('[\n]', RecReg_data_YTD$Small_Unit_Installation_Additional_Address_Information)))
# 
clean_data <- as.data.frame(apply(RecReg_data_YTD, 2, function(x) gsub("[\r\n]", " ", x)))

str(clean_data)
write_csv2(clean_data, "RecReg_data_FYTD_clean.csv")

 # str_replace_all(x, "[\r\n]" , " ") - also  works

# grepl('[^[:punct:]]', val)






##############
## Analysis ##
##############


# After all the exciting analysis then remember to record the
# Execution time of main: 

end_time <- Sys.time()
run_time <- end_time - start_time
run_time

#######################
#### Write outputs ####
#######################

# write_rds(SRES_Raw2, "top100_rows.rds")
# 
# write_rds(RecReg_data, "RR_2021.rds")


# Other text files/tables
## ------------------------------------------------------


write_csv2(RecReg_data_YTD, "RecReg_data_FYTD_check.csv")

## Log file
## ------------------------------------------------------

log_df <- cbind(run_time, date()) %>% as.data.frame()
write.table(log_df, "log_file.csv", sep = ",", col.names = !file.exists("log_file.csv"), append = TRUE)



