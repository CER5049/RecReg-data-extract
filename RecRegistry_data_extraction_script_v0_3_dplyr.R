library(dplyr)
library(dbplyr)
library(DBI)
library(odbc)
library(stringr)
library(clipr)


# Connection -------------------------------------------------------------------

con <- dbConnect(odbc(), "RecRegistry", timeout = 50)


# tbl(con, in_schema("RECREG_PROD", "CERREGISTRY"))

# Call  tables------------------------------------------------------------------


tbl_sgu <- tbl(con, in_schema("CERREGISTRY", "SGU")) %>% 
  select(ID,
         INSTALLATION_ADDRESS_ID,
         ACCREDITATION_CODE, 
         FUEL_SOURCE, 
         MORE_THAN_ONE_SGU_SAME_ADDRESS,
         INSTALLATION_TYPE,
         INSTALLATION_DATE,
         INSTALLATION_LATITUDE,
         INSTALLATION_LONGITUDE,
         ADDITIONAL_CAPACITY_DETAILS,
         VERSION,
         ADDITIONAL_SYSTEM_INFORMATION, 
         INSTALLER_ID,
         DESIGNER_ID,
         ELECTRICIAN_ID
         ) 

tbl_cert_reg <- tbl(con, in_schema("CERREGISTRY","CERTIFICATE_REGISTRATION")) %>% 
  select(ACCREDITATION_CODE,
         REGISTERED_PERSON_ACCOUNT_ID # used to join the ACCOUNT table 
         )

tbl_instal_addr <- tbl(con, in_schema("CERREGISTRY", "ADDRESS")) %>% 
  select(ID,
         SITE_NAME,
         UNIT_TYPE,
         UNIT_NUMBER,
         STREET_NUMBER,
         STREET_NAME,
         STREET_TYPE,
         SUBURB, 
         STATE,
         POSTCODE,
         SPECIAL_ADDRESS
         )



tbl_installer <- tbl(con, in_schema("CERREGISTRY", "SGU_TECHNICAL_PERSON")) %>% 
  select(ID,
         INSTALLER_ACCREDITED_NUMBER,
         FIRST_NAME,
         SURNAME,
         PHONE,
         FAX,
         MOBILE,
         EMAIL,
         ADDRESS_ID
         )

tbl_designer <- tbl(con, in_schema("CERREGISTRY", "SGU_TECHNICAL_PERSON")) %>% 
  select(ID,
         INSTALLER_ACCREDITED_NUMBER,
         FIRST_NAME,
         SURNAME,
         PHONE,
         FAX,
         MOBILE,
         EMAIL,
         ADDRESS_ID
         )

tbl_electrician <- tbl(con, in_schema("CERREGISTRY", "SGU_TECHNICAL_PERSON")) %>% 
  select(ID,
         INSTALLER_ACCREDITED_NUMBER,
         FIRST_NAME,
         SURNAME,
         PHONE,
         FAX,
         MOBILE,
         EMAIL,
         ADDRESS_ID,
         ELECTRICIAN_NUMBER
         )


tbl_owner <- tbl(con, in_schema("CERREGISTRY", "ACCOUNT")) %>% 
  select(ID,
         NAME)

tbl_vacct <- tbl(con, in_schema("RETDim", "vAccount")) %>% 
  select(ACC_ID,
         RPE_ID #Registered person ID
         )

tbl_dsur <- tbl(con, in_schema("RETDim", "SmallUnitRegistrations")) %>% 
  select(Dim_Small_Unit_Registration_ID,
         `Small Unit Accreditation Code`,
         `Small Unit Deeming Period in Years`,
         `Small Unit Zone`,
         `Small Unit Customer Reference`,
         `RECs Multiplier Used`,
         `SGU Rated Output in KW`,
         `SGU Brand`,
         `SGU Model`,
         `SGU Inverter Manufacturer`,
         `SGU Inverter Series`,
         `SGU Inverter Model Number`,
         `RECs Multiplier Used Previously Flag`,
         `SGU Premises Eligible for Solar Credits Flag`,
         `SGU Complete Unit Flag`,
         `SGU Transitional Multiplier Flag`,
         `Site Specific Audit Report Available Flag`,
         `Received Statement for Installer and Designer CEC Accreditation Flag`,
         `Received Statement for Adherence to State Requirements Flag`,
         `Received Certificate of Electrical Safety or Compliance Flag`,
         `Received Statement that System is Off Grid Flag`,
         `All Electrical Work Undertaken by Electrician Flag`,
         `Received Statement Confirming Liability Insurance Flag`,
         `Received Statement for Adherence to CEC Code of Conduct Flag`,
         `Received Statement for Adherence to ANZ Standards Flag`,
         `SGU Number of Panels`, 
         `SGU Default Availability Used Flag`,
         `SGU Availability`,
         `SGU Rebate Approved Flag`,
         `SWH Brand`,
         `SWH Model`,
         `SWH Installation Type`,
         `SWH Technology Type`,
         `SWH Number of Panels`,
         `SWH Capacity over 700L Flag`,
         `Stat Declaration for SWH Capacity Supplied Flag`,
         `SWH Second Hand Flag`,
         `More than One SGU at Address Flag`,
         `Retailer Name`,
         `Retailer ABN`
         )
  

tbl_fsur <- tbl(con, in_schema("RETFact", "SmallUnitRegistration")) %>% 
  select(`Dim_Small_Unit_Registration_ID`,
         `Dim_Validation_Audit_Status_ID`,
         `RECs Created Quantity`,
         `RECs Pending Audit Quantity`,
         `RECs Passed Audit Quantity`,
         `RECs Failed Audit Quantity`,
         `SGU Out of Pocket Expense`
          )


# FROM [RECREG_PROD].CERREGISTRY.SGU sgu
# INNER JOIN CERREGISTRY.CERTIFICATE_REGISTRATION cert_reg
# ON sgu.ACCREDITATION_CODE = cert_reg.ACCREDITATION_CODE 
# AND sgu.INSTALLATION_DATE >= convert(date,'",SQLDateStart,"',112)
# AND sgu.INSTALLATION_DATE <= convert(date,'",SQLDateEnd,"',112)
# 
# --Installation Address 
# LEFT JOIN CERREGISTRY.ADDRESS sgu_addr
# ON sgu.INSTALLATION_ADDRESS_ID = sgu_addr.ID 
# 

 # --Installer
 #      LEFT JOIN CERREGISTRY.SGU_TECHNICAL_PERSON installer
 #       ON installer.ID = sgu.INSTALLER_ID
 #      LEFT JOIN CERREGISTRY.ADDRESS installer_address
 #       ON installer_address.ID = installer.ADDRESS_ID
 #      
 #      --Designer
 #      LEFT JOIN CERREGISTRY.SGU_TECHNICAL_PERSON designer
 #       ON designer.ID = sgu.DESIGNER_ID
 #      LEFT JOIN CERREGISTRY.ADDRESS designer_address
 #       ON designer_address.ID = designer.ADDRESS_ID 
 #      
 #      --Electrician 
 #      LEFT JOIN CERREGISTRY.SGU_TECHNICAL_PERSON electrician
 #       ON electrician.ID = sgu.ELECTRICIAN_ID
 #      LEFT JOIN CERREGISTRY.ADDRESS electrician_address
 #       ON electrician_address.ID = electrician.ADDRESS_ID
 #       
 #      --Agent
 #      LEFT JOIN CERREGISTRY.ACCOUNT acct
 #       ON cert_reg.REGISTERED_PERSON_ACCOUNT_ID = acct.ID
 #      
 #      --registered person - vacct
 #      inner join [RECREG_PROD].RETDim.vAccount vacct
 #        on	acct.ID = vacct.ACC_ID
 #      
 #      --Owner    
 #      inner join [RECREG_PROD].RETDim.vw_Small_Unit_Owner owner
 #        on	cert_reg.OWNER_ID = owner.[Small Unit Owner ID]  
 # 
 #      --Small Unit Registrations - dsur
 #      inner join [RECREG_PROD].RETDim.SmallUnitRegistrations dsur
 #        on sgu.ACCREDITATION_CODE = dsur.[Small Unit Accreditation Code]
 #      
 #      --Fact Small Unit Registrations - fsur
 #      inner join [RECREG_PROD].RETFact.SmallUnitRegistration fsur
 #        on	fsur.[Dim_Small_Unit_Registration_ID] = dsur.[Dim_Small_Unit_Registration_ID] 
 #      
 #      --Validation Audit Status - vas                     
 #      inner join [RECREG_PROD].RETDim.Dim_Validation_Audit_Status vas
 #        on	vas.[Dim_Validation_Audit_Status_ID] = fsur.[Dim_Validation_Audit_Status_ID] 




#concatenate most fields for a full string
# %>% mutate(ADDR_FULL = paste(SITE_NAME, UNIT_TYPE, UNIT_NUMBER, STREET_NUMBER, STREET_NAME, STREET_TYPE, SUBURB, STATE, POSTCODE, sep = " "))

# Join  tables------------------------------------------------------------------

# d1 has cols x, y; d2 has x2, y2:  left_join(d1, d2, by = c("x" = "x2", "y" = "y2")

joined <- tbl_sgu %>% left_join(tbl_instal_addr, by = c("INSTALLATION_ADDRESS_ID" = "ID"))

