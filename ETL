import pandas as pd, datetime as dt, numpy as np, time


#import data
daily = 'daily_28mar_3apr.xlsx'
periode = '28 MAR - 03 APR'
region = 'region_tonase.xlsx'
cs_sicepat = pd.read_excel(daily, sheet_name='sicepat')
cs_idex = pd.read_excel(daily, sheet_name='idex')
cs_spx_sub = pd.read_excel(daily, sheet_name='spx_sub')
cs_spx_bdo = pd.read_excel(daily, sheet_name='spx_bdo')
cs_sc = pd.read_excel(daily, sheet_name='sc')


region_air = pd.read_excel(region, sheet_name='air_freight')
region_sea = pd.read_excel(region, sheet_name='sea_freight')
airlines = pd.read_excel(region, sheet_name='flight')
lh_origin = pd.read_excel(region, sheet_name='origin')

#sicepat
cs_sicepat['date'] = pd.to_datetime(cs_sicepat['RA IN'],dayfirst=True)
cs_sicepat['date'] = cs_sicepat['date'].fillna(cs_sicepat['Tanggal SMU']+pd.DateOffset(days=1))
cs_sicepat['no_smu'] = cs_sicepat['SMU'].astype(str).str.strip()
cs_sicepat['fn_clean'] = cs_sicepat['First Flight'].astype(str).str.strip().str[0:2]
cs_sicepat.loc[cs_sicepat['no_smu']!= '', 'flight_code'] = cs_sicepat['fn_clean']
cs_sicepat.loc[cs_sicepat['no_smu'].astype(str).str[0:3]== '126', 'flight_code'] = "GA"
cs_sicepat.loc[cs_sicepat['no_smu'].astype(str).str[0:3]== '888', 'flight_code'] = "QG"
cs_sicepat.loc[cs_sicepat['no_smu'].astype(str).str[0:3]== '990', 'flight_code'] = "JT"
cs_sicepat.loc[cs_sicepat['no_smu'].astype(str).str[0:3]== '938', 'flight_code'] = "ID"
cs_sicepat.loc[cs_sicepat['no_smu'].astype(str).str[0:1]== 'C', 'flight_code'] = "SJ"
cs_sicepat.loc[cs_sicepat['no_smu'].astype(str).str[0:1]== 'C', 'flight_code'] = "SJ"
cs_sicepat.loc[cs_sicepat['no_smu'].astype(str).str[0:3]== '690', 'flight_code'] = "CA"
cs_sicepat.loc[cs_sicepat['no_smu'].astype(str).str[0:3]== '133', 'flight_code'] = "DR"
cs_sicepat.loc[cs_sicepat['no_smu'].astype(str).str[0:3]== '864', 'flight_code'] = "GM"
cs_sicepat.loc[cs_sicepat['no_smu'].astype(str).str[0:3]== '818', 'flight_code'] = "RA"
cs_sicepat['Destination'] = cs_sicepat['Destination'].astype(str).str.upper().str.strip()
cs_sicepat["ra_in"]=pd.to_datetime(cs_sicepat["RA IN"],dayfirst=True)
cs_sicepat['fn1'] = cs_sicepat['First Flight'].astype('object').str.strip()
cs_sicepat['fn2'] = cs_sicepat['Flight 2']
cs_sicepat['fn3'] = cs_sicepat['Flight 3']
cs_sicepat["eta_1"]=pd.to_datetime(cs_sicepat["First ETA"],dayfirst=True)
cs_sicepat["etd_1"]=pd.to_datetime(cs_sicepat["First ETD"],dayfirst=True)
cs_sicepat["eta_2"]=pd.to_datetime(cs_sicepat["ETA 2"],dayfirst=True)
cs_sicepat["etd_2"]=pd.to_datetime(cs_sicepat["ETD 2"],dayfirst=True)
cs_sicepat["eta_3"]=pd.to_datetime(cs_sicepat["ETA 3"],dayfirst=True)
cs_sicepat["etd_3"]=pd.to_datetime(cs_sicepat["ETD 3"],dayfirst=True)

cs_sicepat['colly'] = cs_sicepat['COLLY'].astype('float64')
cs_sicepat['weight'] = cs_sicepat['KG'].astype('float64')
cs_sicepat['reason']=cs_sicepat['Reason'].str.upper()

cs_sicepat['direct_transit']=cs_sicepat['DIRECT/TRANSIT'].astype(str).str.strip().str.upper()
cs_sicepat.loc[cs_sicepat['direct_transit'] == 'DIRECT', 'transit_via'] = ''
cs_sicepat.loc[cs_sicepat['direct_transit'] != 'DIRECT', 'transit_via'] = cs_sicepat['direct_transit']
cs_sicepat.loc[cs_sicepat['direct_transit'] == 'DIRECT', 'direct_transit'] = 'DIRECT'
cs_sicepat.loc[cs_sicepat['direct_transit'] != 'DIRECT', 'direct_transit'] = 'TRANSIT'
cs_sicepat['direct_transit'] = cs_sicepat['direct_transit'].fillna('ON PROGRESS')
cs_sicepat.loc[cs_sicepat['weight'] != '', 'periode'] = periode
cs_sicepat.loc[cs_sicepat['weight'] != '', 'customer'] = 'SI CEPAT'
cs_sicepat.loc[cs_sicepat['weight'] != '', 'freight'] = 'AIR FREIGHT'
cs_sicepat1 = cs_sicepat.merge(airlines, on='flight_code', how='left')
cs_sicepat2 = cs_sicepat1.merge(lh_origin, on='line_haul_origin', how='left')
cs_sicepat3 = cs_sicepat2.merge(region_air, on='Destination', how='left')
data_sicepat = cs_sicepat3.reindex(columns=['date','periode','customer','freight','no_gabung_paket','no_smu',
                                            'line_haul_origin','origin','destination','city','region','colly','weight','trip',
                                            'airlines','direct_transit','transit_via','fn1','fn2','fn3',
                                            'req_pickup','scan_in','scan_out','wh_origin_in','wh_oirigin_out',
                                            'ra_in','ra_out','etd_1','eta_1','etd_2','eta_2','etd_3','eta_3','wh_in_destination','wh_out_destination',
                                            'hub_last','reason','status_data'])
data_sicepat.drop_duplicates(subset=["no_gabung_paket","no_smu","weight"], keep="first",inplace=True)

#idex
cs_idex['smu_final']=cs_idex['SMU BARU'].fillna(cs_idex['SMU'])
cs_idex['date'] = pd.to_datetime(cs_idex['RA IN'],dayfirst=True)
cs_idex['date'] = cs_idex['date'].fillna(cs_idex['TANGGAL BOOKING'])
cs_idex['fn_clean'] = cs_idex['FLIGHT 1'].astype(str).str.strip().str[0:2]
cs_idex['no_smu'] = cs_idex['smu_final'].astype(str).str.strip()

cs_idex.loc[cs_idex['no_smu']!= '', 'flight_code'] = cs_idex['fn_clean']
cs_idex.loc[cs_idex['no_smu'].astype(str).str[0:3]== '126', 'flight_code'] = "GA"
cs_idex.loc[cs_idex['no_smu'].astype(str).str[0:3]== '888', 'flight_code'] = "QG"
cs_idex.loc[cs_idex['no_smu'].astype(str).str[0:3]== '990', 'flight_code'] = "JT"
cs_idex.loc[cs_idex['no_smu'].astype(str).str[0:3]== '938', 'flight_code'] = "ID"
cs_idex.loc[cs_idex['no_smu'].astype(str).str[0:1]== 'C', 'flight_code'] = "SJ"
cs_idex.loc[cs_idex['no_smu'].astype(str).str[0:1]== 'C', 'flight_code'] = "SJ"
cs_idex.loc[cs_idex['no_smu'].astype(str).str[0:3]== '690', 'flight_code'] = "CA"
cs_idex.loc[cs_idex['no_smu'].astype(str).str[0:3]== '133', 'flight_code'] = "DR"
cs_idex.loc[cs_idex['no_smu'].astype(str).str[0:3]== '864', 'flight_code'] = "GM"
cs_idex.loc[cs_idex['no_smu'].astype(str).str[0:3]== '818', 'flight_code'] = "RA"

cs_idex["ra_in"]=pd.to_datetime(cs_idex["RA IN"],dayfirst=True)
cs_idex["ra_out"]=pd.to_datetime(cs_idex["RA OUT"],dayfirst=True)
cs_idex['fn1'] = cs_idex['FLIGHT 1'].astype(str).str.strip()
cs_idex['fn2'] = cs_idex['FLIGHT 2']
cs_idex['fn3'] = cs_idex['FLIGHT 3']
cs_idex["eta_1"]=pd.to_datetime(cs_idex["ETA 1"],dayfirst=True)
cs_idex["etd_1"]=pd.to_datetime(cs_idex["ETD 1"],dayfirst=True)
cs_idex["eta_2"]=pd.to_datetime(cs_idex["ETA 2"],dayfirst=True)
cs_idex["etd_2"]=pd.to_datetime(cs_idex["ETD 2"],dayfirst=True)
cs_idex["eta_3"]=pd.to_datetime(cs_idex["ETA 3"],dayfirst=True)
cs_idex["etd_3"]=pd.to_datetime(cs_idex["ETD 3"],dayfirst=True)

cs_idex['colly'] = cs_idex['COLLY'].astype('float64')
cs_idex['weight'] = cs_idex['REVISI KG'].fillna(cs_idex['KG']).astype('float64')
cs_idex['reason']=cs_idex['reason'].str.upper()

cs_idex['direct_transit']=cs_idex['direct_transit'].astype(str).str.strip().str.upper()
cs_idex.loc[cs_idex['direct_transit'] == 'DIRECT', 'transit_via'] = ''
cs_idex.loc[cs_idex['direct_transit'] != 'DIRECT', 'transit_via'] = cs_idex['direct_transit']
cs_idex.loc[cs_idex['direct_transit'] == 'DIRECT', 'direct_transit'] = 'DIRECT'
cs_idex.loc[cs_idex['direct_transit'] != 'DIRECT', 'direct_transit'] = 'TRANSIT'
cs_idex['direct_transit'] = cs_idex['direct_transit'].fillna('ON PROGRESS')

cs_idex.loc[cs_idex['weight'] != '', 'periode'] = periode
cs_idex.loc[cs_idex['weight'] != '', 'customer'] = 'ID EXPRESS'
cs_idex.loc[cs_idex['weight'] != '', 'freight'] = 'AIR FREIGHT'
cs_idex.loc[cs_idex['weight'] != '', 'line_haul_origin'] = 'Line Haul Udara Jakarta'
cs_idex.loc[cs_idex['weight'] != '', 'origin'] = 'CGK'

cs_idex1 = cs_idex.merge(airlines, on='flight_code', how='left')
cs_idex2 = cs_idex1.merge(region_air, on='destination', how='left')
data_idex = cs_idex2.reindex(columns=['date','periode','customer','freight','no_gabung_paket','no_smu',
                                            'line_haul_origin','origin','destination','city','region','colly','weight','trip',
                                            'airlines','direct_transit','transit_via','fn1','fn2','fn3',
                                            'req_pickup','scan_in','scan_out','wh_origin_in','wh_oirigin_out',
                                            'ra_in','ra_out','etd_1','eta_1','etd_2','eta_2','etd_3','eta_3','wh_in_destination','wh_out_destination',
                                            'hub_last','reason','status_data'])
data_idex.drop_duplicates(subset=["no_gabung_paket","no_smu","weight","colly"], keep="first",inplace=True)

#sc
cs_sc['date'] = pd.to_datetime(cs_sc['RA IN'],dayfirst=True)
cs_sc['date'] = cs_sc['date'].fillna(cs_sc['Tanggal SMU'])
cs_sc['fn_clean'] = cs_sc['FLIGHT 1'].astype(str).str.strip().str[0:2]
cs_sc['Destination'] = cs_sc['Destination'].astype(str).str.strip().str.upper()
cs_sc['line_haul_origin'] = cs_sc['line_haul_origin'].astype(str).str.strip().str.upper()
cs_sc["ra_in"]=pd.to_datetime(cs_sc["RA IN"],dayfirst=True)
cs_sc['ra_in'] = cs_sc['ra_in'].fillna(cs_sc['Final Date 1'])
cs_sc['no_smu'] = cs_sc['SMU'].astype(str).str.strip()

cs_sc.loc[cs_sc['no_smu']!= '', 'flight_code'] = cs_sc['fn_clean']
cs_sc.loc[cs_sc['no_smu'].astype(str).str[0:3]== '126', 'flight_code'] = "GA"
cs_sc.loc[cs_sc['no_smu'].astype(str).str[0:3]== '888', 'flight_code'] = "QG"
cs_sc.loc[cs_sc['no_smu'].astype(str).str[0:3]== '990', 'flight_code'] = "JT"
cs_sc.loc[cs_sc['no_smu'].astype(str).str[0:3]== '938', 'flight_code'] = "ID"
cs_sc.loc[cs_sc['no_smu'].astype(str).str[0:1]== 'C', 'flight_code'] = "SJ"
cs_sc.loc[cs_sc['no_smu'].astype(str).str[0:1]== 'C', 'flight_code'] = "SJ"
cs_sc.loc[cs_sc['no_smu'].astype(str).str[0:3]== '690', 'flight_code'] = "CA"
cs_sc.loc[cs_sc['no_smu'].astype(str).str[0:3]== '133', 'flight_code'] = "DR"
cs_sc.loc[cs_sc['no_smu'].astype(str).str[0:3]== '864', 'flight_code'] = "GM"
cs_sc.loc[cs_sc['no_smu'].astype(str).str[0:3]== '818', 'flight_code'] = "RA"

cs_sc['fn1'] = cs_sc['FLIGHT 1'].astype(str).str.strip()
cs_sc['fn2'] = cs_sc['FLIGHT 2']
cs_sc['fn3'] = cs_sc['FLIGHT 3']
cs_sc["eta_1"]=pd.to_datetime(cs_sc["ETA 1"],dayfirst=True)
cs_sc["etd_1"]=pd.to_datetime(cs_sc["ETD 1"],dayfirst=True)
cs_sc["eta_2"]=pd.to_datetime(cs_sc["ETA 2"],dayfirst=True)
cs_sc["etd_2"]=pd.to_datetime(cs_sc["ETD 2"],dayfirst=True)
cs_sc["eta_3"]=pd.to_datetime(cs_sc["ETA 3"],dayfirst=True)
cs_sc["etd_3"]=pd.to_datetime(cs_sc["ETD 3"],dayfirst=True)


cs_sc['colly'] = cs_sc['Karung'].astype('float64')
cs_sc['weight'] = cs_sc['Berat Revisi'].fillna(cs_sc['Berat']).astype('float64')
cs_sc['reason']=cs_sc['reason'].str.upper()

cs_sc['direct_transit']=cs_sc['direct_transit'].astype(str).str.strip().str.upper()
cs_sc.loc[cs_sc['direct_transit'] == 'DIRECT', 'transit_via'] = ''
cs_sc.loc[cs_sc['direct_transit'] != 'DIRECT', 'transit_via'] = cs_sc['direct_transit']
cs_sc.loc[cs_sc['direct_transit'] == 'DIRECT', 'direct_transit'] = 'DIRECT'
cs_sc.loc[cs_sc['direct_transit'] != 'DIRECT', 'direct_transit'] = 'TRANSIT'
cs_sc['direct_transit'] = cs_sc['direct_transit'].fillna('ON PROGRESS')

cs_sc.loc[cs_sc['weight'] != '', 'periode'] = periode
cs_sc.loc[cs_sc['weight'] != '', 'customer'] = 'SENTRAL CARGO'
cs_sc.loc[cs_sc['weight'] != '', 'freight'] = 'AIR FREIGHT'

cs_sc1 = cs_sc.merge(airlines, on='flight_code', how='left')
cs_sc2 = cs_sc1.merge(lh_origin, on='line_haul_origin', how='left')
cs_sc3 = cs_sc2.merge(region_air, on='Destination', how='left')
data_sc = cs_sc3.reindex(columns=['date','periode','customer','freight','no_gabung_paket','no_smu',
                                            'line_haul_origin','origin','destination','city','region','colly','weight','trip',
                                            'airlines','direct_transit','transit_via','fn1','fn2','fn3',
                                            'req_pickup','scan_in','scan_out','wh_origin_in','wh_oirigin_out',
                                            'ra_in','ra_out','etd_1','eta_1','etd_2','eta_2','etd_3','eta_3','wh_in_destination','wh_out_destination',
                                            'hub_last','reason','status_data'])
data_sc.drop_duplicates(subset=["no_smu","weight","colly"], keep="first",inplace=True)

#sub_spx
cs_spx_sub['date'] = pd.to_datetime(cs_spx_sub['Date'],dayfirst=True)
cs_spx_sub['fn_clean'] = cs_spx_sub['Flight number 1'].astype(str).str.strip().str[0:2]
cs_spx_sub['no_smu'] = cs_spx_sub['BAST Number'].astype(str).str.strip().str.upper()

cs_spx_sub.loc[cs_spx_sub['no_smu']!= '', 'flight_code'] = cs_spx_sub['fn_clean']
cs_spx_sub.loc[cs_spx_sub['no_smu'].astype(str).str[0:3]== '126', 'flight_code'] = "GA"
cs_spx_sub.loc[cs_spx_sub['no_smu'].astype(str).str[0:3]== '888', 'flight_code'] = "QG"
cs_spx_sub.loc[cs_spx_sub['no_smu'].astype(str).str[0:3]== '990', 'flight_code'] = "JT"
cs_spx_sub.loc[cs_spx_sub['no_smu'].astype(str).str[0:3]== '938', 'flight_code'] = "ID"
cs_spx_sub.loc[cs_spx_sub['no_smu'].astype(str).str[0:1]== 'C', 'flight_code'] = "SJ"
cs_spx_sub.loc[cs_spx_sub['no_smu'].astype(str).str[0:1]== 'C', 'flight_code'] = "SJ"
cs_spx_sub.loc[cs_spx_sub['no_smu'].astype(str).str[0:3]== '690', 'flight_code'] = "CA"
cs_spx_sub.loc[cs_spx_sub['no_smu'].astype(str).str[0:3]== '133', 'flight_code'] = "DR"
cs_spx_sub.loc[cs_spx_sub['no_smu'].astype(str).str[0:3]== '864', 'flight_code'] = "GM"
cs_spx_sub.loc[cs_spx_sub['no_smu'].astype(str).str[0:3]== '818', 'flight_code'] = "RA"

cs_spx_sub['Destination'] = cs_spx_sub['Destination'].astype(str).str.strip().str.upper()
cs_spx_sub['no_gabung_paket'] = cs_spx_sub['TO Number'].astype(str).str.strip()
cs_spx_sub['line_haul_origin'] = cs_spx_sub['DC Origin'].astype(str).str.strip().str.upper()
cs_spx_sub['req_pickup'] = pd.to_datetime(cs_spx_sub['Actual permintaan pickup'],dayfirst=True)
cs_spx_sub["scan_in"]=pd.to_datetime(cs_spx_sub["Actual Pickup at DC"],dayfirst=True)
cs_spx_sub["scan_out"]=pd.to_datetime(cs_spx_sub["Actual Depart from DC"],dayfirst=True)
cs_spx_sub["wh_origin_in"]=pd.to_datetime(cs_spx_sub["In Warehouse Origin"],dayfirst=True)
cs_spx_sub["wh_oirigin_out"]=pd.to_datetime(cs_spx_sub["Out Warehouse Origin"],dayfirst=True)
cs_spx_sub["ra_in"]=pd.to_datetime(cs_spx_sub["Actual in RA"],dayfirst=True)
cs_spx_sub["ra_out"]=pd.to_datetime(cs_spx_sub["Actual out RA"],dayfirst=True)
cs_spx_sub["trip"]=cs_spx_sub["TRIP"]
cs_spx_sub['fn1'] = cs_spx_sub['Flight number 1'].astype(str).str.strip()
cs_spx_sub['fn2'] = cs_spx_sub['Flight number 2']
cs_spx_sub['fn3'] = cs_spx_sub['Flight number 3']
cs_spx_sub["eta_1"]=pd.to_datetime(cs_spx_sub["ATA 1"],dayfirst=True)
cs_spx_sub["etd_1"]=pd.to_datetime(cs_spx_sub["Actual Flight 1"],dayfirst=True)
cs_spx_sub["eta_2"]=pd.to_datetime(cs_spx_sub["ATA 2"],dayfirst=True)
cs_spx_sub["etd_2"]=pd.to_datetime(cs_spx_sub["Actual Flight 2"],dayfirst=True)
cs_spx_sub["eta_3"]=pd.to_datetime(cs_spx_sub["ATA 3"],dayfirst=True)
cs_spx_sub["etd_3"]=pd.to_datetime(cs_spx_sub["Actual Flight 3"],dayfirst=True)
cs_spx_sub["wh_out_destination"]=pd.to_datetime(cs_spx_sub["Out Warehouse Destination"],dayfirst=True)
cs_spx_sub["hub_last"]=pd.to_datetime(cs_spx_sub["Actual Arrival at Hub"],dayfirst=True)
cs_spx_sub.loc[cs_spx_sub['no_gabung_paket'] != '', 'colly'] = 1
cs_spx_sub['weight'] = cs_spx_sub['Chargeable Weight'].astype('float64')
cs_spx_sub['reason']=cs_spx_sub['reason'].str.upper()

cs_spx_sub['direct_transit']=cs_spx_sub['Transit/Direct'].astype(str).str.strip().str.upper()
cs_spx_sub.loc[cs_spx_sub['direct_transit'] == 'DIRECT', 'transit_via'] = ''
cs_spx_sub.loc[cs_spx_sub['direct_transit'] != 'DIRECT', 'transit_via'] = cs_spx_sub['direct_transit']
cs_spx_sub.loc[cs_spx_sub['direct_transit'] == 'DIRECT', 'direct_transit'] = 'DIRECT'
cs_spx_sub.loc[cs_spx_sub['direct_transit'] != 'DIRECT', 'direct_transit'] = 'TRANSIT'
cs_spx_sub['direct_transit'] = cs_spx_sub['direct_transit'].fillna('ON PROGRESS')

cs_spx_sub.loc[cs_spx_sub['weight'] != '', 'periode'] = periode
cs_spx_sub.loc[cs_spx_sub['weight'] != '', 'customer'] = 'SHOPEE EXPRESS'
cs_spx_sub.loc[cs_spx_sub['weight'] != '', 'freight'] = 'AIR FREIGHT'
cs_spx_sub.loc[cs_spx_sub['weight'] != '', 'origin'] = 'SUB'

cs_spx_sub1 = cs_spx_sub.merge(airlines, on='flight_code', how='left')
cs_spx_sub2 = cs_spx_sub1.merge(region_air, on='Destination', how='left')
data_spx_sub = cs_spx_sub2.reindex(columns=['date','periode','customer','freight','no_gabung_paket','no_smu',
                                            'line_haul_origin','origin','destination','city','region','colly','weight','trip',
                                            'airlines','direct_transit','transit_via','fn1','fn2','fn3',
                                            'req_pickup','scan_in','scan_out','wh_origin_in','wh_oirigin_out',
                                            'ra_in','ra_out','etd_1','eta_1','etd_2','eta_2','etd_3','eta_3','wh_in_destination','wh_out_destination',
                                            'hub_last','reason','status_data'])
data_spx_sub.drop_duplicates(subset=["no_gabung_paket","no_smu","weight","colly"], keep="first",inplace=True)

#sub_bdo
cs_spx_bdo['date'] = pd.to_datetime(cs_spx_bdo['Date'],dayfirst=True)
cs_spx_bdo['fn_clean'] = cs_spx_bdo['Flight number 1'].astype(str).str.strip().str[0:2]
cs_spx_bdo['no_smu'] = cs_spx_bdo['NO Resi CKL'].astype(str).str.strip()

cs_spx_bdo.loc[cs_spx_bdo['no_smu']!= '', 'flight_code'] = cs_spx_bdo['fn_clean']
cs_spx_bdo.loc[cs_spx_bdo['no_smu'].astype(str).str[0:3]== '126', 'flight_code'] = "GA"
cs_spx_bdo.loc[cs_spx_bdo['no_smu'].astype(str).str[0:3]== '888', 'flight_code'] = "QG"
cs_spx_bdo.loc[cs_spx_bdo['no_smu'].astype(str).str[0:3]== '990', 'flight_code'] = "JT"
cs_spx_bdo.loc[cs_spx_bdo['no_smu'].astype(str).str[0:3]== '938', 'flight_code'] = "ID"
cs_spx_bdo.loc[cs_spx_bdo['no_smu'].astype(str).str[0:1]== 'C', 'flight_code'] = "SJ"
cs_spx_bdo.loc[cs_spx_bdo['no_smu'].astype(str).str[0:1]== 'C', 'flight_code'] = "SJ"
cs_spx_bdo.loc[cs_spx_bdo['no_smu'].astype(str).str[0:3]== '690', 'flight_code'] = "CA"
cs_spx_bdo.loc[cs_spx_bdo['no_smu'].astype(str).str[0:3]== '133', 'flight_code'] = "DR"
cs_spx_bdo.loc[cs_spx_bdo['no_smu'].astype(str).str[0:3]== '864', 'flight_code'] = "GM"
cs_spx_bdo.loc[cs_spx_bdo['no_smu'].astype(str).str[0:3]== '818', 'flight_code'] = "RA"

cs_spx_bdo['Destination'] = cs_spx_bdo['Destination'].astype(str).str.strip().str.upper()
cs_spx_bdo['no_gabung_paket'] = cs_spx_bdo['TO Number'].astype(str).str.strip()
cs_spx_bdo['line_haul_origin'] = cs_spx_bdo['DC Origin'].astype(str).str.strip().str.upper()
#cs_spx_bdo['req_pickup'] = cs_spx_bdo['Actual permintaan pickup'].astype(str).str.strip()
cs_spx_bdo["scan_in"]=pd.to_datetime(cs_spx_bdo["Actual Pickup at DC"],dayfirst=True)
cs_spx_bdo["scan_out"]=pd.to_datetime(cs_spx_bdo["Actual Depart from DC"],dayfirst=True)
#cs_spx_bdo["wh_origin_in"]=pd.to_datetime(cs_spx_bdo["In Warehouse Origin"],dayfirst=True)
cs_spx_bdo["wh_oirigin_out"]=pd.to_datetime(cs_spx_bdo["Depart from wh ckl"],dayfirst=True)
cs_spx_bdo["ra_in"]=pd.to_datetime(cs_spx_bdo["Actual In RA"],dayfirst=True)
#cs_spx_bdo["ra_out"]=pd.to_datetime(cs_spx_bdo["Actual out RA"],dayfirst=True)
cs_spx_bdo["trip"]=cs_spx_bdo["Truck Numb."]
cs_spx_bdo['fn1'] = cs_spx_bdo['Flight number 1'].astype(str).str.strip()
cs_spx_bdo['fn2'] = cs_spx_bdo['Flight number 2'].astype(str).str.strip()
cs_spx_bdo['fn3'] = cs_spx_bdo['Flight number 3'].astype(str).str.strip()
cs_spx_bdo["eta_1"]=pd.to_datetime(cs_spx_bdo["ATA 1"],dayfirst=True)
cs_spx_bdo["etd_1"]=pd.to_datetime(cs_spx_bdo["Actual Flight 1"],dayfirst=True)
cs_spx_bdo["eta_2"]=pd.to_datetime(cs_spx_bdo["ATA 2"],dayfirst=True)
cs_spx_bdo["etd_2"]=pd.to_datetime(cs_spx_bdo["Actual Flight 2"],dayfirst=True)
cs_spx_bdo["eta_3"]=pd.to_datetime(cs_spx_bdo["ATA 3"],dayfirst=True)
cs_spx_bdo["etd_3"]=pd.to_datetime(cs_spx_bdo["Actual Flight 3"],dayfirst=True)
#cs_spx_bdo["wh_out_destination"]=pd.to_datetime(cs_spx_bdo["Out Warehouse Destination"],dayfirst=True)
cs_spx_bdo["hub_last"]=pd.to_datetime(cs_spx_bdo["Actual Arrival at Hub"],dayfirst=True)

cs_spx_bdo.loc[cs_spx_bdo['no_gabung_paket'] != '', 'colly'] = 1
cs_spx_bdo['weight'] = cs_spx_bdo['Chargeable Weight'].astype('float64')
cs_spx_bdo['reason']=cs_spx_bdo['Reason Hub to Hub'].str.upper()

cs_spx_bdo['direct_transit']=cs_spx_bdo['Transit/Direct'].astype(str).str.strip().str.upper()
cs_spx_bdo.loc[cs_spx_bdo['direct_transit'] == 'DIRECT', 'transit_via'] = ''
cs_spx_bdo.loc[cs_spx_bdo['direct_transit'] != 'DIRECT', 'transit_via'] = cs_spx_bdo['direct_transit']
cs_spx_bdo.loc[cs_spx_bdo['direct_transit'] == 'DIRECT', 'direct_transit'] = 'DIRECT'
cs_spx_bdo.loc[cs_spx_bdo['direct_transit'] != 'DIRECT', 'direct_transit'] = 'TRANSIT'
cs_spx_bdo['direct_transit'] = cs_spx_bdo['direct_transit'].fillna('ON PROGRESS')

cs_spx_bdo.loc[cs_spx_bdo['weight'] != '', 'periode'] = periode
cs_spx_bdo.loc[cs_spx_bdo['weight'] != '', 'customer'] = 'SHOPEE EXPRESS'
cs_spx_bdo.loc[cs_spx_bdo['weight'] != '', 'freight'] = 'AIR FREIGHT'
cs_spx_bdo['Orign airport'] = cs_spx_bdo['Orign airport'].astype(str).str.strip().str.upper()
cs_spx_bdo.loc[cs_spx_bdo['Orign airport'] == 'VIA BDO', 'origin'] = 'BDO'
cs_spx_bdo.loc[cs_spx_bdo['Orign airport'] == 'VIA CGK', 'origin'] = 'CGK'

cs_spx_bdo1 = cs_spx_bdo.merge(airlines, on='flight_code', how='left')
cs_spx_bdo2 = cs_spx_bdo1.merge(region_air, on='Destination', how='left')
data_spx_bdo = cs_spx_bdo2.reindex(columns=['date','periode','customer','freight','no_gabung_paket','no_smu',
                                            'line_haul_origin','origin','destination','city','region','colly','weight','trip',
                                            'airlines','direct_transit','transit_via','fn1','fn2','fn3',
                                            'req_pickup','scan_in','scan_out','wh_origin_in','wh_oirigin_out',
                                            'ra_in','ra_out','etd_1','eta_1','etd_2','eta_2','etd_3','eta_3','wh_in_destination','wh_out_destination',
                                            'hub_last','reason','status_data'])
data_spx_bdo.drop_duplicates(subset=["no_gabung_paket","no_smu","weight","colly"], keep="first",inplace=True)
data_all = data_idex.append([data_sicepat,data_sc,data_spx_sub,data_spx_bdo])

data_all['transit_via'] = data_all['transit_via'].fillna(data_all['direct_transit'])
data_all.loc[data_all['direct_transit'] == 'DIRECT', 'transit_code'] = '0'
data_all.loc[data_all['direct_transit'] == 'TRANSIT', 'transit_code'] = '1'
data_all.loc[data_all['transit_via'].str.contains(','), 'transit_code'] = '2'
data_all['transit_code']=data_all['transit_code'].astype(str)
data_all.loc[(data_all['periode'] != ''), 'status_data'] = 'SUCCESS'
data_all.loc[(data_all['transit_code'] == '0')&(data_all['eta_1'].isnull()), 'status_data'] = 'ON PROCESS'
data_all.loc[(data_all['transit_code'] == '1')&(data_all['eta_2'].isnull()), 'status_data'] = 'ON PROCESS'
data_all.loc[(data_all['transit_code'] == '2')&(data_all['eta_3'].isnull()), 'status_data'] = 'ON PROCESS'
data_all.loc[(data_all['customer'] == 'SHOPEE EXPRESS')&(data_all['hub_last'].isnull()), 'status_data'] = 'ON PROCESS'
data_all.loc[(data_all['region'].isnull()), 'status_data'] = 'ON PROCESS'
data_all.loc[(data_all['origin'].isnull()), 'status_data'] = 'ON PROCESS'
data_all.loc[(data_all['airlines'].isnull()), 'status_data'] = 'ON PROCESS'
data_all.loc[(data_all['city'].isnull()), 'status_data'] = 'ON PROCESS'
data_all.loc[(data_all['line_haul_origin'].isnull()), 'status_data'] = 'ON PROCESS'
data_all.drop(["transit_code"],axis=1, inplace=True)
data_all.to_csv('data'+periode+'.csv',index=False)

import pandas as pd
import pyarrow
from pandas.io import gbq
from sqlalchemy import text
from google.cloud import bigquery
from sqlalchemy import create_engine

from google.oauth2.service_account import Credentials

#connect API
tonase = "data_daily.source_tonnage"
air = "data_daily.source"
project_id = "cklindonesiaraya"
credential_file = "badata_ckl.json"
credential = Credentials.from_service_account_file(credential_file)

client = bigquery.Client.from_service_account_json(json_credentials_path=credential_file)

#Data
data_upload = pd.read_csv('data'+periode+'.csv')
data_upload['date'] = pd.to_datetime(data_upload['date'],dayfirst=True)
data_upload['no_smu'] = data_upload['no_smu'].astype(str).str.strip().str.upper()
data_upload['no_gabung_paket'] = data_upload['no_gabung_paket'].astype(str).str.strip()
data_upload['line_haul_origin'] = data_upload['line_haul_origin'].astype(str).str.strip().str.upper()
data_upload['origin'] = data_upload['origin'].astype(str).str.strip().str.upper()
data_upload['destination'] = data_upload['destination'].astype(str).str.strip().str.upper()
data_upload['region'] = data_upload['region'].astype(str).str.strip().str.upper()
data_upload['city'] = data_upload['city'].astype(str).str.strip().str.upper()

data_upload['req_pickup'] = pd.to_datetime(data_upload['req_pickup'], dayfirst=True)
data_upload["scan_in"]=pd.to_datetime(data_upload["scan_in"],dayfirst=True)
data_upload["scan_out"]=pd.to_datetime(data_upload["scan_out"],dayfirst=True)
data_upload["wh_origin_in"]=pd.to_datetime(data_upload["wh_origin_in"],dayfirst=True)
data_upload["wh_oirigin_out"]=pd.to_datetime(data_upload["wh_oirigin_out"],dayfirst=True)
data_upload["ra_in"]=pd.to_datetime(data_upload["ra_in"],dayfirst=True)
data_upload["ra_out"]=pd.to_datetime(data_upload["ra_out"],dayfirst=True)
data_upload["trip"]=data_upload["trip"].astype(str)
data_upload['fn1'] = data_upload['fn1'].astype(str).str.strip()
data_upload['fn2'] = data_upload['fn2']
data_upload['fn3'] = data_upload['fn3'].astype(str)
data_upload["eta_1"]=pd.to_datetime(data_upload["eta_1"],dayfirst=True)
data_upload["etd_1"]=pd.to_datetime(data_upload["etd_1"],dayfirst=True)
data_upload["eta_2"]=pd.to_datetime(data_upload["eta_2"],dayfirst=True)
data_upload["etd_2"]=pd.to_datetime(data_upload["etd_2"],dayfirst=True)
data_upload["eta_3"]=pd.to_datetime(data_upload["eta_3"],dayfirst=True)
data_upload["etd_3"]=pd.to_datetime(data_upload["etd_3"],dayfirst=True)
data_upload["wh_in_destination"]=pd.to_datetime(data_upload["wh_in_destination"],dayfirst=True)
data_upload["wh_out_destination"]=pd.to_datetime(data_upload["wh_out_destination"],dayfirst=True)
data_upload["hub_last"]=pd.to_datetime(data_upload["hub_last"],dayfirst=True)
data_upload['colly'] = data_upload['colly'].astype('float64')
data_upload['weight'] = data_upload['weight'].astype('float64')
data_upload['reason']=data_upload['reason'].str.upper()
data_upload['direct_transit']=data_upload['direct_transit']

data_tonase=data_upload.reindex(columns=["date", 'periode', 'customer', 'freight', 'no_gabung_paket', 'no_smu', 'line_haul_origin', 'origin', 'destination', 'city', 'colly', 'weight'])

#update data
query1 ="""
DELETE FROM `cklindonesiaraya.data_daily.source`
WHERE periode='"""+periode+"""';"""
query2 ="""
DELETE FROM `cklindonesiaraya.data_daily.source_tonnage`
WHERE periode='"""+periode+"""'AND freight='AIR FREIGHT';"""

job1 = client.query(query1)
job2 = client.query(query2)


data_upload.to_gbq(air, project_id=project_id, if_exists='append', progress_bar=True, credentials=credential)
data_tonase.to_gbq(tonase, project_id=project_id, if_exists='append', progress_bar=True, credentials=credential)

