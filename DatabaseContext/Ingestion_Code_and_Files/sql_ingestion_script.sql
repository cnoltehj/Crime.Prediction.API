SELECT count(ps2.[StationName])
 FROM [SouthAfrica_CrimeStats].[dbo].[stats_1stQuarter_ZAWC_PoliceStationPerProvince] AS st2
 JOIN [dbo].[PoliceStations] AS ps2 ON st2.[PoliceStationCode] = ps2.[StationCode]
 WHERE ps2.[StationName] LIKE '%Bredasdorp%'

  SELECT DISTINCT ps2.[StationName]
 FROM [SouthAfrica_CrimeStats].[dbo].[stats_1stQuarter_ZAWC_PoliceStationPerProvince] AS st2
 JOIN [dbo].[PoliceStations] AS ps2 ON st2.[PoliceStationCode] = ps2.[StationCode]

SELECT TOP (1000) st.[Id]
      ,st.[2016]
      ,st.[2017]
      ,st.[2018]
      ,st.[2019]
      ,st.[2020]
      ,st.[2021]
      ,st.[2022]
      ,st.[2023]
      ,st.[2022_2023_CountDiff]
      ,st.[2022_2023_PercetageChange]
      ,st.[CategoryCode]
      ,st.[CrimeTypeCode]
      ,st.[ProvinceCode]
      ,st.[PoliceStationCode]
	  ,ps.[StationName]
      ,st.[CrimeCategory]
      ,st.[TypeofCount]
      ,st.[QuarterofYear]
  FROM [SouthAfrica_CrimeStats].[dbo].[stats_1stQuarter_ZAWC_PoliceStationPerProvince] AS st
  JOIN [dbo].[PoliceStations] AS ps ON st.[PoliceStationCode] = ps.[StationCode]
  WHERE ps.[StationName] LIKE '%Bredasdorp%'

