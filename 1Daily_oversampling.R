# this change is for the course ese5023 #

x <- 1

# This script is to (1) read daily satellite files, (2) prepare merge files, 
# and (3) proceed daily oversampiling/regridding. 
# This script generates daily RData, merge, and L3 RData files, 
# and compress daily merge files to save space.

# It takes ~ 25 mins to process 1 day on HuangShan using 1 thread.
# Max memory usage is ~ 6.5 G.

#-------------------------------------------------------------------------------------------
# Load libraies
#-------------------------------------------------------------------------------------------

library(doParallel)

#-------------------------------------------------------------------------------------------
# Set parameters
#-------------------------------------------------------------------------------------------

#---> Set the woeking directory
workingdir             <- "/data1/TROPOMI_HCHO_Jan_Apr_2019_2020_Res_0.5/"
setwd(workingdir)

#---> Set date range of interest
Date_limit             <- c("2020-01-01", "2020-04-30")

#---> allocate the threads for this job
cl <- makeCluster(10) 
registerDoParallel(cl)

#---> Set TROPOMI L2 file and daily RData file directory
#L2_nc_folder           <- "data/L2_nc/"
L2_RData_folder        <- "data/L2_RData/"
L2_merge_folder        <- "data/L2_merge/"
L3_daily_RData_folder  <- "data/L3_daily_RData/"

#---> Set oversampling code dir
Cakecut_folder         <- "cakecut_src/"

#---> Set the limits for satellite pixels, used for data quality control
VCD_limit              <- c( -1e15 , 1.0e18 )  # VCD range
CF_limit               <- c(  0.0  , 0.3    )  # Cloud faction range
SZA_limit              <- c(  0.0  , 60.0   )  # SZA fange

#---> Set the max data points for a day, used in defining the arraies
MAX_points             <- 1e7

#---> Get a list of L2 files
#L2_file_list           <- list.files(L2_nc_folder)
#L2_file_date           <- substr(L2_file_list,21,28)
Date_temp              <- seq(as.Date(Date_limit[1]),as.Date(Date_limit[2]),by="days")
Date_list              <- paste(substr(Date_temp,1,4),substr(Date_temp,6,7),substr(Date_temp,9,10),sep="")

#-------------------------------------------------------------------------------------------
# Start computing
#-------------------------------------------------------------------------------------------

#---> Set the Dataloop function for Multithreaded (parallel) computing
Dataloop <- function(){
  
      # #---> Get file index
      # L2_file_day_list     <- grep(Date_list[iday], L2_file_date)
      # 
      # #---> Is this date found in the L2 folder
      # if( length(L2_file_day_list) > 0 ){
      # 
      #   print("")    
      #   print("============================================")
      #   print(paste(" Process :", Date_list[iday]))
      #   print("============================================")
      #   
      #   print(" * Reading satellite files")
      # 
      #   #---> Define data arraies
      #   Pixel_count        <- 0
      #   SATE_Time          <- array(NA,dim=c(MAX_points,2))
      #   SATE_LAT           <- array(NA,dim=c(MAX_points,5))
      #   SATE_LON           <- array(NA,dim=c(MAX_points,5))
      #   SATE_VCD           <- array(NA,dim=c(MAX_points))
      #   SATE_VCDError      <- array(NA,dim=c(MAX_points))
      #   SATE_AMF           <- array(NA,dim=c(MAX_points))
      #   SATE_SZA           <- array(NA,dim=c(MAX_points))
      #   SATE_VZA           <- array(NA,dim=c(MAX_points))
      #   SATE_CloudFraction <- array(NA,dim=c(MAX_points))
      #   SATE_CloudPressure <- array(NA,dim=c(MAX_points))
      #   SATE_CloudHeight   <- array(NA,dim=c(MAX_points))
      #   
      #   #---> Now loop L2 files
      #   for(ifile in 1:length(L2_file_day_list)){
      #     
      #     #---> Get the file name and print
      #     L2_file          <- paste(L2_nc_folder, L2_file_list[L2_file_day_list[ifile]], sep="")
      #     print(paste("   - Orbit", substr(L2_file_list[L2_file_day_list[ifile]],53,57)))
      #     
      #     #---> Read the file
      #     VCD              <- h5read(L2_file,"PRODUCT/formaldehyde_tropospheric_vertical_column")*6.02214E19                    # molec. cm-2
      #     VCDError         <- h5read(L2_file,"PRODUCT/formaldehyde_tropospheric_vertical_column_precision")*6.02214E19          # molec. cm-2
      #     AMF              <- h5read(L2_file,"PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/formaldehyde_tropospheric_air_mass_factor") # -
      #     CloudFraction    <- h5read(L2_file,"PRODUCT/SUPPORT_DATA/INPUT_DATA/cloud_fraction_crb")                              # -
      #     CloudPressure    <- h5read(L2_file,"PRODUCT/SUPPORT_DATA/INPUT_DATA/cloud_pressure_crb")                              # hpa 
      #     CloudHeight      <- h5read(L2_file,"PRODUCT/SUPPORT_DATA/INPUT_DATA/cloud_height_crb")                                # m
      #     SZA              <- h5read(L2_file,"PRODUCT/SUPPORT_DATA/GEOLOCATIONS/solar_zenith_angle")
      #     VZA              <- h5read(L2_file,"PRODUCT/SUPPORT_DATA/GEOLOCATIONS/viewing_zenith_angle")
      #     LATCenter        <- h5read(L2_file,"PRODUCT/latitude")
      #     LATConrner       <- h5read(L2_file,"PRODUCT/SUPPORT_DATA/GEOLOCATIONS/latitude_bounds")
      #     LONCenter        <- h5read(L2_file,"PRODUCT/longitude")
      #     LONConrner       <- h5read(L2_file,"PRODUCT/SUPPORT_DATA/GEOLOCATIONS/longitude_bounds")
      #     QAValue          <- h5read(L2_file,"PRODUCT/qa_value")
      #     TimeReference    <- h5read(L2_file,"PRODUCT/time")       #
      #     TimeDelta        <- h5read(L2_file,"PRODUCT/delta_time") # milliseconds, 0.001 s
      #     h5closeAll()
      #     
      #     #---> Select valid pixels based on the limits set previously
      #     NPixels          <- dim(VCD)[1]
      #     NScans           <- dim(VCD)[2]
      #     for(ipixel in 1:NPixels){
      #       for(iscan in 1:NScans){
      #         if( QAValue[ipixel, iscan, 1]         > 0.5          && 
      #             VCD[ipixel, iscan, 1]            >= VCD_limit[1] && 
      #             VCD[ipixel, iscan, 1]            <= VCD_limit[2] &&
      #             CloudFraction[ipixel, iscan, 1]  >= CF_limit[1]  && 
      #             CloudFraction[ipixel, iscan, 1]  <= CF_limit[2]  && 
      #             SZA[ipixel, iscan, 1]            >= SZA_limit[1] && 
      #             SZA[ipixel, iscan, 1]            <= SZA_limit[2] ){
      #             
      #           #---> Now get a valid pixel, get data fields
      #           Pixel_count                     <- Pixel_count + 1
      #           SATE_Time[Pixel_count,]         <- c(TimeReference, TimeDelta[ipixel, iscan,1])
      #           SATE_LAT[Pixel_count,]          <- c(LATCenter[ipixel, iscan, 1], LATConrner[,ipixel, iscan, 1])
      #           SATE_LON[Pixel_count,]          <- c(LONCenter[ipixel, iscan, 1], LONConrner[,ipixel, iscan, 1])
      #           SATE_VCD[Pixel_count]           <- VCD[ipixel, iscan, 1]
      #           SATE_VCDError[Pixel_count]      <- VCDError[ipixel, iscan, 1]
      #           SATE_AMF[Pixel_count]           <- AMF[ipixel, iscan, 1]
      #           SATE_SZA[Pixel_count]           <- SZA[ipixel, iscan, 1]              
      #           SATE_VZA[Pixel_count]           <- VZA[ipixel, iscan, 1]    
      #           SATE_CloudFraction[Pixel_count] <- CloudFraction[ipixel, iscan, 1]    
      #           SATE_CloudPressure[Pixel_count] <- CloudPressure[ipixel, iscan, 1]    
      #           SATE_CloudHeight[Pixel_count]   <- CloudHeight[ipixel, iscan, 1]    
      #             
      #         } # Valide pixel	
      #       }   #	Loop scan lines (1-3500)
      #     }     # Loop rows (1-450)
      #   }       # Loop L2 file files
      # }         # Date found in the L2 folder
      # 
      # #---> Resize the arraies, to save space
      # SATE_Time            <- SATE_Time[1:Pixel_count,]
      # SATE_LAT             <- SATE_LAT[1:Pixel_count,]
      # SATE_LON             <- SATE_LON[1:Pixel_count,]
      # SATE_VCD             <- SATE_VCD[1:Pixel_count]
      # SATE_VCDError        <- SATE_VCDError[1:Pixel_count]
      # SATE_AMF             <- SATE_AMF[1:Pixel_count]
      # SATE_SZA             <- SATE_SZA[1:Pixel_count]
      # SATE_VZA             <- SATE_VZA[1:Pixel_count]
      # SATE_CloudFraction   <- SATE_CloudFraction[1:Pixel_count]
      # SATE_CloudPressure   <- SATE_CloudPressure[1:Pixel_count]
      # SATE_CloudHeight     <- SATE_CloudHeight[1:Pixel_count]
      # 
      # #---> Save data into the RData file
      # print(" * Saving daily RData")
      # RData_name           <- paste(L2_RData_folder,"TROPOMI_HCHO_",Date_list[iday],".RData",sep="")
      # save(file=RData_name, Pixel_count, SATE_Time, SATE_LAT, SATE_LON, SATE_VCD,
      #                       SATE_VCDError, SATE_AMF, SATE_SZA, SATE_SZA, SATE_VZA, 
      #                       SATE_CloudFraction, SATE_CloudPressure, SATE_CloudHeight )
      
  
      #---> load the daily RData
      L2_RData           <- paste(L2_RData_folder,"TROPOMI_HCHO_",Date_list[iday],".RData",sep="")
      load(L2_RData)
  
      #---> Save the data to merge file, used for daily oversampling
      #     This step takes a while to finish
      print(" * Saving daily merge file")
      Data_temp            <- data.frame(SATE_LAT, SATE_LON, SATE_VCD, SATE_VCDError)
      Merge_file           <- paste(L2_merge_folder,"TROPOMI_merge_daily_",Date_list[iday],sep="")
      write.fwf(Data_temp, Merge_file, width=rep(15,12), colnames=FALSE, scientific=TRUE)
      
      #---> Make sure there is no NA, NaN, Na, or na in the daily merge file
      print(" * Replacing NA values")
      system(sprintf("sed -i 's/NaN/-9999/g' %s", Merge_file))
      system(sprintf("sed -i 's/NA/-9999/g' %s", Merge_file))
      system(sprintf("sed -i 's/na/-9999/g' %s", Merge_file))
      system(sprintf("sed -i 's/Na/-9999/g' %s", Merge_file))
      
      #---> Compress the daily merge file to save space
      print(" * Compressing daily merge file")
      system(sprintf("gzip %s",Merge_file))
      
      #---> Prepare daily oversampling inputs
      print(" * Proceeding daily oversampling")
      
      #---> Make a copy of the cakecut_src folder, use it as the temp oversampling folder
      cakecut_tmp_folder <- paste("tmp/cakecut_",Date_list[iday],sep="")
      system(sprintf("cp -r %s %s",Cakecut_folder,cakecut_tmp_folder))
      
      #---> Copy the daily merge file to the temp folder
      system(sprintf("cp %s %s",paste(Merge_file,".gz",sep=""),paste(cakecut_tmp_folder,"/Merge_temp.gz",sep="")))
      
      #---> Decompress the gz file
      system(sprintf("gzip -d %s",paste(cakecut_tmp_folder,"/Merge_temp.gz",sep="")))
    
      #---> Do daily oversamling in the temp folder
      setwd(cakecut_tmp_folder)
      system(sprintf("csh run_oversampling.sh"))
      
      #---> Save oversampling results as RData
      print(" * Saving daily L3 RData file")
      data_raw                 <- read.table("L3_Daily_temp",header=F)
      save(data_raw,file="L3_Daily_temp.RData")
      
      #---> Reset the wd
      setwd(workingdir)
      
      #---> Move dialy L3 output
      system(sprintf("mv %s %s",paste(cakecut_tmp_folder,"/L3_Daily_temp.RData",sep=""),
                     paste(L3_daily_RData_folder,"/TROPOMI_HCHO_Daily_L3_",Date_list[iday],".RData",sep="")))
      
      #---> Start cleaning
      print(" * Cleaning daily temp files and free the memory")
      
      #---> Remove daily temp cakecut folder
      system(sprintf("rm -rf %s ",cakecut_tmp_folder))
      
      #---> Clean memory
      rm(Pixel_count, SATE_Time, SATE_LAT, SATE_LON, SATE_VCD, SATE_VCDError, SATE_AMF, SATE_SZA, 
         SATE_VZA, SATE_CloudFraction, SATE_CloudPressure, SATE_CloudHeight, Data_temp)
      gc(full = TRUE)  
      
} # End of the daily Loop

#---> Use foreach for parrallel computing
foreach(iday = 1:length(Date_list), .combine='rbind', .packages= c('rhdf5','gdata')) %dopar% {
  Dataloop()
}

stopCluster(cl)