#!usr/bin/env Rscript
#
# Purpose: Read acoustic tag detection data for green sturgeon. 
#
# Notes: 
#   - 4GB data file with a *csv file extension, but is is not comma delimited. 
#  
# Author: John R. Brandon, PhD (ICF)
#         john.brandon at icf.com
# Date: 2022-05-31
library(pacman)
p_load(here, tidyverse, lubridate, sf)

# Initialize -------------------------------------------------------------------
f_gs = here::here('data', 'NOAA_GS_all.csv')

# Fixed-Width Field Template for parser ----------------------------------------
headers = read_csv(file = here::here('data', 'headers_and_whitespace.csv'))  # manual counts of white spaces for fixed width

# Pre-process Chunks -----------------------------------------------------------
n_lines = 3511145  # Determined by reading chunks of lines in R until reaching end of file 
skip_lines = seq(from = 2, to = n_lines, by = 5e4)  # line numbers to skip to; first line read for each chunk
n_chunks = length(skip_lines)  # 71
chunk_list = vector(mode = 'list', length = n_chunks)  # initialize empty list for chunks

# Loop through chunks in raw data and output each chunk to an individual file 
# 
# --- WARNING ------------------------------------------------------------------
# This will write a large amount of data (~4GB) onto your disk in the form
#   of 71 *.csv files (3-4 MB each)
# 
for(ii in 1:n_chunks){
  cat(ii, ' chunk_skip_to: ', skip_lines[ii], '\n')
  chunk = read_fwf(file = f_gs, fwf_widths(headers$wspace, headers$header), 
                              skip = skip_lines[ii], n_max = 50000,
                              trim_ws = TRUE, na = c('', ' ', 'NULL'),
                              col_select = c('FishID', 'Length', 'Length_Type', 'Lat', 'Lon', 'DetectDate', 'DetectionLocation'))
  write_csv(x = chunk, file = here::here('out', paste0('chunk_', ii, '.csv')))
}
