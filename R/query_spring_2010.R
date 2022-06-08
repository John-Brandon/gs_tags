#!usr/bin/env Rscript
#
# Purpose: Process chunks of data from large DB of green sturgeon tagging data
#
# Notes: 
#   - The large DB is read and split into chunks (of 50,000 lines each) by
#     the 'read_data.R' script. Those chunks are written as individual *.csv files 
#     to the 'out' directory.
#   - As a first cut we're looking at records from spring 2010 to verify that the
#     large DB includes expected tagging data
#  
# Author: John R. Brandon, PhD (ICF)
#         john.brandon at icf.com
# Date: 2022-06-02
library(pacman)
p_load(here, tidyverse, lubridate, sf, foreach, doParallel)

# Process Raw Data -------------------------------------------------------------
# This script below takes a minute or two to read and write the chunks, and the resulting
#   set of files (n = 71) will take up a few GB of disk space in the 'out' directory. 
# YOU HAVE BEEN WARNED ---------------------------------------------------------
# source(here::here('R', 'read_data.R'))  # Read large data base in chunks and output those chunks

# Initialize -------------------------------------------------------------------
headers = read_csv(file = here::here('data', 'headers_and_whitespace.csv'))  # manual counts of white spaces for fixed width file read
n_lines = 3511145  # This number from manual investigation (reading lines in chunks until reaching end of raw data file)
skip_lines = seq(from = 2, to = n_lines, by = 5e4)  # line numbers to skip to; first line read for each chunk
n_chunks = length(skip_lines)  # 71
chunk_out_ls = vector(mode = 'list', length = n_chunks)  # Initialize empty list for processing chunks in parallel across multiple CPUs
myCluster = makeCluster(detectCores() - 1, # number of cores to use
                        type = "FORK") # "FORK" cluster copies packages etc. into R session running on each thread (Mac only?)
registerDoParallel(myCluster)           # Initialize multi-core process

# Query Data -------------------------------------------------------------------
# Uses Parallel CPUs 
# system.time({  # this way (parallel reading from chunked out files) takes about 2sec or less
chunk_out_ls = foreach(ii = 1:n_chunks) %dopar% {
  read_csv(file = here::here('out', paste0('chunk_', ii, '.csv')),
           show_col_types = FALSE) %>% 
    mutate(year = lubridate::year(DetectDate),
           month = lubridate::month(DetectDate)) %>% 
    filter(year == 2010) %>% 
    mutate(across(where(is.logical), as.character))  # Workaround for empty data.frames to get them to play nicely with `bind_rows` below
}
# })

# Collapse Chunks --------------------------------------------------------------
dat_2010 = bind_rows(chunk_out_ls)
write_csv(filter(dat_2010, month %in% 2:6), # Output for manual inspection
          file = here::here('out', 'gs_acoustic_tag_detections_feb-jun_2010.csv'))

# Zoomed out Map ---------------------------------------------------------------
# Prepare data for map
crs_gs = 4326
dat_plt = filter(dat_2010, month %in% 2:6) %>% 
  mutate(month = month.abb[month]) %>% 
  mutate(month = factor(month, levels = c('Feb', 'Mar', 'Apr', 'May', 'Jun'))) %>% 
  st_as_sf(coords = c("Lon", "Lat"), crs = crs_gs)

shp_cal = st_read(here::here('shapefiles', 'California', 'California.shp')) %>% 
  st_transform(crs = st_crs(dat_plt)) %>%  # Make CRS(polygon) == CRS(points) 
  st_crop(xmin = -124.6, xmax = -120.7, ymin = 37.25, ymax = 42.0)
# Plot map
ggplot(shp_cal) +  
  geom_sf() +
  theme_bw() + 
  geom_sf(data = dat_plt,
          alpha = 0.1, size = 3) +
  facet_wrap(nrow = 2, facets = vars(month)) + 
  labs(title = 'Feb-June 2010 Green Sturgeon Acoustic Tag Detections')

ggsave(filename = here::here('img', 'gs_2010_feb-jun_detect_wide.png'))

# Zoom in Map ------------------------------------------------------------------
dat_plt_cropped = filter(dat_2010, month %in% 2:6) %>% 
  mutate(month = month.abb[month]) %>% 
  mutate(month = factor(month, levels = c('Feb', 'Mar', 'Apr', 'May', 'Jun'))) %>% 
  st_as_sf(coords = c("Lon", "Lat"), crs = crs_gs) %>% 
  st_crop(xmin = -124.6, xmax = -120.7, ymin = 37.25, ymax = 38.5)

shp_cal_zoomed = st_read(here::here('shapefiles', 'California', 'California.shp')) %>% 
  st_transform(crs = st_crs(dat_plt)) %>%  # Make CRS(polygon) == CRS(points) 
  st_crop(xmin = -124.6, xmax = -120.7, ymin = 37.25, ymax = 38.5)

ggplot(shp_cal_zoomed) +
  geom_sf() +
  theme_bw() + 
  geom_sf(data = dat_plt_cropped,
          alpha = 0.1, size = 3) +
  facet_wrap(nrow = 3, facets = vars(month)) + 
  labs(title = 'Feb-June 2010 Green Sturgeon Acoustic Tag Detections')

ggsave(filename = here::here('img', 'gs_2010_feb-jun_detect_cropped.png'))

# Compare serial processing ----------------------------------------------------
# chunk_years = vector(mode = 'list', length = n_chunks)
# system.time({  # this way (serial read from chunked out files) takes about 7sec
# for(jj in 1:n_chunks){
#   chunk_years[[jj]] = read_csv(file = here::here('out', paste0('chunk_', jj, '.csv')),
#                                show_col_types = FALSE) %>%
#     mutate(year = year(DetectDate),
#            month = month(DetectDate)) %>%
#     filter(year == 2010) %>%
#     mutate(across(where(is.logical), as.character))  # Workaround for empty data.frames to get them to play nicely with `bind_rows` below
# }
# })
# dat_2010 = bind_rows(chunk_years)
