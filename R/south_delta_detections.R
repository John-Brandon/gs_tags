#!usr/bin/env Rscript
#
# Purpose: Retrieve Green Sturgeon acoustic tag data within a specified bounding polygon
#
# Notes: Coordinates for polygon from Eric Chapman via email 2022-07-12
#  
# Author: John R. Brandon, PhD (ICF)
#         john.brandon at icf.com
# Date: 2022-07-12
library(pacman)
p_load(here, tidyverse, readxl, sf, lubridate, sf, foreach, doParallel)

# Initialize -------------------------------------------------------------------
CRS = 4326  # Coordinate Reference System for GIS
headers = read_csv(file = here::here('data', 'headers_and_whitespace.csv'))  # manual counts of white spaces for fixed width file read
n_lines = 3511145  # This number from manual investigation (reading lines in chunks until reaching end of raw data file)
skip_lines = seq(from = 2, to = n_lines, by = 5e4)  # line numbers to skip to; first line read for each chunk
n_chunks = length(skip_lines)  # 71
chunk_out_ls = vector(mode = 'list', length = n_chunks)  # Initialize empty list for processing chunks in parallel across multiple CPUs
myCluster = makeCluster(detectCores() - 1, # number of cores to use
                        type = "FORK") # "FORK" cluster copies packages etc. into R session running on each thread (Mac only?)
registerDoParallel(myCluster)           # Initialize multi-core process

# Bounding polygon -------------------------------------------------------------
bbox = readxl::read_excel(path = here::here("data", "bounding_polygon.xlsx"))

# Transform points to spatial (GIS) polygon object
bbox_sf = bbox %>% 
  st_as_sf(coords = c("x", "y"), crs = CRS) %>% 
  summarise(geometry = st_combine(geometry)) %>%
  st_cast("POLYGON")

# Query Data -------------------------------------------------------------------
# Uses Parallel CPUs 
# system.time({  # this way (parallel reading from chunked out files) takes about 2sec or less
chunk_out_ls = foreach(ii = 1:n_chunks) %dopar% {
  read_csv(file = here::here('out', paste0('chunk_', ii, '.csv')),
           show_col_types = FALSE) %>% 
    filter(!is.na(Lon) & !is.na(Lat)) %>%  # Filter out records with missing Lat/Lon values
    st_as_sf(coords = c("Lon", "Lat"), crs = CRS) %>%  # Transform acoustic detections (points) to spatial GIS object
    st_intersection(bbox_sf) %>%                        # Filter out only points within the polygon 
    mutate(across(where(is.logical), as.character))  # Workaround for empty data.frames to get them to play nicely with `bind_rows` below
}

# Test query
# test_chunk = read_csv(file = here::here('out', paste0('chunk_', 4, '.csv')),
#                         show_col_types = FALSE) %>% 
#   filter(!is.na(Lon) & !is.na(Lat)) %>%  # Filter out records with missing Lat/Lon values
#   st_as_sf(coords = c("Lon", "Lat"), crs = 4326) %>%  # Transform acoustic detections (points) to spatial GIS object
#   st_intersection(bbox_sf) %>%                        # Filter out only points within the polygon 
#   mutate(across(where(is.logical), as.character))  # Workaround for empty data.frames to get them to play nicely with `bind_rows` below
# 
# test_chunk

# Collapse Chunks --------------------------------------------------------------
sf_dat = bind_rows(chunk_out_ls)
sf_fish_days = sf_dat %>% 
  mutate(year = year(DetectDate),
         month = month(DetectDate),
         day = day(DetectDate),
         date = as.Date(DetectDate)) %>% 
  arrange(DetectDate) %>% 
  distinct(FishID, date, .keep_all = TRUE)
  
# This gets us back to a simple data.frame, now with GIS region assignments
flat_dat = sf_dat %>% 
  mutate(long = st_coordinates(.)[, 1],  # Create columns with Lat/Lon
         lat = st_coordinates(.)[, 2]) %>% 
  st_set_geometry(NULL) %>%              # Drops "geometry" column, but we have copied XY in Lat/Lon columns above
  mutate(year = year(DetectDate),
         month = month(DetectDate),
         day = day(DetectDate),
         date = as.Date(DetectDate)) %>% 
  arrange(DetectDate)

# flat_dat  # Check
write_csv(x = flat_dat, file = here::here("out", "unfiltered_gs_detections_within_box.csv"))
unfilt_gs = read_csv(here::here("out", "unfiltered_gs_detections_within_box.csv"))
unique(unfilt_gs$year)

# Unique Fish-Days -------------------------------------------------------------
fish_days = flat_dat %>% 
  distinct(FishID, date, .keep_all = TRUE) %>% 
  mutate(month_abb = month.abb[month]) %>%
  select(FishID:Length_Type, month_abb, month, DetectDate, everything()) %>%
  mutate(month_abb = factor(month_abb, levels = month.abb))
fish_days
View(fish_days)
write_csv(x = fish_days, file = here::here("out", "filtered_gs_fish_days_within_box.csv"))
# fish_days %>% 
#   distinct(FishID) %>% 
#   summarize(length(FishID))

# Fish Days to GIS -------------------------------------------------------------
sf_fish_days = st_as_sf(fish_days, coords = c("long", "lat"), crs = CRS)  # Transform acoustic detections (points) to spatial GIS object

# California Map ---------------------------------------------------------------
shp_cal = st_read(here::here('shapefiles', 'California', 'California.shp')) %>% 
  st_transform(crs = CRS) %>%  # Make CRS(polygon) == CRS(points) 
  st_crop(xmin = -123.5, xmax = -120.7, ymin = 37.25, ymax = 38.25)

# Map BBox ---------------------------------------------------------------------
ggplot(shp_cal) +
  geom_sf() +
  theme_bw() + 
  geom_sf(data = bbox_sf,
          alpha = 0.1, size = 1, color = "red") +
  geom_sf(data = sf_fish_days, alpha = 0.3, size = 2) + 
  facet_wrap(nrow = 4, facets = vars(month_abb)) +
  labs(title = 'Unique Fish Days: Green Sturgeon Acoustic Tag Detections') +
  NULL

ggsave(filename = here::here("img", "gs_fish_day_detections.png"))

# Plot Fish-Days per month -----------------------------------------------------
# NOTE: Removes juvenile with lengths less than 150 (units? mm?)
month_table = tibble(month = 1:12, mm = month.abb)
fish_days = read_csv(here::here("out", "filtered_gs_fish_days_within_box.csv")) %>% 
  filter(Length > 150)  
filter(fish_days, month == 1) %>% 
  count(FishID)

fish_tmp = calc_wyt(date = fish_days$DetectDate, wyt = water_year_indices) %>% 
  left_join(fish_days, by = c("dates" = "DetectDate")) %>% 
  select(dates, mm_abb, year, wy_yr, Yr_type, FishID, Length, DetectionLocation, everything()) %>% 
  rename(wyt = Yr_type)

# Number of individual fish detected each month (if one fish detected multiple times, that counts as one detection)
n_fish_month = fish_tmp %>% 
  select(month, year, date, FishID) %>% 
  group_by(month) %>% 
  summarize(n_fish = n_distinct(FishID)) %>% 
  right_join(month_table, by = "month") %>% 
  mutate(n_fish = ifelse(is.na(n_fish), 0, n_fish),
         mm = factor(mm, levels = month.abb)) %>% 
  arrange(mm)

days_w_detection = fish_tmp %>%  # Number of unique FishID.x.Day 
  group_by(month, date, wyt) %>% 
  distinct(FishID) %>% 
  ungroup() %>% 
  group_by(month) %>% 
  summarize(n_days = n_distinct(date)) %>% 
  right_join(month_table, by = "month") %>% 
  mutate(n_days = ifelse(is.na(n_days), 0, n_days),
         mm = factor(mm, levels = month.abb)) %>% 
  arrange(mm)

n_fish_month
days_w_detection  # Check

fish_plt_1 = days_w_detection %>% 
  select(-month) %>% 
  left_join(n_fish_month, by = "mm")

fish_plt_1 %>% 
  ggplot(aes(x = mm, y = n_fish, fill = n_days)) +
  geom_col() +
  theme_bw(base_size = 16) + 
  scale_fill_viridis_c() +
  # scale_x_discrete(breaks = c("Jan","Mar","May", "Jul", "Sep", "Nov")) +
  labs(x = "Month", y = "Individuals", title = "Individual Green Sturgeon Detected \nin the South Delta",
       subtitle = "Acoustic Tag Data: 2005-2014",
       fill = "Number of Days Individuals Detected") +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
        legend.position = "bottom")

ggsave(here::here("img", "gs_individuals_by_month.png"))

# 
yrs_w_detection = fish_tmp %>%  # Number of unique FishID.x.Day 
  group_by(year, month, date, wyt) %>% 
  distinct(FishID) %>% 
  ungroup() %>% 
  group_by(month) %>% 
  summarize(n_yrs = n_distinct(year)) %>% 
  right_join(month_table, by = "month") %>% 
  mutate(n_yrs = ifelse(is.na(n_yrs), 0, n_yrs),
         mm = factor(mm, levels = month.abb)) %>% 
  arrange(mm)

yrs_w_detection

fish_plt_mm_yy = yrs_w_detection %>% 
  
fish_tmp %>% 
  group_by(month, wyt, wy_yr) %>%
  # group_by(wyt, wy_yr) %>%
  summarize(distinct_fishdays = n_distinct(FishID)) %>% 
  arrange(wy_yr)

# Water Year Type --------------------------------------------------------------
fish_plt_wyt = month_table %>% 
  left_join(fish_tmp, by = "month") %>% 
  group_by(month, wyt, wy_yr) %>%
  # group_by(wyt, wy_yr) %>%
  summarize(distinct_fishdays = n_distinct(FishID)) %>% 
  ungroup() %>% 
  arrange(wy_yr) %>% 
  mutate(distinct_gs = ifelse(is.na(wy_yr), 0, distinct_fishdays)) 

fish_plt

fish_plt %>% 
  # group_by(month) %>% 
  # summarize(distinct_fishdays = sum(distinct_gs)) %>% 
  mutate(mm_abb = month.abb[month]) %>% 
  mutate(mm_abb = factor(mm_abb, levels = month.abb)) %>% 
  ggplot(aes(x = mm_abb, y = distinct_gs)) +
  geom_col() +
  theme_bw(base_size = 16) + 
  labs(x = "Month", y = "Days", title = "Days Individual Green Sturgeon Detected",
       subtitle = "Acoustic Tagging Data: 2005-2014") +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))

tail(water_year_indices)
