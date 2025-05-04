# ------------------------------------------------------------------------
# Dockerfile for running the RAS2FIM-2D workflow
# Dockhub Image name: civileng127/ras2fim-2d:v0
# Description: Realtime update of FAST Database
# Flood Assesment System for TxDOT
# From a hydrologic forecast (National Water Model)
# update the flooded roads (lines), flood limits (polygons)
# and bridge warnings (points)
#
# Base Image: continuumio/miniconda3:latest
#  -- Uses the Debian 11; 'bullseye'
# Version: 1.0.0
# Created by: Andy Carter, PE 
# -- Center for Water and the Environment
# -- University of Texas at Austin
# Date: 2025-05-04
# License: BSD 3-Clause License
# ------------------------------------------------------------------------

# Example use: docker run -it -v E:\ras-docker-20240908:/ras/Linux_RAS_v65/mac-test civileng127/ras_v65:v0 /bin/bash -c "cd /ras/Linux_RAS_v65/mac-test && RasUnsteady sample_ras_name.p01,hdf.tmp x01"

# Use the Miniconda base image from Continuum
FROM continuumio/miniconda3:latest

# Install apt-get packages first
RUN apt-get update && apt-get install -y git nano wget proj-bin

# Set the Python version (3.8.12 in this case)
RUN conda install python=3.8.12 -y

# Install the latest gdal via conda
RUN conda install gdal -y

# Install python libraries via pip
RUN pip install geopandas==0.12.1 boto3==1.28.80 xarray s3fs sqlalchemy psycopg2-binary tqdm numpy shapely

# Clean up conda cache to reduce image size
RUN conda clean -a

# Remove unnecessary apt-get lists to reduce image size
RUN rm -rf /var/lib/apt/lists/*

# Set environment variables to ensure conda is available in PATH
ENV PATH /opt/conda/bin:$PATH

# Clone the desired GitHub repository
RUN git clone https://github.com/andycarter-pe/fast_realtime_update.git /fast_realtime_update

# Create necessary directories
RUN mkdir /global_input /model_input /model_output

# Set working directory
WORKDIR /fast_realtime_update/src

# Default command to run when container starts
CMD [ "bash" ]