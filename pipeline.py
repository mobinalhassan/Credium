import os
import requests
from urllib.parse import urlparse, parse_qs
import geopandas as gpd
import logging
import json
from shapely import wkt
from downloader import Downloader
import pdal
from shapely.geometry import Polygon
from shapely.ops import transform
from pyproj import Transformer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Pipeline:
    """
    A class to represent a data processing pipeline for extracting LiDAR Cloud point data 
    based on a given address and saves results in 'intersection_cutout' folder. 

    Attributes:
        city (str): The city of the address.
        street (str): The street of the address.
        house_number (str): The house number of the address.
        zip_code (str): The ZIP code of the address.
        subscription_key (str): The subscription key for accessing APIs.
        config (dict): The configuration settings for processing, refer to config.json.
        state_config (dict): State-specific configuration settings.
        universal_crs (str): The universal coordinate reference system (default: EPSG:4326).
    """
    def __init__(self, address, subscription_key, config):
        self.city = address['city']
        self.street = address['street']
        self.house_number = address['house_number']
        self.zip_code = address['zip_code']
        self.subscription_key =subscription_key
        self.config = config
        self.state_config = None
        self.universal_crs= "EPSG:4326"


    def run_pipeline(self):
        """
        Executes the data processing pipeline method in sequence.

        This method queries the building polygon for the given address,
        retrieves the appropriate LiDAR tiles, converts LAZ files to LAS format,
        and extracts intersection of building footprint by calling methods in sequence.
        """
        building_geom, state_name = self.query_building_polygon()
        if building_geom is None:
            logger.error("Failed to get the building geometry.")
            return
        
        if state_name not in self.config:
            logger.error(f"No configuration found for state: {state_name}")
            return

        # Access state-specific configurations
        self.state_config = self.config[state_name]

        # Get tile names
        tile_file_name = self.get_tile_names(building_geom)

        if not tile_file_name:
            logger.error("No LiDAR tiles found covering the building footprint.")
            return

        dwl=Downloader(self.state_config)
        local_tile_path =dwl.download_any_file(tile_file_name)
        logger.info(f"Downloaded tile path: {local_tile_path}")

        las_file_path=''
        if '.laz' in local_tile_path:
            las_file_path = f"{local_tile_path.split('.')[0]}.las"

        self.convert_laz_to_las(local_tile_path, las_file_path, crs=self.state_config["EPSG"])

        os.makedirs('intersection_cutout', exist_ok=True)
        output_las_path = f"intersection_cutout/{self.street} {self.house_number} {self.zip_code} {self.city}-intersection.las"


        self.extract_points_within_polygon(las_file_path, output_las_path, las_crs=self.state_config["EPSG"], polygon_crs=self.universal_crs, polygon_wkt=str(building_geom))
        

    def query_building_polygon(self):
        """
        Queries the API to retrieve the building polygon for the specified address.

        Returns:
            tuple: A tuple containing the building geometry (shapely Polygon) 
                   and the state name (str). Returns (None, None) if no building found.
        """
        logger.info(f"Querying Building Polygons")
        base_url = "https://credium-api.azure-api.net/dev/data-product/base/v1/"
        params = {
            "City": self.city,
            "Street": self.street,
            "HouseNumber": self.house_number,
            "ZipCode": self.zip_code,
            "PageNumber": 1
        }
        headers = {
            "Subscription-Key": self.subscription_key
        }
        response = requests.get(base_url, params=params, headers=headers)
        logger.info(f"API Response: {response}")
        if response.status_code == 200:
            data = response.json()
            # import pprint
            # pprint.pprint(data)
            
            if 'buildings' in data and len(data['buildings']) > 0:
                result = data['buildings'][0]['buildingInformation']
                building_footprint_url = result.get('buildingFootprintGeometry')
                logger.info(f"Building FootPrint URL: {building_footprint_url}")

                state_name = data['buildings'][0]['addressInformation']['addresses'][0]['state']
                logger.info(f"State Name: {state_name}")

                building_ploygon = self.fetch_building_footprint_geometry(building_footprint_url)
                logger.info(f"Building Ploygons: {building_ploygon}")
                return building_ploygon, state_name
            else:
                logger.error("No results found for the given address.")
                return None
        else:
            logger.error(f"API request failed with status code {response.status_code}")
            logger.error(f"Response content: {response.text}")
            return None
        
    def fetch_building_footprint_geometry(self, building_geom_url):
        """
        Fetches the building footprint geometry from the provided URL by calling API.

        Args:
            building_geom_url (str): The URL for the building footprint geometry.

        Returns:
            shapely.geometry.Polygon: The building footprint geometry as a shapely Polygon.
        """
        parsed_url = urlparse(building_geom_url)
        query_params = parse_qs(parsed_url.query)
        geom_id = query_params.get('GeomId', [None])[0]
        geom_type = query_params.get('GeomType', [None])[0]

        if not geom_id or not geom_type:
            logger.error("GeomId or GeomType not found in the geometry URL.")
            return None

        geometry_api_url = "https://credium-api.azure-api.net/dev/geometry/"
        params = {
            "GeomId": geom_id,
            "GeomType": geom_type
        }
        headers = {
            "Subscription-Key": self.subscription_key
        }

        response = requests.get(geometry_api_url, params=params, headers=headers)
        if response.status_code == 200:
            geometry_data = response.json()
            # Assuming the geometry is returned in WKT format under 'geometryAsWKT'
            if 'geometryAsWKT' in geometry_data:
                geom_wkt = geometry_data['geometryAsWKT']
                building_ploygon = wkt.loads(geom_wkt)
                return building_ploygon
            else:
                logger.error("geometryAsWKT not found in the response.")
                return None
        else:
            logger.error(f"Geometry API request failed with status code {response.status_code}")
            logger.error(f"Response content: {response.text}")
            return None
        

    def get_tile_names(self, building_geom):
        """
        Generates tile names for LiDAR cloud point data based on the building geometry.
        This method generate tile name according to name given on source website.
        To add more state you need to extend the logic of this function.

        Args:
            building_geom (shapely.geometry.Polygon): The building footprint geometry.

        Returns:
            str: The file name of the LiDAR tile.
        """
        logger.info(f"Fetching Lidar 1 X 1 file name!")
        building_geom_reprojected = self.reproject_geometry(building_geom, self.universal_crs, self.state_config["EPSG"])
        # Get bounding box
        minx, miny, maxx, maxy = building_geom_reprojected.bounds

        min_easting_index = int(minx) // 100
        max_easting_index = int(maxx) // 100

        min_northing_index = int(miny) // 1000
        max_northing_index = int(maxy) // 1000

        tile_names = []
        for easting_index in range(min_easting_index, max_easting_index + 1):
            for northing_index in range(min_northing_index, max_northing_index + 1):
                tile_name = f"{easting_index}-{northing_index}"
                tile_names.append(tile_name)

        tile_file_name =''
        if self.state_config["state_name"] == "Brandenburg":
            splitted_time_name=tile_name.split('-')
            tile_file_name =f"als_33{''.join(list(splitted_time_name[0])[:-1])}-{splitted_time_name[-1]}.{self.state_config['source_ext']}"
            logger.info(f"Tile name on web: {tile_file_name}")

        elif self.state_config["state_name"] == "Bayern":
            splitted_time_name=tile_name.split('-')
            tile_file_name =f"{''.join(list(splitted_time_name[0])[:-1])}_{splitted_time_name[-1]}.{self.state_config['source_ext']}"
            logger.info(f"Tile name on web: {tile_file_name}")

        elif self.state_config["state_name"] == "Nordrhein-Westfalen":
            splitted_time_name=tile_name.split('-')
            tile_file_name =f"3dm_32_{''.join(list(splitted_time_name[0])[:-1])}_{splitted_time_name[-1]}_1_nw.{self.state_config['source_ext']}"
            logger.info(f"Tile name on web: {tile_file_name}")

        # elif self.state_config["state_name"] == "Berlin":
        #     splitted_time_name=tile_name.split('-')
        #     print(splitted_time_name)
        #     print(''.join(list(splitted_time_name[0])[:-1]))
        #     tile_file_name =f"3dm_32_{''.join(list(splitted_time_name[0])[:-1])}_{splitted_time_name[-1]}_1_nw.{self.state_config['source_ext']}"
        #     print(tile_file_name)
        #     logger.info(f"Tile name on web: {tile_file_name}")
            
        return tile_file_name
    
    def reproject_geometry(self, building_geom, src_crs, dst_crs):
        """
        Reprojects the given building geometry from source CRS to destination CRS.

        Args:
            building_geom (shapely.geometry.Polygon): The building footprint geometry.
            src_crs (str): The source coordinate reference system.
            dst_crs (str): The destination coordinate reference system.

        Returns:
            shapely.geometry.Polygon: The reprojected building geometry.
        """
        building_gdf = gpd.GeoDataFrame(geometry=[building_geom], crs=src_crs)
        building_gdf = building_gdf.to_crs(dst_crs)
        return building_gdf.geometry.iloc[0]
    
    def convert_laz_to_las(self, laz_file_path, las_file_path, crs):
        """
        Converts a LAZ file to LAS format using PDAL.

        Args:
            laz_file_path (str): The file path of the input LAZ file.
            las_file_path (str): The file path for the output LAS file.
            crs (str): The coordinate reference system to use for the LAS file.
        """
        logger.info(f"Converting LAZ to LAS formate!")
        
        #PDAL pipeline
        pipeline_json = {
            "pipeline": [
                {
                    "type": "readers.las",
                    "filename": laz_file_path,
                    "spatialreference": crs
                },
                {
                    "type": "writers.las",
                    "filename": las_file_path,
                    "compression": "none",
                    "a_srs": crs
                }
            ]
        }

        # create and execute the PDAL pipeline
        pipeline = pdal.Pipeline(json.dumps(pipeline_json))
        try:
            pipeline.execute()
            logger.info(f"Converted {laz_file_path} to {las_file_path}")
        except RuntimeError as e:
            logger.error(f"PDAL pipeline execution failed: {e}")


    def extract_points_within_polygon(self, las_file_path, output_las_path, las_crs, polygon_crs,  polygon_wkt):
        """
        Extracts points from the LAS file that fall within the specified polygon.

        Args:
            las_file_path (str): The file path of the input LAS file.
            output_las_path (str): The file path for the output LAS file containing extracted points.
            las_crs (str): The coordinate reference system of the LAS file.
            polygon_crs (str): The coordinate reference system of the polygon.
            polygon_wkt (str): The polygon geometry in WKT format.
        """
        logger.info(f"Cutting Out Building Footprints!")
        polygon = wkt.loads(polygon_wkt)

        if las_crs != polygon_crs:
            logger.info(f"Reprojecting polygon from {polygon_crs} to {las_crs}")
            transformer = Transformer.from_crs(polygon_crs, las_crs, always_xy=True)
            polygon = transform(transformer.transform, polygon)

        polygon_wkt_reprojected = polygon.wkt

        # PDAL pipeline to cutout footprint
        pipeline_json = {
            "pipeline": [
                {
                    "type": "readers.las",
                    "filename": las_file_path,
                    "spatialreference": las_crs
                },
                {
                    "type": "filters.crop",
                    "polygon": polygon_wkt_reprojected
                },
                {
                    "type": "writers.las",
                    "filename": output_las_path
                }
            ]
        }

        # create and execute the PDAL pipeline
        pipeline = pdal.Pipeline(json.dumps(pipeline_json))
        try:
            pipeline.execute()
            logger.info(f"Extracted points saved to {output_las_path}")
        except RuntimeError as e:
            logger.error(f"PDAL pipeline execution failed: {e}")