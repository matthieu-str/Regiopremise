"""
This Python package regionalizes processes from the ecoinvent database using trade date from the UN COMTRADE database.
In a first time (that I call first_order_regionalization) electricity, heat and municipal solid waste processes inputs
are adapted to the geographical context. In a second time, all created processes are linked to the rest of the database.

file name: regioinvent.py
author: Maxime Agez
e-mail: maxime.agez@polymtl.ca
date created: 06-04-24
"""

import pandas as pd
import numpy as np
import json
import pkg_resources
import brightway2 as bw2
import uuid
import sqlite3
import logging
import pickle
import collections
import wurst
import wurst.searching as ws
import copy
from tqdm import tqdm


class Regioinvent:
    def __init__(self, trade_database_path, regioinvent_database_name, bw_project_name, ecoinvent_database_name,
                 regionalized_elementary_flows=True, cutoff=0.95):
        """
        :param trade_database_path:         [str] the path to the trade database, the latter should be downloaded from Zenodo:
                                            https://doi.org/10.5281/zenodo.11583815
        :param regioinvent_database_name:   [str] the name to be given to the created regionalized ecoinvent database
        :param bw_project_name:             [str] the name of a brightway2 project containing an ecoinvent database
        :param ecoinvent_database_name:     [str] the name of the ecoinvent database within the brightway2 project
        :param regionalized_elementary_flows: [boolean] should elementary flows be regionalized or not?
        :param cutoff:                      [float] cut-off up to which countries will be covered as separate geographies (not in RoW).
                                            A higher cut-off means a bigger resulting database (careful it's not linear)
        """

        # set up logging tool
        self.logger = logging.getLogger('Regioinvent')
        self.logger.setLevel(logging.INFO)
        self.logger.handlers = []
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)
        self.logger.propagate = False

        # set up brightway project
        bw2.projects.set_current(bw_project_name)

        # set up necessary variables
        self.regioinvent_database_name = regioinvent_database_name
        self.ecoinvent_database_name = ecoinvent_database_name
        self.name_ei_with_regionalized_biosphere = ecoinvent_database_name + ' regionalized'
        self.name_regionalized_biosphere_database = 'biosphere3_regionalized_flows'
        self.cutoff = cutoff
        self.regio_bio = regionalized_elementary_flows
        self.trade_conn = sqlite3.connect(trade_database_path)
        self.ecoinvent_conn = sqlite3.connect(bw2.Database(self.regioinvent_database_name).filepath_processed().split(
            '\\processed\\')[0] + '\\lci\\databases.db')
        self.ecoinvent_cursor = self.ecoinvent_conn.cursor()

        # load data from the different mapping files and such
        with open(pkg_resources.resource_filename(__name__, '/Data/ecoinvent_to_HS.json'), 'r') as f:
            self.eco_to_hs_class = json.load(f)
        with open(pkg_resources.resource_filename(__name__, '/Data/HS_to_exiobase_name.json'), 'r') as f:
            self.hs_class_to_exio = json.load(f)
        with open(pkg_resources.resource_filename(__name__, '/Data/country_to_ecoinvent_regions.json'), 'r') as f:
            self.country_to_ecoinvent_regions = json.load(f)
        with open(pkg_resources.resource_filename(__name__, '/Data/electricity_processes.json'), 'r') as f:
            self.electricity_geos = json.load(f)
        with open(pkg_resources.resource_filename(__name__, '/Data/electricity_aluminium_processes.json'), 'r') as f:
            self.electricity_aluminium_geos = json.load(f)
        with open(pkg_resources.resource_filename(__name__, '/Data/waste_processes.json'), 'r') as f:
            self.waste_geos = json.load(f)
        with open(pkg_resources.resource_filename(__name__, '/Data/water_processes.json'), 'r') as f:
            self.water_geos = json.load(f)
        with open(pkg_resources.resource_filename(__name__, '/Data/heat_industrial_ng_processes.json'), 'r') as f:
            self.heat_district_ng = json.load(f)
        with open(pkg_resources.resource_filename(__name__, '/Data/heat_industrial_non_ng_processes.json'), 'r') as f:
            self.heat_district_non_ng = json.load(f)
        with open(pkg_resources.resource_filename(__name__, '/Data/heat_small_scale_non_ng_processes.json'), 'r') as f:
            self.heat_small_scale_non_ng = json.load(f)
        with open(pkg_resources.resource_filename(__name__, '/Data/COMTRADE_to_ecoinvent_geographies.json'), 'r') as f:
            self.convert_ecoinvent_geos = json.load(f)
        with open(pkg_resources.resource_filename(__name__, '/Data/COMTRADE_to_exiobase_geographies.json'), 'r') as f:
            self.convert_exiobase_geos = json.load(f)

        # initialize attributes used within code
        self.assigned_random_geography = []
        self.regioinvent_in_wurst = []
        self.regioinvent_in_dict = {}
        self.ei_regio_data = {}
        self.distribution_technologies = {}
        self.transportation_modes = {}
        self.created_geographies = dict.fromkeys(self.eco_to_hs_class.keys())
        self.unit = dict.fromkeys(self.eco_to_hs_class.keys())
        self.base_iw_water_flows = ['Water',
                                    'Water, cooling, unspecified natural origin',
                                    'Water, lake',
                                    'Water, river',
                                    'Water, turbine use, unspecified natural origin',
                                    'Water, unspecified natural origin',
                                    'Water, well, in ground']
        self.base_iw_land_flows = ['Occupation, agriculture, mosaic (agroforestry)',
                                   'Occupation, annual crops',
                                   'Occupation, artificial areas',
                                   'Occupation, forest, used',
                                   'Occupation, forest/grassland, not used',
                                   'Occupation, pasture/meadow',
                                   'Occupation, permanent crops',
                                   'Occupation, secondary vegetation',
                                   'Occupation, unspecified',
                                   'Occupation, agriculture, mosaic (agroforestry)',
                                   'Transformation, from annual crops',
                                   'Transformation, from artificial areas',
                                   'Transformation, from forest, used',
                                   'Transformation, from forest/grassland, not used',
                                   'Transformation, from pasture/meadow',
                                   'Transformation, from permanent crops',
                                   'Transformation, from secondary vegetation',
                                   'Transformation, from unspecified',
                                   'Transformation, to agriculture, mosaic (agroforestry)',
                                   'Transformation, to annual crops',
                                   'Transformation, to artificial areas',
                                   'Transformation, to forest, used',
                                   'Transformation, to forest/grassland, not used',
                                   'Transformation, to pasture/meadow',
                                   'Transformation, to permanent crops',
                                   'Transformation, to secondary vegetation',
                                   'Transformation, to unspecified']
        self.base_iw_acid_eutro_flows = ['Ammonia',
                                         'Ammonia, as N',
                                         'Ammonium carbonate',
                                         'Ammonium nitrate',
                                         'Ammonium, ion',
                                         'Dinitrogen monoxide',
                                         'Kjeldahl-N',
                                         'Nitrate',
                                         'Nitric acid',
                                         'Nitric oxide',
                                         'Nitrite',
                                         'Nitrogen dioxide',
                                         'Nitrogen oxides',
                                         'Nitrogen',
                                         'Nitrogen, organic bound',
                                         'Nitrogen, total',
                                         'Sulfate',
                                         'Sulfur dioxide',
                                         'Sulfur trioxide',
                                         'Sulfuric acid',
                                         'BOD5, Biological Oxygen Demand',
                                         'COD, Chemical Oxygen Demand',
                                         'Phosphate',
                                         'Phosphoric acid',
                                         'Phosphorus compounds, unspecified',
                                         'Phosphorus pentoxide',
                                         'Phosphorus',
                                         'Phosphorus, total']

        if self.regio_bio:
            if self.name_regionalized_biosphere_database not in bw2.databases:
                self.logger.info("Creating regionalized biosphere flows...")
                self.create_regionalized_biosphere_flows()
            if 'IMPACT World+ Damage 2.0.1_regionalized' not in [i[0] for i in list(bw2.methods)]:
                self.logger.info("Importing regionalized LCIA method...")
                self.importing_impact_world_plus()
            if self.name_ei_with_regionalized_biosphere not in bw2.databases:
                self.logger.info("Extracting ecoinvent to wurst...")
                self.ei_wurst = wurst.extract_brightway2_databases(self.ecoinvent_database_name, add_identifiers=True)
                self.ei_in_dict = {(i['reference product'], i['location'], i['name']): i for i in self.ei_wurst}
                self.create_ecoinvent_with_regionalized_biosphere_flows()
            elif self.name_ei_with_regionalized_biosphere in bw2.databases:
                self.logger.info("Extracting ecoinvent to wurst...")
                self.ei_wurst = wurst.extract_brightway2_databases(self.name_ei_with_regionalized_biosphere,
                                                                   add_identifiers=True)
        else:
            if self.name_ei_with_regionalized_biosphere not in bw2.databases:
                self.logger.info("Extracting ecoinvent to wurst...")
                self.ei_wurst = wurst.extract_brightway2_databases(self.ecoinvent_database_name, add_identifiers=True)
                self.create_ecoinvent_copy_without_regionalized_biosphere_flows()
            elif self.name_ei_with_regionalized_biosphere in bw2.databases:
                self.logger.info("Extracting ecoinvent to wurst...")
                self.ei_wurst = wurst.extract_brightway2_databases(self.name_ei_with_regionalized_biosphere,
                                                                   add_identifiers=True)

        # as a dictionary to speed things up later
        self.ei_in_dict = {(i['reference product'], i['location'], i['name']): i for i in self.ei_wurst}

    def create_regionalized_biosphere_flows(self):
        """
        Function creates a regionalized version of the biosphere3 database of a brightway2 project
        """

        with open(pkg_resources.resource_filename(__name__, '/Data/regionalized_biosphere_database.pickle'), 'rb') as f:
            regionalized_biosphere = pickle.load(f)

        bw2.Database(self.name_regionalized_biosphere_database).write(regionalized_biosphere)

    def importing_impact_world_plus(self):
        """
        Function loads the IMPACT World+ LCIA methodology, the fully regionalized version
        """

        bw2.BW2Package.import_file(pkg_resources.resource_filename(
            __name__, '/Data/impact_world_plus_201_regionalized.ea4b5ad0766f4bb60242724bd3ee92ec.bw2package'))

    def create_ecoinvent_with_regionalized_biosphere_flows(self):
        """
        Function regionalizes the water biosphere flows of the ecoinvent database
        :return: self.new_regionalized_ecoinvent_database_name
        """

        self.logger.info("Regionalizing the biosphere inputs of the original ecoinvent database...")

        base_regionalized_flows = set([', '.join(i.as_dict()['name'].split(', ')[:-1]) for i in
                                       bw2.Database(self.name_regionalized_biosphere_database)])
        regionalized_flows = {(i.as_dict()['name'], i.as_dict()['categories']): i.as_dict()['code'] for i in
                              bw2.Database(self.name_regionalized_biosphere_database)}
        ei_iw_geo_mapping = pd.read_excel(pkg_resources.resource_filename(
            __name__, '/Data/ei_iw_geo_mapping.xlsx')).fillna('NA').set_index('ecoinvent')

        self.fix_ecoinvent_water()

        for process in self.ei_wurst:
            process['database'] = self.name_ei_with_regionalized_biosphere
            for exc in process['exchanges']:
                if exc['type'] == 'biosphere':
                    if exc['name'] in base_regionalized_flows:
                        if 'Water' in exc['name']:
                            if (exc['name'] + ', ' + ei_iw_geo_mapping.loc[process['location'], 'water'],
                                exc['categories']) in regionalized_flows.keys():
                                exc['code'] = regionalized_flows[(
                                exc['name'] + ', ' + ei_iw_geo_mapping.loc[process['location'], 'water'],
                                exc['categories'])]
                                exc['database'] = self.name_regionalized_biosphere_database
                                exc['name'] = exc['name'] + ', ' + ei_iw_geo_mapping.loc[process['location'], 'water']
                                exc['input'] = (exc['database'], exc['code'])
                        elif 'Occupation' in exc['name'] or 'Transformation' in exc['name']:
                            if (exc['name'] + ', ' + ei_iw_geo_mapping.loc[process['location'], 'land'],
                                exc['categories']) in regionalized_flows.keys():
                                exc['code'] = regionalized_flows[(
                                exc['name'] + ', ' + ei_iw_geo_mapping.loc[process['location'], 'land'],
                                exc['categories'])]
                                exc['database'] = self.name_regionalized_biosphere_database
                                exc['name'] = exc['name'] + ', ' + ei_iw_geo_mapping.loc[process['location'], 'land']
                                exc['input'] = (exc['database'], exc['code'])
                            elif (exc['name'] + ', GLO', exc['categories']) in regionalized_flows.keys():
                                exc['code'] = regionalized_flows[(exc['name'] + ', GLO', exc['categories'])]
                                exc['database'] = self.name_regionalized_biosphere_database
                                exc['name'] = exc['name'] + ', GLO'
                                exc['input'] = (exc['database'], exc['code'])
                        elif exc['name'] in ['BOD5, Biological Oxygen Demand', 'COD, Chemical Oxygen Demand',
                                             'Phosphoric acid', 'Phosphorus']:
                            if (exc['name'] + ', ' + ei_iw_geo_mapping.loc[process['location'], 'eutro'],
                                exc['categories']) in regionalized_flows.keys():
                                exc['code'] = regionalized_flows[(
                                exc['name'] + ', ' + ei_iw_geo_mapping.loc[process['location'], 'eutro'],
                                exc['categories'])]
                                exc['database'] = self.name_regionalized_biosphere_database
                                exc['name'] = exc['name'] + ', ' + ei_iw_geo_mapping.loc[process['location'], 'eutro']
                                exc['input'] = (exc['database'], exc['code'])
                        else:
                            if (exc['name'] + ', ' + ei_iw_geo_mapping.loc[process['location'], 'acid'],
                                exc['categories']) in regionalized_flows.keys():
                                exc['code'] = regionalized_flows[(
                                exc['name'] + ', ' + ei_iw_geo_mapping.loc[process['location'], 'acid'],
                                exc['categories'])]
                                exc['database'] = self.name_regionalized_biosphere_database
                                exc['name'] = exc['name'] + ', ' + ei_iw_geo_mapping.loc[process['location'], 'acid']
                                exc['input'] = (exc['database'], exc['code'])
                else:
                    exc['database'] = self.name_ei_with_regionalized_biosphere

        # add input key to each exchange
        for pr in self.ei_wurst:
            for exc in pr['exchanges']:
                try:
                    exc['input']
                except KeyError:
                    exc['input'] = (exc['database'], exc['code'])

        # modify structure of data from wurst to bw2
        self.ei_regio_data = {(i['database'], i['code']): i for i in self.ei_wurst}

        # recreate inputs in edges (exchanges)
        for pr in self.ei_regio_data:
            for exc in self.ei_regio_data[pr]['exchanges']:
                try:
                    exc['input']
                except KeyError:
                    exc['input'] = (exc['database'], exc['code'])
        # wurst creates empty categories for activities, this creates an issue when you try to write the bw2 database
        for pr in self.ei_regio_data:
            try:
                del self.ei_regio_data[pr]['categories']
            except KeyError:
                pass
        # same with parameters
        for pr in self.ei_regio_data:
            try:
                del self.ei_regio_data[pr]['parameters']
            except KeyError:
                pass

        # write ecoinvent-regionalized database
        bw2.Database(self.name_ei_with_regionalized_biosphere).write(self.ei_regio_data)

    def create_ecoinvent_copy_without_regionalized_biosphere_flows(self):
        """
        In case the user does not want to regionalize biosphere flows, we still need a copy of ecoinvent to be able to
        regionalize it later on. The goal is to always keep a "pristine" ecoinvent version.
        """

        # change the database name everywhere
        for pr in self.ei_wurst:
            pr['database'] = self.name_ei_with_regionalized_biosphere
            for exc in pr['exchanges']:
                if exc['type'] in ['technosphere', 'production']:
                    exc['input'] = (self.name_ei_with_regionalized_biosphere, exc['code'])
                    exc['database'] = self.name_ei_with_regionalized_biosphere

        # add input key to each exchange
        for pr in self.ei_wurst:
            for exc in pr['exchanges']:
                try:
                    exc['input']
                except KeyError:
                    exc['input'] = (exc['database'], exc['code'])

        # modify structure of data from wurst to bw2
        self.ei_regio_data = {(i['database'], i['code']): i for i in self.ei_wurst}

        # recreate inputs in edges (exchanges)
        for pr in self.ei_regio_data:
            for exc in self.ei_regio_data[pr]['exchanges']:
                try:
                    exc['input']
                except KeyError:
                    exc['input'] = (exc['database'], exc['code'])
        # wurst creates empty categories for activities, this creates an issue when you try to write the bw2 database
        for pr in self.ei_regio_data:
            try:
                del self.ei_regio_data[pr]['categories']
            except KeyError:
                pass
        # same with parameters
        for pr in self.ei_regio_data:
            try:
                del self.ei_regio_data[pr]['parameters']
            except KeyError:
                pass

        # write ecoinvent-regionalized database
        bw2.Database(self.name_ei_with_regionalized_biosphere).write(self.ei_regio_data)

    def format_export_data(self):
        """
        Function extracts and formats the export data from the trade database
        :return: self.export_data
        """

        self.logger.info("Extracting and formatting export data from UN COMTRADE...")

        self.export_data = pd.read_sql('SELECT * FROM "Export_data"', con=self.trade_conn)
        # only keep total export values (no need for destination detail)
        self.export_data = self.export_data[self.export_data.partnerISO == 'W00']
        # check if AtlQty is defined whenever Qty is empty. If it is, then use that value.
        self.export_data.loc[self.export_data.qty == 0, 'qty'] = self.export_data.loc[self.export_data.qty == 0, 'altQty']
        # don't need AtlQty afterwards and drop zero values
        self.export_data = self.export_data.drop('altQty', axis=1)
        self.export_data = self.export_data.loc[self.export_data.qty != 0]
        # DEAL WITH UNITS IN TRADE DATA
        # remove mistakes in units from data reporter
        self.export_data = self.export_data.drop([i for i in self.export_data.index if (
                (self.export_data.loc[i, 'cmdCode'] == '7005' and self.export_data.loc[i, 'qtyUnitAbbr'] == 'kg') or
                (self.export_data.loc[i, 'cmdCode'] == '850720' and self.export_data.loc[i, 'qtyUnitAbbr'] in ['kg','ce/el']) or
                (self.export_data.loc[i, 'cmdCode'] == '280421' and self.export_data.loc[i, 'qtyUnitAbbr'] == 'u') or
                (self.export_data.loc[i, 'cmdCode'] == '280440' and self.export_data.loc[i, 'qtyUnitAbbr'] == 'u'))])
        # convert data in different units
        self.export_data.loc[[i for i in self.export_data.index if self.export_data.loc[i, 'cmdCode'] == '2804' and self.export_data.loc[
            i, 'qtyUnitAbbr'] == 'kg'], 'qty'] /= 0.08375  # kg/m3 density of hydrogen
        self.export_data.loc[[i for i in self.export_data.index if self.export_data.loc[i, 'cmdCode'] == '280421' and self.export_data.loc[
            i, 'qtyUnitAbbr'] == 'kg'], 'qty'] /= 1.784  # kg/m3 density of argon
        self.export_data.loc[[i for i in self.export_data.index if self.export_data.loc[i, 'cmdCode'] == '280429' and self.export_data.loc[
            i, 'qtyUnitAbbr'] == 'kg'], 'qty'] /= 0.166  # kg/m3 density of helium
        self.export_data.loc[[i for i in self.export_data.index if self.export_data.loc[i, 'cmdCode'] == '4412' and self.export_data.loc[
            i, 'qtyUnitAbbr'] == 'kg'], 'qty'] /= 700  # kg/m3 density of wood
        # change unit name
        self.export_data.loc[[i for i in self.export_data.index if self.export_data.loc[i, 'cmdCode'] == '2804' and self.export_data.loc[
            i, 'qtyUnitAbbr'] == 'kg'], 'qtyUnitAbbr'] = 'm³'
        self.export_data.loc[[i for i in self.export_data.index if self.export_data.loc[i, 'cmdCode'] == '280421' and self.export_data.loc[
            i, 'qtyUnitAbbr'] == 'kg'], 'qtyUnitAbbr'] = 'm³'
        self.export_data.loc[[i for i in self.export_data.index if self.export_data.loc[i, 'cmdCode'] == '280429' and self.export_data.loc[
            i, 'qtyUnitAbbr'] == 'kg'], 'qtyUnitAbbr'] = 'm³'
        self.export_data.loc[[i for i in self.export_data.index if self.export_data.loc[i, 'cmdCode'] == '4412' and self.export_data.loc[
            i, 'qtyUnitAbbr'] == 'kg'], 'qtyUnitAbbr'] = 'm³'
        # check there are no other instances of data in multiple units
        data_in_kg = set(self.export_data.loc[self.export_data.qtyUnitAbbr == 'kg'].cmdCode)
        data_in_L = set(self.export_data.loc[self.export_data.qtyUnitAbbr == 'l'].cmdCode)
        data_in_m2 = set(self.export_data.loc[self.export_data.qtyUnitAbbr == 'm²'].cmdCode)
        data_in_m3 = set(self.export_data.loc[self.export_data.qtyUnitAbbr == 'm³'].cmdCode)
        data_in_u = set(self.export_data.loc[self.export_data.qtyUnitAbbr == 'u'].cmdCode)
        assert not (data_in_kg & data_in_L & data_in_m2 & data_in_m3 & data_in_u)

    def estimate_domestic_production(self):
        """
        Function estimates domestic production data
        :return: self.domestic_data
        """

        self.logger.info("Estimating domestic production data...")

        domestic_prod = pd.read_sql('SELECT * FROM "Domestic production"', con=self.trade_conn)

        self.domestic_data = self.export_data.copy('deep')
        self.domestic_data.loc[:, 'COMTRADE_reporter'] = self.domestic_data.reporterISO.copy()
        # go from ISO3 codes to country codes for respective databases
        self.export_data.reporterISO = [self.convert_ecoinvent_geos[i] for i in self.export_data.reporterISO]
        self.domestic_data.reporterISO = [self.convert_ecoinvent_geos[i] for i in self.domestic_data.reporterISO]
        self.domestic_data.COMTRADE_reporter = [self.convert_exiobase_geos[i] for i in
                                                self.domestic_data.COMTRADE_reporter]

        self.domestic_data = self.domestic_data.merge(
            pd.DataFrame.from_dict(self.hs_class_to_exio, orient='index', columns=['cmdExio']).reset_index().rename(
                columns={'index': 'cmdCode'}))
        self.domestic_data = self.domestic_data.merge(domestic_prod, left_on=['COMTRADE_reporter', 'cmdExio'],
                                                      right_on=['country', 'commodity'], how='left')
        self.domestic_data.qty = (self.domestic_data.qty / (1 - self.domestic_data.loc[:, 'domestic use (%)'] / 100) *
                                  self.domestic_data.loc[:, 'domestic use (%)'] / 100)
        self.domestic_data.partnerISO = self.domestic_data.reporterISO

    def first_order_regionalization(self):
        """
        Function to regionalized the key inputs of each process: electricity, municipal solid waste and heat.
        :return: self.regioinvent_in_wurst with new regionalized processes
        """

        self.logger.info("Regionalizing main inputs of traded products of ecoinvent...")

        for product in tqdm(self.eco_to_hs_class, leave=True):
            cmd_export_data = self.export_data[self.export_data.cmdCode.isin([self.eco_to_hs_class[product]])].copy(
                'deep')
            # calculate the average export volume for each country
            cmd_export_data = cmd_export_data.groupby('reporterISO').agg({'qty': 'mean'})
            exporters = (cmd_export_data.qty / cmd_export_data.qty.sum()).sort_values(ascending=False)
            # only keep the countries representing XX% of global exports of the product and create a RoW from that
            limit = exporters.index.get_loc(exporters[exporters.cumsum() > self.cutoff].index[0]) + 1
            remainder = exporters.iloc[limit:].sum()
            exporters = exporters.iloc[:limit]
            if 'RoW' in exporters.index:
                exporters.loc['RoW'] += remainder
            else:
                exporters.loc['RoW'] = remainder

            # register created geographies for each product
            self.created_geographies[product] = [i for i in exporters.index]

            # identify the processes producing the product
            filter_processes = ws.get_many(self.ei_wurst, ws.equals("reference product", product),
                                           ws.exclude(ws.contains("name", "market for")),
                                           ws.exclude(ws.contains("name", "market group for")),
                                           ws.exclude(ws.contains("name", "generic market")),
                                           ws.exclude(ws.contains("name", "import from")))
            # there can be multiple technologies (i.e., activities) to product the same product
            available_geographies = []
            available_technologies = []
            for dataset in filter_processes:
                available_geographies.append(dataset['location'])
                available_technologies.append(dataset['name'])
            # extract each available geography processes of ecoinvent, per technology of production
            possibilities = {tech: [] for tech in available_technologies}
            for i, geo in enumerate(available_geographies):
                possibilities[available_technologies[i]].append(geo)

            # determine the market share of each technology
            self.transportation_modes[product] = {}
            self.distribution_technologies[product] = {tech: 0 for tech in available_technologies}
            market_processes = ws.get_many(self.ei_wurst,
                                           ws.equals('reference product', product),
                                           ws.either(ws.contains('name', 'market for'),
                                                     ws.contains('name', 'market group for')))
            number_of_markets = 0
            for ds in market_processes:
                number_of_markets += 1
                for exc in ds['exchanges']:
                    if exc['product'] == product:
                        if exc['name'] in possibilities.keys():
                            self.distribution_technologies[product][exc['name']] += exc['amount']
                    if 'transport' in exc['name'] and ('market for' in exc['name'] or 'market group for' in exc['name']):
                        self.transportation_modes[product][exc['code']] = exc['amount']
            # average the technology market share
            sum_ = sum(self.distribution_technologies[product].values())
            if sum_ != 0:
                self.distribution_technologies[product] = {k: v / sum_ for k, v in
                                                           self.distribution_technologies[product].items()}
            else:
                self.distribution_technologies[product] = {k: 1/len(self.distribution_technologies[product])
                                                           for k, v in self.distribution_technologies[product].items()}
            # average the transportation modes
            if number_of_markets > 1:
                self.transportation_modes[product] = {k: v / number_of_markets for k, v in
                                                      self.transportation_modes[product].items()}

            # create the global production market process within regioinvent
            global_market_activity = copy.deepcopy(dataset)

            # rename activity
            global_market_activity['name'] = f"""production market for {product}"""

            # add a comment
            global_market_activity['comment'] = f"""This process represents the global production market for {product}. The shares come from export data from the UN COMTRADE database. Data from UN COMTRADE is already in physical units. An average of the 5 last years of export trade available data is taken (in general from 2019 to 2023). Countries are taken until 95% of the global production amounts are covered, the rest of the data is aggregated in a RoW (Rest-of-the-World) region."""

            # location will be global (global market)
            global_market_activity['location'] = 'GLO'

            # new code needed
            global_market_activity['code'] = uuid.uuid4().hex

            # change database
            global_market_activity['database'] = self.regioinvent_database_name

            # reset exchanges with only the production exchange
            global_market_activity['exchanges'] = [{'amount': 1.0,
                                                    'type': 'production',
                                                    'product': global_market_activity['reference product'],
                                                    'name': global_market_activity['name'],
                                                    'unit': global_market_activity['unit'],
                                                    'location': global_market_activity['location'],
                                                    'database': self.regioinvent_database_name,
                                                    'code': global_market_activity['code'],
                                                    'input': (global_market_activity['database'],
                                                              global_market_activity['code']),
                                                    'output': (global_market_activity['database'],
                                                               global_market_activity['code'])}]
            self.unit[product] = global_market_activity['unit']

            def copy_process(product, activity, region, export_country):
                """
                Fonction that copies a process from ecoinvent
                :param product: [str] name of the reference product
                :param activity: [str] name of the activity
                :param region: [str] name of the location of the original ecoinvent process
                :param export_country: [str] name of the location of the created regioinvent process
                :return: a copied and modified process of ecoinvent
                """
                process = ws.get_one(self.ei_wurst,
                                     ws.equals("reference product", product),
                                     ws.equals("name", activity),
                                     ws.equals("location", region),
                                     ws.equals("database", self.name_ei_with_regionalized_biosphere),
                                     ws.exclude(ws.contains("name", "market for")),
                                     ws.exclude(ws.contains("name", "market group for")),
                                     ws.exclude(ws.contains("name", "generic market")),
                                     ws.exclude(ws.contains("name", "import from")))
                regio_process = copy.deepcopy(process)
                # change location
                regio_process['location'] = export_country
                # change code
                regio_process['code'] = uuid.uuid4().hex
                # change database
                regio_process['database'] = self.regioinvent_database_name
                # add comment
                regio_process['comment'] = f'This process is a regionalized adaptation of the following process of the ecoinvent database: {activity} | {product} | {region}. No amount values were modified in the regionalization process, only their origin.'
                # update production exchange. Should always be the first one that comes out
                regio_process['exchanges'][0]['code'] = regio_process['code']
                regio_process['exchanges'][0]['database'] = regio_process['database']
                regio_process['exchanges'][0]['location'] = regio_process['location']
                regio_process['exchanges'][0]['input'] = (regio_process['database'], regio_process['code'])
                # input the regionalized process into the global production market
                global_market_activity['exchanges'].append(
                    {"amount": exporters.loc[export_country] * self.distribution_technologies[product][activity],
                     "type": "technosphere",
                     "name": regio_process['name'],
                     "product": regio_process['reference product'],
                     "unit": regio_process['unit'],
                     "location": export_country,
                     "database": self.regioinvent_database_name,
                     "code": global_market_activity['code'],
                     "input": (regio_process['database'], regio_process['code']),
                     "output": (global_market_activity['database'],
                                global_market_activity['code'])})
                return regio_process

            # loop through technologies and exporters
            for technology in possibilities.keys():
                for exporter in exporters.index:
                    # reset regio_process
                    regio_process = None
                    # if the exporting country is available in the geographies of the ecoinvent production technologies
                    if exporter in possibilities[technology] and exporter not in ['RoW']:
                        regio_process = copy_process(product, technology, exporter, exporter)
                    # if a region associated with exporting country is available in the geographies of the ecoinvent production technologies
                    elif exporter in self.country_to_ecoinvent_regions:
                        for potential_region in self.country_to_ecoinvent_regions[exporter]:
                            if potential_region in possibilities[technology]:
                                regio_process = copy_process(product, technology, potential_region, exporter)
                    # otherwise, take either RoW, GLO or a random available geography
                    if not regio_process:
                        if 'RoW' in possibilities[technology]:
                            regio_process = copy_process(product, technology, 'RoW', exporter)
                        elif 'GLO' in possibilities[technology]:
                            regio_process = copy_process(product, technology, 'GLO', exporter)
                        else:
                            if possibilities[technology]:
                                # if no RoW/GLO processes, take the first available geography by default...
                                regio_process = copy_process(product, technology, possibilities[technology][0],
                                                             exporter)
                                self.assigned_random_geography.append([product, technology, exporter])

                    # for each input, we test the presence of said inputs and regionalize that input
                    if regio_process:
                        if self.test_input_presence(regio_process, 'electricity', extra='aluminium/electricity'):
                            regio_process = self.change_aluminium_electricity(regio_process, exporter)
                        elif self.test_input_presence(regio_process, 'electricity', extra='cobalt/electricity'):
                            regio_process = self.change_cobalt_electricity(regio_process)
                        elif self.test_input_presence(regio_process, 'electricity', extra='voltage'):
                            regio_process = self.change_electricity(regio_process, exporter)
                        if self.test_input_presence(regio_process, 'municipal solid waste'):
                            regio_process = self.change_waste(regio_process, exporter)
                        if self.test_input_presence(regio_process, 'heat, district or industrial, natural gas'):
                            regio_process = self.change_heat(regio_process, exporter,
                                                             'heat, district or industrial, natural gas')
                        if self.test_input_presence(regio_process,
                                                    'heat, district or industrial, other than natural gas'):
                            regio_process = self.change_heat(regio_process, exporter,
                                                             'heat, district or industrial, other than natural gas')
                        if self.test_input_presence(regio_process,
                                                    'heat, central or small-scale, other than natural gas'):
                            regio_process = self.change_heat(regio_process, exporter,
                                                             'heat, central or small-scale, other than natural gas')
                    # register the regionalized process within the database
                    if regio_process:
                        self.regioinvent_in_wurst.append(regio_process)

            # add transportation to production market
            for transportation_mode in self.transportation_modes[product]:
                global_market_activity['exchanges'].append({
                    'amount': self.transportation_modes[product][transportation_mode],
                    'type': 'technosphere',
                    'database': self.name_ei_with_regionalized_biosphere,
                    'code': transportation_mode,
                    'product': bw2.Database(self.name_ei_with_regionalized_biosphere).get(
                        transportation_mode).as_dict()['reference product'],
                    'input': (self.name_ei_with_regionalized_biosphere, transportation_mode)
                })
            # and register the production market
            self.regioinvent_in_wurst.append(global_market_activity)

    def format_import_data(self):
        """
        Function extracts and formats import data from the trade database
        :return: self.consumption_data
        """

        self.logger.info("Extracting and formatting import data from UN COMTRADE...")

        self.import_data = pd.read_sql('SELECT * FROM "Import_data"', con=self.trade_conn)
        # remove trade with "World"
        self.import_data = self.import_data.drop(self.import_data[self.import_data.partnerISO == 'W00'].index)
        # go from ISO3 codes to ISO2 codes
        self.import_data.reporterISO = [self.convert_ecoinvent_geos[i] for i in self.import_data.reporterISO]
        self.import_data.partnerISO = [self.convert_ecoinvent_geos[i] for i in self.import_data.partnerISO]
        # check if AtlQty is defined whenever Qty is empty. If it is, then use that value.
        self.import_data.loc[self.import_data.qty == 0, 'qty'] = self.import_data.loc[self.import_data.qty == 0, 'altQty']
        # don't need AtlQty afterwards and drop zero values
        self.import_data = self.import_data.drop('altQty', axis=1)
        self.import_data = self.import_data.loc[self.import_data.qty != 0]
        # convert data in different units
        self.import_data.loc[self.import_data.loc[self.import_data.loc[:, 'qtyUnitAbbr'] == 'kg'].loc[
                                 self.import_data.loc[:,
                                 'cmdCode'] == '2804'].index, 'qty'] /= 0.08375  # kg/m3 density of hydrogen
        self.import_data.loc[self.import_data.loc[self.import_data.loc[:, 'qtyUnitAbbr'] == 'kg'].loc[
                                 self.import_data.loc[:,
                                 'cmdCode'] == '280421'].index, 'qty'] /= 1.784  # kg/m3 density of argon
        self.import_data.loc[self.import_data.loc[self.import_data.loc[:, 'qtyUnitAbbr'] == 'kg'].loc[
                                 self.import_data.loc[:,
                                 'cmdCode'] == '4412'].index, 'qty'] /= 700  # kg/m3 density of wood
        # change unit name
        self.import_data.loc[self.import_data.loc[self.import_data.loc[:, 'qtyUnitAbbr'] == 'kg'].loc[
                                 self.import_data.loc[:, 'cmdCode'] == '2804'].index, 'qtyUnitAbbr'] = 'm³'
        self.import_data.loc[self.import_data.loc[self.import_data.loc[:, 'qtyUnitAbbr'] == 'kg'].loc[
                                 self.import_data.loc[:, 'cmdCode'] == '280421'].index, 'qtyUnitAbbr'] = 'm³'
        self.import_data.loc[self.import_data.loc[self.import_data.loc[:, 'qtyUnitAbbr'] == 'kg'].loc[
                                 self.import_data.loc[:, 'cmdCode'] == '4412'].index, 'qtyUnitAbbr'] = 'm³'
        # correct mistake for the trade of natural gas (271121) between Mexico and the US in 2021, 2022 and 2023
        self.import_data.loc[[i for i in self.import_data.index if self.import_data.loc[i, 'cmdCode'] == '271121' and
                              self.import_data.loc[i, 'reporterISO'] == 'MEX'
                              and self.import_data.loc[i, 'refYear'] in [2021, 2022, 2023]], 'qty'] *= 0.000712 # kg/L density of natural gas
        # check there are no other instances of data in multiple units
        data_in_kg = set(self.import_data.loc[self.import_data.qtyUnitAbbr == 'kg'].cmdCode)
        data_in_L = set(self.import_data.loc[self.import_data.qtyUnitAbbr == 'l'].cmdCode)
        data_in_m2 = set(self.import_data.loc[self.import_data.qtyUnitAbbr == 'm²'].cmdCode)
        data_in_m3 = set(self.import_data.loc[self.import_data.qtyUnitAbbr == 'm³'].cmdCode)
        data_in_u = set(self.import_data.loc[self.import_data.qtyUnitAbbr == 'u'].cmdCode)
        assert not (data_in_kg & data_in_L & data_in_m2 & data_in_m3 & data_in_u)

        # remove artefacts of domestic trade from international trade data
        self.import_data = self.import_data.drop(
            self.import_data.loc[self.import_data.loc[:, 'reporterISO'] == self.import_data.loc[:, 'partnerISO']].index)
        # concatenate import and domestic data
        self.consumption_data = pd.concat([self.import_data, self.domestic_data.loc[:, self.import_data.columns]])
        # get rid of infinite values
        self.consumption_data.qty = self.consumption_data.qty.replace(np.inf, 0.0)

        # save RAM
        del self.import_data
        del self.domestic_data

    def create_consumption_markets(self):
        """
        Function creating consumption markets for each regionalized process
        :return:  self.regioinvent_in_wurst with new regionalized processes
        """

        self.logger.info("Creating consumption markets...")

        self.regioinvent_in_dict = {tech: [] for tech in
                                    [(i['reference product'], i['location']) for i in self.regioinvent_in_wurst]}

        for process in self.regioinvent_in_wurst:
            self.regioinvent_in_dict[(process['reference product'], process['location'])].append(
                {process['name']: process})

        for product in tqdm(self.eco_to_hs_class, leave=True):
            cmd_consumption_data = self.consumption_data[
                self.consumption_data.cmdCode == self.eco_to_hs_class[product]].copy('deep')
            # calculate the average import volume for each country
            cmd_consumption_data = cmd_consumption_data.groupby(['reporterISO', 'partnerISO']).agg({'qty': 'mean'})
            # change to relative
            importers = (cmd_consumption_data.groupby(level=0).sum() /
                         cmd_consumption_data.sum().sum()).sort_values(by='qty', ascending=False)
            # only keep importers till the cut-off of total imports
            limit = importers.index.get_loc(importers[importers.cumsum() > self.cutoff].dropna().index[0]) + 1
            # aggregate the rest
            remainder = cmd_consumption_data.loc[importers.index[limit:]].groupby(level=1).sum()
            cmd_consumption_data = cmd_consumption_data.loc[importers.index[:limit]]
            # assign the aggregate to RoW
            cmd_consumption_data = pd.concat([cmd_consumption_data, pd.concat([remainder], keys=['RoW'])])
            cmd_consumption_data.index = pd.MultiIndex.from_tuples([i for i in cmd_consumption_data.index])
            cmd_consumption_data = cmd_consumption_data.sort_index()

            for importer in cmd_consumption_data.index.levels[0]:
                cmd_consumption_data.loc[importer, 'qty'] = (cmd_consumption_data.loc[importer, 'qty'] /
                                                             cmd_consumption_data.loc[importer, 'qty'].sum()).values
                # we need to add the aggregate to potentially already existing RoW exchanges
                cmd_consumption_data = pd.concat([cmd_consumption_data.drop('RoW', level=0),
                                pd.concat([cmd_consumption_data.loc['RoW'].groupby(level=0).sum()], keys=['RoW'])])
                cmd_consumption_data = cmd_consumption_data.fillna(0)

                new_import_data = {
                    'name': 'consumption market for ' + product,
                    'reference product': product,
                    'location': importer,
                    'type': 'process',
                    'unit': self.unit[product],
                    'code': uuid.uuid4().hex,
                    'comment': f'This process represents the consumption market of {product} in {importer}. The shares were determined based on two aspects. The imports of the product taken from the UN COMTRADE (average over the last 5 available years) as well as the domestic production estimate for the corresponding sector in the corresponding region of taken from the EXIOBASE database. Shares are considered until 95% of the imports+domestic consumption are covered. Residual imports are aggregated in a RoW (Rest-of-the-World) region.',
                    'database': self.regioinvent_database_name,
                    'exchanges': []
                }

                new_import_data['exchanges'].append({'amount': 1,
                                                     'type': 'production',
                                                     'input': (self.regioinvent_database_name,
                                                               new_import_data['code'])})
                available_trading_partners = self.created_geographies[product]

                for trading_partner in cmd_consumption_data.loc[importer].index:
                    if trading_partner in available_trading_partners:
                        for technology in self.distribution_technologies[product]:
                            code = [i for i in self.regioinvent_in_dict[(product, trading_partner)] if
                                    list(i.keys())[0] == technology][0][technology]['code']
                            share = self.distribution_technologies[product][technology]

                            new_import_data['exchanges'].append({
                                'amount': cmd_consumption_data.loc[(importer, trading_partner), 'qty'] * share,
                                'type': 'technosphere',
                                'input': (self.regioinvent_database_name, code),
                                'name': product
                            })
                    else:
                        for technology in self.distribution_technologies[product]:
                            code = [i for i in self.regioinvent_in_dict[(product, 'RoW')] if
                                     list(i.keys())[0] == technology][0][technology]['code']
                            share = self.distribution_technologies[product][technology]

                            new_import_data['exchanges'].append({
                                'amount': cmd_consumption_data.loc[(importer, trading_partner), 'qty'] * share,
                                'type': 'technosphere',
                                'input': (self.regioinvent_database_name, code),
                                'name': product
                            })
                # add transportation to consumption market
                for transportation_mode in self.transportation_modes[product]:
                    new_import_data['exchanges'].append({
                        'amount': self.transportation_modes[product][transportation_mode],
                        'type': 'technosphere',
                        'input': (self.name_ei_with_regionalized_biosphere, transportation_mode)
                    })

                # check for duplicate input codes with different values (coming from RoW)
                duplicates = [item for item, count in
                              collections.Counter([i['input'] for i in new_import_data['exchanges']]).items() if
                              count > 1]
                # aggregate duplicates in one flow
                for duplicate in duplicates:
                    total = sum([i['amount'] for i in new_import_data['exchanges'] if i['input'] == duplicate])
                    new_import_data['exchanges'] = [i for i in new_import_data['exchanges'] if
                                                    i['input'] != duplicate] + [
                                                       {'amount': total, 'name': product, 'type': 'technosphere',
                                                        'input': duplicate}]

                self.regioinvent_in_wurst.append(new_import_data)

    def second_order_regionalization(self):
        """
        Function that links newly created consumption markets to inputs of the different processes of the regionalized
        ecoinvent database.
        :return:  self.regioinvent_in_wurst with new regionalized processes
        """

        self.logger.info("Performing second order regionalization...")

        if self.regio_bio:
            with open(pkg_resources.resource_filename(__name__, '/Data/magic_plumbering_geographies.json'), 'r') as f:
                magic_plumbering_geographies = json.load(f)
            techno_water_flows = ['irrigation', 'water, deionised', 'water, ultrapure', 'water, decarbonised',
                                  'water, completely softened', 'tap water', 'wastewater, average',
                                  'wastewater, unpolluted']
            # connect to water flows created in regioinvent specifically
            for process in self.regioinvent_in_wurst:
                if 'consumption market' not in process['name'] and 'production market' not in process['name']:
                    for exc in process['exchanges']:
                        if exc['type'] == 'technosphere':
                            if exc['product'] in techno_water_flows:
                                if (not (exc['product'] == 'water, decarbonised' and
                                         exc['name'] == 'diethyl ether production') and not (
                                        exc['product'] == 'water, ultrapure' and process['location'] == 'CA-QC')):
                                    try:
                                        replace_process = self.ei_in_dict[
                                            (exc['product'], magic_plumbering_geographies[process['location']][1],
                                             exc['name'])]
                                    except KeyError:
                                        if exc['name'] == 'market for tap water':
                                            replace_process = self.ei_in_dict[(
                                                exc['product'], magic_plumbering_geographies[process['location']][1],
                                                'market group for tap water')]
                                        if exc['name'] == 'market group for tap water':
                                            replace_process = self.ei_in_dict[(
                                                exc['product'], magic_plumbering_geographies[process['location']][1],
                                                'market for tap water')]
                                    exc['code'] = replace_process['code']
                                    exc['name'] = replace_process['name']
                                    exc['product'] = replace_process['reference product']
                                    exc['input'] = (self.name_ei_with_regionalized_biosphere, exc['code'])

        consumption_markets_data = {(i['name'], i['location']): i for i in self.regioinvent_in_wurst if
                                    'consumption market' in i['name']}

        # connect processes together
        for process in self.regioinvent_in_wurst:
            if 'consumption market' not in process['name'] and 'production market' not in process['name']:
                for exc in process['exchanges']:
                    if exc['product'] in self.eco_to_hs_class.keys() and exc['type'] == 'technosphere':
                        exc['name'] = 'consumption market for ' + exc['product']
                        exc['location'] = process['location']
                        if ('consumption market for ' + exc['product'],
                            process['location']) in consumption_markets_data.keys():
                            exc['database'] = (consumption_markets_data[('consumption market for ' + exc['product'],
                                                                         process['location'])]['database'])
                            exc['code'] = (consumption_markets_data[('consumption market for ' + exc['product'],
                                                                     process['location'])]['code'])
                        else:
                            exc['database'] = (consumption_markets_data[('consumption market for ' +
                                                                         exc['product'], 'RoW')]['database'])
                            exc['code'] = (consumption_markets_data[('consumption market for ' +
                                                                     exc['product'], 'RoW')]['code'])
                        exc['input'] = (exc['database'], exc['code'])

        # aggregating duplicate inputs (e.g., multiple consumption markets RoW callouts)
        for process in self.regioinvent_in_wurst:
            for exc in process['exchanges']:
                try:
                    exc['input']
                except KeyError:
                    exc['input'] = (exc['database'], exc['code'])

            duplicates = [item for item, count in
                          collections.Counter([i['input'] for i in process['exchanges']]).items() if count > 1]

            for duplicate in duplicates:
                total = sum([i['amount'] for i in process['exchanges'] if i['input'] == duplicate])
                process['exchanges'] = [i for i in process['exchanges'] if
                                        i['input'] != duplicate] + [
                                           {'amount': total, 'type': 'technosphere', 'input': duplicate}]

    def regionalize_elem_flows(self):
        """
        Function regionalizes the elementary flows of the regioinvent processes to the location of process.
        """

        if not self.regio_bio:
            self.logger.warning("If you wish to regionalize elementary flows, you need to select the correct boolean "
                                "in the initialization of the class...")
            return

        self.logger.info("Regionalizing the elementary flows of the regioinvent database...")

        base_regionalized_flows = set([', '.join(i.as_dict()['name'].split(', ')[:-1]) for i in
                                       bw2.Database(self.name_regionalized_biosphere_database)])
        regionalized_flows = {(i.as_dict()['name'], i.as_dict()['categories']): i.as_dict()['code'] for i in
                              bw2.Database(self.name_regionalized_biosphere_database)}
        regio_iw_geo_mapping = pd.read_excel(pkg_resources.resource_filename(
            __name__, '/Data/regio_iw_geo_mapping.xlsx')).fillna('NA').set_index('regioinvent')

        for process in self.regioinvent_in_wurst:
            for exc in process['exchanges']:
                if exc['type'] == 'biosphere':
                    if ', '.join(exc['name'].split(', ')[:-1]) in base_regionalized_flows:
                        exc['name'] = ', '.join(exc['name'].split(', ')[:-1])
                        if 'Water' in exc['name']:
                            if (exc['name'] + ', ' + regio_iw_geo_mapping.loc[process['location'], 'water'],
                                exc['categories']) in regionalized_flows.keys():
                                exc['code'] = regionalized_flows[(
                                exc['name'] + ', ' + regio_iw_geo_mapping.loc[process['location'], 'water'],
                                exc['categories'])]
                                exc['database'] = self.name_regionalized_biosphere_database
                                exc['name'] = exc['name'] + ', ' + regio_iw_geo_mapping.loc[
                                    process['location'], 'water']
                                exc['input'] = (exc['database'], exc['code'])
                        elif 'Occupation' in exc['name'] or 'Transformation' in exc['name']:
                            if (exc['name'] + ', ' + regio_iw_geo_mapping.loc[process['location'], 'land'],
                                exc['categories']) in regionalized_flows.keys():
                                exc['code'] = regionalized_flows[(
                                exc['name'] + ', ' + regio_iw_geo_mapping.loc[process['location'], 'land'],
                                exc['categories'])]
                                exc['database'] = self.name_regionalized_biosphere_database
                                exc['name'] = exc['name'] + ', ' + regio_iw_geo_mapping.loc[process['location'], 'land']
                                exc['input'] = (exc['database'], exc['code'])
                            elif (exc['name'] + ', GLO', exc['categories']) in regionalized_flows.keys():
                                exc['code'] = regionalized_flows[(exc['name'] + ', GLO', exc['categories'])]
                                exc['database'] = self.name_regionalized_biosphere_database
                                exc['name'] = exc['name'] + ', GLO'
                                exc['input'] = (exc['database'], exc['code'])
                        elif exc['name'] in ['BOD5, Biological Oxygen Demand', 'COD, Chemical Oxygen Demand',
                                             'Phosphoric acid', 'Phosphorus']:
                            if (exc['name'] + ', ' + regio_iw_geo_mapping.loc[process['location'], 'eutro'],
                                exc['categories']) in regionalized_flows.keys():
                                exc['code'] = regionalized_flows[(
                                exc['name'] + ', ' + regio_iw_geo_mapping.loc[process['location'], 'eutro'],
                                exc['categories'])]
                                exc['database'] = self.name_regionalized_biosphere_database
                                exc['name'] = exc['name'] + ', ' + regio_iw_geo_mapping.loc[
                                    process['location'], 'eutro']
                                exc['input'] = (exc['database'], exc['code'])
                        else:
                            if (exc['name'] + ', ' + regio_iw_geo_mapping.loc[process['location'], 'acid'],
                                exc['categories']) in regionalized_flows.keys():
                                exc['code'] = regionalized_flows[(
                                exc['name'] + ', ' + regio_iw_geo_mapping.loc[process['location'], 'acid'],
                                exc['categories'])]
                                exc['database'] = self.name_regionalized_biosphere_database
                                exc['name'] = exc['name'] + ', ' + regio_iw_geo_mapping.loc[process['location'], 'acid']
                                exc['input'] = (exc['database'], exc['code'])

    def write_regioinvent_to_database(self):
        """
        Function write a dictionary of datasets to the brightway2 SQL database
        """

        # change regioinvent data from wurst to bw2 structure
        regioinvent_data = {(i['database'], i['code']): i for i in self.regioinvent_in_wurst}

        # recreate inputs in edges (exchanges)
        for pr in regioinvent_data:
            for exc in regioinvent_data[pr]['exchanges']:
                try:
                    exc['input']
                except KeyError:
                    exc['input'] = (exc['database'], exc['code'])
        # wurst creates empty categories for activities, this creates an issue when you try to write the bw2 database
        for pr in regioinvent_data:
            try:
                del regioinvent_data[pr]['categories']
            except KeyError:
                pass

        # write regioinvent database
        bw2.Database(self.regioinvent_database_name).write(regioinvent_data)

    def connect_ecoinvent_to_regioinvent(self):
        """
        Now that regioinvent exists, we can make ecoinvent use regioinvent processes to further deepen the
        regionalization. Only countries and sub-countries are connected to regioinvent, simply because in regioinvent
        we do not have consumption mixes for the different regions of ecoinvent (e.g., RER, RAS, etc.).
        However, Swiss processes are not affected, as ecoinvent was already tailored for the Swiss case.
        I am not sure regioinvent would bring more precision in that specific case.
        """

        self.logger.info("Connecting ecoinvent to regioinvent processes...")

        # notice that here we are directly manipulating (through bw2) the already-written ecoinvent database

        consumption_markets_data = {(i['name'], i['location']): i for i in self.regioinvent_in_wurst if
                                    'consumption market' in i['name']}

        # first we connect ecoinvent to consumtpion markets of regioinvent
        for process in bw2.Database(self.name_ei_with_regionalized_biosphere):
            location = None
            # for countries (e.g., CA)
            if process.as_dict()['location'] in self.country_to_ecoinvent_regions.keys():
                location = process.as_dict()['location']
            # for sub-countries (e.g., CA-QC)
            elif process.as_dict()['location'].split('-')[0] in self.country_to_ecoinvent_regions.keys():
                location = process.as_dict()['location'].split('-')[0]
            if location and location != 'CH':
                for exc in process.technosphere():
                    if exc.as_dict()['product'] in self.eco_to_hs_class.keys():
                        exc.as_dict()['name'] = 'consumption market for ' + exc.as_dict()['product']
                        exc.as_dict()['location'] = location
                        if ('consumption market for ' + exc.as_dict()['product'], location) in consumption_markets_data.keys():
                            exc.as_dict()['database'] = (consumption_markets_data[
                                ('consumption market for ' + exc.as_dict()['product'], location)]['database'])
                            exc.as_dict()['code'] = (consumption_markets_data[
                                ('consumption market for ' + exc.as_dict()['product'], location)]['code'])
                        else:
                            exc.as_dict()['database'] = (consumption_markets_data[
                                ('consumption market for ' + exc.as_dict()['product'], 'RoW')]['database'])
                            exc.as_dict()['code'] = (consumption_markets_data[
                                ('consumption market for ' + exc.as_dict()['product'], 'RoW')]['code'])
                        exc.as_dict()['input'] = (exc.as_dict()['database'], exc.as_dict()['code'])
                        exc.save()

        # aggregating duplicate inputs (e.g., multiple consumption markets RoW callouts)
        for process in bw2.Database(self.name_ei_with_regionalized_biosphere):
            duplicates = [item for item, count in collections.Counter(
                [(i.as_dict()['input'], i.as_dict()['name'], i.as_dict()['product'],
                  i.as_dict()['location'], i.as_dict()['database'], i.as_dict()['code']) for i
                 in process.technosphere()]).items() if count > 1]

            for duplicate in duplicates:
                total = sum([i['amount'] for i in process.technosphere() if i['input'] == duplicate[0]])
                [i.delete() for i in process.technosphere() if i['input'] == duplicate[0]]
                new_exc = process.new_exchange(amount=total, type='technosphere', input=duplicate[0],
                                               name=duplicate[1], product=duplicate[2], location=duplicate[3],
                                               database=duplicate[4], code=duplicate[5])
                new_exc.save()

        # we also change production processes of ecoinvent for regionalized production processes of regioinvent
        regio_dict = {(i.as_dict()['reference product'], i.as_dict()['name'], i.as_dict()['location']): i for i in
                      bw2.Database(self.regioinvent_database_name)}

        for process in bw2.Database(self.name_ei_with_regionalized_biosphere):
            for exc in process.technosphere():
                if exc.as_dict()['product'] in self.eco_to_hs_class.keys():
                    # same thing, we don't touch Swiss processes
                    if exc.as_dict()['location'] not in ['RoW', 'CH']:
                        try:
                            exc.as_dict()['database'] = self.regioinvent_database_name
                            exc.as_dict()['code'] = regio_dict[
                                (exc.as_dict()['product'], exc.as_dict()['name'], exc.as_dict()['location'])].as_dict()[
                                'code']
                            exc.as_dict()['input'] = (exc.as_dict()['database'], exc.as_dict()['code'])
                        except KeyError:
                            pass

    # -------------------------------------------Supporting functions---------------------------------------------------

    def change_electricity(self, process, export_country):
        """
        This function changes an electricity input of a process by the national (or regional) electricity mix
        :param process: the copy of the regionalized process as a dictionnary
        :param export_country: the country of the newly regionalized process
        """
        # identify electricity related exchanges
        electricity_product_names = list(set([i['product'] for i in process['exchanges'] if
                                             'electricity' in i['name'] and 'aluminium' not in i['name'] and
                                              'cobalt' not in i['name'] and 'voltage' in i['name']]))
        for electricity_product_name in electricity_product_names:
            unit_name = list(set([i['unit'] for i in process['exchanges'] if 'electricity' in i['name'] and
                                  'aluminium' not in i['name'] and 'cobalt' not in i['name'] and 'voltage' in i['name']]))
            assert len(unit_name) == 1
            unit_name = unit_name[0]
            qty_of_electricity = sum(
                [i['amount'] for i in process['exchanges'] if electricity_product_name == i['product']])

            # remove electricity flows from non-appropriated geography
            for exc in process['exchanges'][:]:
                if (electricity_product_name == exc['product'] and 'aluminium' not in exc['name'] and
                        'cobalt' not in exc['name'] and 'voltage' in exc['name']):
                    process['exchanges'].remove(exc)

            electricity_region = None
            if export_country in self.electricity_geos:
                electricity_region = export_country
            elif export_country != 'RoW' and export_country in self.country_to_ecoinvent_regions and not electricity_region:
                for potential_region in self.country_to_ecoinvent_regions[export_country]:
                    if potential_region in self.electricity_geos:
                        electricity_region = potential_region
            if not electricity_region:
                electricity_region = 'GLO'

            if electricity_region in ['BR', 'CA', 'CN', 'GLO', 'IN', 'RAF', 'RAS', 'RER', 'RLA', 'RME', 'RNA', 'US']:
                electricity_activity_name = 'market group for ' + electricity_product_name
            else:
                electricity_activity_name = 'market for ' + electricity_product_name

            electricity_code = self.ei_in_dict[(electricity_product_name,
                                                electricity_region,
                                                electricity_activity_name)]['code']

            # create the regionalized flow for electricity
            process['exchanges'].append({"amount": qty_of_electricity,
                                           "product": electricity_product_name,
                                           "name": electricity_activity_name,
                                           "location": electricity_region,
                                           "unit": unit_name,
                                           "database": self.regioinvent_database_name,
                                           "type": "technosphere",
                                           "input": (self.name_ei_with_regionalized_biosphere, electricity_code),
                                           "output": (process['database'], process['code'])})

        return process

    def change_aluminium_electricity(self, process, export_country):
        """
        This function changes an electricity input of a process by the national (or regional) electricity mix
        specifically for aluminium electricity mixes
        :param process: the copy of the regionalized process as a dictionnary
        :param export_country: the country of the newly regionalized process
        """
        # identify electricity related exchanges
        electricity_product_names = list(set([i['product'] for i in process['exchanges'] if
                                              'electricity' in i['name'] and 'aluminium' in i['name'] and 'voltage' in i['name']]))
        for electricity_product_name in electricity_product_names:
            unit_name = list(set([i['unit'] for i in process['exchanges'] if
                                              'electricity' in i['name'] and 'aluminium' in i['name'] and 'voltage' in i['name']]))

            assert len(unit_name) == 1
            unit_name = unit_name[0]
            qty_of_electricity = sum(
                [i['amount'] for i in process['exchanges'] if electricity_product_name == i['product']])

            # remove electricity flows from non-appropriated geography
            for exc in process['exchanges'][:]:
                if electricity_product_name == exc['product'] and 'aluminium' in exc['name']:
                    process['exchanges'].remove(exc)

            electricity_region = None
            if export_country in self.electricity_aluminium_geos:
                electricity_region = export_country
            elif export_country != 'RoW' and export_country in self.country_to_ecoinvent_regions and not electricity_region:
                for potential_region in self.country_to_ecoinvent_regions[export_country]:
                    if potential_region in self.electricity_aluminium_geos:
                        electricity_region = potential_region
            if not electricity_region:
                electricity_region = 'RoW'

            electricity_activity_name = 'market for ' + electricity_product_name

            electricity_code = self.ei_in_dict[(electricity_product_name,
                                                electricity_region,
                                                electricity_activity_name)]['code']

            # create the regionalized flow for electricity
            process['exchanges'].append({"amount": qty_of_electricity,
                                           "product": electricity_product_name,
                                           "name": electricity_activity_name,
                                           "location": electricity_region,
                                           "unit": unit_name,
                                           "database": self.regioinvent_database_name,
                                           "type": "technosphere",
                                           "input": (self.name_ei_with_regionalized_biosphere, electricity_code),
                                           "output": (process['database'], process['code'])})

        return process

    def change_cobalt_electricity(self, process):
        """
        This function changes an electricity input of a process by the national (or regional) electricity mix
        specifically for the cobalt electricity mix
        :param process: the copy of the regionalized process as a dictionnary
        """
        # identify electricity related exchanges
        electricity_product_names = list(set([i['product'] for i in process['exchanges'] if
                                              'electricity' in i['name'] and 'cobalt' in i['name']]))
        for electricity_product_name in electricity_product_names:
            unit_name = list(set([i['unit'] for i in process['exchanges'] if 'electricity' in i['name'] and
                                  'cobalt' in i['name']]))

            assert len(unit_name) == 1
            unit_name = unit_name[0]
            qty_of_electricity = sum(
                [i['amount'] for i in process['exchanges'] if electricity_product_name == i['product']])

            # remove electricity flows from non-appropriated geography
            for exc in process['exchanges'][:]:
                if electricity_product_name == exc['product'] and 'cobalt' in exc['name']:
                    process['exchanges'].remove(exc)

            # GLO is the only geography available for electricity, cobalt industry
            electricity_region = 'GLO'

            electricity_activity_name = 'market for ' + electricity_product_name

            electricity_code = self.ei_in_dict[(electricity_product_name,
                                                electricity_region,
                                                electricity_activity_name)]['code']

            # create the regionalized flow for electricity
            process['exchanges'].append({"amount": qty_of_electricity,
                                           "product": electricity_product_name,
                                           "name": electricity_activity_name,
                                           "location": electricity_region,
                                           "unit": unit_name,
                                           "database": self.regioinvent_database_name,
                                           "type": "technosphere",
                                           "input": (self.name_ei_with_regionalized_biosphere, electricity_code),
                                           "output": (process['database'], process['code'])})

        return process

    def change_waste(self, process, export_country):
        """
        This function changes a municipal solid waste treatment input of a process by the national (or regional) mix
        :param process: the copy of the regionalized process as a dictionnary
        :param export_country: the country of the newly regionalized process
        """

        waste_product_name = 'municipal solid waste'
        unit_name = list(set([i['unit'] for i in process['exchanges'] if waste_product_name == i['product']]))
        # if fail -> huh. It should never fail
        assert len(unit_name) == 1
        unit_name = unit_name[0]
        qty_of_waste = sum([i['amount'] for i in process['exchanges'] if waste_product_name == i['product']])

        # remove waste flows from non-appropriated geography
        for exc in process['exchanges'][:]:
            if waste_product_name == exc['product']:
                process['exchanges'].remove(exc)

        if export_country in self.waste_geos:
            waste_region = export_country
        elif (export_country in self.country_to_ecoinvent_regions and
              self.country_to_ecoinvent_regions[export_country][0] == "RER"):
            waste_region = 'Europe without Switzerland'
        else:
            waste_region = 'RoW'

        if waste_region == 'Europe without Switzerland':
            waste_activity_name = 'market group for ' + waste_product_name
        else:
            waste_activity_name = 'market for ' + waste_product_name

        waste_code = self.ei_in_dict[(waste_product_name,
                                      waste_region,
                                      waste_activity_name)]['code']

        # create the regionalized flow for waste
        process['exchanges'].append({"amount": qty_of_waste,
                                           "product": waste_product_name,
                                           "name": waste_activity_name,
                                           "location": waste_region,
                                           "unit": unit_name,
                                           "database": self.regioinvent_database_name,
                                           "type": "technosphere",
                                           "input": (self.name_ei_with_regionalized_biosphere, waste_code),
                                           "output": (process['database'], process['code'])})

        return process

    def change_heat(self, process, export_country, heat_flow):
        """
        This function changes a heat input of a process by the national (or regional) mix
        :param process: the copy of the regionalized process as a dictionnary
        :param export_country: the country of the newly regionalized process
        :param heat_flow: the heat flow being regionalized (could be industrial, natural gas, or industrial other than
                          natural gas, or small-scale other than natural gas)
        """
        if heat_flow == 'heat, district or industrial, natural gas':
            heat_process_countries = self.heat_district_ng
        if heat_flow == 'heat, district or industrial, other than natural gas':
            heat_process_countries = self.heat_district_non_ng
        if heat_flow == 'heat, central or small-scale, other than natural gas':
            heat_process_countries = self.heat_small_scale_non_ng

        unit_name = list(set([i['unit'] for i in process['exchanges'] if heat_flow == i['product']]))
        assert len(unit_name) == 1
        unit_name = unit_name[0]
        qty_of_heat = sum(
            [i['amount'] for i in process['exchanges'] if heat_flow == i['product']])

        # remove heat flows from non-appropriated geography
        for exc in process['exchanges'][:]:
            if heat_flow == exc['product']:
                process['exchanges'].remove(exc)

        # determine qty of heat for national mix through its share of the regional mix (e.g., DE in RER market for heat)
        # CH is its own market
        if export_country == 'CH':
            region_heat = export_country
        elif (export_country in self.country_to_ecoinvent_regions and
              self.country_to_ecoinvent_regions[export_country][0] == 'RER'):
            region_heat = "Europe without Switzerland"
        else:
            region_heat = "RoW"

        # check if the country has a national production heat process, if not take the region or RoW
        if export_country not in heat_process_countries:
            if (export_country in self.country_to_ecoinvent_regions and
                    self.country_to_ecoinvent_regions[export_country][0] == 'RER'):
                export_country = "Europe without Switzerland"
            else:
                export_country = "RoW"

        # select region heat market process
        region_heat_process = ws.get_many(self.ei_wurst,
                                          ws.equals("reference product", heat_flow),
                                          ws.equals("location", region_heat),
                                          ws.equals("database", self.name_ei_with_regionalized_biosphere),
                                          ws.contains("name", "market for"))

        # countries with sub-region markets of heat require a special treatment
        if export_country in ['CA', 'US', 'CN', 'BR', 'IN']:
            region_heat_process = ws.get_many(self.ei_wurst,
                                              ws.equals("reference product", heat_flow),
                                              ws.equals("location", region_heat),
                                              ws.equals("database", self.name_ei_with_regionalized_biosphere),
                                              ws.either(ws.contains("name", "market for"),
                                                        ws.contains("name", "market group for")))

            # extracting amount of heat of country within region heat market process
            heat_exchanges = {}
            for ds in region_heat_process:
                for exc in ws.technosphere(ds, *[ws.equals("product", heat_flow),
                                                 ws.contains("location", export_country)]):
                    heat_exchanges[exc['name'], exc['location']] = exc['amount']

            # special case for some Quebec heat flows
            if export_country == 'CA' and heat_flow != 'heat, central or small-scale, other than natural gas':
                global_heat_process = ws.get_one(self.ei_wurst,
                                                 ws.equals("reference product", heat_flow),
                                                 ws.equals("location", 'GLO'),
                                                 ws.equals("database", self.name_ei_with_regionalized_biosphere),
                                                 ws.either(ws.contains("name", "market for"),
                                                           ws.contains("name", "market group for")))

                heat_exchanges = {k: v * [i['amount'] for i in global_heat_process['exchanges'] if
                                          i['location'] == 'RoW'][0] for k, v in heat_exchanges.items()}
                heat_exchanges[
                    ([i for i in global_heat_process['exchanges'] if i['location'] == 'CA-QC'][0]['name'], 'CA-QC')] = (
                    [i for i in global_heat_process['exchanges'] if i['location'] == 'CA-QC'][0]['amount'])
            # make it relative amounts
            heat_exchanges = {k: v / sum(heat_exchanges.values()) for k, v in heat_exchanges.items()}
            # scale the relative amount to the qty of heat of process
            heat_exchanges = {k: v * qty_of_heat for k, v in heat_exchanges.items()}

            # add regionalized exchange of heat
            for heat_exc in heat_exchanges.keys():
                process['exchanges'].append({"amount": heat_exchanges[heat_exc],
                                             "product": heat_flow,
                                             "name": heat_exc[0],
                                             "location": heat_exc[1],
                                             "unit": unit_name,
                                             "database": self.regioinvent_database_name,
                                             "type": "technosphere",
                                             "input": (
                                             self.ei_in_dict[(heat_flow, heat_exc[1], heat_exc[0])]['database'],
                                             self.ei_in_dict[(heat_flow, heat_exc[1], heat_exc[0])]['code']),
                                             "output": (process['database'], process['code'])})

        else:
            # extracting amount of heat of country within region heat market process
            heat_exchanges = {}
            for ds in region_heat_process:
                for exc in ws.technosphere(ds, *[ws.equals("product", heat_flow),
                                                 ws.equals("location", export_country)]):
                    heat_exchanges[exc['name']] = exc['amount']
            # make it relative amounts
            heat_exchanges = {k: v / sum(heat_exchanges.values()) for k, v in heat_exchanges.items()}
            # scale the relative amount to the qty of heat of process
            heat_exchanges = {k: v * qty_of_heat for k, v in heat_exchanges.items()}

            # add regionalized exchange of heat
            for heat_exc in heat_exchanges.keys():

                process['exchanges'].append({"amount": heat_exchanges[heat_exc],
                                               "product": heat_flow,
                                               "name": heat_exc,
                                               "location": export_country,
                                               "unit": unit_name,
                                               "database": self.regioinvent_database_name,
                                               "type": "technosphere",
                                               "input": (self.ei_in_dict[(heat_flow, export_country, heat_exc)]['database'],
                                                         self.ei_in_dict[(heat_flow, export_country, heat_exc)]['code']),
                                               "output": (process['database'], process['code'])})

        return process

    def test_input_presence(self, process, input_name, extra=None):
        """
        Function that checks if an input is present in a given process
        :param process: The process to check whether the input is in it or not
        :param input_name: The name of the input to check
        :param extra: Extra information to look for very specific inputs
        :return: a boolean of whether the input is present or not
        """
        if extra == 'aluminium/electricity':
            for exc in ws.technosphere(process, ws.contains("name", input_name), ws.contains("name", "aluminium")):
                return True
        elif extra == 'cobalt/electricity':
            for exc in ws.technosphere(process, ws.contains("name", input_name), ws.contains("name", "cobalt")):
                return True
        elif extra == 'voltage':
            for exc in ws.technosphere(process, ws.contains("name", input_name), ws.contains("name", "voltage")):
                return True
        else:
            for exc in ws.technosphere(process, ws.contains("name", input_name)):
                return True

    def fix_ecoinvent_water(self):
        """
        This function corrects inconsistencies of ecoinvent processes with water flows by creating required regionalized
        processes for the following water processes: 'irrigation', 'water, deionised', 'water, ultrapure',
        'water, decarbonised', 'water, completely softened', 'tap water', 'wastewater, average', 'wastewater, unpolluted'.
        If you are unfamiliar with the issue, here is an example: an apple produced in Chile within ecoinvent 3.9.1
        requires irrigation  from the RoW. There is therefore an input of, e.g., Indian water (from the RoW irrigation)
        within the culture of apples in Chile, which creates incoherence in the result of water scarcity footprint methods
        such as AWARE.
        :return:
        """

        with open(pkg_resources.resource_filename(__name__, '/Data/magic_plumbering_geographies.json'), 'r') as f:
            magic_plumbering_geographies = json.load(f)

        # first, let's identify which processes need to be created
        techno_water_flows = ['irrigation', 'water, deionised', 'water, ultrapure', 'water, decarbonised',
                              'water, completely softened', 'tap water', 'wastewater, average', 'wastewater, unpolluted']

        # here we get all regions of processes using the identified technosphere water flows
        used_regions = list(magic_plumbering_geographies.keys())

        already_existing = {i: [] for i in techno_water_flows}
        # now we check if some of those already exist in ecoinvent
        for techno in techno_water_flows:
            for region in used_regions:
                if (techno, region, 'market for ' + techno) in self.ei_in_dict.keys() or (
                        techno, region, 'market group for ' + techno) in self.ei_in_dict.keys():
                    already_existing[techno].append(region)

        already_existing = {k: set(already_existing[k]) for k, v in already_existing.items()}

        # remove sub-national flows (does not work with IW+ v2.0.1)
        used_regions = [i.split('-')[0] if i.split('-')[0] in ['CA', 'BR', 'CN', 'IN', 'US'] else i for i in used_regions]
        # remove regions for which no regionalized water flows exist
        used_regions = [magic_plumbering_geographies[i][1] for i in used_regions]
        used_regions = set(used_regions)

        # copying relevant market process
        for techno in techno_water_flows:
            for region in used_regions:
                if region not in already_existing[techno]:
                    if magic_plumbering_geographies[region] == 'RoW':
                        market_ds = copy.deepcopy(
                            self.ei_in_dict[(techno, magic_plumbering_geographies[region][0], 'market for ' + techno)])
                    # for European countries, multiple possibilities
                    else:
                        # the RER location could be defined for the techno_water_flow
                        try:
                            market_ds = copy.deepcopy(
                                self.ei_in_dict[(techno, magic_plumbering_geographies[region][0], 'market for ' + techno)])
                        except KeyError:
                            # if not we check for Europe without Switzerland
                            try:
                                market_ds = copy.deepcopy(
                                    self.ei_in_dict[(techno, 'Europe without Switzerland', 'market for ' + techno)])
                            # and if nothing works we take RoW
                            except KeyError:
                                market_ds = copy.deepcopy(self.ei_in_dict[(techno, 'RoW', 'market for ' + techno)])

                    # adapt metadata to newly created regionalized process
                    name = market_ds['name']
                    product = market_ds['reference product']
                    original_region = market_ds['location']
                    market_ds[
                        'comment'] = f'This process is a regionalized adaptation of the following process of the ecoinvent database: {name} | {product} | {original_region}. No amount values were modified in the regionalization process, only their origin.'
                    market_ds['location'] = region
                    market_ds['code'] = uuid.uuid4().hex
                    market_ds['database'] = self.name_ei_with_regionalized_biosphere
                    market_ds['input'] = (market_ds['database'], market_ds['code'])
                    market_ds['exchanges'][0]['code'] = market_ds['code']
                    market_ds['exchanges'][0]['database'] = self.name_ei_with_regionalized_biosphere
                    market_ds['exchanges'][0]['location'] = market_ds['location']
                    market_ds['exchanges'][0]['input'] = (self.name_ei_with_regionalized_biosphere, market_ds['code'])

                    self.ei_wurst.append(market_ds)

        # copy relevant production processes
        for techno in techno_water_flows:
            if techno in ['water, deionised', 'water, ultrapure', 'water, decarbonised', 'water, completely softened']:
                for region in used_regions:
                    if region not in already_existing[techno]:
                        if magic_plumbering_geographies[region] == 'RoW':
                            production_ds = copy.deepcopy(self.ei_in_dict[(
                            techno, magic_plumbering_geographies[region][0], techno.replace('water', 'water production'))])
                        # for European countries, multiple possibilities
                        else:
                            # the RER location could be defined for the techno_water_flow
                            try:
                                production_ds = copy.deepcopy(self.ei_in_dict[(
                                techno, magic_plumbering_geographies[region][0],
                                techno.replace('water', 'water production'))])
                            except KeyError:
                                # if not we check for Europe without Switzerland
                                try:
                                    production_ds = copy.deepcopy(self.ei_in_dict[(
                                    techno, 'Europe without Switzerland', techno.replace('water', 'water production'))])
                                # and if nothing works we take RoW
                                except KeyError:
                                    production_ds = copy.deepcopy(
                                        self.ei_in_dict[(techno, 'RoW', techno.replace('water', 'water production'))])

                        # adapt metadata to newly created regionalized process
                        name = production_ds['name']
                        product = production_ds['reference product']
                        original_region = production_ds['location']
                        production_ds[
                            'comment'] = f'This process is a regionalized adaptation of the following process of the ecoinvent database: {name} | {product} | {original_region}. No amount values were modified in the regionalization process, only their origin.'
                        production_ds['location'] = region
                        production_ds['code'] = uuid.uuid4().hex
                        production_ds['database'] = self.name_ei_with_regionalized_biosphere
                        production_ds['input'] = (production_ds['database'], production_ds['code'])
                        production_ds['exchanges'][0]['code'] = production_ds['code']
                        production_ds['exchanges'][0]['database'] = self.name_ei_with_regionalized_biosphere
                        production_ds['exchanges'][0]['location'] = production_ds['location']
                        production_ds['exchanges'][0]['input'] = (
                            self.name_ei_with_regionalized_biosphere, production_ds['code'])
                        self.ei_wurst.append(production_ds)

            if techno in ['wastewater, average', 'wastewater, unpolluted']:
                for region in used_regions:
                    if region not in already_existing[techno]:
                        if magic_plumbering_geographies[region] == 'RoW':
                            production_ds = copy.deepcopy(
                                self.ei_in_dict[(techno, 'RoW', 'treatment of ' + techno + ', wastewater treatment')])
                        else:
                            try:
                                production_ds = copy.deepcopy(self.ei_in_dict[(
                                    techno, 'Europe without Switzerland', 'treatment of ' + techno + ', wastewater treatment')])
                            except KeyError:
                                production_ds = copy.deepcopy(
                                    self.ei_in_dict[(techno, 'RoW', 'treatment of ' + techno + ', wastewater treatment')])

                        # adapt metadata to newly created regionalized process
                        name = production_ds['name']
                        product = production_ds['reference product']
                        original_region = production_ds['location']
                        production_ds['comment'] = f'This process is a regionalized adaptation of the following process of the ecoinvent database: {name} | {product} | {original_region}. No amount values were modified in the regionalization process, only their origin.'
                        production_ds['location'] = region
                        production_ds['code'] = uuid.uuid4().hex
                        production_ds['database'] = self.name_ei_with_regionalized_biosphere
                        production_ds['input'] = (production_ds['database'], production_ds['code'])
                        production_ds['exchanges'][0]['code'] = production_ds['code']
                        production_ds['exchanges'][0]['database'] = self.name_ei_with_regionalized_biosphere
                        production_ds['exchanges'][0]['location'] = production_ds['location']
                        production_ds['exchanges'][0]['input'] = (
                            self.name_ei_with_regionalized_biosphere, production_ds['code'])
                        self.ei_wurst.append(production_ds)

            if techno == 'irrigation':
                for region in used_regions:
                    if region not in already_existing[techno]:
                        for irrigation_type in ['irrigation, drip', 'irrigation, surface', 'irrigation, sprinkler']:
                            if magic_plumbering_geographies[region] == 'RoW':
                                production_ds = copy.deepcopy(
                                    self.ei_in_dict[(techno, magic_plumbering_geographies[region][0], irrigation_type)])
                            # for European countries, multiple possibilities
                            else:
                                # the RER location could be defined for the techno_water_flow
                                try:
                                    production_ds = copy.deepcopy(
                                        self.ei_in_dict[(techno, magic_plumbering_geographies[region][0], irrigation_type)])
                                except KeyError:
                                    # if not we check for Europe without Switzerland
                                    try:
                                        production_ds = copy.deepcopy(
                                            self.ei_in_dict[(techno, 'Europe without Switzerland', irrigation_type)])
                                    # and if nothing works we take RoW
                                    except KeyError:
                                        production_ds = copy.deepcopy(self.ei_in_dict[(techno, 'RoW', irrigation_type)])

                            # adapt metadata to newly created regionalized process
                            name = production_ds['name']
                            product = production_ds['reference product']
                            original_region = production_ds['location']
                            production_ds['comment'] = f'This process is a regionalized adaptation of the following process of the ecoinvent database: {name} | {product} | {original_region}. No amount values were modified in the regionalization process, only their origin.'
                            production_ds['location'] = region
                            production_ds['code'] = uuid.uuid4().hex
                            production_ds['database'] = self.name_ei_with_regionalized_biosphere
                            production_ds['input'] = (production_ds['database'], production_ds['code'])
                            production_ds['exchanges'][0]['code'] = production_ds['code']
                            production_ds['exchanges'][0]['database'] = self.name_ei_with_regionalized_biosphere
                            production_ds['exchanges'][0]['location'] = production_ds['location']
                            production_ds['exchanges'][0]['input'] = (
                                self.name_ei_with_regionalized_biosphere, production_ds['code'])

                            self.ei_wurst.append(production_ds)

            if techno == 'tap water':
                for region in used_regions:
                    if region not in already_existing[techno]:
                        for tap_water_techno in ['tap water production, artificial recharged wells',
                                                 'tap water production, conventional treatment',
                                                 'tap water production, conventional with biological treatment',
                                                 'tap water production, direct filtration treatment',
                                                 'tap water production, microstrainer treatment',
                                                 'tap water production, ultrafiltration treatment',
                                                 'tap water production, seawater reverse osmosis, conventional pretreatment, baseline module, single stage',
                                                 'tap water production, seawater reverse osmosis, conventional pretreatment, enhance module, single stage',
                                                 'tap water production, seawater reverse osmosis, conventional pretreatment, enhance module, two stages',
                                                 'tap water production, seawater reverse osmosis, ultrafiltration pretreatment, baseline module, single stage',
                                                 'tap water production, seawater reverse osmosis, ultrafiltration pretreatment, enhance module, single stage',
                                                 'tap water production, seawater reverse osmosis, ultrafiltration pretreatment, enhance module, two stages',
                                                 'tap water production, underground water with chemical treatment',
                                                 'tap water production, underground water with disinfection',
                                                 'tap water production, underground water without treatment']:
                            if magic_plumbering_geographies[region][0] == 'RoW':
                                if 'seawater reverse osmosis' in tap_water_techno:
                                    production_ds = copy.deepcopy(self.ei_in_dict[(techno, 'GLO', tap_water_techno)])
                                else:
                                    production_ds = copy.deepcopy(self.ei_in_dict[(techno, 'RoW', tap_water_techno)])
                            else:
                                if 'seawater reverse osmosis' not in tap_water_techno:
                                    try:
                                        production_ds = copy.deepcopy(
                                            self.ei_in_dict[(techno, 'Europe without Switzerland', tap_water_techno)])
                                    except KeyError:
                                        production_ds = copy.deepcopy(self.ei_in_dict[(techno, 'RoW', tap_water_techno)])

                            # adapt metadata to newly created regionalized process
                            name = production_ds['name']
                            product = production_ds['reference product']
                            original_region = production_ds['location']
                            production_ds['comment'] = f'This process is a regionalized adaptation of the following process of the ecoinvent database: {name} | {product} | {original_region}. No amount values were modified in the regionalization process, only their origin.'
                            production_ds['location'] = region
                            production_ds['code'] = uuid.uuid4().hex
                            production_ds['database'] = self.name_ei_with_regionalized_biosphere
                            production_ds['input'] = (production_ds['database'], production_ds['code'])
                            production_ds['exchanges'][0]['code'] = production_ds['code']
                            production_ds['exchanges'][0]['database'] = self.name_ei_with_regionalized_biosphere
                            production_ds['exchanges'][0]['location'] = production_ds['location']
                            production_ds['exchanges'][0]['input'] = (
                                self.name_ei_with_regionalized_biosphere, production_ds['code'])
                            self.ei_wurst.append(production_ds)

        # need to re-extract the dictionary because we added new processes to ei_wurst
        self.ei_in_dict = {(i['reference product'], i['location'], i['name']): i for i in self.ei_wurst}

        # now ensure that any ecoinvent process uses the newly created regionalized techno_water_flows
        for process in self.ei_wurst:
            for exc in process['exchanges']:
                if exc['type'] == 'technosphere':
                    if exc['product'] in techno_water_flows:
                        if process['location'] not in already_existing[exc['product']]:
                            if (not (exc['product'] == 'water, decarbonised' and exc[
                                'name'] == 'diethyl ether production') and
                                    not (exc['product'] == 'water, ultrapure' and process['location'] == 'CA-QC')):
                                try:
                                    replace_process = self.ei_in_dict[
                                        (exc['product'], magic_plumbering_geographies[process['location']][1], exc['name'])]
                                except KeyError:
                                    if exc['name'] == 'market for tap water':
                                        replace_process = self.ei_in_dict[(
                                            exc['product'], magic_plumbering_geographies[process['location']][1],
                                            'market group for tap water')]
                                    if exc['name'] == 'market group for tap water':
                                        replace_process = self.ei_in_dict[(
                                            exc['product'], magic_plumbering_geographies[process['location']][1],
                                            'market for tap water')]
                                exc['code'] = replace_process['code']
                                exc['name'] = replace_process['name']
                                exc['product'] = replace_process['reference product']
                                exc['input'] = (self.name_ei_with_regionalized_biosphere, exc['code'])


def clean_up_dataframe(df):
    # remove duplicates
    df = df.drop_duplicates()
    # fix index
    df = df.reset_index().drop('index',axis=1)
    return df
