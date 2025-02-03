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
    def __init__(self, bw_project_name, ecoinvent_database_name, ecoinvent_version):
        """
        :param bw_project_name:         [str] the name of a brightway2 project containing an ecoinvent database.
        :param ecoinvent_database_name: [str] the name of the ecoinvent database within the brightway2 project.
        :param ecoinvent_version:       [str] the version of the ecoinvent database within the brightway2 project,
                                            values can be "3.9", "3.9.1", "3.10" or "3.10.1".
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
        if bw_project_name not in bw2.projects:
            raise KeyError("The brightway project name passed does not match with any existing brightway projects.")
        bw2.projects.set_current(bw_project_name)
        if ecoinvent_database_name not in bw2.databases:
            raise KeyError("The ecoinvent database name passed does not match with the existing databases within the brightway project.")

        # set up necessary variables
        self.ecoinvent_database_name = ecoinvent_database_name
        self.name_ei_with_regionalized_biosphere = ecoinvent_database_name + ' regionalized'
        if ecoinvent_version not in ["3.9", "3.9.1", "3.10", "3.10.1"]:
            raise KeyError("The version of ecoinvent you provided is not supported by Regioinvent."
                           "Supported versions are: 3.9, 3.9.1, 3.10 or 3.10.1")
        if ecoinvent_version in ["3.9", "3.9.1"]:
            self.ecoinvent_version = '3.9'
        elif ecoinvent_version in ["3.10", "3.10.1"]:
            self.ecoinvent_version = '3.10'
        # name is fixed
        self.name_spatialized_biosphere = 'biosphere3_spatialized_flows'
        # parameter useful only for article
        self.regio_bio = True

        # load data from the different mapping files and such
        with open(pkg_resources.resource_filename(__name__, '/Data/Regionalization/ei' + self.ecoinvent_version +
                                                            '/ecoinvent_to_HS.json'), 'r') as f:
            self.eco_to_hs_class = json.load(f)
        with open(pkg_resources.resource_filename(__name__, '/Data/Regionalization/ei' + self.ecoinvent_version +
                                                            '/HS_to_exiobase_name.json'), 'r') as f:
            self.hs_class_to_exio = json.load(f)
        with open(pkg_resources.resource_filename(__name__, '/Data/Regionalization/ei' + self.ecoinvent_version +
                                                            '/country_to_ecoinvent_regions.json'), 'r') as f:
            self.country_to_ecoinvent_regions = json.load(f)
        with open(pkg_resources.resource_filename(__name__, '/Data/Regionalization/ei' + self.ecoinvent_version +
                                                            '/electricity_processes.json'), 'r') as f:
            self.electricity_geos = json.load(f)
        with open(pkg_resources.resource_filename(__name__, '/Data/Regionalization/ei' + self.ecoinvent_version +
                                                            '/electricity_aluminium_processes.json'), 'r') as f:
            self.electricity_aluminium_geos = json.load(f)
        with open(pkg_resources.resource_filename(__name__, '/Data/Regionalization/ei' + self.ecoinvent_version +
                                                            '/waste_processes.json'), 'r') as f:
            self.waste_geos = json.load(f)
        with open(pkg_resources.resource_filename(__name__, '/Data/Regionalization/ei' + self.ecoinvent_version +
                                                            '/heat_industrial_ng_processes.json'), 'r') as f:
            self.heat_district_ng = json.load(f)
        with open(pkg_resources.resource_filename(__name__, '/Data/Regionalization/ei' + self.ecoinvent_version +
                                                            '/heat_industrial_non_ng_processes.json'), 'r') as f:
            self.heat_district_non_ng = json.load(f)
        with open(pkg_resources.resource_filename(__name__, '/Data/Regionalization/ei' + self.ecoinvent_version +
                                                            '/heat_small_scale_non_ng_processes.json'), 'r') as f:
            self.heat_small_scale_non_ng = json.load(f)
        with open(pkg_resources.resource_filename(__name__, '/Data/Regionalization/ei' + self.ecoinvent_version +
                                                            '/COMTRADE_to_ecoinvent_geographies.json'), 'r') as f:
            self.convert_ecoinvent_geos = json.load(f)
        with open(pkg_resources.resource_filename(__name__, '/Data/Regionalization/ei' + self.ecoinvent_version +
                                                            '/COMTRADE_to_exiobase_geographies.json'), 'r') as f:
            self.convert_exiobase_geos = json.load(f)

        # initialize attributes used within package
        self.assigned_random_geography = []
        self.regioinvent_in_wurst = []
        self.regioinvent_in_dict = {}
        self.ei_regio_data = {}
        self.ei_wurst = []
        self.ei_in_dict = {}
        self.distribution_technologies = {}
        self.transportation_modes = {}
        self.created_geographies = dict.fromkeys(self.eco_to_hs_class.keys())
        self.unit = dict.fromkeys(self.eco_to_hs_class.keys())
        self.trade_data = pd.DataFrame()
        self.export_data = pd.DataFrame()
        self.import_data = pd.DataFrame()
        self.consumption_data = pd.DataFrame()
        self.trade_conn = ''
        self.regioinvent_database_name = ''
        self.cutoff = 0

    def spatialize_my_ecoinvent(self):
        """
        Function creates a copy of the original ecoinvent database and modifies this copy to spatialize the elementary
        flows used by ecoinvent. It also creates additional technosphere water processes to remediate imbalances due to
        technosphere misrepresentations.

        :return: nothing but creates multiple databases in your brightway2 project
        """

        # ---------------------------- Create the spatialized biosphere ----------------------------

        if 'biosphere3_spatialized_flows' not in bw2.databases:
            # load the correct pickle file with the different spatialized elementary flows metadata
            with open(pkg_resources.resource_filename(
                    __name__, '/Data/Spatialization_of_elementary_flows/ei' + self.ecoinvent_version +
                              '/spatialized_biosphere_database.pickle'), 'rb') as f:
                spatialized_biosphere = pickle.load(f)
            # create the new biosphere3 database with spatialized elementary flows
            bw2.Database(self.name_spatialized_biosphere).write(spatialized_biosphere)
        else:
            self.logger.info("biosphere3_spatialized_flows already exists in this project.")

        # ---------------------------- Spatialize ecoinvent ----------------------------

        if self.name_ei_with_regionalized_biosphere not in bw2.databases:
            # transform format of ecoinvent to wurst format for speed-up
            self.logger.info("Extracting ecoinvent to wurst...")
            self.ei_wurst = wurst.extract_brightway2_databases(self.ecoinvent_database_name, add_identifiers=True)

            # also get ecoinvent in a format for more efficient searching
            self.ei_in_dict = {(i['reference product'], i['location'], i['name']): i for i in self.ei_wurst}

            # load the list of the base name of all spatialized elementary flows
            with open(pkg_resources.resource_filename(
                    __name__, '/Data/Spatialization_of_elementary_flows/ei' + self.ecoinvent_version +
                              '/spatialized_elementary_flows.json'), 'r') as f:
                base_spatialized_flows = json.load(f)

            # store the codes of the spatialized flows in a dictionary
            spatialized_flows = {(i.as_dict()['name'], i.as_dict()['categories']): i.as_dict()['code'] for i in
                                 bw2.Database(self.name_spatialized_biosphere)}

            # fix some problems with water balance in ecoinvent processes (see description of function for more info)
            self.logger.info("Fixing water processes in ecoinvent...")
            self.fix_ecoinvent_water()

            self.logger.info("Spatializing ecoinvent...")
            # loop through the whole ecoinvent database
            for process in self.ei_wurst:
                # create a copy, but in the new ecoinvent database
                process['database'] = self.name_ei_with_regionalized_biosphere
                # loop through exchanges of a process
                for exc in process['exchanges']:
                    # if it's a biosphere exchange
                    if exc['type'] == 'biosphere':
                        # check if it's a flow that should be spatialized
                        if exc['name'] in base_spatialized_flows:
                            # check if the category makes sense (don't regionalize mineral resources for instance)
                            if exc['categories'][0] in base_spatialized_flows[exc['name']]:
                                # to spatialize it, we need to get the uuid of the existing spatialized flow
                                exc['code'] = spatialized_flows[(exc['name'] + ', ' + process['location'],
                                                                 exc['categories'])]
                                # change the database of the exchange as well
                                exc['database'] = self.name_spatialized_biosphere
                                # update its name
                                exc['name'] = exc['name'] + ', ' + process['location']
                                # and finally its input key
                                exc['input'] = (exc['database'], exc['code'])
                    # if it's a technosphere exchange, just update the database value
                    else:
                        exc['database'] = self.name_ei_with_regionalized_biosphere

            # sometimes input keys disappear with wurst, make sure there is always one
            for pr in self.ei_wurst:
                for exc in pr['exchanges']:
                    try:
                        exc['input']
                    except KeyError:
                        exc['input'] = (exc['database'], exc['code'])

            # modify structure of data from wurst to bw2
            self.ei_regio_data = {(i['database'], i['code']): i for i in self.ei_wurst}

            # same as before, ensure input key is here
            for pr in self.ei_regio_data:
                for exc in self.ei_regio_data[pr]['exchanges']:
                    try:
                        exc['input']
                    except KeyError:
                        exc['input'] = (exc['database'], exc['code'])
            # wurst creates empty categories for technosphere activities, delete those
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

            # write the ecoinvent-regionalized database to brightway
            bw2.Database(self.name_ei_with_regionalized_biosphere).write(self.ei_regio_data)
        else:
            self.logger.info("There is already a spatialized version of ecoinvent in your project. If you want to redo "
                             "spatialization, please delete it and re-run.")

    def import_fully_regionalized_impact_method(self, lcia_method):
        """
        Function to import a fully regionalized impact method into your brightway project, to-be-used with the
        spatialized version of ecoinvent. You can choose between IMPACT World+, EF and ReCiPe, or simply all of them.

        :param lcia_method: [str] the name of the LCIA method to be imported to be used with the spatialized ecoinvent,
                                available methods are "IW v2.1", "EF v3.1", "ReCiPe2016 v1.1 (E)" or "all".
        :return:
        """

        # just load the correct BW2Package file from Data storage folder

        if lcia_method == 'all' and self.ecoinvent_version == '3.10':
            self.logger.info("Importing all available fully regionalized lcia methods for ecoinvent3.10.")
            bw2.BW2Package.import_file(pkg_resources.resource_filename(
                __name__,
                '/Data/IW/impact_world_plus_21_regionalized-for-ecoinvent-v310.0fffd5e3daa5f4cf11ef83e49c375827.bw2package'))
        if lcia_method == 'all' and self.ecoinvent_version == '3.9':
            self.logger.info("Importing all available fully regionalized lcia methods for ecoinvent3.9.")
            bw2.BW2Package.import_file(pkg_resources.resource_filename(
                __name__,
                '/Data/IW/impact_world_plus_21_regionalized-for-ecoinvent-v39.af770e84bfd0f4365d509c026796639a.bw2package'))

        if lcia_method == "IW v2.1" and self.ecoinvent_version == '3.10':
            self.logger.info("Importing the fully regionalized version of IMPACT World+ v2.1 for ecoinvent3.10.")
            bw2.BW2Package.import_file(pkg_resources.resource_filename(
                __name__,
                '/Data/IW/impact_world_plus_21_regionalized-for-ecoinvent-v310.0fffd5e3daa5f4cf11ef83e49c375827.bw2package'))
        elif lcia_method == "IW v2.1" and self.ecoinvent_version == '3.9':
            self.logger.info("Importing the fully regionalized version of IMPACT World+ v2.1 for ecoinvent3.9.")
            bw2.BW2Package.import_file(pkg_resources.resource_filename(
                __name__,
                '/Data/IW/impact_world_plus_21_regionalized-for-ecoinvent-v39.af770e84bfd0f4365d509c026796639a.bw2package'))
        elif lcia_method == "EF v3.1":
            self.logger.info("Importing the fully regionalized version of EF v3.1.")
        elif lcia_method == "ReCiPe2016 v1.1 (E)":
            self.logger.info("Importing the fully regionalized version of ReCiPe2016 v1.1 (E).")

    def regionalize_ecoinvent_with_trade(self, trade_database_path, regioinvent_database_name, cutoff):
        """
        Function runs all the necessary sub-functions to incorporate trade data within ecoinvent supply chains
        descriptions
        :param trade_database_path: [str] the path to the trade database
        :param regioinvent_database_name: [str] the name to be given to the generated regioinvent database in brightway2
        :param cutoff: [float] the amount (between 0 and 1) after which exports/imports values of countries will be aggregated
                        into a Rest-of-theWorld aggregate.
        :return:
        """

        self.trade_conn = sqlite3.connect(trade_database_path)
        self.regioinvent_database_name = regioinvent_database_name
        self.cutoff = cutoff

        if not self.ei_wurst:
            self.ei_wurst = wurst.extract_brightway2_databases(self.name_ei_with_regionalized_biosphere,
                                                               add_identifiers=True)
        if not self.ei_in_dict:
            self.ei_in_dict = {(i['reference product'], i['location'], i['name']): i for i in self.ei_wurst}

        self.format_trade_data()
        self.first_order_regionalization()
        self.create_consumption_markets()
        self.second_order_regionalization()
        self.spatialize_elem_flows()
        self.write_regioinvent_to_database()
        self.connect_ecoinvent_to_regioinvent()

    # TODO we use this function for showing the influence of spatialization for the article, after that, remove it
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

    def format_trade_data(self):
        """
        Function extracts and formats the export data from the trade database
        :return: self.export_data
        """

        self.logger.info("Extracting and formatting trade data...")

        # load BACI data from SQL database
        self.trade_data = pd.read_sql('SELECT * FROM [International trade data]', self.trade_conn)

        # format export information
        self.export_data = self.trade_data.set_index(['cmdCode', 'refYear', 'exporter']).drop(
            ['value (1,000 USD)', 'importer'], axis=1).groupby(['cmdCode', 'refYear', 'exporter']).sum().reset_index()

        # estimate domestic production information
        domestic_prod = pd.read_sql('SELECT * FROM "Domestic production"', con=self.trade_conn)
        # domestic production is estimated from export so copy exports
        self.domestic_data = self.export_data.copy('deep')
        self.domestic_data.loc[:, 'COMTRADE_reporter'] = self.domestic_data.exporter.copy()
        # go from ISO3 codes to country codes for respective databases
        self.export_data.exporter = [self.convert_ecoinvent_geos[i] for i in self.export_data.exporter]
        self.domestic_data.exporter = [self.convert_ecoinvent_geos[i] for i in self.domestic_data.exporter]
        self.domestic_data.COMTRADE_reporter = [self.convert_exiobase_geos[i] for i in
                                                self.domestic_data.COMTRADE_reporter]
        # add HS codes
        self.domestic_data = self.domestic_data.merge(
            pd.DataFrame.from_dict(self.hs_class_to_exio, orient='index', columns=['cmdExio']).reset_index().rename(
                columns={'index': 'cmdCode'}))
        # merge with domestic production ratios
        self.domestic_data = self.domestic_data.merge(domestic_prod, left_on=['COMTRADE_reporter', 'cmdExio'],
                                                      right_on=['country', 'commodity'], how='left')
        # apply the ratios to determine domestic production
        self.domestic_data.loc[:, 'quantity (t)'] = (self.domestic_data.loc[:, 'quantity (t)'] /
                                                     (1 - self.domestic_data.loc[:, 'domestic use (%)'] / 100) *
                                                     self.domestic_data.loc[:, 'domestic use (%)'] / 100)
        # add importer column equal to export column (i.e., domestic production)
        self.domestic_data.loc[:, 'importer'] = self.domestic_data.exporter
        # some infinites came up, replace them by zero
        self.domestic_data.loc[:, 'quantity (t)'] = self.domestic_data.loc[:, 'quantity (t)'].replace(np.inf, 0.0)

        # format import information
        self.import_data = self.trade_data.set_index(['cmdCode', 'refYear', 'importer', 'exporter']).drop(
            ['value (1,000 USD)'], axis=1).groupby(['cmdCode', 'refYear', 'importer', 'exporter']).sum().reset_index()

        # go from ISO3 codes to ISO2 codes
        self.import_data.importer = [self.convert_ecoinvent_geos[i] for i in self.import_data.importer]
        self.import_data.exporter = [self.convert_ecoinvent_geos[i] for i in self.import_data.exporter]

        # concatenate import and domestic data
        self.consumption_data = pd.concat([self.import_data, self.domestic_data.loc[:, self.import_data.columns]])

    def first_order_regionalization(self):
        """
        Function to regionalized the key inputs of each process: electricity, municipal solid waste and heat.
        :return: self.regioinvent_in_wurst with new regionalized processes
        """

        self.logger.info("Regionalizing main inputs of traded products of ecoinvent...")

        for product in tqdm(self.eco_to_hs_class, leave=True):
            # filter commodity code from export_data
            cmd_export_data = self.export_data[self.export_data.cmdCode.isin([self.eco_to_hs_class[product]])].copy(
                'deep')
            # calculate the average export volume over the available years for each country
            cmd_export_data = cmd_export_data.groupby('exporter').agg({'quantity (t)': 'mean'})
            exporters = (cmd_export_data.loc[:, 'quantity (t)'] / cmd_export_data.loc[:,
                                                                  'quantity (t)'].sum()).sort_values(ascending=False)
            # only keep the countries representing XX% of global exports of the product and create a RoW from that
            limit = exporters.index.get_loc(exporters[exporters.cumsum() > self.cutoff].index[0]) + 1
            remainder = exporters.iloc[limit:].sum()
            exporters = exporters.iloc[:limit]
            if 'RoW' in exporters.index:
                exporters.loc['RoW'] += remainder
            else:
                exporters.loc['RoW'] = remainder

            # register the information about created geographies for each product
            self.created_geographies[product] = [i for i in exporters.index]

            # identify the processes producing the product
            filter_processes = ws.get_many(self.ei_wurst, ws.equals("reference product", product),
                                           ws.exclude(ws.contains("name", "market for")),
                                           ws.exclude(ws.contains("name", "market group for")),
                                           ws.exclude(ws.contains("name", "generic market")),
                                           ws.exclude(ws.contains("name", "import from")))
            # there can be multiple technologies to produce the same product, register all possibilities
            available_geographies = []
            available_technologies = []
            for dataset in filter_processes:
                available_geographies.append(dataset['location'])
                available_technologies.append(dataset['name'])
            # extract each available geography processes of ecoinvent, per technology of production
            possibilities = {tech: [] for tech in available_technologies}
            for i, geo in enumerate(available_geographies):
                possibilities[available_technologies[i]].append(geo)

            # determine the market share of each technology that produces the product, also determine the transportation
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
                self.distribution_technologies[product] = {k: 1 / len(self.distribution_technologies[product])
                                                           for k, v in self.distribution_technologies[product].items()}
            # average the transportation modes
            if number_of_markets > 1:
                self.transportation_modes[product] = {k: v / number_of_markets for k, v in
                                                      self.transportation_modes[product].items()}

            # create the global export market process within regioinvent
            global_market_activity = copy.deepcopy(dataset)

            # rename activity
            global_market_activity['name'] = f"""export market for {product}"""

            # add a comment
            global_market_activity['comment'] = f"""This process represents the global export market for {product}. It can be used as a proxy for global production market but is not a global production market as it does not include domestic production data. The shares come from export data from the BACI database for the commodity {self.eco_to_hs_class[product]}. Data from BACI is already in physical units. An average of the 5 last years of export trade available data is taken (in general from 2018 to 2022). Countries are taken until {self.cutoff*100}% of the global production amounts are covered. The rest of the data is aggregated in a RoW (Rest-of-the-World) region."""

            # location will be global (it's a global market)
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
            # store unit of the product, need it later on
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
                # filter the process to-be-copied
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
                regio_process['comment'] = f'This process is a regionalized adaptation of the following process of the ecoinvent database: {activity} | {product} | {region}. No amount values were modified in the regionalization process, only the origin of the flows.'
                # update production exchange
                [i for i in regio_process['exchanges'] if i['type'] == 'production'][0]['code'] = regio_process['code']
                [i for i in regio_process['exchanges'] if i['type'] == 'production'][0]['database'] = regio_process['database']
                [i for i in regio_process['exchanges'] if i['type'] == 'production'][0]['location'] = regio_process['location']
                [i for i in regio_process['exchanges'] if i['type'] == 'production'][0]['input'] = (regio_process['database'], regio_process['code'])
                # put the regionalized process' share into the global export market
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
                    # reset regio_process variable
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
                                # if no RoW/GLO processes available, take the first available geography by default...
                                regio_process = copy_process(product, technology, possibilities[technology][0],
                                                             exporter)
                                self.assigned_random_geography.append([product, technology, exporter])

                    # for each input, we test the presence of said inputs and regionalize that input
                    # testing the presence allows to save time if the input in question is just not used by the process
                    if regio_process:
                        # aluminium specific electricity input
                        if self.test_input_presence(regio_process, 'electricity', extra='aluminium/electricity'):
                            regio_process = self.change_aluminium_electricity(regio_process, exporter)
                        # cobalt specific electricity input
                        elif self.test_input_presence(regio_process, 'electricity', extra='cobalt/electricity'):
                            regio_process = self.change_cobalt_electricity(regio_process)
                        # normal electricity input
                        elif self.test_input_presence(regio_process, 'electricity', extra='voltage'):
                            regio_process = self.change_electricity(regio_process, exporter)
                        # municipal solid waste input
                        if self.test_input_presence(regio_process, 'municipal solid waste'):
                            regio_process = self.change_waste(regio_process, exporter)
                        # heat, district or industrial, natural gas input
                        if self.test_input_presence(regio_process, 'heat, district or industrial, natural gas'):
                            regio_process = self.change_heat(regio_process, exporter,
                                                             'heat, district or industrial, natural gas')
                        # heat, district or industrial, other than natural gas input
                        if self.test_input_presence(regio_process,
                                                    'heat, district or industrial, other than natural gas'):
                            regio_process = self.change_heat(regio_process, exporter,
                                                             'heat, district or industrial, other than natural gas')
                        # heat, central or small-scale, other than natural gas input
                        if self.test_input_presence(regio_process,
                                                    'heat, central or small-scale, other than natural gas'):
                            regio_process = self.change_heat(regio_process, exporter,
                                                             'heat, central or small-scale, other than natural gas')
                    # register the regionalized process within the database
                    if regio_process:
                        self.regioinvent_in_wurst.append(regio_process)

            # add transportation to export market
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
            # and register the export market in wurst
            self.regioinvent_in_wurst.append(global_market_activity)

    def create_consumption_markets(self):
        """
        Function creating consumption markets for each regionalized process
        :return:  self.regioinvent_in_wurst with new regionalized processes
        """

        self.logger.info("Creating consumption markets...")

        # change to dictionary to speed searching for info
        self.regioinvent_in_dict = {tech: [] for tech in
                                    [(i['reference product'], i['location']) for i in self.regioinvent_in_wurst]}
        # populate the empty dictionary
        for process in self.regioinvent_in_wurst:
            self.regioinvent_in_dict[(process['reference product'], process['location'])].append(
                {process['name']: process})

        # extracting link between ecoinvent and exiobase regions for documentation in created process description
        eco_to_exio_region = dict(zip(self.domestic_data.exporter, self.domestic_data.COMTRADE_reporter))

        for product in tqdm(self.eco_to_hs_class, leave=True):
            # filter the product in self.consumption_data
            cmd_consumption_data = self.consumption_data[
                self.consumption_data.cmdCode == self.eco_to_hs_class[product]].copy('deep')
            # calculate the average import volume for each country
            cmd_consumption_data = cmd_consumption_data.groupby(['importer', 'exporter']).agg({'quantity (t)': 'mean'})
            # change to relative values
            importers = (cmd_consumption_data.groupby(level=0).sum() /
                         cmd_consumption_data.sum().sum()).sort_values(by='quantity (t)', ascending=False)
            # only keep importers till the user-defined cut-off of total imports
            limit = importers.index.get_loc(importers[importers.cumsum() > self.cutoff].dropna().index[0]) + 1
            # aggregate the rest
            remainder = cmd_consumption_data.loc[importers.index[limit:]].groupby(level=1).sum()
            cmd_consumption_data = cmd_consumption_data.loc[importers.index[:limit]]
            # assign the aggregate to RoW location
            cmd_consumption_data = pd.concat([cmd_consumption_data, pd.concat([remainder], keys=['RoW'])])
            cmd_consumption_data.index = pd.MultiIndex.from_tuples([i for i in cmd_consumption_data.index])
            cmd_consumption_data = cmd_consumption_data.sort_index()

            # loop through each selected importers of the commodity
            for importer in cmd_consumption_data.index.levels[0]:
                # change to relative values
                cmd_consumption_data.loc[importer, 'quantity (t)'] = (
                        cmd_consumption_data.loc[importer, 'quantity (t)'] /
                        cmd_consumption_data.loc[importer, 'quantity (t)'].sum()).values
                # we need to add the aggregate to potentially already existing RoW exchanges
                cmd_consumption_data = pd.concat([
                    cmd_consumption_data.drop('RoW', level=0),
                    pd.concat([cmd_consumption_data.loc['RoW'].groupby(level=0).sum()], keys=['RoW'])])
                cmd_consumption_data = cmd_consumption_data.fillna(0)

                # create the process information
                new_import_data = {
                    'name': 'consumption market for ' + product,
                    'reference product': product,
                    'location': importer,
                    'type': 'process',
                    'unit': self.unit[product],
                    'code': uuid.uuid4().hex,
                    'comment': f'This process represents the consumption market of {product} in {importer}. The shares were determined based on two aspects. The imports of the commodity {self.eco_to_hs_class[product]} taken from the BACI database (average over the years 2018, 2019, 2020, 2021, 2022) as well as the domestic production estimate for the sector {self.hs_class_to_exio[self.eco_to_hs_class[product]]} in the region {eco_to_exio_region[importer]} taken from the EXIOBASE database. Shares are considered until {self.cutoff*100}% of the imports+domestic consumption are covered. Residual imports are aggregated in a RoW (Rest-of-the-World) region.',
                    'database': self.regioinvent_database_name,
                    'exchanges': []
                }

                # create the production exchange
                new_import_data['exchanges'].append({'amount': 1,
                                                     'type': 'production',
                                                     'input': (self.regioinvent_database_name,
                                                               new_import_data['code'])})
                # identify regionalized processes that were created in self.first_order_regionalization()
                available_trading_partners = self.created_geographies[product]
                # loop through the selected importers
                for trading_partner in cmd_consumption_data.loc[importer].index:
                    # check if a regionalized process exist for that importer
                    if trading_partner in available_trading_partners:
                        # loop through available technologies to produce the commodity
                        for technology in self.distribution_technologies[product]:
                            # get the uuid
                            code = [i for i in self.regioinvent_in_dict[(product, trading_partner)] if
                                    list(i.keys())[0] == technology][0][technology]['code']
                            # get the share
                            share = self.distribution_technologies[product][technology]
                            # create the exchange
                            new_import_data['exchanges'].append({
                                'amount': cmd_consumption_data.loc[(importer, trading_partner), 'quantity (t)'] * share,
                                'type': 'technosphere',
                                'input': (self.regioinvent_database_name, code),
                                'name': product
                            })
                    # if a regionalized process does not exist for importer, take the RoW aggregate
                    else:
                        # loop through available technologies to produce the commodity
                        for technology in self.distribution_technologies[product]:
                            # get the uuid
                            code = [i for i in self.regioinvent_in_dict[(product, 'RoW')] if
                                     list(i.keys())[0] == technology][0][technology]['code']
                            # get the share
                            share = self.distribution_technologies[product][technology]
                            # create the exchange
                            new_import_data['exchanges'].append({
                                'amount': cmd_consumption_data.loc[(importer, trading_partner), 'quantity (t)'] * share,
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
                # add duplicates into one single flow
                for duplicate in duplicates:
                    total = sum([i['amount'] for i in new_import_data['exchanges'] if i['input'] == duplicate])
                    new_import_data['exchanges'] = [i for i in new_import_data['exchanges'] if
                                                    i['input'] != duplicate] + [
                                                       {'amount': total, 'name': product, 'type': 'technosphere',
                                                        'input': duplicate}]
                # add to database in wurst
                self.regioinvent_in_wurst.append(new_import_data)

    def second_order_regionalization(self):
        """
        Function that links newly created consumption markets to inputs of the different processes of the regionalized
        ecoinvent database.
        :return:  self.regioinvent_in_wurst with new regionalized processes
        """

        self.logger.info("Performing second order regionalization...")

        # first step is to connect regioinvent processes to created water processes from self.fix_ecoinvent_water()
        if self.regio_bio:
            techno_water_flows = ['irrigation', 'water, deionised', 'water, ultrapure', 'water, decarbonised',
                                  'water, completely softened', 'tap water', 'wastewater, average',
                                  'wastewater, unpolluted']
            # loop through all created processes
            for process in self.regioinvent_in_wurst:
                # only for production processes (not for markets)
                if 'consumption market' not in process['name'] and 'export market' not in process['name']:
                    # loop through exchanges
                    for exc in process['exchanges']:
                        # if it's a technosphere exchange
                        if exc['type'] == 'technosphere':
                            # about water
                            if exc['product'] in techno_water_flows:
                                # except some that are annoying
                                if (not (exc['product'] == 'water, decarbonised' and
                                         exc['name'] == 'diethyl ether production') and not (
                                        exc['product'] == 'water, ultrapure' and process['location'] == 'CA-QC')):
                                    # then replace the wrong water process by the regionalized version
                                    try:
                                        replace_process = self.ei_in_dict[
                                            (exc['product'], process['location'], exc['name'])]
                                    except KeyError:
                                        if exc['name'] == 'market for tap water':
                                            replace_process = self.ei_in_dict[(
                                                exc['product'], process['location'], 'market group for tap water')]
                                        if exc['name'] == 'market group for tap water':
                                            replace_process = self.ei_in_dict[(
                                                exc['product'], process['location'], 'market for tap water')]
                                        if exc['name'] == 'market group for irrigation':
                                            replace_process = self.ei_in_dict[(
                                                exc['product'], process['location'], 'market for irrigation')]
                                    exc['code'] = replace_process['code']
                                    exc['name'] = replace_process['name']
                                    exc['product'] = replace_process['reference product']
                                    exc['input'] = (self.name_ei_with_regionalized_biosphere, exc['code'])

        # as dictionary to speed up searching for info
        consumption_markets_data = {(i['name'], i['location']): i for i in self.regioinvent_in_wurst if
                                    'consumption market' in i['name']}

        # loop through created processes
        for process in self.regioinvent_in_wurst:
            # only for production processes (not for markets)
            if 'consumption market' not in process['name'] and 'export market' not in process['name']:
                # loop through exchanges
                for exc in process['exchanges']:
                    # if the exchange is about one of the products that have been regionalized
                    if exc['product'] in self.eco_to_hs_class.keys() and exc['type'] == 'technosphere':
                        # then get the name of the created consumption market for that product
                        exc['name'] = 'consumption market for ' + exc['product']
                        # and get its location (same as the process)
                        exc['location'] = process['location']
                        # if the consumption market exists for the location of process
                        if ('consumption market for ' + exc['product'],
                            process['location']) in consumption_markets_data.keys():
                            # change database
                            exc['database'] = (consumption_markets_data[('consumption market for ' + exc['product'],
                                                                         process['location'])]['database'])
                            # change code
                            exc['code'] = (consumption_markets_data[('consumption market for ' + exc['product'],
                                                                     process['location'])]['code'])
                        # if the consumption market does not exist for the location of process, use RoW
                        else:
                            # change database
                            exc['database'] = (consumption_markets_data[('consumption market for ' +
                                                                         exc['product'], 'RoW')]['database'])
                            # change code
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

    def spatialize_elem_flows(self):
        """
        Function spatializes the elementary flows of the regioinvent processes to the location of process.
        """

        if not self.regio_bio:
            self.logger.warning("If you wish to regionalize elementary flows, you need to select the correct boolean "
                                "in the initialization of the class...")
            return

        self.logger.info("Regionalizing the elementary flows of the regioinvent database...")

        # the list of all spatialized flows
        with open(pkg_resources.resource_filename(
                __name__, '/Data/Spatialization_of_elementary_flows/ei' + self.ecoinvent_version +
                          '/spatialized_elementary_flows.json'), 'r') as file:
            spatialized_elem_flows = json.load(file)
        # a dictionary with all the associated uuids of the spatialized flows
        regionalized_flows = {(i.as_dict()['name'], i.as_dict()['categories']): i.as_dict()['code'] for i in
                              bw2.Database(self.name_spatialized_biosphere)}

        # loop through regioinvent processes
        for process in self.regioinvent_in_wurst:
            # loop through exchanges of process
            for exc in process['exchanges']:
                # if the exchange is a biosphere exchange
                if exc['type'] == 'biosphere':
                    # strip the potential region from the spatialized flow name and check if it's a spatialized flow
                    # if region had a comma in the name it would be a problem, but it's not happening as the geographies
                    # used for copies in regioinvent don't contain commas
                    if ', '.join(exc['name'].split(', ')[:-1]) in spatialized_elem_flows.keys():
                        base_name_flow = ', '.join(exc['name'].split(', ')[:-1])
                        # check that the flow is spatialized for the compartment
                        if exc['categories'][0] in spatialized_elem_flows[base_name_flow]:
                            # get code of spatialized flow for process['location']
                            exc['code'] = regionalized_flows[
                                (base_name_flow + ', ' + process['location'], exc['categories'])]
                            # change database name of exchange
                            exc['database'] = self.name_spatialized_biosphere
                            # change name of exchange
                            exc['name'] = base_name_flow + ', ' + process['location']
                            # change input key of exchange
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

        # write regioinvent database in brightway2
        bw2.Database(self.regioinvent_database_name).write(regioinvent_data)

    def connect_ecoinvent_to_regioinvent(self):
        """
        Now that regioinvent exists, we can make ecoinvent use regioinvent processes to further deepen the
        regionalization. Only countries and sub-countries are connected to regioinvent, simply because in regioinvent
        we do not have consumption mixes for the different regions of ecoinvent (e.g., RER, RAS, etc.).
        However, Swiss processes are not affected, as ecoinvent was already tailored for the Swiss case.
        I am not sure regioinvent would bring more precision in that specific case.
        """

        # Here we are directly manipulating (through bw2) the already-written ecoinvent database [self.name_ei_with_regionalized_biosphere]
        self.logger.info("Connecting ecoinvent to regioinvent processes...")

        # as dictionary to speed searching for information
        consumption_markets_data = {(i['name'], i['location']): i for i in self.regioinvent_in_wurst if
                                    'consumption market' in i['name']}

        # first we connect ecoinvent to consumption markets of regioinvent
        for process in bw2.Database(self.name_ei_with_regionalized_biosphere):
            location = None
            # for countries (e.g., CA)
            if process.as_dict()['location'] in self.country_to_ecoinvent_regions.keys():
                location = process.as_dict()['location']
            # for sub-countries (e.g., CA-QC)
            elif process.as_dict()['location'].split('-')[0] in self.country_to_ecoinvent_regions.keys():
                location = process.as_dict()['location'].split('-')[0]
            # check if location is not None and not Switzerland
            if location and location != 'CH':
                # loop through technosphere exchanges
                for exc in process.technosphere():
                    # if the production of the exchange among the regionalized products
                    if exc.as_dict()['product'] in self.eco_to_hs_class.keys():
                        # get the name of the corresponding consumtion market
                        exc.as_dict()['name'] = 'consumption market for ' + exc.as_dict()['product']
                        # get the location of the process
                        exc.as_dict()['location'] = location
                        # if the consumption market exists for the process location
                        if ('consumption market for ' + exc.as_dict()['product'], location) in consumption_markets_data.keys():
                            exc.as_dict()['database'] = (consumption_markets_data[
                                ('consumption market for ' + exc.as_dict()['product'], location)]['database'])
                            exc.as_dict()['code'] = (consumption_markets_data[
                                ('consumption market for ' + exc.as_dict()['product'], location)]['code'])
                        # if the consumption market does not exist for the process location, take RoW
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
        # loop through the identified process
        for electricity_product_name in electricity_product_names:
            unit_name = list(set([i['unit'] for i in process['exchanges'] if 'electricity' in i['name'] and
                                  'aluminium' not in i['name'] and 'cobalt' not in i['name'] and 'voltage' in i['name']]))
            # if somehow different units used for electricity flows -> problem
            assert len(unit_name) == 1
            unit_name = unit_name[0]
            # sum quantity of all electricity exchanges
            qty_of_electricity = sum([i['amount'] for i in process['exchanges'] if electricity_product_name == i['product']])

            # remove electricity flows from non-appropriated geography
            for exc in process['exchanges'][:]:
                if (electricity_product_name == exc['product'] and 'aluminium' not in exc['name'] and
                        'cobalt' not in exc['name'] and 'voltage' in exc['name']):
                    process['exchanges'].remove(exc)

            electricity_region = None
            # if the country of the process has a specific electricity market defined in ecoinvent
            if export_country in self.electricity_geos:
                electricity_region = export_country
            # if it's a sub-country (e.g., CA-QC)
            elif '-' in export_country:
                # look for the national market group for electricity
                if export_country.split('-')[0] in self.electricity_geos:
                    electricity_region = export_country.split('-')[0]
            # if there is no electricity market for the country, take the one for the region it belongs to
            elif export_country != 'RoW' and export_country in self.country_to_ecoinvent_regions and not electricity_region:
                for potential_region in self.country_to_ecoinvent_regions[export_country]:
                    if potential_region in self.electricity_geos:
                        electricity_region = potential_region
            # if nothing works, take global electricity market
            if not electricity_region:
                electricity_region = 'GLO'

            # store the name of the electricity process. Some countries have market groups and not just markets
            if electricity_region in ['BR', 'CA', 'CN', 'GLO', 'IN', 'RAF', 'RAS', 'RER', 'RLA', 'RME', 'RNA', 'US']:
                electricity_activity_name = 'market group for ' + electricity_product_name
            else:
                electricity_activity_name = 'market for ' + electricity_product_name

            # get the uuid
            electricity_code = self.ei_in_dict[(electricity_product_name,
                                                electricity_region,
                                                electricity_activity_name)]['code']

            # create the regionalized flow for electricity
            process['exchanges'].append({"amount": qty_of_electricity,
                                           "product": electricity_product_name,
                                           "name": electricity_activity_name,
                                           "location": electricity_region,
                                           "unit": unit_name,
                                           "database": process['database'],
                                           "code": electricity_code,
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
        # identify aluminium-specific electricity related exchanges
        electricity_product_names = list(set([i['product'] for i in process['exchanges'] if
                                              'electricity' in i['name'] and 'aluminium' in i['name'] and 'voltage' in i['name']]))
        # loop through the identified process
        for electricity_product_name in electricity_product_names:
            unit_name = list(set([i['unit'] for i in process['exchanges'] if
                                              'electricity' in i['name'] and 'aluminium' in i['name'] and 'voltage' in i['name']]))
            # if somehow different units used for electricity flows -> problem
            assert len(unit_name) == 1
            unit_name = unit_name[0]
            # sum quantity of all electricity exchanges
            qty_of_electricity = sum([i['amount'] for i in process['exchanges'] if electricity_product_name == i['product']])

            # remove electricity flows from non-appropriated geography
            for exc in process['exchanges'][:]:
                if electricity_product_name == exc['product'] and 'aluminium' in exc['name']:
                    process['exchanges'].remove(exc)

            electricity_region = None
            # if the country of the process has a specific electricity market defined in ecoinvent
            if export_country in self.electricity_aluminium_geos:
                electricity_region = export_country
            # if there is no electricity market for the country, take the one for the region it belongs to
            elif export_country != 'RoW' and export_country in self.country_to_ecoinvent_regions and not electricity_region:
                for potential_region in self.country_to_ecoinvent_regions[export_country]:
                    if potential_region in self.electricity_aluminium_geos:
                        electricity_region = potential_region
            # if nothing works, take RoW electricity market
            if not electricity_region:
                electricity_region = 'RoW'

            # store the name of the electricity process
            electricity_activity_name = 'market for ' + electricity_product_name
            # get the uuid code
            electricity_code = self.ei_in_dict[(electricity_product_name,
                                                electricity_region,
                                                electricity_activity_name)]['code']

            # create the regionalized flow for electricity
            process['exchanges'].append({"amount": qty_of_electricity,
                                           "product": electricity_product_name,
                                           "name": electricity_activity_name,
                                           "location": electricity_region,
                                           "unit": unit_name,
                                           "database": process['database'],
                                           "code": electricity_code,
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
        # identify cobalt-specific electricity related exchanges
        electricity_product_names = list(set([i['product'] for i in process['exchanges'] if
                                              'electricity' in i['name'] and 'cobalt' in i['name']]))
        # loop through the identified process
        for electricity_product_name in electricity_product_names:
            unit_name = list(set([i['unit'] for i in process['exchanges'] if 'electricity' in i['name'] and
                                  'cobalt' in i['name']]))
            # if somehow different units used for electricity flows -> problem
            assert len(unit_name) == 1
            unit_name = unit_name[0]
            # sum quantity of all electricity exchanges
            qty_of_electricity = sum(
                [i['amount'] for i in process['exchanges'] if electricity_product_name == i['product']])

            # remove electricity flows from non-appropriated geography
            for exc in process['exchanges'][:]:
                if electricity_product_name == exc['product'] and 'cobalt' in exc['name']:
                    process['exchanges'].remove(exc)

            # GLO is the only geography available for electricity, cobalt industry in ei3.9 and 3.10
            electricity_region = 'GLO'
            # store the name of the electricity process
            electricity_activity_name = 'market for ' + electricity_product_name
            # get the uuid code
            electricity_code = self.ei_in_dict[(electricity_product_name,
                                                electricity_region,
                                                electricity_activity_name)]['code']

            # create the regionalized flow for electricity
            process['exchanges'].append({"amount": qty_of_electricity,
                                           "product": electricity_product_name,
                                           "name": electricity_activity_name,
                                           "location": electricity_region,
                                           "unit": unit_name,
                                           "database": process['database'],
                                           "code": electricity_code,
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
        # municipal solid waste exchanges all have the same name
        waste_product_name = 'municipal solid waste'
        unit_name = list(set([i['unit'] for i in process['exchanges'] if waste_product_name == i['product']]))
        # if somehow different units used for MSW flows -> problem
        assert len(unit_name) == 1
        unit_name = unit_name[0]
        # sum quantity of all MSW exchanges
        qty_of_waste = sum([i['amount'] for i in process['exchanges'] if waste_product_name == i['product']])

        # remove waste flows from non-appropriated geography
        for exc in process['exchanges'][:]:
            if waste_product_name == exc['product']:
                process['exchanges'].remove(exc)

        # if the country of the process has a specific MSW market defined in ecoinvent
        if export_country in self.waste_geos:
            waste_region = export_country
        # if there is no MSW market for the country, take the one for the region it belongs to
        elif (export_country in self.country_to_ecoinvent_regions and
              self.country_to_ecoinvent_regions[export_country][0] == "RER"):
            waste_region = 'Europe without Switzerland'
        # if nothing works, take global MSW market
        else:
            waste_region = 'RoW'

        # store the name of the electricity process
        if waste_region == 'Europe without Switzerland':
            waste_activity_name = 'market group for ' + waste_product_name
        else:
            waste_activity_name = 'market for ' + waste_product_name

        # get the uuid code
        waste_code = self.ei_in_dict[(waste_product_name,
                                      waste_region,
                                      waste_activity_name)]['code']

        # create the regionalized flow for waste
        process['exchanges'].append({"amount": qty_of_waste,
                                           "product": waste_product_name,
                                           "name": waste_activity_name,
                                           "location": waste_region,
                                           "unit": unit_name,
                                           "database": process['database'],
                                           "code": waste_code,
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
        # depending on the heat process, the geographies covered in ecoinvent are different
        if heat_flow == 'heat, district or industrial, natural gas':
            heat_process_countries = self.heat_district_ng
        if heat_flow == 'heat, district or industrial, other than natural gas':
            heat_process_countries = self.heat_district_non_ng
        if heat_flow == 'heat, central or small-scale, other than natural gas':
            heat_process_countries = self.heat_small_scale_non_ng

        unit_name = list(set([i['unit'] for i in process['exchanges'] if heat_flow == i['product']]))
        # if somehow different units used for electricity flows -> problem
        assert len(unit_name) == 1
        unit_name = unit_name[0]
        # sum quantity of all heat exchanges
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
                if self.name_ei_with_regionalized_biosphere in bw2.databases:
                    global_heat_process = ws.get_one(self.ei_wurst,
                                                     ws.equals("reference product", heat_flow),
                                                     ws.equals("location", 'GLO'),
                                                     ws.equals("database", self.name_ei_with_regionalized_biosphere),
                                                     ws.either(ws.contains("name", "market for"),
                                                               ws.contains("name", "market group for")))
                else:
                    global_heat_process = ws.get_one(self.ei_wurst,
                                                     ws.equals("reference product", heat_flow),
                                                     ws.equals("location", 'GLO'),
                                                     ws.equals("database", self.ecoinvent_database_name),
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
                                             "database": process['database'],
                                             "code": self.ei_in_dict[(heat_flow, heat_exc[1], heat_exc[0])]['code'],
                                             "type": "technosphere",
                                             "input": (self.name_ei_with_regionalized_biosphere,
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
                                               "database": process['database'],
                                               "code": self.ei_in_dict[(heat_flow, export_country, heat_exc)]['code'],
                                               "type": "technosphere",
                                               "input": (self.name_ei_with_regionalized_biosphere,
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

        with open(pkg_resources.resource_filename(
                __name__, '/Data/Spatialization_of_elementary_flows/ei' + self.ecoinvent_version +
                          '/magic_plumbering.json'), 'r') as f:
            magic_plumbering = json.load(f)

        # first, let's identify which processes need to be created
        techno_water_flows = ['irrigation', 'water, deionised', 'water, ultrapure', 'water, decarbonised',
                              'water, completely softened', 'tap water', 'wastewater, average', 'wastewater, unpolluted']

        # here we get all regions of processes using the identified technosphere water flows
        used_regions = list(magic_plumbering.keys())

        already_existing = {i: [] for i in techno_water_flows}
        # now we check if some of those already exist in ecoinvent
        for techno in techno_water_flows:
            for region in used_regions:
                if (techno, region, 'market for ' + techno) in self.ei_in_dict.keys() or (
                        techno, region, 'market group for ' + techno) in self.ei_in_dict.keys():
                    already_existing[techno].append(region)

        already_existing = {k: set(already_existing[k]) for k, v in already_existing.items()}

        # first we go through the market processes
        for techno in techno_water_flows:
            # loop through the different regions using these processes
            for region in used_regions:
                # if a process for the region does not exist, we create that process
                if region not in already_existing[techno]:
                    # for regions based on a copy of RoW, simply copy the "RoW" process
                    if magic_plumbering[region] == 'RoW':
                        market_ds = copy.deepcopy(
                            self.ei_in_dict[(techno, magic_plumbering[region], 'market for ' + techno)])
                    # for European countries, multiple possibilities
                    else:
                        # some processes use the "RER" geography
                        try:
                            market_ds = copy.deepcopy(
                                self.ei_in_dict[(techno, magic_plumbering[region], 'market for ' + techno)])
                        except KeyError:
                            # others use "Europe without Switzerland"
                            try:
                                market_ds = copy.deepcopy(
                                    self.ei_in_dict[(techno, 'Europe without Switzerland', 'market for ' + techno)])
                            # and if nothing works we take "RoW"
                            except KeyError:
                                market_ds = copy.deepcopy(self.ei_in_dict[(techno, 'RoW', 'market for ' + techno)])

                    # adapt metadata to newly created regionalized process
                    name = market_ds['name']
                    product = market_ds['reference product']
                    original_region = market_ds['location']
                    market_ds['comment'] = f'This process is a regionalized adaptation of the following process of the ecoinvent database: {name} | {product} | {original_region}. No amount values were modified in the regionalization process, only their origin.'
                    market_ds['location'] = region
                    market_ds['code'] = uuid.uuid4().hex
                    market_ds['database'] = self.name_ei_with_regionalized_biosphere
                    market_ds['input'] = (market_ds['database'], market_ds['code'])
                    # adapt the metadata of the production flow itself
                    [i for i in market_ds['exchanges'] if i['type'] == 'production'][0]['code'] = market_ds['code']
                    [i for i in market_ds['exchanges'] if i['type'] == 'production'][0][
                        'database'] = self.name_ei_with_regionalized_biosphere
                    [i for i in market_ds['exchanges'] if i['type'] == 'production'][0]['location'] = market_ds[
                        'location']
                    [i for i in market_ds['exchanges'] if i['type'] == 'production'][0]['input'] = (
                        self.name_ei_with_regionalized_biosphere, market_ds['code'])

                    self.ei_wurst.append(market_ds)

        # then we go through the production processes
        for techno in techno_water_flows:
            # filter specific technologies
            if techno in ['water, deionised', 'water, ultrapure', 'water, decarbonised', 'water, completely softened']:
                # loop through the different regions using these processes
                for region in used_regions:
                    # if a process for the region does not exist, we create that process
                    if region not in already_existing[techno]:
                        # for regions based on a copy of RoW, simply copy the "RoW" process
                        if magic_plumbering[region] == 'RoW':
                            production_ds = copy.deepcopy(self.ei_in_dict[(
                                techno, magic_plumbering[region], techno.replace('water', 'water production'))])
                        # for European countries, multiple possibilities
                        else:
                            # some processes use the "RER" geography
                            try:
                                production_ds = copy.deepcopy(self.ei_in_dict[(
                                    techno, magic_plumbering[region], techno.replace('water', 'water production'))])
                            except KeyError:
                                # others use "Europe without Switzerland"
                                try:
                                    production_ds = copy.deepcopy(self.ei_in_dict[(
                                        techno, 'Europe without Switzerland',
                                        techno.replace('water', 'water production'))])
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
                        # adapt the metadata of the production flow itself
                        [i for i in production_ds['exchanges'] if i['type'] == 'production'][0]['code'] = production_ds[
                            'code']
                        [i for i in production_ds['exchanges'] if i['type'] == 'production'][0][
                            'database'] = self.name_ei_with_regionalized_biosphere
                        [i for i in production_ds['exchanges'] if i['type'] == 'production'][0]['location'] = \
                        production_ds[
                            'location']
                        [i for i in production_ds['exchanges'] if i['type'] == 'production'][0]['input'] = (
                            self.name_ei_with_regionalized_biosphere, production_ds['code'])

                        # need to regionalize created process
                        if self.test_input_presence(production_ds, 'electricity', extra='aluminium/electricity'):
                            production_ds = self.change_aluminium_electricity(production_ds, region)
                        elif self.test_input_presence(production_ds, 'electricity', extra='cobalt/electricity'):
                            production_ds = self.change_cobalt_electricity(production_ds)
                        elif self.test_input_presence(production_ds, 'electricity', extra='voltage'):
                            production_ds = self.change_electricity(production_ds, region)
                        if self.test_input_presence(production_ds, 'municipal solid waste'):
                            production_ds = self.change_waste(production_ds, region)
                        if self.test_input_presence(production_ds, 'heat, district or industrial, natural gas'):
                            production_ds = self.change_heat(production_ds, region,
                                                             'heat, district or industrial, natural gas')
                        if self.test_input_presence(production_ds,
                                                    'heat, district or industrial, other than natural gas'):
                            production_ds = self.change_heat(production_ds, region,
                                                             'heat, district or industrial, other than natural gas')
                        if self.test_input_presence(production_ds,
                                                    'heat, central or small-scale, other than natural gas'):
                            production_ds = self.change_heat(production_ds, region,
                                                             'heat, central or small-scale, other than natural gas')

                        self.ei_wurst.append(production_ds)
            # filter specific technologies
            if techno in ['wastewater, average', 'wastewater, unpolluted']:
                # loop through the different regions using these processes
                for region in used_regions:
                    # if a process for the region does not exist, we create that process
                    if region not in already_existing[techno]:
                        # for regions based on a copy of RoW, simply copy the "RoW" process
                        if magic_plumbering[region] == 'RoW':
                            production_ds = copy.deepcopy(
                                self.ei_in_dict[(techno, 'RoW', 'treatment of ' + techno + ', wastewater treatment')])
                        else:
                            try:
                                production_ds = copy.deepcopy(self.ei_in_dict[(
                                    techno, 'Europe without Switzerland',
                                    'treatment of ' + techno + ', wastewater treatment')])
                            except KeyError:
                                production_ds = copy.deepcopy(
                                    self.ei_in_dict[
                                        (techno, 'RoW', 'treatment of ' + techno + ', wastewater treatment')])

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
                        # adapt the metadata of the production flow itself
                        [i for i in production_ds['exchanges'] if i['type'] == 'production'][0]['code'] = production_ds[
                            'code']
                        [i for i in production_ds['exchanges'] if i['type'] == 'production'][0][
                            'database'] = self.name_ei_with_regionalized_biosphere
                        [i for i in production_ds['exchanges'] if i['type'] == 'production'][0]['location'] = \
                        production_ds[
                            'location']
                        [i for i in production_ds['exchanges'] if i['type'] == 'production'][0]['input'] = (
                            self.name_ei_with_regionalized_biosphere, production_ds['code'])

                        # regionalize created process
                        if self.test_input_presence(production_ds, 'electricity', extra='aluminium/electricity'):
                            production_ds = self.change_aluminium_electricity(production_ds, region)
                        elif self.test_input_presence(production_ds, 'electricity', extra='cobalt/electricity'):
                            production_ds = self.change_cobalt_electricity(production_ds)
                        elif self.test_input_presence(production_ds, 'electricity', extra='voltage'):
                            production_ds = self.change_electricity(production_ds, region)
                        if self.test_input_presence(production_ds, 'municipal solid waste'):
                            production_ds = self.change_waste(production_ds, region)
                        if self.test_input_presence(production_ds, 'heat, district or industrial, natural gas'):
                            production_ds = self.change_heat(production_ds, region,
                                                             'heat, district or industrial, natural gas')
                        if self.test_input_presence(production_ds,
                                                    'heat, district or industrial, other than natural gas'):
                            production_ds = self.change_heat(production_ds, region,
                                                             'heat, district or industrial, other than natural gas')
                        if self.test_input_presence(production_ds,
                                                    'heat, central or small-scale, other than natural gas'):
                            production_ds = self.change_heat(production_ds, region,
                                                             'heat, central or small-scale, other than natural gas')

                        self.ei_wurst.append(production_ds)
            # filter specific technologies
            if techno == 'irrigation':
                # loop through the different regions using these processes
                for region in used_regions:
                    # if a process for the region does not exist, we create that process
                    if region not in already_existing[techno]:
                        # for the technology "irrigation" there are multiple "sub-technologies" available
                        for irrigation_type in ['irrigation, drip', 'irrigation, surface', 'irrigation, sprinkler']:
                            # for regions based on a copy of RoW, simply copy the "RoW" process
                            if magic_plumbering[region] == 'RoW':
                                production_ds = copy.deepcopy(
                                    self.ei_in_dict[(techno, magic_plumbering[region], irrigation_type)])
                            # for European countries, multiple possibilities
                            else:
                                # the RER location could be defined for the techno_water_flow
                                try:
                                    production_ds = copy.deepcopy(
                                        self.ei_in_dict[(techno, magic_plumbering[region], irrigation_type)])
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
                            production_ds[
                                'comment'] = f'This process is a regionalized adaptation of the following process of the ecoinvent database: {name} | {product} | {original_region}. No amount values were modified in the regionalization process, only their origin.'
                            production_ds['location'] = region
                            production_ds['code'] = uuid.uuid4().hex
                            production_ds['database'] = self.name_ei_with_regionalized_biosphere
                            production_ds['input'] = (production_ds['database'], production_ds['code'])
                            # adapt the metadata of the production flow itself
                            [i for i in production_ds['exchanges'] if i['type'] == 'production'][0]['code'] = \
                            production_ds['code']
                            [i for i in production_ds['exchanges'] if i['type'] == 'production'][0][
                                'database'] = self.name_ei_with_regionalized_biosphere
                            [i for i in production_ds['exchanges'] if i['type'] == 'production'][0]['location'] = \
                            production_ds[
                                'location']
                            [i for i in production_ds['exchanges'] if i['type'] == 'production'][0]['input'] = (
                                self.name_ei_with_regionalized_biosphere, production_ds['code'])

                            # regionalize created process
                            if self.test_input_presence(production_ds, 'electricity', extra='aluminium/electricity'):
                                production_ds = self.change_aluminium_electricity(production_ds, region)
                            elif self.test_input_presence(production_ds, 'electricity', extra='cobalt/electricity'):
                                production_ds = self.change_cobalt_electricity(production_ds)
                            elif self.test_input_presence(production_ds, 'electricity', extra='voltage'):
                                production_ds = self.change_electricity(production_ds, region)
                            if self.test_input_presence(production_ds, 'municipal solid waste'):
                                production_ds = self.change_waste(production_ds, region)
                            if self.test_input_presence(production_ds, 'heat, district or industrial, natural gas'):
                                production_ds = self.change_heat(production_ds, region,
                                                                 'heat, district or industrial, natural gas')
                            if self.test_input_presence(production_ds,
                                                        'heat, district or industrial, other than natural gas'):
                                production_ds = self.change_heat(production_ds, region,
                                                                 'heat, district or industrial, other than natural gas')
                            if self.test_input_presence(production_ds,
                                                        'heat, central or small-scale, other than natural gas'):
                                production_ds = self.change_heat(production_ds, region,
                                                                 'heat, central or small-scale, other than natural gas')

                            self.ei_wurst.append(production_ds)
            # filter specific technologies
            if techno == 'tap water':
                # loop through the different regions using these processes
                for region in used_regions:
                    # if a process for the region does not exist, we create that process
                    if region not in already_existing[techno]:
                        # for the technology "tap water" there are multiple "sub-technologies" available
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
                            # for regions based on a copy of RoW, simply copy the "RoW" process
                            if magic_plumbering[region] == 'RoW':
                                # tap water production with seawater reverse osmosis is only available at GLO level
                                if 'seawater reverse osmosis' in tap_water_techno:
                                    production_ds = copy.deepcopy(self.ei_in_dict[(techno, 'GLO', tap_water_techno)])
                                # others are available with RoW
                                else:
                                    production_ds = copy.deepcopy(self.ei_in_dict[(techno, 'RoW', tap_water_techno)])
                            else:
                                # tap water production with seawater reverse osmosis is only available at GLO level
                                if 'seawater reverse osmosis' not in tap_water_techno:
                                    try:
                                        production_ds = copy.deepcopy(
                                            self.ei_in_dict[(techno, 'Europe without Switzerland', tap_water_techno)])
                                    except KeyError:
                                        production_ds = copy.deepcopy(
                                            self.ei_in_dict[(techno, 'RoW', tap_water_techno)])

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
                            # adapt the metadata of the production flow itself
                            [i for i in production_ds['exchanges'] if i['type'] == 'production'][0]['code'] = \
                            production_ds['code']
                            [i for i in production_ds['exchanges'] if i['type'] == 'production'][0][
                                'database'] = self.name_ei_with_regionalized_biosphere
                            [i for i in production_ds['exchanges'] if i['type'] == 'production'][0]['location'] = \
                            production_ds[
                                'location']
                            [i for i in production_ds['exchanges'] if i['type'] == 'production'][0]['input'] = (
                                self.name_ei_with_regionalized_biosphere, production_ds['code'])

                            # regionalize created process
                            if self.test_input_presence(production_ds, 'electricity', extra='aluminium/electricity'):
                                production_ds = self.change_aluminium_electricity(production_ds, region)
                            elif self.test_input_presence(production_ds, 'electricity', extra='cobalt/electricity'):
                                production_ds = self.change_cobalt_electricity(production_ds)
                            elif self.test_input_presence(production_ds, 'electricity', extra='voltage'):
                                production_ds = self.change_electricity(production_ds, region)
                            if self.test_input_presence(production_ds, 'municipal solid waste'):
                                production_ds = self.change_waste(production_ds, region)
                            if self.test_input_presence(production_ds, 'heat, district or industrial, natural gas'):
                                production_ds = self.change_heat(production_ds, region,
                                                                 'heat, district or industrial, natural gas')
                            if self.test_input_presence(production_ds,
                                                        'heat, district or industrial, other than natural gas'):
                                production_ds = self.change_heat(production_ds, region,
                                                                 'heat, district or industrial, other than natural gas')
                            if self.test_input_presence(production_ds,
                                                        'heat, central or small-scale, other than natural gas'):
                                production_ds = self.change_heat(production_ds, region,
                                                                 'heat, central or small-scale, other than natural gas')

                            self.ei_wurst.append(production_ds)

        # need to re-extract the dictionary because we added new processes to ei_wurst
        self.ei_in_dict = {(i['reference product'], i['location'], i['name']): i for i in self.ei_wurst}

        # final step, make the ecoinvent processes use these newly created regionalized processes
        for process in self.ei_wurst:
            # loop through exchanges of the process
            for exc in process['exchanges']:
                # if the exchange is a technosphere exchange
                if exc['type'] == 'technosphere':
                    # if the reference product is one of the technology producing water
                    if exc['product'] in techno_water_flows:
                        # if the location if not part of the already existing processes
                        if process['location'] not in already_existing[exc['product']]:
                            # annoying little flow at 0.0001g/kg of decarbonised water produced in RoW, just remove it
                            if not (exc['product'] == 'water, decarbonised' and exc[
                                'name'] == 'diethyl ether production'):
                                try:
                                    replace_process = self.ei_in_dict[
                                        (exc['product'], process['location'], exc['name'])]
                                except KeyError:
                                    if exc['name'] == 'market for tap water':
                                        replace_process = self.ei_in_dict[
                                            (exc['product'], process['location'], 'market group for tap water')]
                                    if exc['name'] == 'market group for tap water':
                                        replace_process = self.ei_in_dict[
                                            (exc['product'], process['location'], 'market for tap water')]
                                    else:
                                        try:
                                            replace_process = self.ei_in_dict[
                                                (exc['product'], 'Europe without Switzerland', exc['name'])]
                                        except KeyError:
                                            print(exc['name'], process['location'], exc['product'], process['name'])
                                exc['code'] = replace_process['code']
                                exc['name'] = replace_process['name']
                                exc['product'] = replace_process['reference product']
                                exc['input'] = (self.name_ei_with_regionalized_biosphere, exc['code'])
