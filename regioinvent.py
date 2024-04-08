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
import json
import pkg_resources
import brightway2 as bw2
import uuid
import sqlite3
import logging
import sys
import os


class Regioinvent:
    def __init__(self, trade_database_path, regioinvent_database_name, bw_project_name, ecoinvent_database_name):

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

        # block prints from brightway (remove for debugging as brightway can have important info)
        sys.stdout = open(os.devnull, 'w')

        bw2.projects.set_current(bw_project_name)
        self.ecoinvent_database_name = ecoinvent_database_name
        self.new_regionalized_ecoinvent_database_name = ecoinvent_database_name + ' biosphere flows regionalized'
        self.name_regionalized_biosphere_database = 'biosphere_regionalized_biosphere_flows'
        self.conn = sqlite3.connect(trade_database_path)

        with open(pkg_resources.resource_filename(__name__, '/Data/HS_to_ecoinvent_name.json'), 'r') as f:
            self.hs_class_to_eco = json.load(f)
        with open(pkg_resources.resource_filename(__name__, '/Data/HS_to_exiobase_name.json'), 'r') as f:
            self.hs_class_to_exio = json.load(f)
        with open(pkg_resources.resource_filename(__name__, '/Data/country_to_ecoinvent_regions.json'), 'r') as f:
            self.country_to_ecoinvent_regions = json.load(f)
        with open(pkg_resources.resource_filename(__name__, '/Data/country_to_ecoinvent_regions.json'), 'r') as f:
            self.country_to_exiobase_regions = json.load(f)
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

        self.water_flows_in_ecoinvent = ['Water', 'Water, cooling, unspecified natural origin',
                                         'Water, lake',
                                         'Water, river',
                                         'Water, turbine use, unspecified natural origin',
                                         'Water, unspecified natural origin',
                                         'Water, well, in ground']

        if regioinvent_database_name in bw2.databases:
            self.logger.info("Overwriting existing "+regioinvent_database_name+" database...")
            del bw2.databases[regioinvent_database_name]

        self.logger.info("Creating empty database for "+regioinvent_database_name+"...")
        self.create_empty_bw2_database(regioinvent_database_name)

        if self.name_regionalized_biosphere_database not in bw2.databases:
            self.logger.info("Creating regionalized biosphere database...")
            self.create_regionalized_biosphere_flows()

        if self.new_regionalized_ecoinvent_database_name not in bw2.databases:
            self.logger.info("Creating regionalized ecoinvent database...")
            self.create_ecoinvent_with_regionalized_biosphere_flows()

        if 'IMPACT World+ Damage 2.0.1_regionalized' not in [i[0] for i in list(bw2.methods)]:
            self.logger.info("Linking regionalized LCIA method to regionalized biosphere database...")
            self.link_regionalized_biosphere_to_regionalized_CFs()

    def create_regionalized_biosphere_flows(self):

        locations_for_water_flows = []
        for act in [i for i in bw2.Database(self.ecoinvent_database_name)]:
            for exc in [i for i in act.biosphere()]:
                if 'name' in exc.as_dict():
                    if exc.as_dict()['name'] in self.water_flows_in_ecoinvent:
                        if act.as_dict()['location'] not in locations_for_water_flows:
                            locations_for_water_flows.append(act.as_dict()['location'])

        water_flows_to_create = []
        for water_flow_type in self.water_flows_in_ecoinvent:
            for location in locations_for_water_flows:
                water_flows_to_create.append(', '.join([water_flow_type, location]))

        biosphere_data = {}
        for water_flow in water_flows_to_create:
            for location in locations_for_water_flows:
                if len(water_flow.split(', ' + location)) > 1 and water_flow.split(', ' + location)[1] == '':
                    if ','.join(water_flow.split(', ' + location)[:-1]) != 'Water':
                        # unique identifier
                        unique_id = uuid.uuid4().hex
                        # format data into brightway-readable format
                        biosphere_data[(self.name_regionalized_biosphere_database, unique_id)] = {
                            "name": water_flow,
                            "unit": 'cubic meter',
                            "type": 'emission',
                            "categories": ('natural resource', 'in water'),
                            "code": unique_id}
                        if ','.join(water_flow.split(', ' + location)[:-1]) == 'Water, unspecified natural origin':
                            unique_id = uuid.uuid4().hex
                            biosphere_data[(self.name_regionalized_biosphere_database, unique_id)] = {
                                "name": water_flow,
                                "unit": 'cubic meter',
                                "type": 'emission',
                                "categories": ('natural resource', 'in ground'),
                                "code": unique_id}
                            unique_id = uuid.uuid4().hex
                            biosphere_data[(self.name_regionalized_biosphere_database, unique_id)] = {
                                "name": water_flow,
                                "unit": 'cubic meter',
                                "type": 'emission',
                                "categories": ('natural resource', 'fossil well'),
                                "code": unique_id}
                    else:
                        for subcomp in ['surface water', 'ground-', 'ground-, long-term', 'fossil well']:
                            # unique identifier
                            unique_id = uuid.uuid4().hex
                            # format data into brightway-readable format
                            biosphere_data[(self.name_regionalized_biosphere_database, unique_id)] = {
                                "name": water_flow,
                                "unit": 'cubic meter',
                                "type": 'emission',
                                "categories": ('water', subcomp),
                                "code": unique_id}

                        # specific for unspecified subcomp
                        unique_id = uuid.uuid4().hex
                        biosphere_data[(self.name_regionalized_biosphere_database, unique_id)] = {
                            "name": water_flow,
                            "unit": 'cubic meter',
                            "type": 'emission',
                            "categories": ('water',),
                            "code": unique_id}

        bw2.Database(self.name_regionalized_biosphere_database).write(biosphere_data)

    def create_ecoinvent_with_regionalized_biosphere_flows(self):

        relevant_subcomps = [('natural resource', 'in water'), ('natural resource', 'in ground'),
                             ('natural resource', 'fossil well'),
                             ('water',), ('water', 'surface water'), ('water', 'ground-'),
                             ('water', 'ground-, long-term'),
                             ('water', 'fossil well')]

        data = {}
        for act in [i for i in bw2.Database(self.ecoinvent_database_name)]:
            # first, create all activities with modified metadata
            dict_activity = act.as_dict().copy()
            dict_activity['database'] = self.new_regionalized_ecoinvent_database_name
            data[(self.new_regionalized_ecoinvent_database_name, act.key[1])] = dict_activity
            data[(self.new_regionalized_ecoinvent_database_name, act.key[1])]['exchanges'] = []

            # second, create the exchanges with new regionalized water flows
            for exc in [i for i in act.exchanges()]:
                # check exchange has a name
                if 'name' in exc.as_dict():
                    # check exchange is a water flow
                    if exc.as_dict()['name'] in self.water_flows_in_ecoinvent:
                        # check it's a water flows in water comp and not ocean subcomp
                        if (bw2.Database('biosphere3').get(exc.as_dict()['flow']).as_dict()['categories'] in relevant_subcomps):

                            # identify the regionalized water flow ID
                            new_water_flow_id = [i for i in bw2.Database(self.name_regionalized_biosphere_database) if (
                                    i['name'] == exc.as_dict()['name'] + ', ' + act.as_dict()['location'] and
                                    i['categories'] == bw2.Database('biosphere3').get(
                                exc.as_dict()['flow']).as_dict()['categories'])][0].key

                            # add the exchange to the list
                            exc.as_dict()['flow'] = new_water_flow_id[1]
                            exc.as_dict()['input'] = new_water_flow_id
                            exc.as_dict()['name'] = exc.as_dict()['name'] + ', ' + act.as_dict()['location']
                            exc.as_dict()['output'] = (self.new_regionalized_ecoinvent_database_name, act.key[1])
                            data[(self.new_regionalized_ecoinvent_database_name, act.key[1])]['exchanges'].append(
                                exc.as_dict())

                        else:
                            # if it's not a regionalized water flow exchange, just copy paste the exchange
                            exc.as_dict()['output'] = (self.new_regionalized_ecoinvent_database_name, act.key[1])
                            data[(self.new_regionalized_ecoinvent_database_name, act.key[1])]['exchanges'].append(
                                exc.as_dict())
                    else:
                        # if it's not a regionalized water flow exchange, just copy paste the exchange
                        if exc.as_dict()['type'] == 'biosphere':
                            exc.as_dict()['input'] = ('biosphere3', exc.as_dict()['input'][1])
                        else:
                            exc.as_dict()['input'] = (
                            self.new_regionalized_ecoinvent_database_name, exc.as_dict()['input'][1])
                        exc.as_dict()['output'] = (self.new_regionalized_ecoinvent_database_name, act.key[1])
                        data[(self.new_regionalized_ecoinvent_database_name, act.key[1])]['exchanges'].append(exc.as_dict())
                else:
                    # if it's not a regionalized water flow exchange, just copy paste the exchange
                    if exc.as_dict()['type'] == 'biosphere':
                        exc.as_dict()['input'] = ('biosphere3', exc.as_dict()['input'][1])
                    else:
                        exc.as_dict()['input'] = (self.new_regionalized_ecoinvent_database_name, exc.as_dict()['input'][1])
                    exc.as_dict()['output'] = (self.new_regionalized_ecoinvent_database_name, act.key[1])
                    data[(self.new_regionalized_ecoinvent_database_name, act.key[1])]['exchanges'].append(exc.as_dict())

        bw2.Database(self.new_regionalized_ecoinvent_database_name).write(data)

    def link_regionalized_biosphere_to_regionalized_CFs(self):

        # load regular IW+
        ei_iw = pd.read_excel(pkg_resources.resource_filename(
            __name__, '/Data/impact_world_plus_2.0.1_expert_version_ecoinvent_v39.xlsx')).drop('Unnamed: 0', axis=1)

        not_regio_bw_flows_with_codes = (pd.DataFrame(
                [(i.as_dict()['name'], i.as_dict()['categories'][0], i.as_dict()['categories'][1], i.as_dict()['code'])
                 if len(i.as_dict()['categories']) == 2
                 else (i.as_dict()['name'], i.as_dict()['categories'][0], 'unspecified', i.as_dict()['code'])
                 for i in bw2.Database('biosphere3')],
                columns=['Elem flow name', 'Compartment', 'Sub-compartment', 'code']))

        ei_iw_db = ei_iw.merge(not_regio_bw_flows_with_codes, on=['Elem flow name', 'Compartment', 'Sub-compartment'])

        # make regionalized iw+
        iw = pd.read_excel(pkg_resources.resource_filename(
            __name__, '/Data/impact_world_plus_2.0.1_water_categories.xlsx')).drop(['Unnamed: 0'], axis=1)
        regio_bw_flows_with_codes = (pd.DataFrame(
                [(i.as_dict()['name'], i.as_dict()['categories'][0], i.as_dict()['categories'][1], i.as_dict()['code'])
                 if len(i.as_dict()['categories']) == 2
                 else (i.as_dict()['name'], i.as_dict()['categories'][0], 'unspecified', i.as_dict()['code'])
                 for i in bw2.Database(self.name_regionalized_biosphere_database)],
                columns=['Elem flow name', 'Compartment', 'Sub-compartment', 'code']))

        with open(pkg_resources.resource_filename(__name__, '/Data/comps.json'), 'r') as f:
            comps = json.load(f)

        with open(pkg_resources.resource_filename(__name__, '/Data/subcomps.json'), 'r') as f:
            subcomps = json.load(f)

        iw.Compartment = [comps[i] for i in iw.Compartment]
        iw.loc[:, 'Sub-compartment'] = [subcomps[i] for i in iw.loc[:, 'Sub-compartment']]

        # special cases: fossil well subcomp in water comp = ground- subcomp
        df = iw.loc[[i for i in iw.index if (iw.loc[i, 'Sub-compartment'] == 'ground-' and
                                             iw.loc[i, 'Compartment'] == 'water')]].copy()
        df.loc[:, 'Sub-compartment'] = 'fossil well'
        iw = pd.concat([iw, df])
        iw = clean_up_dataframe(iw)

        # special cases: fossil well subcomp in raw comp = in ground subcomp
        df = iw.loc[[i for i in iw.index if (iw.loc[i, 'Sub-compartment'] == 'in ground' and
                                             iw.loc[i, 'Compartment'] == 'natural resource')]].copy()
        df.loc[:, 'Sub-compartment'] = 'fossil well'
        iw = pd.concat([iw, df])
        iw = clean_up_dataframe(iw)

        concordance = pd.read_excel(pkg_resources.resource_filename(__name__, '/Data/concordance.xlsx'))
        concordance = dict(list(zip(concordance.loc[:, 'Name ecoinvent'], concordance.loc[:, 'Name iw+'])))
        regio_bw_flows_with_codes.loc[:, 'Elem flow name'] = [
            concordance[i] for i in regio_bw_flows_with_codes.loc[:, 'Elem flow name']]

        ei_in_bw = pd.concat(
            [ei_iw_db, iw.merge(regio_bw_flows_with_codes, on=['Elem flow name', 'Compartment', 'Sub-compartment'])])
        ei_in_bw.set_index(['Impact category', 'CF unit'], inplace=True)

        # make total HH and total EQ categories
        total_HH = ei_in_bw.loc(axis=0)[:, 'DALY'].groupby(
            by=['code', 'Compartment', 'Sub-compartment', 'Elem flow name', 'CAS number',
                'Elem flow unit', 'MP or Damage']).sum().reset_index()
        total_HH.loc[:, 'Impact category'] = 'Total human health'
        total_HH.loc[:, 'CF unit'] = 'DALY'
        total_HH = total_HH.set_index(['Impact category', 'CF unit'])

        total_EQ = ei_in_bw.loc(axis=0)[:, 'PDF.m2.yr'].groupby(
            by=['code', 'Compartment', 'Sub-compartment', 'Elem flow name', 'CAS number',
                'Elem flow unit', 'MP or Damage']).sum().reset_index()
        total_EQ.loc[:, 'Impact category'] = 'Total ecosystem quality'
        total_EQ.loc[:, 'CF unit'] = 'PDF.m2.yr'
        total_EQ = total_EQ.set_index(['Impact category', 'CF unit'])

        ei_in_bw = pd.concat([ei_in_bw, total_HH, total_EQ])

        impact_categories = ei_in_bw.index.drop_duplicates()
        for ic in impact_categories:
            if ei_in_bw.loc[[ic], 'MP or Damage'].iloc[0] == 'Midpoint':
                mid_end = 'Midpoint'
                # create the name of the method
                name = ('IMPACT World+ ' + mid_end + ' ' + '2.0.1_regionalized', 'Midpoint', ic[0])
            else:
                mid_end = 'Damage'
                # create the name of the method
                if ic[1] == 'DALY':
                    name = ('IMPACT World+ ' + mid_end + ' ' + '2.0.1_regionalized', 'Human health', ic[0])
                else:
                    name = ('IMPACT World+ ' + mid_end + ' ' + '2.0.1_regionalized', 'Ecosystem quality', ic[0])

            # initialize the "Method" method
            new_method = bw2.Method(name)
            # register the new method
            new_method.register()
            # set its unit
            new_method.metadata["unit"] = ic[1]

            df = ei_in_bw.loc[[ic], ['code', 'CF value']].copy()
            df.set_index('code', inplace=True)

            data = []
            biosphere3 = [i.as_dict()['code'] for i in bw2.Database('biosphere3')]
            biosphere3_regio = [i.as_dict()['code'] for i in bw2.Database(self.name_regionalized_biosphere_database)]
            for stressor in df.index:
                if stressor in biosphere3:
                    data.append((('biosphere3', stressor), df.loc[stressor, 'CF value']))
                elif stressor in biosphere3_regio:
                    data.append(((self.name_regionalized_biosphere_database, stressor), df.loc[stressor, 'CF value']))
            new_method.write(data)

    def first_order_regionalization(self):

        self.logger.info("Performing first order regionalization...")

        export_data = pd.read_sql('SELECT * FROM "Export data"', con=self.conn)
        domestic_prod = pd.read_sql('SELECT * FROM "Domestic production"', con=self.conn)
        # only keep total export values (no need for destination detail)
        export_data = export_data[export_data.partnerISO == 'W00']
        # check if AtlQty is defined whenever Qty is empty. If it is, then use that value.
        export_data.loc[export_data.qty == 0, 'qty'] = export_data.loc[export_data.qty == 0, 'altQty']
        # don't need AtlQty afterwards and drop zero values
        export_data = export_data.drop('altQty', axis=1)
        export_data = export_data.loc[export_data.qty != 0]
        # check units match for all data
        assert len(set(export_data.loc[export_data.qty != 0, 'qtyUnitAbbr']) - {'N/A'}) == 1

        self.domestic_data = export_data.copy('deep')
        self.domestic_data.loc[:, 'COMTRADE_reporter'] = self.domestic_data.reporterISO.copy('deep')
        # go from ISO3 codes to country codes for respective databases
        export_data.reporterISO = [self.convert_ecoinvent_geos[i] for i in export_data.reporterISO]
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

        for product in self.hs_class_to_eco:
            cmd_export_data = export_data[export_data.cmdCode == product].copy('deep')
            # calculate the average export volume for each country
            cmd_export_data = cmd_export_data.groupby('reporterISO').agg({'qty': 'mean'})
            exporters = (cmd_export_data.qty / cmd_export_data.qty.sum()).sort_values(ascending=False)
            # only keep the countries representing 99% of global exports of the product and create a RoW from that
            limit = exporters.index.get_loc(exporters[exporters.cumsum() > 0.99].index[0]) + 1
            remainder = exporters.iloc[limit:].sum()
            exporters = exporters.iloc[:limit]
            if 'RoW' in exporters.index:
                exporters.loc['RoW'] += remainder
            else:
                exporters.loc['RoW'] = remainder
            production_archetypes = [i for i in bw2.Database(self.new_regionalized_ecoinvent_database_name).search(
                self.hs_class_to_eco[product]) if i.as_dict()['reference product'] == self.hs_class_to_eco[product] and
                                     "market" not in i.as_dict()['name']]
            available_geographies = [i.as_dict()['location'] for i in production_archetypes]

            # create a global market activity for the commodity
            global_market_activity = bw2.Database("Regioinvent").new_activity(uuid.uuid4().hex)
            global_market_activity['name'] = 'production market for ' + self.hs_class_to_eco[product]
            global_market_activity['reference product'] = self.hs_class_to_eco[product]
            global_market_activity['type'] = 'process'
            global_market_activity['unit'] = 'kilogram'
            global_market_activity.save()
            # create the production flow
            new_exc = global_market_activity.new_exchange(input=global_market_activity.key, amount=1, type='production')
            new_exc.save()

            def copy_reference_process(reference_geography, new_geography):
                reference_activity = [i for i in production_archetypes if i.as_dict()['location'] == reference_geography][0]
                new_act = reference_activity.copy()
                new_act.update({'database': 'Regioinvent'})
                new_act.update({'location': new_geography})
                new_act.save()
                # add new activity to global market
                new_exc = global_market_activity.new_exchange(input=new_act.key, amount=exporters.loc[new_geography],
                                                              type='technosphere')
                new_exc.save()
                return new_act

            for exporter in exporters.index:
                if exporter in available_geographies and exporter not in ['RoW']:
                    # copy activity from reference
                    new_act = copy_reference_process(exporter, exporter)

                    # check if process uses inputs that can already be regionalized.
                    electricity_input = self.test_input_presence(new_act, 'electricity', 'technosphere')
                    waste_input = self.test_input_presence(new_act, 'municipal solid waste', 'technosphere')
                    heat_district_ng_input = self.test_input_presence(
                        new_act, 'heat, district or industrial, natural gas', 'technosphere')
                    heat_district_non_ng_input = self.test_input_presence(
                        new_act, 'heat, district or industrial, other than natural gas', 'technosphere')
                    heat_small_scale_non_ng_input = self.test_input_presence(
                        new_act, 'heat, central or small-scale, other than natural gas', 'technosphere')
                    water_input = self.test_input_presence(new_act, self.water_flows_in_ecoinvent, 'biosphere',
                                                           region=exporter)

                    # check if electricity needs spatialization for country (maybe it is already spatialized)
                    if electricity_input:
                        if ([bw2.Database(self.new_regionalized_ecoinvent_database_name).get(i.input[1]) for i in new_act.exchanges()
                             if 'electricity' in i.as_dict()['name']][0].as_dict()['location'] != exporter):
                            # spatialize electricity
                            key, amount, name = self.change_electricity(new_act, exporter)
                            new_elec_exc = new_act.new_exchange(input=key, amount=amount, type='technosphere', name=name)
                            new_elec_exc.save()
                    if waste_input:
                        if ([bw2.Database(self.new_regionalized_ecoinvent_database_name).get(i.input[1]) for i in new_act.exchanges()
                             if 'municipal solid waste' in i.as_dict()['name']][0].as_dict()['location'] != exporter):
                            # spatialize waste
                            key, amount = self.change_waste(new_act, exporter)
                            new_waste_exc = new_act.new_exchange(input=key, amount=amount, type='technosphere',
                                                                 name='municipal solid waste')
                            new_waste_exc.save()
                    if heat_district_ng_input:
                        # check if heat needs spatialization for country
                        heat_flow = 'heat, district or industrial, natural gas'
                        if ([bw2.Database(self.new_regionalized_ecoinvent_database_name).get(i.input[1]) for i in new_act.exchanges()
                             if heat_flow in i.as_dict()['name']][0].as_dict()['location'] != exporter):
                            # spatialize heat
                            distrib_heat = self.change_heat(new_act, exporter, heat_flow)
                            for heat_flow in distrib_heat:
                                new_heat_exc = new_act.new_exchange(input=heat_flow[0], amount=heat_flow[1],
                                                                    type='technosphere', name=heat_flow)
                                new_heat_exc.save()
                    if heat_district_non_ng_input:
                        # check if heat needs spatialization for country
                        heat_flow = 'heat, district or industrial, other than natural gas'
                        if ([bw2.Database(self.new_regionalized_ecoinvent_database_name).get(i.input[1]) for i in new_act.exchanges()
                             if heat_flow in i.as_dict()['name']][0].as_dict()['location'] != exporter):
                            # spatialize heat
                            distrib_heat = self.change_heat(new_act, exporter, heat_flow)
                            for heat_flow in distrib_heat:
                                new_heat_exc = new_act.new_exchange(input=heat_flow[0], amount=heat_flow[1],
                                                                    type='technosphere', name=heat_flow)
                                new_heat_exc.save()
                    if heat_small_scale_non_ng_input:
                        # check if heat needs spatialization for country
                        heat_flow = 'heat, central or small-scale, other than natural gas'
                        if ([bw2.Database(self.new_regionalized_ecoinvent_database_name).get(i.input[1]) for i in new_act.exchanges()
                             if heat_flow in i.as_dict()['name']][0].as_dict()['location'] != exporter):
                            # spatialize heat
                            distrib_heat = self.change_heat(new_act, exporter, heat_flow)
                            for heat_flow in distrib_heat:
                                new_heat_exc = new_act.new_exchange(input=heat_flow[0], amount=heat_flow[1],
                                                                    type='technosphere', name=heat_flow)
                                new_heat_exc.save()
                    if water_input:
                        # check if water elementary flows need spatialization for country
                        if new_act.as_dict()['location'] != exporter:
                            water_values = self.change_water(new_act, exporter)
                            for water_type in water_values:
                                new_water_exc = new_act.new_exchange(
                                    input=water_values[water_type]['key'],
                                    amount=water_values[water_type]['amount'],
                                    type='biosphere',
                                    name=bw2.Database(self.name_regionalized_biosphere_database).get(
                                        water_values[water_type]['key'][1]).as_dict()['name'],
                                    flow=water_values[water_type]['key'][1])
                                new_water_exc.save()

                elif (exporter in self.country_to_ecoinvent_regions and
                      self.country_to_ecoinvent_regions[exporter] in available_geographies):
                    # copy activity from reference
                    new_act = copy_reference_process(self.country_to_ecoinvent_regions[exporter], exporter)

                    # check if process uses inputs that can already be regionalized.
                    electricity_input = self.test_input_presence(new_act, 'electricity', 'technosphere')
                    waste_input = self.test_input_presence(new_act, 'municipal solid waste', 'technosphere')
                    heat_district_ng_input = self.test_input_presence(
                        new_act, 'heat, district or industrial, natural gas', 'technosphere')
                    heat_district_non_ng_input = self.test_input_presence(
                        new_act, 'heat, district or industrial, other than natural gas', 'technosphere')
                    heat_small_scale_non_ng_input = self.test_input_presence(
                        new_act, 'heat, central or small-scale, other than natural gas', 'technosphere')
                    water_input = self.test_input_presence(new_act, self.water_flows_in_ecoinvent, 'biosphere',
                                                         region=self.country_to_ecoinvent_regions[exporter])

                    if electricity_input:
                        # spatialize electricity
                        key, amount, name = self.change_electricity(new_act, exporter)
                        new_elec_exc = new_act.new_exchange(input=key, amount=amount, type='technosphere', name=name)
                        new_elec_exc.save()
                    if waste_input:
                        # spatialize waste
                        key, amount = self.change_waste(new_act, exporter,
                                                        region=self.country_to_ecoinvent_regions[exporter])
                        new_waste_exc = new_act.new_exchange(input=key, amount=amount, type='technosphere',
                                                             name='municipal solid waste')
                        new_waste_exc.save()
                    if heat_district_ng_input:
                        # spatialize heat
                        heat_flow = 'heat, district or industrial, natural gas'
                        distrib_heat = self.change_heat(new_act, exporter, heat_flow,
                                                        region=self.country_to_ecoinvent_regions[exporter])
                        for heat_flow in distrib_heat:
                            new_heat_exc = new_act.new_exchange(input=heat_flow[0], amount=heat_flow[1],
                                                                type='technosphere', name=heat_flow)
                            new_heat_exc.save()
                    if heat_district_non_ng_input:
                        # spatialize heat
                        heat_flow = 'heat, district or industrial, other than natural gas'
                        distrib_heat = self.change_heat(new_act, exporter, heat_flow,
                                                        region=self.country_to_ecoinvent_regions[exporter])
                        for heat_flow in distrib_heat:
                            new_heat_exc = new_act.new_exchange(input=heat_flow[0], amount=heat_flow[1],
                                                                type='technosphere', name=heat_flow)
                            new_heat_exc.save()
                    if heat_small_scale_non_ng_input:
                        # spatialize heat
                        heat_flow = 'heat, central or small-scale, other than natural gas'
                        distrib_heat = self.change_heat(new_act, exporter, heat_flow,
                                                        region=self.country_to_ecoinvent_regions[exporter])
                        for heat_flow in distrib_heat:
                            new_heat_exc = new_act.new_exchange(input=heat_flow[0], amount=heat_flow[1],
                                                                type='technosphere', name=heat_flow)
                            new_heat_exc.save()
                    if water_input:
                        water_values = self.change_water(new_act, exporter,
                                                         region=self.country_to_ecoinvent_regions[exporter])
                        for water_type in water_values:
                            new_water_exc = new_act.new_exchange(
                                input=water_values[water_type]['key'],
                                amount=water_values[water_type]['amount'],
                                type='biosphere',
                                name=bw2.Database(self.name_regionalized_biosphere_database).get(
                                    water_values[water_type]['key'][1]).as_dict()['name'],
                                flow=water_values[water_type]['key'][1])
                            new_water_exc.save()

                else:
                    # copy activity from reference
                    new_act = copy_reference_process("RoW", exporter)

                    # check if process uses inputs that can already be regionalized.
                    electricity_input = self.test_input_presence(new_act, 'electricity', 'technosphere')
                    waste_input = self.test_input_presence(new_act, 'municipal solid waste', 'technosphere')
                    heat_district_ng_input = self.test_input_presence(
                        new_act, 'heat, district or industrial, natural gas', 'technosphere')
                    heat_district_non_ng_input = self.test_input_presence(
                        new_act, 'heat, district or industrial, other than natural gas', 'technosphere')
                    heat_small_scale_non_ng_input = self.test_input_presence(
                        new_act, 'heat, central or small-scale, other than natural gas', 'technosphere')
                    water_input = self.test_input_presence(new_act, self.water_flows_in_ecoinvent, 'biosphere', region="RoW")

                    if electricity_input:
                        # spatialize electricity
                        key, amount, name = self.change_electricity(new_act, exporter)
                        new_elec_exc = new_act.new_exchange(input=key, amount=amount, type='technosphere', name=name)
                        new_elec_exc.save()
                    if waste_input:
                        # spatialize waste
                        key, amount = self.change_waste(new_act, exporter, region='RoW')
                        new_waste_exc = new_act.new_exchange(input=key, amount=amount, type='technosphere',
                                                             name='municipal solid waste')
                        new_waste_exc.save()
                    if heat_district_ng_input:
                        # spatialize heat
                        heat_flow = 'heat, district or industrial, natural gas'
                        distrib_heat = self.change_heat(new_act, exporter, heat_flow, region='RoW')
                        for heat_flow in distrib_heat:
                            new_heat_exc = new_act.new_exchange(input=heat_flow[0], amount=heat_flow[1],
                                                                type='technosphere', name=heat_flow)
                            new_heat_exc.save()
                    if heat_district_non_ng_input:
                        # spatialize heat
                        heat_flow = 'heat, district or industrial, other than natural gas'
                        distrib_heat = self.change_heat(new_act, exporter, heat_flow, region='RoW')
                        for heat_flow in distrib_heat:
                            new_heat_exc = new_act.new_exchange(input=heat_flow[0], amount=heat_flow[1],
                                                                type='technosphere', name=heat_flow)
                            new_heat_exc.save()
                    if heat_small_scale_non_ng_input:
                        # spatialize heat
                        heat_flow = 'heat, central or small-scale, other than natural gas'
                        distrib_heat = self.change_heat(new_act, exporter, heat_flow, region='RoW')
                        for heat_flow in distrib_heat:
                            new_heat_exc = new_act.new_exchange(input=heat_flow[0], amount=heat_flow[1],
                                                                type='technosphere', name=heat_flow)
                            new_heat_exc.save()
                    if water_input:
                        water_values = self.change_water(new_act, exporter, region='RoW')
                        for water_type in water_values:
                            new_water_exc = new_act.new_exchange(
                                input=water_values[water_type]['key'],
                                amount=water_values[water_type]['amount'],
                                type='biosphere',
                                name=bw2.Database(self.name_regionalized_biosphere_database).get(
                                    water_values[water_type]['key'][1]).as_dict()['name'],
                                flow=water_values[water_type]['key'][1])
                            new_water_exc.save()

    def create_consumption_markets(self):

        self.logger.info("Creating consumption markets...")

        import_data = pd.read_sql('SELECT * FROM "Import data"', con=self.conn)
        # remove trade with "World"
        import_data = import_data.drop(import_data[import_data.partnerISO == 'W00'].index)
        # go from ISO3 codes to ISO2 codes
        import_data.reporterISO = [self.convert_ecoinvent_geos[i] for i in import_data.reporterISO]
        import_data.partnerISO = [self.convert_ecoinvent_geos[i] for i in import_data.partnerISO]
        # check if AtlQty is defined whenever Qty is empty. If it is, then use that value.
        import_data.loc[import_data.qty == 0, 'qty'] = import_data.loc[import_data.qty == 0, 'altQty']
        # don't need AtlQty afterwards and drop zero values
        import_data = import_data.drop('altQty', axis=1)
        import_data = import_data.loc[import_data.qty != 0]
        # check units match for all data
        assert len(set(import_data.loc[import_data.qty != 0, 'qtyUnitAbbr']) - {'N/A'}) == 1

        # remove artefacts of domestic trade from international trade data
        import_data = import_data.drop(
            [i for i in import_data.index if import_data.reporterISO[i] == import_data.partnerISO[i]])
        # concatenate import and domestic data
        consumption_data = pd.concat([import_data, self.domestic_data.loc[:, import_data.columns]])

        for product in self.hs_class_to_eco:
            cmd_consumption_data = consumption_data[consumption_data.cmdCode == product].copy('deep')
            # calculate the average import volume for each country
            cmd_consumption_data = cmd_consumption_data.groupby(['reporterISO', 'partnerISO']).agg({'qty': 'mean'})
            # change to relative
            importers = (cmd_consumption_data.groupby(level=0).sum() / cmd_consumption_data.sum().sum()).sort_values(
                by='qty', ascending=False)
            # only keep importers till the 99% of total imports
            limit = importers.index.get_loc(importers[importers.cumsum() > 0.99].dropna().index[0]) + 1
            # aggregate the rest
            remainder = cmd_consumption_data.loc[importers.index[limit:]].groupby(level=1).sum()
            cmd_consumption_data = cmd_consumption_data.loc[importers.index[:limit]]
            # assign the aggregate to RoW
            cmd_consumption_data = pd.concat([cmd_consumption_data, pd.concat([remainder], keys=['RoW'])])
            cmd_consumption_data.index = pd.MultiIndex.from_tuples([i for i in cmd_consumption_data.index])
            cmd_consumption_data = cmd_consumption_data.sort_index()
            # for each importer, calculate the relative shares of each country it is importing from
            for importer in cmd_consumption_data.index.levels[0]:
                cmd_consumption_data.loc[importer, 'qty'] = (
                        cmd_consumption_data.loc[importer, 'qty'] / cmd_consumption_data.loc[importer, 'qty'].sum()).values
            # we need to add the aggregate to potentially already existing RoW exchanges
            cmd_consumption_data = pd.concat([cmd_consumption_data.drop('RoW'),
                                         pd.concat([cmd_consumption_data.loc['RoW'].groupby(level=0).sum()], keys=['RoW'])])
            # create the consumption markets
            for importer in cmd_consumption_data.index.levels[0]:
                new_import_data = bw2.Database("Regioinvent").new_activity(uuid.uuid4().hex)
                new_import_data['name'] = 'consumption market for ' + self.hs_class_to_eco[product]
                new_import_data['reference product'] = self.hs_class_to_eco[product]
                new_import_data['location'] = importer
                new_import_data['type'] = 'process'
                new_import_data['unit'] = 'kilogram'
                new_import_data.save()
                # create the production flow
                new_exc = new_import_data.new_exchange(input=new_import_data.key, amount=1, type='production')
                new_exc.save()

                available_trading_partners = [i.as_dict()['location'] for i in
                                              bw2.Database('Regioinvent').search(
                                                  self.hs_class_to_eco[product], limit=1000) if
                                              (i.as_dict()['reference product'] == self.hs_class_to_eco[product] and
                                               'consumption market' not in i.as_dict()['name'])]

                for trading_partner in cmd_consumption_data.loc[importer].index:
                    if trading_partner in available_trading_partners:
                        trading_partner_key = [i for i in bw2.Database('Regioinvent').search(
                            self.hs_class_to_eco[product], limit=1000) if (
                                i.as_dict()['reference product'] == self.hs_class_to_eco[product] and
                                'consumption market' not in i.as_dict()['name'] and
                                i.as_dict()['location'] == trading_partner)][0].key
                        new_trade_exc = new_import_data.new_exchange(
                            input=trading_partner_key,
                            amount=cmd_consumption_data.loc[(importer, trading_partner), 'qty'],
                            type='technosphere', name=self.hs_class_to_eco[product])
                        new_trade_exc.save()
                    elif trading_partner in self.country_to_ecoinvent_regions and self.country_to_ecoinvent_regions[
                        trading_partner] in available_trading_partners:
                        trading_partner_key = [i for i in bw2.Database('Regioinvent').search(
                            self.hs_class_to_eco[product], limit=1000) if
                         (i.as_dict()['reference product'] == self.hs_class_to_eco[product] and
                          'consumption market' not in i.as_dict()['name'] and i.as_dict()['location'] ==
                          self.country_to_ecoinvent_regions[trading_partner])][0].key
                        new_trade_exc = new_import_data.new_exchange(
                            input=trading_partner_key,
                            amount=cmd_consumption_data.loc[(importer, trading_partner), 'qty'],
                            type='technosphere', name=self.hs_class_to_eco[product])
                        new_trade_exc.save()
                    else:
                        trading_partner_key = [i for i in bw2.Database('Regioinvent').search(
                            self.hs_class_to_eco[product], limit=1000) if
                         (i.as_dict()['reference product'] == self.hs_class_to_eco[product] and
                          'consumption market' not in i.as_dict()['name'] and i.as_dict()['location'] == 'RoW')][0].key
                        new_trade_exc = new_import_data.new_exchange(
                            input=trading_partner_key,
                            amount=cmd_consumption_data.loc[(importer, trading_partner), 'qty'],
                            type='technosphere', name=self.hs_class_to_eco[product])
                        new_trade_exc.save()

    def second_order_regionalization(self):

        self.logger.info("Performing second order regionalization...")

        for process in bw2.Database('Regioinvent'):
            for inputt in process.technosphere():
                # if it's an ecoinvent input
                if inputt.as_dict()['input'][0] == self.new_regionalized_ecoinvent_database_name:
                    # check if we regionalized the commodity
                    if inputt.as_dict()['name'] in self.hs_class_to_eco.values():
                        exchange_amount = sum([i.amount for i in process.technosphere() if
                                               i.as_dict()['name'] == inputt.as_dict()['name']])
                        available_geographies = [j.as_dict()['location'] for j in
                                                 bw2.Database('Regioinvent').search('consumption market', limit=1000) if
                                                 j.as_dict()['reference product'] == inputt.as_dict()['name']]
                        if process.as_dict()['location'] in available_geographies:
                            new_exchange_key = \
                            [j for j in bw2.Database('Regioinvent').search('consumption market', limit=1000) if (
                                    j.as_dict()['reference product'] == inputt.as_dict()['name'] and j.as_dict()[
                                'location'] == process.as_dict()['location'])][0].key
                        elif (process.as_dict()['location'] in self.country_to_ecoinvent_regions and
                              self.country_to_ecoinvent_regions[process.as_dict()['location']] in available_geographies):
                            new_exchange_key = \
                            [j for j in bw2.Database('Regioinvent').search('consumption market', limit=1000) if (
                                    j.as_dict()['reference product'] == inputt.as_dict()['name'] and j.as_dict()[
                                'location'] ==
                                    self.country_to_ecoinvent_regions[process.as_dict()['location']])][0].key
                        else:
                            new_exchange_key = \
                            [j for j in bw2.Database('Regioinvent').search('consumption market', limit=1000) if (
                                    j.as_dict()['reference product'] == inputt.as_dict()['name'] and j.as_dict()[
                                'location'] == 'RoW')][0].key

                        [i.delete() for i in process.technosphere() if i.as_dict()['name'] == inputt.as_dict()['name']]
                        new_exc = process.new_exchange(input=new_exchange_key, amount=exchange_amount,
                                                       type='technosphere', name=inputt.as_dict()['name'])
                        new_exc.save()

    # ==================================================================================================================
    # -------------------------------------------Supporting functions---------------------------------------------------

    def test_input_presence(self, process, input_name, flow_type, region=None):
        if flow_type == 'technosphere':
            return len([bw2.Database(self.new_regionalized_ecoinvent_database_name).get(i.input[1]) for i in
                        process.exchanges() if input_name in i.as_dict()['name']]) != 0
        elif flow_type == 'biosphere':
            for water_type in self.water_flows_in_ecoinvent:
                try:
                    if len([bw2.Database(self.name_regionalized_biosphere_database).get(i.input[1]) for i in
                            process.exchanges() if (i.input[0] == self.name_regionalized_biosphere_database and
                         i.as_dict()['name'].split(', ' + region)[0] == water_type)][0]):
                        return True
                except IndexError:
                    pass

    def change_electricity(self, new_act, exporter):
        # limit: RoW associated with GLO instead of recalculating
        # limit: same for regions exporting (e.g., RAS)
        qty_of_electricity = sum(
            [i.as_dict()['amount'] for i in new_act.exchanges() if 'electricity' in i.as_dict()['name']])
        type_of_electricity = [i.as_dict()['name'] for i in new_act.exchanges() if 'electricity' in i.as_dict()['name']][0]
        if exporter != 'RoW':
            key = [i for i in
                   bw2.Database(self.new_regionalized_ecoinvent_database_name).search(
                       type_of_electricity, filter={'name': 'market'}, limit=1000)
                   if i.as_dict()['reference product'] == type_of_electricity and i.as_dict()['location'] == exporter
                   and (i.as_dict()['activity type'] == 'market activity' or i.as_dict()[
                    'activity type'] == 'market group')][0].key
        else:
            # if the exporter is RoW -> assign global electricity process
            key = [i for i in
                   bw2.Database(self.new_regionalized_ecoinvent_database_name).search(
                       type_of_electricity, filter={'name': 'market'}, limit=1000)
                   if i.as_dict()['reference product'] == type_of_electricity and i.as_dict()['location'] == 'GLO'
                   and (i.as_dict()['activity type'] == 'market activity' or i.as_dict()[
                    'activity type'] == 'market group')][0].key
        # delete old flow(s) of electricity
        [i.delete() for i in new_act.exchanges() if 'electricity' in i.as_dict()['name']]

        return key, qty_of_electricity, type_of_electricity

    def change_heat(self, new_act, exporter, heat_flow, region=None):
        if heat_flow == 'heat, district or industrial, natural gas':
            heat_process_countries = self.heat_district_ng
        elif heat_flow == 'heat, district or industrial, other than natural gas':
            heat_process_countries = self.heat_district_non_ng
        elif heat_flow == 'heat, central or small-scale, other than natural gas':
            heat_process_countries = self.heat_small_scale_non_ng

        qty_heat = sum([i.amount for i in new_act.exchanges() if heat_flow in i.as_dict()['name']])

        # 'CH' is defined outside of RER (e.g., CH + Europe without Switzerland)
        if exporter == 'CH':
            return [([i for i in bw2.Database(self.new_regionalized_ecoinvent_database_name).search(heat_flow, limit=1000)
                      if (i.as_dict()['reference product'] == heat_flow and i.as_dict()['location'] == exporter)][0].key,
                     qty_heat)]

        elif region == 'RER':
            region_heat_key = [i for i in bw2.Database(self.new_regionalized_ecoinvent_database_name).search(
                heat_flow, limit=1000) if (i.as_dict()['reference product'] == heat_flow and i.as_dict()[
                    'location'] == 'Europe without Switzerland')][0].key
        else:
            region_heat_key = [i for i in bw2.Database(self.new_regionalized_ecoinvent_database_name).search(
                heat_flow, limit=1000) if (i.as_dict()['reference product'] == heat_flow and i.as_dict()[
                    'location'] == 'RoW')][0].key

        # if country does not have a specific heat market, use the ones from Europe w/o Swizterland and RoW as proxies
        if exporter not in heat_process_countries:
            if region == 'RER':
                exporter = "Europe without Switzerland"
            else:
                exporter = "RoW"

        # for countries with sub-regions defined (e.g., CA-QC or US-MRO)
        if exporter in heat_process_countries and exporter in ['CA', 'US', 'CN', 'BR', 'IN']:
            heat_inputs = [_.as_dict()['input'] for _ in
                           bw2.Database(self.new_regionalized_ecoinvent_database_name).get(region_heat_key[1]).exchanges()]
            distrib_heat = [
                (i.key, [j.amount for j in bw2.Database(self.new_regionalized_ecoinvent_database_name).get(region_heat_key[1]).exchanges() if
                         j.as_dict()['input'] == i.key][0]) for i in bw2.Database(self.new_regionalized_ecoinvent_database_name).search(
                    heat_flow, limit=1000) if (
                            i.key in heat_inputs and i.as_dict()['reference product'] == heat_flow and exporter in
                            i.as_dict()['location'])]
        else:
            heat_inputs = [_.as_dict()['input'] for _ in
                           bw2.Database(self.new_regionalized_ecoinvent_database_name).get(region_heat_key[1]).exchanges()]
            distrib_heat = [
                (i.key, [j.amount for j in bw2.Database(self.new_regionalized_ecoinvent_database_name).get(region_heat_key[1]).exchanges() if
                         j.input == i.key][0]) for i in bw2.Database(self.new_regionalized_ecoinvent_database_name).search(
                    heat_flow, limit=1000) if (
                            i.key in heat_inputs and i.as_dict()['reference product'] == heat_flow and i.as_dict()[
                        'location'] == exporter)]
        # change distribution into relative terms
        distrib_heat = [(j[0], j[1] / sum([i[1] for i in distrib_heat]) * qty_heat) for j in distrib_heat]
        # delete old existing flows
        [i.delete() for i in new_act.exchanges() if heat_flow in i.as_dict()['name']]

        return distrib_heat

    def change_waste(self, new_act, exporter, region=None):
        qty_of_waste = sum([i.as_dict()['amount'] for i in new_act.exchanges() if 'municipal solid waste' in
                            i.as_dict()['name']])

        try:
            key = [i for i in bw2.Database(self.new_regionalized_ecoinvent_database_name).search(
                'municipal solid waste', filter={'name':'market'}, limit=1000)
                 if i.as_dict()['reference product'] == 'municipal solid waste' and i.as_dict()['location'] == exporter
                 and (i.as_dict()['activity type'] == 'market activity' or i.as_dict()['activity type'] == 'market group')][0]
        except IndexError:
            if region == 'RER':
                key = [i for i in bw2.Database(self.new_regionalized_ecoinvent_database_name).search(
                    'municipal solid waste', limit=1000)
                     if (i.as_dict()['reference product'] == 'municipal solid waste' and
                         i.as_dict()['location'] == 'Europe without Switzerland')][0].key
            else:
                key = [i for i in bw2.Database(self.new_regionalized_ecoinvent_database_name).search(
                    'municipal solid waste', limit=1000)
                     if (i.as_dict()['reference product'] == 'municipal solid waste' and
                         i.as_dict()['location'] == 'RoW')][0].key

        # delete old flow(s) of waste
        [i.delete() for i in new_act.exchanges() if 'municipal solid waste' in i.as_dict()['name']]

        return key, qty_of_waste

    def change_water(self, new_act, exporter, region=None):
        water_values = {}
        for water_type in self.water_flows_in_ecoinvent:
            try:
                key_original_flow = [bw2.Database(self.name_regionalized_biosphere_database).get(i.input[1]) for i in
                                     new_act.exchanges() if (i.input[0] == self.name_regionalized_biosphere_database and
                         i.as_dict()['name'].split(', ' + region)[0] == water_type)][0].key
                categories = bw2.Database(self.name_regionalized_biosphere_database).get(
                    key_original_flow[1]).as_dict()['categories']
                amount = [i.amount for i in new_act.biosphere() if i.as_dict()['flow'] == key_original_flow[1]][0]
                [i.delete() for i in new_act.biosphere() if i.as_dict()['flow'] == key_original_flow[1]]
                try:  # try to find it fast, if fail -> loop through all regionalized flows = TOO LONG
                    key_new_geography_flow = [i for i in bw2.Database(self.name_regionalized_biosphere_database).search(
                        'Water, ' + exporter) if (
                            i.as_dict()['name'].split(', ' + new_act.as_dict()['location'])[0] == water_type and
                            i.as_dict()['categories'] == categories)][0].key
                except IndexError:
                    key_new_geography_flow = [i for i in bw2.Database(self.name_regionalized_biosphere_database).search(
                        'Water, ' + exporter, limit=3000) if (
                            i.as_dict()['name'].split(', ' + new_act.as_dict()['location'])[0] == water_type and
                            i.as_dict()['categories'] == categories)][0].key
                water_values[water_type] = {'key': key_new_geography_flow, 'amount': amount}
            except IndexError:
                pass

        return water_values

    def create_empty_bw2_database(self, database_name):
        # there must be a more elegant way of doing this...
        bw2.Database(database_name).write({(database_name, '_'): {}})
        [i.delete() for i in bw2.Database(database_name)]


def clean_up_dataframe(df):
    # remove duplicates
    df = df.drop_duplicates()
    # fix index
    df = df.reset_index().drop('index',axis=1)
    return df
