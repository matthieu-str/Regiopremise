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


class Regioinvent:
    def __init__(self, regioinvent_database_name, bw_project_name, ecoinvent_database_name):
        bw2.projects.set_current(bw_project_name)
        self.ecoinvent_database_name = ecoinvent_database_name
        self.create_empty_bw2_database(regioinvent_database_name)

        with open(pkg_resources.resource_filename(__name__, '/country_to_ecoinvent_regions.json'), 'r') as f:
            self.country_to_ecoinvent_regions = json.load(f)
        with open(pkg_resources.resource_filename(__name__, '/heat_industrial_ng_processes.json'), 'r') as f:
            self.heat_district_ng = json.load(f)
        with open(pkg_resources.resource_filename(__name__, '/heat_industrial_non_ng_processes.json'), 'r') as f:
            self.heat_district_non_ng = json.load(f)
        with open(pkg_resources.resource_filename(__name__, '/heat_small_scale_non_ng_processes.json'), 'r') as f:
            self.heat_small_scale_non_ng = json.load(f)
        with open(pkg_resources.resource_filename(__name__, 'COMTRADE_to_ecoinvent_geographies.json'), 'r') as f:
            self.convert_geos = json.load(f)

    def first_order_regionalization(self, trade_data, product_name_in_ecoinvent):
        # only keep relevant columns
        production_market = trade_data.loc[:, ['RefYear', 'ReporterISO', 'CmdCode', 'QtyUnitAbbr', 'Qty', 'AtlQty']]
        production_market.ReporterISO = [self.convert_geos[i] for i in production_market.ReporterISO]
        # Check if AtlQty is defined whenever Qty is empty. If it is, then use that value.
        production_market.loc[production_market.Qty == 0, 'Qty'] = production_market.loc[
            production_market.Qty == 0, 'AtlQty']
        # don't need AtlQty afterwards
        production_market = production_market.drop('AtlQty', axis=1)
        # check units match for all data
        assert len(set(production_market.loc[production_market.Qty != 0, 'QtyUnitAbbr'].dropna())) == 1
        unit = list(set(production_market.loc[production_market.Qty != 0, 'QtyUnitAbbr'].dropna()))[0]
        commodity = list(set(production_market.CmdCode))[0]
        # calculate the average export volume for each country, but exclude zero values from the average
        production_market = production_market.loc[production_market.Qty != 0]
        production_market = production_market.groupby('ReporterISO').agg({'Qty': 'mean'})
        exporters = (production_market.Qty / production_market.Qty.sum()).sort_values(ascending=False)
        # only keep the countries representing 99% of global exports of the product and create a RoW from that
        limit = exporters.index.get_loc(exporters[exporters.cumsum() > 0.99].index[0]) + 1
        remainder = exporters.iloc[limit:].sum()
        exporters = exporters.iloc[:limit]
        if 'RoW' in exporters.index:
            exporters.loc['RoW'] += remainder
        else:
            exporters.loc['RoW'] = remainder
        production_archetypes = [i for i in bw2.Database(self.ecoinvent_database_name).search(product_name_in_ecoinvent)
                                 if i.as_dict()['reference product'] == product_name_in_ecoinvent and "market" not in
                                 i.as_dict()['name']]
        available_geographies = [i.as_dict()['location'] for i in production_archetypes]

        # create a global market activity for the commodity
        global_market_activity = bw2.Database("Regioinvent").new_activity(uuid.uuid4().hex)
        global_market_activity['name'] = 'production market for ' + product_name_in_ecoinvent
        global_market_activity['reference product'] = product_name_in_ecoinvent
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

                # check if process uses electricity and/or various heat processes.
                electricity_input = self.test_input_presence(new_act, 'electricity')
                waste_input = self.test_input_presence(new_act, 'municipal solid waste')
                heat_district_ng_input = self.test_input_presence(new_act, 'heat, district or industrial, natural gas')
                heat_district_non_ng_input = self.test_input_presence(new_act,
                                                                 'heat, district or industrial, other than natural gas')
                heat_small_scale_non_ng_input = self.test_input_presence(new_act,
                                                                    'heat, central or small-scale, other than natural gas')

                # check if electricity needs spatilization for country (maybe it is already spatialized)
                if electricity_input:
                    if ([bw2.Database(self.ecoinvent_database_name).get(i.input[1]) for i in new_act.exchanges()
                         if 'electricity' in i.as_dict()['name']][0].as_dict()['location'] != exporter):
                        # spatialize electricity
                        key, amount, name = self.change_electricity(new_act, exporter)
                        new_elec_exc = new_act.new_exchange(input=key, amount=amount, type='technosphere', name=name)
                        new_elec_exc.save()
                if waste_input:
                    if ([bw2.Database(self.ecoinvent_database_name).get(i.input[1]) for i in new_act.exchanges()
                         if 'municipal solid waste' in i.as_dict()['name']][0].as_dict()['location'] != exporter):
                        # spatialize waste
                        key, amount = self.change_waste(new_act, exporter)
                        new_waste_exc = new_act.new_exchange(input=key, amount=amount, type='technosphere',
                                                             name='municipal solid waste')
                        new_waste_exc.save()
                if heat_district_ng_input:
                    # check if heat needs spatialization for country
                    heat_flow = 'heat, district or industrial, natural gas'
                    if ([bw2.Database(self.ecoinvent_database_name).get(i.input[1]) for i in new_act.exchanges()
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
                    if ([bw2.Database(self.ecoinvent_database_name).get(i.input[1]) for i in new_act.exchanges()
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
                    if ([bw2.Database(self.ecoinvent_database_name).get(i.input[1]) for i in new_act.exchanges()
                         if heat_flow in i.as_dict()['name']][0].as_dict()['location'] != exporter):
                        # spatialize heat
                        distrib_heat = self.change_heat(new_act, exporter, heat_flow)
                        for heat_flow in distrib_heat:
                            new_heat_exc = new_act.new_exchange(input=heat_flow[0], amount=heat_flow[1],
                                                                type='technosphere', name=heat_flow)
                            new_heat_exc.save()

            elif exporter in self.country_to_ecoinvent_regions and self.country_to_ecoinvent_regions[
                exporter] in available_geographies:
                # copy activity from reference
                new_act = copy_reference_process(self.country_to_ecoinvent_regions[exporter], exporter)

                # check if process uses electricity and/or various heat processes.
                electricity_input = self.test_input_presence(new_act, 'electricity')
                waste_input = self.test_input_presence(new_act, 'municipal solid waste')
                heat_district_ng_input = self.test_input_presence(new_act, 'heat, district or industrial, natural gas')
                heat_district_non_ng_input = self.test_input_presence(
                    new_act, 'heat, district or industrial, other than natural gas')
                heat_small_scale_non_ng_input = self.test_input_presence(
                    new_act, 'heat, central or small-scale, other than natural gas')

                if electricity_input:
                    # spatialize electricity
                    key, amount, name = self.change_electricity(new_act, exporter)
                    new_elec_exc = new_act.new_exchange(input=key, amount=amount, type='technosphere', name=name)
                    new_elec_exc.save()
                if waste_input:
                    # spatialize waste
                    key, amount = self.change_waste(new_act, exporter)
                    new_waste_exc = new_act.new_exchange(input=key, amount=amount, type='technosphere',
                                                         name='municipal solid waste')
                    new_waste_exc.save()
                if heat_district_ng_input:
                    # spatialize heat
                    heat_flow = 'heat, district or industrial, natural gas'
                    distrib_heat = self.change_heat(new_act, exporter, heat_flow)
                    for heat_flow in distrib_heat:
                        new_heat_exc = new_act.new_exchange(input=heat_flow[0], amount=heat_flow[1],
                                                            type='technosphere', name=heat_flow)
                        new_heat_exc.save()
                if heat_district_non_ng_input:
                    # spatialize heat
                    heat_flow = 'heat, district or industrial, other than natural gas'
                    distrib_heat = self.change_heat(new_act, exporter, heat_flow)
                    for heat_flow in distrib_heat:
                        new_heat_exc = new_act.new_exchange(input=heat_flow[0], amount=heat_flow[1],
                                                            type='technosphere', name=heat_flow)
                        new_heat_exc.save()
                if heat_small_scale_non_ng_input:
                    # spatialize heat
                    heat_flow = 'heat, central or small-scale, other than natural gas'
                    distrib_heat = self.change_heat(new_act, exporter, heat_flow)
                    for heat_flow in distrib_heat:
                        new_heat_exc = new_act.new_exchange(input=heat_flow[0], amount=heat_flow[1],
                                                            type='technosphere', name=heat_flow)
                        new_heat_exc.save()

            else:
                # copy activity from reference
                new_act = copy_reference_process("RoW", exporter)

                # check if process uses electricity and/or various heat processes.
                electricity_input = self.test_input_presence(new_act, 'electricity')
                waste_input = self.test_input_presence(new_act, 'municipal solid waste')
                heat_district_ng_input = self.test_input_presence(new_act, 'heat, district or industrial, natural gas')
                heat_district_non_ng_input = self.test_input_presence(
                    new_act, 'heat, district or industrial, other than natural gas')
                heat_small_scale_non_ng_input = self.test_input_presence(
                    new_act, 'heat, central or small-scale, other than natural gas')

                if electricity_input:
                    # spatialize electricity
                    key, amount, name = self.change_electricity(new_act, exporter)
                    new_elec_exc = new_act.new_exchange(input=key, amount=amount, type='technosphere', name=name)
                    new_elec_exc.save()
                if waste_input:
                    # spatialize waste
                    key, amount = self.change_waste(new_act, exporter)
                    new_waste_exc = new_act.new_exchange(input=key, amount=amount, type='technosphere',
                                                         name='municipal solid waste')
                    new_waste_exc.save()
                if heat_district_ng_input:
                    # spatialize heat
                    heat_flow = 'heat, district or industrial, natural gas'
                    distrib_heat = self.change_heat(new_act, exporter, heat_flow)
                    for heat_flow in distrib_heat:
                        new_heat_exc = new_act.new_exchange(input=heat_flow[0], amount=heat_flow[1],
                                                            type='technosphere', name=heat_flow)
                        new_heat_exc.save()
                if heat_district_non_ng_input:
                    # spatialize heat
                    heat_flow = 'heat, district or industrial, other than natural gas'
                    distrib_heat = self.change_heat(new_act, exporter, heat_flow)
                    for heat_flow in distrib_heat:
                        new_heat_exc = new_act.new_exchange(input=heat_flow[0], amount=heat_flow[1],
                                                            type='technosphere', name=heat_flow)
                        new_heat_exc.save()
                if heat_small_scale_non_ng_input:
                    # spatialize heat
                    heat_flow = 'heat, central or small-scale, other than natural gas'
                    distrib_heat = self.change_heat(new_act, exporter, heat_flow)
                    for heat_flow in distrib_heat:
                        new_heat_exc = new_act.new_exchange(input=heat_flow[0], amount=heat_flow[1],
                                                            type='technosphere', name=heat_flow)
                        new_heat_exc.save()

    def create_consumption_markets(self, trade_data, product_name_in_ecoinvent):
        # only keep relevant columns
        consumption_market = trade_data.loc[:,
                            ['RefYear', 'ReporterISO', 'PartnerISO', 'CmdCode', 'QtyUnitAbbr', 'Qty', 'AtlQty']]
        # remove totals
        consumption_market = consumption_market.drop(consumption_market[consumption_market.PartnerISO == 'W00'].index)
        consumption_market.ReporterISO = [self.convert_geos[i] for i in consumption_market.ReporterISO]
        consumption_market.PartnerISO = [self.convert_geos[i] for i in consumption_market.PartnerISO]
        # Check if AtlQty is defined whenever Qty is empty. If it is, then use that value.
        consumption_market.loc[consumption_market.Qty == 0, 'Qty'] = consumption_market.loc[
            consumption_market.Qty == 0, 'AtlQty']
        # don't need AtlQty afterwards
        consumption_market = consumption_market.drop('AtlQty', axis=1)
        # check units match for all data
        assert len(set(consumption_market.loc[consumption_market.Qty != 0, 'QtyUnitAbbr'].dropna())) == 1
        unit = list(set(consumption_market.loc[consumption_market.Qty != 0, 'QtyUnitAbbr'].dropna()))[0]
        commodity = list(set(consumption_market.CmdCode))[0]
        # calculate the average export volume for each country, but exclude zero values from the average
        consumption_market = consumption_market.loc[consumption_market.Qty != 0]
        consumption_market = consumption_market.groupby(['ReporterISO', 'PartnerISO']).agg({'Qty': 'mean'})
        importers = (consumption_market.groupby(level=0).sum() / consumption_market.sum().sum()).sort_values(
            by='Qty', ascending=False)
        limit = importers.index.get_loc(importers[importers.cumsum() > 0.99].dropna().index[0]) + 1
        remainder = consumption_market.loc[importers.index[limit:]].groupby(level=1).sum()
        consumption_market = consumption_market.loc[importers.index[:limit]]
        consumption_market = pd.concat([consumption_market, pd.concat([remainder], keys=['RoW'])])
        consumption_market.index = pd.MultiIndex.from_tuples([i for i in consumption_market.index])
        consumption_market = consumption_market.sort_index()
        for importer in consumption_market.index.levels[0]:
            consumption_market.loc[importer, 'Qty'] = (
                    consumption_market.loc[importer, 'Qty'] / consumption_market.loc[importer, 'Qty'].sum()).values
        consumption_market = pd.concat([consumption_market.drop('RoW'),
                                        pd.concat([consumption_market.loc['RoW'].groupby(level=0).sum()],keys=['RoW'])])

        for importer in consumption_market.index.levels[0]:
            new_consumption_market = bw2.Database("Regioinvent").new_activity(uuid.uuid4().hex)
            new_consumption_market['name'] = 'consumption market for ' + product_name_in_ecoinvent
            new_consumption_market['reference product'] = product_name_in_ecoinvent
            new_consumption_market['location'] = importer
            new_consumption_market['type'] = 'process'
            new_consumption_market['unit'] = 'kilogram'
            new_consumption_market.save()
            # create the production flow
            new_exc = new_consumption_market.new_exchange(input=new_consumption_market.key, amount=1, type='production')
            new_exc.save()

            available_trading_partners = [i.as_dict()['location'] for i in
                                          bw2.Database('Regioinvent').search(product_name_in_ecoinvent, limit=1000) if
                                          (i.as_dict()['reference product'] == product_name_in_ecoinvent and
                                           'consumption market' not in i.as_dict()['name'])]

            for trading_partner in consumption_market.loc[importer].index:
                if trading_partner in available_trading_partners:
                    trading_partner_key = \
                    [i for i in bw2.Database('Regioinvent').search(product_name_in_ecoinvent, limit=1000) if
                     (i.as_dict()['reference product'] == product_name_in_ecoinvent and
                      'consumption market' not in i.as_dict()['name'] and i.as_dict()[
                          'location'] == trading_partner)][0].key
                    new_trade_exc = new_consumption_market.new_exchange(
                        input=trading_partner_key,
                        amount=consumption_market.loc[(importer, trading_partner), 'Qty'],
                        type='technosphere', name=product_name_in_ecoinvent)
                    new_trade_exc.save()
                elif trading_partner in self.country_to_ecoinvent_regions and self.country_to_ecoinvent_regions[
                    trading_partner] in available_trading_partners:
                    trading_partner_key = \
                    [i for i in bw2.Database('Regioinvent').search(product_name_in_ecoinvent, limit=1000) if
                     (i.as_dict()['reference product'] == product_name_in_ecoinvent and
                      'consumption market' not in i.as_dict()['name'] and i.as_dict()['location'] ==
                      self.country_to_ecoinvent_regions[trading_partner])][0].key
                    new_trade_exc = new_consumption_market.new_exchange(
                        input=trading_partner_key,
                        amount=consumption_market.loc[(importer, trading_partner), 'Qty'],
                        type='technosphere', name=product_name_in_ecoinvent)
                    new_trade_exc.save()
                else:
                    trading_partner_key = \
                    [i for i in bw2.Database('Regioinvent').search(product_name_in_ecoinvent, limit=1000) if
                     (i.as_dict()['reference product'] == product_name_in_ecoinvent and
                      'consumption market' not in i.as_dict()['name'] and i.as_dict()['location'] == 'RoW')][0].key
                    new_trade_exc = new_consumption_market.new_exchange(
                        input=trading_partner_key,
                        amount=consumption_market.loc[(importer, trading_partner), 'Qty'],
                        type='technosphere', name=product_name_in_ecoinvent)
                    new_trade_exc.save()

    def second_order_regionalization(self, product_name_in_ecoinvent):
        for process in bw2.Database('Regioinvent'):
            for inputt in process.technosphere():
                # if it's an ecoinvent input
                if inputt.as_dict()['input'][0] == self.ecoinvent_database_name:
                    # check if we regionalized the commodity
                    if inputt.as_dict()['name'] == product_name_in_ecoinvent:
                        exchange_amount = sum([i.amount for i in process.technosphere() if
                                               i.as_dict()['name'] == product_name_in_ecoinvent])
                        available_geographies = [j.as_dict()['location'] for j in
                                                 bw2.Database('Regioinvent').search('consumption market', limit=1000) if
                                                 j.as_dict()['reference product'] == product_name_in_ecoinvent]
                        if process.as_dict()['location'] in available_geographies:
                            new_exchange_key = \
                            [j for j in bw2.Database('Regioinvent').search('consumption market', limit=1000) if (
                                    j.as_dict()['reference product'] == product_name_in_ecoinvent and j.as_dict()[
                                'location'] == process.as_dict()['location'])][0].key
                        elif (process.as_dict()['location'] in self.country_to_ecoinvent_regions and
                              self.country_to_ecoinvent_regions[process.as_dict()['location']] in available_geographies):
                            new_exchange_key = \
                            [j for j in bw2.Database('Regioinvent').search('consumption market', limit=1000) if (
                                    j.as_dict()['reference product'] == product_name_in_ecoinvent and j.as_dict()[
                                'location'] ==
                                    self.country_to_ecoinvent_regions[process.as_dict()['location']])][0].key
                        else:
                            new_exchange_key = \
                            [j for j in bw2.Database('Regioinvent').search('consumption market', limit=1000) if (
                                    j.as_dict()['reference product'] == product_name_in_ecoinvent and j.as_dict()[
                                'location'] == 'RoW')][0].key

                        [i.delete() for i in process.technosphere() if i.as_dict()['name'] == product_name_in_ecoinvent]
                        new_exc = process.new_exchange(input=new_exchange_key, amount=exchange_amount,
                                                       type='technosphere', name=inputt.as_dict()['name'])
                        new_exc.save()

    def test_input_presence(self, process, input):
        return len([bw2.Database(self.ecoinvent_database_name).get(i.input[1]) for i in process.exchanges()
                 if input in i.as_dict()['name']]) != 0

    def change_electricity(self, new_act, exporter):
        # limit: RoW associated with GLO instead of recalculating
        # limit: same for regions exporting (e.g., RAS)
        qty_of_electricity = sum(
            [i.as_dict()['amount'] for i in new_act.exchanges() if 'electricity' in i.as_dict()['name']])
        type_of_electricity = [i.as_dict()['name'] for i in new_act.exchanges() if 'electricity' in i.as_dict()['name']][0]
        if exporter != 'RoW':
            key = [i for i in
                   bw2.Database(self.ecoinvent_database_name).search(type_of_electricity, filter={'name': 'market'}, limit=1000)
                   if i.as_dict()['reference product'] == type_of_electricity and i.as_dict()['location'] == exporter
                   and (i.as_dict()['activity type'] == 'market activity' or i.as_dict()[
                    'activity type'] == 'market group')][0].key
        else:
            # if the exporter is RoW -> assign global electricity process
            key = [i for i in
                   bw2.Database(self.ecoinvent_database_name).search(type_of_electricity, filter={'name': 'market'}, limit=1000)
                   if i.as_dict()['reference product'] == type_of_electricity and i.as_dict()['location'] == 'GLO'
                   and (i.as_dict()['activity type'] == 'market activity' or i.as_dict()[
                    'activity type'] == 'market group')][0].key
        # delete old flow(s) of electricity
        [i.delete() for i in new_act.exchanges() if 'electricity' in i.as_dict()['name']]

        return key, qty_of_electricity, type_of_electricity

    def change_heat(self, new_act, exporter, heat_flow):
        if heat_flow == 'heat, district or industrial, natural gas':
            heat_process_countries = self.heat_district_ng
        elif heat_flow == 'heat, district or industrial, other than natural gas':
            heat_process_countries = self.heat_district_non_ng
        elif heat_flow == 'heat, central or small-scale, other than natural gas':
            heat_process_countries = self.heat_small_scale_non_ng

        qty_heat = sum([i.amount for i in new_act.exchanges() if heat_flow in i.as_dict()['name']])

        # 'CH' is defined outside of RER (e.g., CH + Europe without Switzerland)
        if exporter == 'CH':
            return [([i for i in bw2.Database(self.ecoinvent_database_name).search(heat_flow, limit=1000)
                      if (i.as_dict()['reference product'] == heat_flow and i.as_dict()['location'] == exporter)][0].key,
                     qty_heat)]

        elif (exporter in self.country_to_ecoinvent_regions and self.country_to_ecoinvent_regions[exporter] == 'RER'):
            region_heat_key = [i for i in bw2.Database(self.ecoinvent_database_name).search(heat_flow, limit=1000)
                               if (i.as_dict()['reference product'] == heat_flow and i.as_dict()[
                    'location'] == 'Europe without Switzerland')][0].key
        else:
            region_heat_key = [i for i in bw2.Database(self.ecoinvent_database_name).search(heat_flow, limit=1000)
                               if (i.as_dict()['reference product'] == heat_flow and i.as_dict()['location'] == 'RoW')][
                0].key

        # if country does not have a specific heat market, use the ones from Europe without Swizterland and RoW as proxies
        if exporter not in heat_process_countries:
            if (exporter in self.country_to_ecoinvent_regions and self.country_to_ecoinvent_regions[exporter] == 'RER'):
                exporter = "Europe without Switzerland"
            else:
                exporter = "RoW"

        # for countries with sub-regions defined (e.g., CA-QC or US-MRO)
        if exporter in heat_process_countries and exporter in ['CA', 'US', 'CN', 'BR', 'IN']:
            heat_inputs = [_.as_dict()['input'] for _ in
                           bw2.Database(self.ecoinvent_database_name).get(region_heat_key[1]).exchanges()]
            distrib_heat = [
                (i.key, [j.amount for j in bw2.Database(self.ecoinvent_database_name).get(region_heat_key[1]).exchanges() if
                         j.as_dict()['input'] == i.key][0]) for i in bw2.Database(self.ecoinvent_database_name).search(
                    heat_flow, limit=1000) if (
                            i.key in heat_inputs and i.as_dict()['reference product'] == heat_flow and exporter in
                            i.as_dict()['location'])]
        else:
            heat_inputs = [_.as_dict()['input'] for _ in
                           bw2.Database(self.ecoinvent_database_name).get(region_heat_key[1]).exchanges()]
            distrib_heat = [
                (i.key, [j.amount for j in bw2.Database(self.ecoinvent_database_name).get(region_heat_key[1]).exchanges() if
                         j.input == i.key][0]) for i in bw2.Database(self.ecoinvent_database_name).search(
                    heat_flow, limit=1000) if (
                            i.key in heat_inputs and i.as_dict()['reference product'] == heat_flow and i.as_dict()[
                        'location'] == exporter)]
        # change distribution into relative terms
        distrib_heat = [(j[0], j[1] / sum([i[1] for i in distrib_heat]) * qty_heat) for j in distrib_heat]
        # delete old existing flows
        [i.delete() for i in new_act.exchanges() if heat_flow in i.as_dict()['name']]

        return distrib_heat

    def change_waste(self, new_act, exporter):
        qty_of_waste = sum([i.as_dict()['amount'] for i in new_act.exchanges() if 'municipal solid waste' in i.as_dict()['name']])

        try:
            key = [i for i in bw2.Database(self.ecoinvent_database_name).search('municipal solid waste', filter={'name':'market'}, limit=1000)
                 if i.as_dict()['reference product'] == 'municipal solid waste' and i.as_dict()['location'] == exporter
                 and (i.as_dict()['activity type'] == 'market activity' or i.as_dict()['activity type'] == 'market group')][0]
        except IndexError:
            if (exporter in self.country_to_ecoinvent_regions and self.country_to_ecoinvent_regions[exporter] == 'RER'):
                key = [i for i in bw2.Database(self.ecoinvent_database_name).search('municipal solid waste', limit=1000)
                     if (i.as_dict()['reference product'] == 'municipal solid waste' and i.as_dict()['location'] == 'Europe without Switzerland')][0].key
            else:
                key = [i for i in bw2.Database(self.ecoinvent_database_name).search('municipal solid waste', limit=1000)
                     if (i.as_dict()['reference product'] == 'municipal solid waste' and i.as_dict()['location'] == 'RoW')][0].key

        # delete old flow(s) of waste
        [i.delete() for i in new_act.exchanges() if 'municipal solid waste' in i.as_dict()['name']]

        return key, qty_of_waste

    def create_empty_bw2_database(self, database_name):
        # there must be a more elegant way of doing this...
        bw2.Database(database_name).write({(database_name, '_'): {}})
        [i.delete() for i in bw2.Database(database_name)]
