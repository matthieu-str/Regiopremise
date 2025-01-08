# Methodology of regioinvent

This document describes in details the methodology that is followed by regioinvent to regionalize ecoinvent to its 
fullest potential.

The methodology is divided into multiple sections:
1. [Selection of the trade database](#selection-of-the-trade-database)
2. [Estimation of production data](#estimation-of-production-data)
3. [Selection of commodities to regionalize](#selection-of-commodities-to-regionalize)
4. [Selection of countries for regionalization](#selection-of-countries-for-regionalization)
5. [Regionalizing - creating national production process](#regionalizing---creating-national-production-process)
6. [Regionalizing - creating global export market processes](#regionalizing---creating-global-export-market-processes)
7. [Regionalizing - creating national consumption market processes](#regionalizing---creating-national-consumption-market-processes)
8. [Modeling transportation in the created markets](#modeling-transportation-in-the-created-markets)
9. [How to deal with multiple technologies of production](#how-to-deal-with-multiple-technologies-of-production)
10. [Dealing with yearly fluctuation of trade data](#dealing-with-yearly-fluctuation-of-trade-data)
11. [Spatialization of elementary flows](#spatialization-of-elementary-flows)
12. [Regionalized life cycle impact assessment methods](#regionalized-life-cycle-impact-assessment-methods)
13. [The water regionalization issue with ecoinvent](#the-water-regionalization-issue-with-ecoinvent)
14. [Connecting ecoinvent processes to UN COMTRADE commodities](#connecting-ecoinvent-processes-to-un-comtrade-commodities)
15. [Connecting ecoinvent geographies to UN COMTRADE geographies](#connecting-ecoinvent-geographies-to-un-comtrade-geographies)
16. [Connecting EXIOBASE sectors to UN COMTRADE commodities](#connecting-exiobase-sectors-to-un-comtrade-commodities)
17. [Connecting EXIOBASE geographies to UN COMTRADE geographies](#connecting-exiobase-geographies-to-un-comtrade-geographies)
 

### Selection of the trade database
The trade database that is used in regionvent is the UN COMTRADE database (https://comtradeplus.un.org/TradeFlow). The
advantage of this database compared to other candidates (such as Multi-Regional Input-Output tables) is its completeness.
When the most detailed IO databases describe an economy in maximum 500 sectors, the UN COMTRADE database covers the trade
at a much finer level (i.e., thousands of different commodities). In addition, it covers all countries worldwide, while
IO databases tends to aggregate smaller economies.

However, the UN COMTRADE database has inconsistencies. Imports and exports are not reported in the same values (CIF vs FOB).
Furthermore, there are a few missing data points for some years/commodities/countries combinations, as well as blatant
reporting mistakes. In the first versions of regionvent (< v1.2) we tried to correct these inconsistencies ourselves. 
Unfortunately it is very time-consuming and requires a lot of expertise which we lack. Therefore, from the v1.2 of 
regioinvent, we rely on the BACI database (https://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37), 
which is an adaptation of the UN COMTRADE database where the aforementioned inconsistencies are already corrected.

### Estimation of production data
While the UN COMTRADE provides import and export data with extreme precision, both for commodities and countries, there
is a significant lack of similar data for production. Indeed, to the best of my knowledge, there is no comprehensive 
database with as much detail as the COMTRADE database for national production volumes. Therefore, 
how do we estimate these national production volumes? Because we need this data to have a proper representation of the 
supply chains.

A first approach, the most logical one, would be to scrap the Internet to "simply" gather this data which most probably
exists but is not centralized in a database. The problem is that we are talking about getting national production data
for more than 1,800 commodities in many countries, so probably more than 200,000 production data to find. This approach 
is therefore something regioinvent will strive towards slowly, but cannot be the first implemented approach as this 
undertaking will take a long time.

So how to proceed in the meantime? We have thought of two ways to estimate production data, which require minimal to 
basically no additional information.
1. We can estimate national production data based on the export data provided by UN COMTRADE, scaled with the ratio 
production/export which can be obtained from Input-Output databases. **This is the approach that is currently implemented 
in the v1.2 of regioinvent, using EXIOBASE.** In other words, if Canada exports 1,000 tonnes of tomatoes (we know that 
from UN COMTRADE) and that (according to EXIOBASE) Canada exports 61.6% of its total production of Vegetables, 
fruit, nuts, then we can estimate the total production of apples in Canada to 1,623 tonnes and the domestic consumption
of apples in Canada to 623 tonnes. This approach is extremely
reliant on the ratios provided by the Input-Output database which can sometimes be really wrong. While a few % error is 
not troubling, there are cases where the ratio is estimated to be 0% of export or 0.00001% of export, which can lead
to significant distortions of the estimation. Furthermore, this approach is subject to typical aggregation bias of
the IO approach, as any "Vegetables, fruit, nuts" will have the same national production/export ratio. And finally, the
calculated ratio is based on financial data and is then reapplied to physical data which further create inconsistency as
the price of commodities is typically not the same between exported products vs domestically consumed products.
2. We can estimate national production data based on the global production data of commodities, redistributed through
their share of the global export. In other words, if we get the global production data (say 100,000 tonnes for apples)
we can then apply the share of each country to the global export market (provided by the UN COMTRADE). So if Canada
represents 3% of the global export of apples, then we consider that Canada also represent 3% of the global production, 
which leads to an estimate of 3,000 tonnes of apples produced in Canada, from which we subtract the export values 
according to UN COMTRADE to derive a domestic consumption estimate. This approach requires finding global production 
data for each of the 1,800+ commodities. However, it straight up considers that export and total production are
100% correlated, which is wrong.

Both of these approaches have clear limits and issues and most likely do not provide satisfactory estimates, when compared
to the degree of precision and robustness of the import/export data from the UN COMTRADE. In the short-term future, the 
2nd approach will also be implemented in regioinvent and analyses will be carried out to see which of the two approaches
estimates production data the least worst.

### Selection of commodities to regionalize
How does regioinvent select which of the thousands of reference products of ecoinvent to regionalized or not? To do so,
we follow a simple set of rules:
- the product must not be a waste/residue/recycled content cut-off/etc. (<u>unlike</u>: *drilling waste*)
- there must be at least one process producing the product that is not aggregated (S) (<u>unlike</u>: *zeolite powder*)
- the product must not be country-specific (<u>unlike</u>: *barley grain, Swiss integrated production*)
- the product must not be an activity (<u>unlike</u>: *potato planting*)
- the product must be traded internationally  (<u>unlike</u>: *building, hall* or *land tenure, arable land*)

Why this set of rules? Well in this first version of regioinvent, the focus is on the link with international trade, so
we simply exclude activities, wastes and not international-traded commodities. In future versions of regioinvent, those
could also be regionalized to depict the metalworking of steel in relevant countries instead of relying on the RER, RoW 
and GLO sets from ecoinvent. But here there is a compromise to be made. The regionalization of the internationally-traded
commodities already expands the size of the database significantly. Further regionalization would start to require 
fairly long calculation times (> 15 minutes) and fairly strong computation machines (most likely minimum 32GB RAM).

In the end there are __1,848__ commodities regionalized for ecoinvent 3.9.1 cut-off and __1,860__ commodities regionalized for
ecoinvent 3.10 cut-off. You can find the list of these commodities in the Data/Regionalization/ei3.x/ecoinvent_to_HS.json
files.

### Selection of countries for regionalization
Ideally, the 1,800+ commodities could be regionalized for all countries in the world. Practically speaking though, there
is clearly a constraint of calculation power. The more processes are created, the longer and harder the calculations will
be. Therefore, regioinvent only regionalize for countries that are deemed relevant for each commodity. Why would I create
a banana production process in Canada while Canada probably does not even produce bananas. How do we choose which countries
are relevant? We look at the production and consumption data and cut-off any country lower than 1% of the cumulative sum.
So concretely, if we have such a distribution for production: China 75%, US 20%, Canada 4.5%, Peru 0.4%, Chile 0.1%, we 
calculate the cumulative sum of the production: 
1. China 75%
2. China + US 95%
3. China + US + Canada 99.5%
4. China + US + Canada + Peru 99.9% 
5. China + US + Canada + Peru + Chile 100%
As soon as the cumulative sum passes the cutoff threshold, any country remaining will be aggregated in a Rest-of-the-World 
(RoW) region. So in the little example provided, processes for China, the US and Canada would be created, but processes
for Peru and Chile would not be created. Instead, a RoW process would be created which is based on the production data of
all aggregated countries (Peru and Chile in this example).

The same principal is applied with consumption data. Production data is used for determining the producing countries 
that will be represented, while consumption data is used to determine for which countries consumption markets will be
created.

The cut-off in regioinvent is one of the parameters the users can play around with. By default, it is put to 0.99. Which
means the countries up to 99% of the cumulative sum of either production or consumption will be regionalized while others 
will be aggregated in RoW.

### Regionalizing - creating national production process
Each combination of commodity/country previously selected is created within regioinvent. How do we go about that? Well
we simply automate what LCA practitioners worldwide already do manually. We simply copy the most relevant existing process
within the ecoinvent database and sprinkle some regionalization on it, namely, we regionalize the electricity, heat and 
municipal solid waste (MSW) treatment processes, and we also spatialize the elementary flows. <br>
__*In the end, regioinvent only adapts the origin of flows and does not modify values.*__ <br>

Technically speaking, we identify all existing geographies for each of the adapted inputs and store these in json files:
(for example see Data/Regionalization/ei3.x/electricity_processes.json). That way the code knows that it can look for
existing processes within the ecoinvent database and switch them within the national production process created. <br>

To know which region is best adapted to each country, we stored this information in another json file (see 
Data/Regionalization/ei3.x/country_to_ecoinvent_regions.json). Concretely, the problem is that if the previous selection
steps require to create a process for the production of *random_commodity1* in France, we must identify what is the 
closest available process in ecoinvent. And so the code will go into the json file, and see that France is linked to the 
following potential ecoinvent regions ["RER", "IAI Area, EU27 & EFTA", "RER w/o DE+NL+RU", "RER w/o CH+DE", 
"Europe without Switzerland and Austria", "Europe without Switzerland", "Europe without Austria", "RER w/o RU", 
"RoE", "UCTE", "UCTE without Germany", "WEU"]. If none of these regions are defined, then the code will search for a RoW
or GLO process instead. And if not even GLO and RoW process exist (which is the case for "aluminium, primary, ingot" for 
example), then the code will select a random available geography to be the base for the copy.

### Regionalizing - creating global export market processes
The export data from the UN COMTRADE database are simply extracted for each commodity and used. Transportation is also 
added (see [Transportation section](#modeling-transportation-in-the-created-markets)).

### Regionalizing - creating national consumption market processes
The import data from the UN COMTRADE as well as the estimated domestic consumption data are summed up together to obtain
consumption data per country. Then this data is simply used to generate consumption markets. Transportation is also 
added (see [Transportation section](#modeling-transportation-in-the-created-markets)).

### Modeling transportation in the created markets
In the global export market and various national consumption markets created by regioinvent, transportation modes must be
added. Regioinvent v1.2 only simply copies the transportation distribution of the original market (the one used for copy)
and adds it to the new market. This simple copy and paste creates inconsistencies by adding transportation modes that are
unadapted to the actual transportation that would occur between two countries. For example, there are processes of 
transportation by tanker for imports from Germany to Switzerland, which obviously makes no sense. In later version of 
regioinvent, we intend to regionalize at the very least the transportation distances and modes depending on the origin
and destination of each markets within regioinvent. After that, we might even further regionalize transportation by
creating (and thus adapting) transportation modes per countries, so that the tanker that leaves from Shanghai, using
kerosene that is typically bought in China, instead of using the GLO or RoW kerosene markets. But once again there is a 
compromise to-be-made between adding more regionalization and calculation time when performing LCAs.

### How to deal with multiple technologies of production
Some commodities can be produced through various means. For instance, 1-butanol can be produced through the 
"hydroformylation of propylene" but also through "synthetic fuel production, from coal, high temperature Fisher-Tropsch 
operation". Production and trade data however, are only provided for the commodity overall. How then can we distribute
each national market shares to the different technologies? Well in regioinvent, we calculate the distribution of the 
technologies within the global production market (or its closest substitute if there is no GLO market, e.g, RoW) and
then simply reapply that technology share, to the national share. So, more concretely, if the hydroformylation of 
propylene represents 80% of the production of 1-butanol overall, and that Canada represents 10% of global production 
shares for 1-butanol, then regioinvent creates a Canadian process for both hydroformylation and Fisher-Tropsch, and adds
their respective shares (10% * 80% = 8% and 10% * 20% = 2%) for both technologies. <br>
What does this choice entails? This assumption does not respect the actual technologies implemented nationally. 
Fisher-Tropsch from coal is probably not a technology that is used in Europe to produce 1-butanol for example, but is used
in South Africa. But with the assumption made, we consider that Europe and South Africa produce 1-butanol with both 
technologies. Refining these choices entails a deep expertise of technologies deployed throughout the world, and for many
different commodities. However, this is not an expertise that is available to regioinvent.

### Dealing with yearly fluctuation of trade data
To not be subject to the whims of the trade market changing constantly, which would affect the robustness from year to 
year of the assessments carried out with regioinvent (and also requiring to redo regioinvent every year), we chose to
take the average trade data over the 5 last years available. For regioinvent v1.2, this mainly corresponds to data for 
the years 2018, 2019, 2020, 2021 and 2022. A simply arithmetic average is performed. <br>
There are a few exceptions for which only the year 2022 was considered because the corresponding commodity only exists 
in the HS22 version of UN COMTRADE, and not in the HS17 version. For instance, the commodity 290341 - trifluomethane (HFC-23)
is only available in HS22, otherwise it would be under 290339 - Other fluorinated, brominated or iodinated derivatives of 
acyclic hydrocarbons, which is much less precise.

Here are the commodities which are only under the year 2022: ['290341', '290343', '290349', '854142', '854143', '854159']

### Spatialization of elementary flows
The spatialization of elementary flows is an important aspect of the work of regioinvent. However, it is quite easy to 
implement and understand actually. Regioinvent simply allocates the location of the process to the extraction or emission
of relevant elementary flows. What are the relevant elementary flows you ask? Well to determine those, we focused on the
coverage of regionalized impact categories for three LCIA methods: IMPACT World+, ReCiPe and EF. For these three methods,
we simply looked at the different elementary flows for which there are regionalized characterization factors. These are
the relevant elementary flows mentioned previously. Typically, these are: water flows, land flows, acidification flows,
eutrophication flows, photochemical oxidant formation flows and particulate matter flows.

Regioinvent creates a __*biosphere3_spatialized_flows*__ database in your brightway project which contains all these 
relevant spatialized elementary flows, which needed to be created.

Later on, the code simply picks up from this biosphere3_spatialized_flows database to switch non-spatialized elementary 
flows for spatialized elementary flows.

### Regionalized life cycle impact assessment methods
With spatialized elementary flows, characterization factors must be attributed to them, otherwise nothing will be 
characterized and the results will mean nothing. You therefore cannot use regioinvent with the typical packaged methods
of brightway2. To be clear, there won't be any error and everything will work, but for impact categories with spatialized
elementary flows, the results will be wrong.

Regioinvent v1.2 operates only with IMPACT World+ as the programmers of regioinvent are also the maintainers of that 
impact method. However, in the short-term future, ReCiPe and EF will also be made available with Regioinvent. Users are
welcome to contribute by matching with other impact methods.

### The water regionalization issue with ecoinvent
The simple rule followed by regioinvent to spatialize elementary flows (i.e, just assigning the region of the process)
creates imbalances for the water-related impact category (e.g., AWARE). This issues can also be found in regionalizations
performed by Pré consultants (SimaPro) and GreenDelta (OpenLCA). What is the issue exactly? To explain it, we can take a
look at the ecoinvent process for the production of apples in Chile (CL). In this process, there are releases of water, 
both in air (evaporated water) and in water (rundown) as elementary flows and there is also an input of water that comes
from the "market for irrigation" of the RoW region, so from the technosphere. After spatialization of elementary flows,
we would thus have Chilean water coming out and RoW water coming in. This creates an imbalance as Chilean water might be 
more scarce than the global average (for the RoW region). In reality, the water from irrigation likely is also Chilean 
water. It's just that there is no Chilean irrigation process in the ecoinvent database. <br>
So the solution is actually quite simple. Let's create those missing processes to ensure the coherence. Regioinvent 
does just that. It focuses on 7 identified technosphere water flows where similar issues arise: ['irrigation', 
'water, deionised', 'water, ultrapure', 'water, decarbonised', 'water, completely softened', 'tap water', 
'wastewater, average', 'wastewater, unpolluted']. This creates more than 13,000 processes.

### Connecting ecoinvent processes to UN COMTRADE commodities
This step is basically a giant mapping effort to be done between the names of ecoinvent commodities selected for 
regionalization (e.g., "1-propanol") and UN COMTRADE commodity codes (e.g., 290512: "Alcohols; saturated monohydric, 
propan-1-ol (propyl alcohol) and propan-2-ol (isopropyl alcohol)"). This mapping was first based on the existing mapping
provided in the ecospold files as of the 3.10 of ecoinvent. However, the mapping of ecoinvent is lacking in many aspects.
It was thus entirely redone "manually" with the help of ChatGPT4.0. While previous versions of chatGPT struggled with
mapping correctly, chatGPT4.0 was capable of doing it reliably. Each mapping was checked again by the programmer.

This mapping can be found in the Data/Regionalization/ei3.x/ecoinvent_to_HS.json files.

### Connecting ecoinvent geographies to UN COMTRADE geographies
Ecoinvent relies on the ISO 2-letter country code while the UN COMTRADE relies on the ISO 3-letter country code system.
Furthermore, COMTRADE has a better resolution in terms of countries than ecoinvent, so there are cases when there is not
a match between the countries in both databases. A mapping is therefore required. 

This mapping can be consulted here: Data/Regionalization/ei3.x/COMTRADE_to_ecoinvent_geographies.json

### Connecting EXIOBASE sectors to UN COMTRADE commodities
Another mapping step. Similarly to the one with ecoinvent, the mapping was conducted manually with the help of chatGPT4.0.
EXIOBASE is mainly based on the CPAv2.1 classification and is used in the domestic consumption estimation approach with 
the national production/export ratios.

This mapping can be found in the Data/Regionalization/ei3.x/HS_to_exiobase_name.json files.

### Connecting EXIOBASE geographies to UN COMTRADE geographies
EXIOBASE relies on the ISO 2-letter country code while the UN COMTRADE relies on the ISO 3-letter country code system.
Furthermore, COMTRADE has a much better resolution in terms of countries than EXIOBASE, so there are cases when there 
is not a match between the countries in both databases. A mapping is therefore required. 

This mapping can be consulted here: Data/Regionalization/ei3.x/COMTRADE_to_exiobase_geographies.json

