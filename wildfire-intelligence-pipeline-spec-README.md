# 🔥 Wildfire Intelligence Pipeline: Architecture & Engineering Specification

**A Complete Technical Design Reference for System Implementation**

Developed by Austin Addington Berlin | AECE Omnis LLC
[LinkedIn](https://linkedin.com/in/austinberlin) · [GitHub](https://github.com/Austin-AECEomnis) · [aeceomnis.com](https://aeceomnis.com)

This repository documents original architectural thinking developed independently by AECE Omnis LLC and is shared freely for public benefit. No proprietary data, employer resources, or third-party confidential information was used in developing this specification. All vendor and platform names referenced (LANDFIRE, NWS, RAWS, WindNinja, Shovels, Regrid, FlamMap, FARSITE, BehavePlus, Snowflake, PostgreSQL/PostGIS, Apache Airflow, GitHub Actions) are independent, publicly documented tools and data providers, not proprietary to any single company, and are used here as illustrative implementation choices for a generalized architecture.

**Release note:** This is a generalized public release of an architecture originally developed in the context of a specific commercial data platform. Company-specific product names and any named individuals have been replaced with generic architectural equivalents throughout. The underlying design and technical reasoning are unchanged from the original specification.

---

## 📋 Executive Summary

This document provides the complete engineering specification for a unified, automated fire behavior modeling pipeline that serves two distinct end-user populations from one underlying engine through a single MCP natural language interface. It is written with sufficient technical detail for an engineering team to implement each component independently, and with sufficient architectural context for those engineers to understand how every component connects to the whole.

The pipeline addresses a fragmentation problem that exists in every fire behavior modeling workflow today: a skilled analyst must manually touch six or more separate systems in sequence, with a handoff at every step. LANDFIRE data is acquired manually. Weather inputs are looked up from separate sources. WindNinja runs as a standalone desktop tool. FlamMap or FARSITE executes in IFTDSS or on a local desktop. Outputs are loaded into ArcGIS Pro manually. Risk scores are produced through a separate analytical step. No component knows what any other component produced.

This specification replaces every manual handoff with automation, wraps the entire pipeline in a defensive architecture that catches failures before they propagate, and exposes the results through an MCP interface that gives both a fire scientist running an on-demand simulation and an insurance underwriter querying pre-computed portfolio exposure the same clean natural language interaction with the same underlying engine.

One principle runs underneath every layer described above: a risk score or fire behavior surface is only as valuable as how current the inputs behind it are. A dataset refreshed once a year describes where risk generally sits. It cannot describe what is happening on the ground this hour. This pipeline treats currency as a first-class design requirement rather than an afterthought. The event-driven WindNinja trigger in Section 3.2 recalculates terrain-adjusted wind fields the moment conditions actually shift, not on a fixed timer. The hourly NWS and RAWS ingestion in Sections 1.2 and 1.3 keeps weather and fuel moisture inputs as fresh as an operational forecast. The on-demand interactive lane in Section 9.3 compresses what would otherwise be fifteen or more minutes of manual data assembly into a single request, so a live simulation reflects present conditions rather than a stale snapshot from the last scheduled run. The same currency discipline that makes a pre-computed portfolio score defensible to an underwriter also makes an on-demand simulation trustworthy to a fire scientist or emergency responder making a real-time operational call. This architecture was not designed to choose between speed and freshness. It was designed to deliver both from the same underlying engine.

**Version 2** of this specification introduces a significant architectural addition: a pre-APN structure identification pipeline that produces scored parcel records for newly permitted structures before a county assessor assigns a permanent Assessor Parcel Number. Triggered by the Shovels permit API on a bi-monthly cadence, the subsystem retrieves subdivision plat PDFs from county recorder endpoints, extracts lot boundary geometry, scores lots against existing fire behavior surfaces, and attaches Shovels structural attributes, all within the 15-day Shovels capture window and ahead of the assessor lag that can run months. This closes a gap no current commercial wildfire risk product has addressed: the period between permit issuance and APN assignment during which new WUI structures are invisible to every APN-keyed scoring system.

**Version 2.1 refinement:** following publication of Version 2, verification against the Shovels data dictionary confirmed two capabilities that refine the pre-APN subsystem without altering its architecture. First, every Shovels permit record carries vendor-geocoded coordinates (LAT/LONG in warehouse delivery, `address.latlng` in the API, both marked "Enhanced by Shovels"), meaning the address geocoding fallback described in Sections 1A.10, 2.3, and 6.4 is consumed as a field from the vendor rather than built as a pipeline component. Second, the Shovels Decisions dataset tracks Final Plat approvals as a distinct geocoded decision category, providing a subdivision-level trigger that fires before any individual building permit exists. Both refinements are annotated inline in the affected sections. The Version 2 text is retained unmodified so the design lineage from first-principles assumption to vendor-verified capability remains visible.

**Version 2.2 refinement:** this revision adds one new section and three inline annotations. Section 1.6 documents the one acquisition mechanism this specification cannot resolve from outside a specific implementing organization (how commercial parcel geometry becomes locally available for the Section 6.2 spatial join), labeled explicitly as an unknown internal systems process rather than a design decision, with companion annotations at Sections 6.2 and 7.4 showing the downstream architecture is unaffected by whichever mechanism resolves it. Two further annotations tighten technical precision: Section 1.3 clarifies that the 100-hr timelag class is time-integrated running state rather than a per-cycle calculation, and Section 5.5 documents the actual mechanics of the Auto-97th percentile process (ERC-G percentile ranking, two-stage moisture initialization and forward conditioning, and pyrome-calibrated extreme weather selection). As with Version 2.1, all prior text is retained unmodified.

The architecture was derived through first-principles reasoning applied to the specific technical, data, and operational characteristics of the fire behavior modeling and wildfire risk scoring domain. Every named concept in this specification represents a deliberate design decision with a documented rationale.

---

## 🏗️ Architecture Overview

The pipeline is organized into nine primary functional layers plus one parallel subsystem. The layers communicate through file-based outputs and structured API calls rather than tight coupling, which allows each layer to be tested, replaced, or scaled independently.

| Layer | Name | Responsibility |
|---|---|---|
| 1 | Data Sources | External data acquisition from LANDFIRE, NWS, RAWS, WindNinja, and Shovels at source-specific refresh cadences |
| 1A | Pre-APN Structure ID | Parallel subsystem: Shovels-triggered plat retrieval, PDF geometry extraction, and pre-APN scoring for new construction in priority WUI counties |
| 2 | Defensive Architecture | Schema validation, circuit breakers, fallback hierarchy, and daily health checks at every ingestion boundary |
| 3 | Orchestration Engine | Refresh-aware dependency graph managing scheduling relationships between all layers including the pre-APN subsystem |
| 4 | National Tiling | Checkerboard grid dividing the WUI into manageable AOI tiles for parallel batch processing |
| 5 | Simulation | Landscape file assembly, weather file assembly, and fire behavior simulation execution (FlamMap, FARSITE, BehavePlus) |
| 6 | GIS Processing & Scoring | Headless reclassification, parcel join, four-source data currency model, GeoAI scoring, and SHAP attribution |
| 7 | Pre-Computed Storage | APN-indexed and pre-APN record storage with version tagging, quality flags, and temporal inconsistency detection |
| 8 | Data Delivery | Structural attribute enrichment and columnar/commercial data delivery |
| 9 | MCP Interface | Natural language interface exposing both the on-demand interactive lane and the pre-computed batch lane |

The architecture implements a **two-lane design**. The on-demand interactive lane serves fire scientists who need live simulation against current conditions for a specific AOI. The pre-computed batch lane serves insurance clients querying stored risk surfaces across portfolio APNs. Both lanes are exposed through the same MCP server. The end user has no awareness of which lane their query is routed to.

---

## Section 1: Data Sources Layer

Each data source operates on its own refresh cadence. The orchestration engine manages these cadences independently rather than applying a single uniform timer to all sources. A source failing to deliver on schedule does not halt the others.

### 1.1 LANDFIRE LFPS

LANDFIRE provides the static landscape foundation: vegetation type, canopy structure, surface fuel models, and terrain derivatives at 30-meter resolution nationally.

- **Endpoint:** LANDFIRE Product Service (LFPS) REST API at `lfps.usgs.gov/api`
- **Authentication:** none required
- **Request pattern:** POST JSON payload containing `Layer_List` (semicolon-delimited product codes), `Area_of_Interest` (bounding box in decimal degrees), `Email` (notification), and `Output_Projection`
- **`Output_Projection` parameter:** must be set to **5070** for every job submission. Without this parameter LFPS returns a localized Albers CRS centered on the submitted bounding box, producing a different coordinate reference system definition on every pull. This breaks any pipeline that assumes a consistent CRS across runs. EPSG:5070 (NAD83 Conus Albers, standard parallels 29.5 and 45.5, central meridian -96, latitude of origin 23) is the correct fixed output projection for all national WUI coverage work.
- **Job pattern:** submit returns a `jobId`, poll the status endpoint on a 5-second interval until status is `Succeeded`, download the `outputFile` URL, then extract the ZIP
- **Output:** multi-band GeoTIFF, 16-bit signed integer, 30-meter resolution, NoData value -9999
- **Refresh cadence:** annual, triggered by new LANDFIRE version release. The current production band set is 11 bands (see Section 5 for band definitions)

> **API version note:** LFPS migrated from v1 to v2 in 2024. The v1 endpoint and its job ID field naming convention are no longer supported. Pin the implementation to the v2 base URL. If a future migration occurs, the versioned API contract pattern (see Section 2) will catch it before it reaches production.

### 1.2 NWS Weather.gov API

- **Endpoint:** `api.weather.gov` (public REST API, no authentication required)
- **Request pattern:** submit AOI centroid as lat/lon to the `points` endpoint to receive a grid reference, then request hourly forecast from the `forecast/hourly` endpoint for that grid
- **Output fields consumed:** temperature, relativeHumidity, windSpeed, windDirection, probabilityOfPrecipitation at hourly intervals
- **Output format:** JSON response transformed to `.WXS` Weather Stream ASCII format consumed by FlamMap and FARSITE
- **Refresh cadence:** hourly. NWS is the only source that both feeds the simulation directly and triggers another source (WindNinja) to run

### 1.3 RAWS / Synoptic Data API

- **Provider:** Synoptic Data (formerly MesoWest), aggregating Remote Automated Weather Stations operated by USFS, BLM, and state agencies
- **Authentication:** API key required
- **Request pattern:** query nearest RAWS station to AOI centroid, retrieve current observations for temperature and relative humidity
- **Fuel moisture derivation:** dead fuel moisture values (1-hour, 10-hour, 100-hour timelag classes) are derived simultaneously from the same RAWS temperature and humidity inputs using NFDRS equations published by the USDA. All three timelag classes are parallel outputs of the same inputs, not alternatives executed sequentially. Results populate the `.FMS` Initial Fuel Moisture file required for FlamMap runs.

> **Version 2.2 refinement:** the parallel-outputs description above is retained but understates a real distinction in how the classes respond. Each timelag class is defined by the time required for that fuel size to move approximately 63.2 percent of the way toward a new equilibrium moisture content when conditions change (1-hr fuels: 0–0.25 inch diameter, 10-hr: 0.25–1 inch, 100-hr: 1–3 inches). The 1-hr and 10-hr classes equilibrate quickly enough that current RAWS temperature and humidity readings largely determine their values each cycle. The 100-hr class is a running, time-integrated state carried forward across cycles rather than freshly computed from a single observation, since a fuel that takes days to equilibrate cannot be meaningfully derived from one snapshot. The NFDRS 1000-hr class (3–8 inch fuels, requiring 30–45 days of prior observations to stabilize) is not consumed by the `.FMS` file, which takes only the three classes listed, and is therefore intentionally outside this pipeline's ingestion scope.

- **Live herbaceous moisture:** site-specific and seasonally variable. Cannot be reliably automated from RAWS observations alone. Requires scenario-based values configured by fire science personnel in the scenario library (see Section 5).
- **Refresh cadence:** hourly, running alongside the NWS pull

### 1.4 WindNinja

- **Nature:** WindNinja is a desktop application with a command-line interface (CLI), not a web API. The pipeline calls it via Python subprocess, passing arguments that replicate what a user would enter in the GUI.
- **Inputs:** elevation GeoTIFF (Band 1 from the LFPS acquisition output), initial wind speed and direction from the current NWS hourly pull, atmospheric stability parameters
- **Output:** spatially resolved wind grid files written to a specified output directory, accounting for terrain channeling and acceleration effects that a uniform single-value wind input cannot capture
- **Trigger pattern:** event-driven, not calendar-driven. The orchestration engine compares each NWS hourly pull against the prior hour's values. If wind speed or direction differs beyond a defined threshold, the orchestration engine fires the WindNinja subprocess with the new values. If wind conditions are materially unchanged, WindNinja does not rerun.
- **Installation requirement:** WindNinja must be installed on the host machine running the pipeline. It is not a service that can be called remotely.

### 1.5 Shovels Permit API

Shovels serves a fundamentally different function from all other data sources in Section 1. It is not a fire behavior input. It is the trigger for the pre-APN structure identification subsystem (Section 1A).

- **Provider:** Shovels.ai, aggregating building permit records from 1,800+ jurisdictions nationwide, covering approximately 85 percent of the US population
- **Authentication:** API key required
- **Endpoint:** REST API
- **Refresh cadence:** bi-monthly. Shovels updates its database on the 1st and 15th of each month, producing a maximum 15-day lag between a permit filing at the jurisdiction level and its appearance in the Shovels database.
- **Filter applied:** new construction permits only. Shovels classifies permit types using LLM-based classification condensing 600,000+ jurisdiction-specific variations into 22 standardized categories. The pipeline filters for permits classified as new construction with status `in_review` (filed but not yet approved) or `active` (approved, construction authorized).
- **Output fields consumed:** `file_date`, `permit_number`, address fields (street, street_no, city, zip, state), subdivision name and lot/block reference (from description or legal description fields), residential flag, construction type, roofing material, utility service type, `owner_name`, Shovels `address_id` (maps to a persistent parcel identifier via the parcel-permit linkage relationship described in Section 1.6)

> **Version 2.1 refinement:** the consumed field list above omits two verified fields. Every permit record also carries `LAT` and `LONG` (the geocoded latitude and longitude of the permit address, delivered as `address.latlng` in the API), geocoded by Shovels rather than passed through from the jurisdiction. Consequence for this specification: wherever the fallback paths below reference address geocoding as a fallback geometry source, the pipeline consumes the Shovels-provided geocode directly rather than operating its own geocoding service. This removes a component from the build: no geocoder dependency, no geocoder API contract, no geocoder health check.

- **Role in pipeline:** Shovels permit records serve as the search query for county recorder endpoint retrieval. The subdivision name and lot/block reference on each new construction permit are the inputs to the county recorder plat lookup. Shovels also provides the structural attributes that attach to the extracted lot geometry after the join.

> **Shovels coverage note:** Shovels covers 85 percent of the US population. The 15 percent gap is concentrated in small rural jurisdictions with limited permit digitization. In the priority WUI county scope (Section 1A.2), coverage is materially higher because the priority counties are in states with more developed county digital infrastructure.

### 1.6 Commercial Parcel Data Acquisition *(Unknown Internal Systems Process)*

A commercial nationwide parcel dataset (e.g. Regrid) provides the parcel polygon layer that the fire behavior surface is spatially joined against in Section 6.2. Providers of this kind commonly embed a persistent parcel identifier directly in their parcel datasets, accessible via a parcel-permit linkage relationship referenced throughout this specification. What has not been established, and cannot be established without direct access to a specific implementing organization's own systems, is the specific mechanism by which that parcel geometry becomes locally available to this pipeline for the Section 6.2 join to execute at national scale.

**Unknown internal systems process:** how commercial parcel geometry physically lands inside the implementing organization's GIS environment ahead of the Section 6.2 spatial join. Three plausible mechanisms, consistent with a typical vendor data-linking relationship and Section 4's parallel processing requirement (up to 50 simultaneous tiles per national pass):

- **Periodic bulk delivery** into an internally maintained enterprise geodatabase feature class, refreshed on the vendor's county-assessor-tied cadence (Section 6.3). Most compatible with the high-throughput parallel tile joins described in Section 4, since geometry would already be locally indexed at join time rather than fetched per parcel.
- **A live feature service** queried per tile at join time. Would require the same schema-validation and circuit-breaker contract already defined for every other source in Section 2, plus rate-limit and latency validation against Section 4's throughput target before being confirmed compatible at national scale.
- **Direct query through the vendor's linkage relationship** at scoring time, bypassing local storage entirely and treating parcel geometry as a call made alongside the structural-attribute enrichment already described in Section 6.5.

**Why the pipeline remains intact regardless of which mechanism resolves this section:** Sections 6.2, 6.3, and 7.4 were all designed against commercial parcel geometry as an abstract input, a polygon layer available at join time, not against any specific delivery mechanism. The spatial join logic in 6.2, the source-competition currency model in 6.3, and the selective PostGIS scope in 7.4 do not change based on how the geometry arrives. Resolving this gap adds a new acquisition subsystem parallel in structure to Sections 1.1 through 1.5, and, if a live feature service is confirmed, a corresponding schema-validation and circuit-breaker contract in Section 2. It does not require redesigning any downstream layer.

This is the kind of question that can only be resolved by whoever owns the target organization's parcel data vendor relationship internally. It is not a gap this specification's own independent research process could have closed from outside the organization.

---

## Section 1A: Pre-APN Structure Identification Pipeline

The pre-APN structure identification pipeline is a parallel subsystem that runs independently of the main fire behavior simulation pipeline. It does not replace or alter the simulation pipeline. It adds a new capability: producing scored parcel records for newly permitted structures before the county assessor assigns a permanent APN.

**The problem it solves:** every APN-keyed wildfire risk scoring system is blind to structures during the assessor lag window, the period between permit issuance and APN assignment that can span several months to over a year in active development markets. During this window a structure moves from permit application through foundation, framing, and occupancy without appearing in any APN-keyed dataset. It is invisible to commercial parcel datasets, invisible to parcel-permit linkage relationships, and invisible to every downstream insurance scoring product. In WUI areas where new construction is concentrated in the highest-risk zones, this gap is not a minor edge case. It is a systematic blind spot in the most important part of the insured portfolio.

The subsystem is triggered by Shovels permit records, retrieves the corresponding subdivision plat from the county recorder, extracts lot boundary geometry via automated PDF analysis, scores the lot against existing fire behavior surfaces already stored in Layer 7, and produces a complete scored parcel record using the Shovels `address_id` as a temporary persistent identifier until the APN is assigned.

### 1A.1 Shovels as the Trigger

On the 1st and 15th of each month, the orchestration engine queries the Shovels API for new construction permits filed since the prior pull date. Each returned record represents a structure that is planned or in progress somewhere in the priority county scope.

- The `in_review` status (permit filed, not yet approved) is the earliest available signal. A permit `in_review` has been filed by the builder with the jurisdiction. Construction has not started. This is the moment at which the risk profile of the structure is first knowable from public records.
- The permit record contains the subdivision name and lot/block reference in the address or legal description fields. These two fields are the search terms for the county recorder endpoint query.
- Records without a subdivision name and lot/block reference (large rural acreage parcels described by metes and bounds or PLSS) cannot be processed by the plat extraction subsystem and are routed to the NG911 or address geocoding fallback path (see Section 1A.10).

> **Version 2.1 refinement:** the Shovels Decisions dataset tracks municipal land-use decisions as geocoded records, and its category taxonomy includes **Final Plat** as a distinct decision type with lot acreage and coordinates. A Final Plat approval is an earlier signal than any building permit: it marks the moment the subdivision plat itself is recorded, before individual lots begin permitting. This enables a batch pre-extraction pattern the Version 2 per-permit trigger does not capture: retrieve the plat once at Final Plat approval, extract geometry for every lot in the subdivision in a single pass, and store the polygons so that individual permits arriving over the following months join instantly to already-extracted geometry. The per-permit trigger above is retained as the path for subdivisions platted before pipeline deployment. The Decisions trigger is the optimization for all subdivisions platted after it.

### 1A.2 Priority County Scope

The initial deployment of the pre-APN subsystem targets 150–200 counties in the highest wildfire risk states rather than attempting national coverage from day one. This scope reduction is both a practical engineering decision and the correct commercial decision: these counties represent approximately 80 percent or more of WUI-exposed insured property value nationally, making them the highest-value scope and the most defensible starting point.

- **Priority states:** California, Colorado, Oregon, Washington, Arizona, Texas, Montana, Idaho
- **California priority counties** *(representative, not exhaustive)*: Los Angeles, San Diego, Riverside, Placer, El Dorado, Butte, Sonoma, Santa Barbara, Napa, Ventura, Nevada, Tuolumne
- **Colorado priority counties:** Boulder, Jefferson, Larimer, El Paso, Douglas, Clear Creek, Gilpin, Teller
- **Texas priority counties:** Kendall, Bastrop, Hays, Comal, Travis, Burnet, Llano, Kerr
- **Oregon priority counties:** Jackson, Josephine, Klamath, Douglas, Lane, Deschutes
- **Washington priority counties:** Chelan, Okanogan, Spokane, Kittitas, Yakima
- **Montana and Idaho priority counties:** Flathead, Missoula, Ravalli (MT), and Ada, Canyon, Kootenai (ID)

**Scope expansion rationale:** endpoint connection patterns within a state generalize because counties within the same state often procure from the same document management vendor. The scraper or API integration built for Los Angeles County typically works with minor modification for other California counties using the same underlying system. The priority scope builds the pattern library. National expansion applies the library.

### 1A.3 County Recorder Endpoint Tiers

County recorder offices vary significantly in how they expose recorded plat documents. The pipeline implements a three-tier connection architecture that routes each county to the most automated available connection method.

- **Tier 1 (REST API):** counties with modern open data portals or document management systems expose recorded plats through queryable REST APIs or ArcGIS Feature Service endpoints. Submit subdivision name or plat book and page number, receive a PDF download URL programmatically. California, Colorado, and Arizona have significant Tier 1 county presence.
- **Tier 2 (Web Scraper):** counties with queryable web portals where document retrieval is triggered through HTML form submission. Automated via `requests` and BeautifulSoup for static pages, Selenium for JavaScript-rendered pages. The majority of mid-tier county recorders fall into this category.
- **Tier 3 (Commercial Aggregator):** counties whose recorder systems sit behind authenticated portals, proprietary document management platforms, or legacy systems that resist scraping. Accessed through commercial document aggregators that license access to recorded documents across multiple counties through a single authenticated connection. One licensing relationship opens hundreds of counties simultaneously.
- **Gap flag:** counties that cannot be reached through any of the three tiers are flagged as coverage gaps in the county endpoint index, logged with a timestamp, and monitored for infrastructure improvements. New construction permits in gap counties are routed to the address geocoding fallback with a low positional confidence flag.

### 1A.4 County Endpoint Index

The county endpoint index is a lookup table maintained by the pipeline keyed by county FIPS code.

- **Fields:** county FIPS code, county name, state, endpoint tier (1/2/3/gap), endpoint URL or aggregator name, authentication method, last successful connection timestamp, connection status (active/degraded/gap), document management vendor if known, notes on endpoint-specific behavior
- The vendor field enables generalization: if Tier 2 counties in Texas predominantly use the same document management platform, a scraper built for one county on that platform is a template for all counties in the priority scope using it.

### 1A.5 PDF Classifier

Every plat PDF retrieved from a county recorder endpoint passes through a classifier before geometry extraction is attempted, using PyMuPDF.

- **Vector PDF classification:** if the PDF contains embedded vector paths or extractable text with coordinate values, it is classified as vector and routed to the geometry extraction path (Section 1A.6).
- **Raster PDF classification:** if the PDF contains only rasterized image content with no extractable text layer, it is classified as raster and routed to the exception queue (Section 1A.9).
- For the defined scope of this pipeline (current filings only), vector PDF is the expected and dominant case. Modern surveying platforms (AutoCAD Civil 3D, Carlson Survey, MicroStation) produce vector PDFs by default. A county still producing raster scans of current plat filings represents an anomaly in any active WUI development market.

### 1A.6 Vector PDF Geometry Extraction

Two extraction methods are applied in sequence. Method 1 is attempted first. If it does not yield usable coordinates, Method 2 is applied.

**Method 1: Coordinate Table Extraction**
Many vector plat PDFs contain an explicit coordinate table listing state plane or geographic coordinates for each lot corner as text embedded in the document. PyMuPDF or pdfplumber extracts this table directly. Coordinates in state plane systems are converted to WGS84 using `pyproj`. Output is a point dataset of lot corners that is assembled into boundary polygons.

**Method 2: COGO Boundary Traversal**
If no coordinate table is present, the bearing and distance calls from the legal boundary description are extracted from the PDF text layer. These calls are present on every legally recorded plat as a mandatory recording requirement. No plat can be recorded without a complete boundary description. A COGO (Coordinate Geometry) traversal walks each bearing and distance mathematically from a starting coordinate (the tie-out statement connecting the subdivision to a known survey monument or section corner) to reconstruct the full lot boundary polygon for each lot.

**Output of either method:** full lot boundary polygons in WGS84 for each lot in the subdivision, geometrically equivalent to polygons derived from a commercial parcel layer because both are derived from the same legally binding boundary description on the same recorded document.

> A manually digitized subdivision plat in ArcGIS Pro (the workflow seen in surveying tutorials) uses exactly the same bearing and distance calls to reconstruct the polygon. Method 2 replicates that manual process programmatically. The output geometry is identical because the input source is identical.

### 1A.7 Lot/Block Join to Shovels Structural Attributes

The Shovels permit record contains the subdivision name and lot/block reference as text fields in the permit address or legal description. The extracted lot polygons from the plat are identified by the same lot/block reference from the plat document. The join is a text match between the lot/block from the plat and the lot/block from the Shovels permit record.

- After the join, each extracted lot polygon carries: full boundary geometry, `file_date` from Shovels, construction type, roofing material, utility service type, `owner_name`, and Shovels `address_id` as a temporary persistent identifier.
- Lot/block joins are reliable for platted subdivisions where lot numbers are unique within a named subdivision. A lot number is not a unique identifier nationally, but the combination of subdivision name plus lot number plus block number is unique within a county.
- The Shovels `address_id` field maps directly to a commercial parcel dataset's own persistent identifier through the parcel-permit linkage relationship described in Section 1.6. This means that when the APN is eventually assigned and the commercial parcel layer updates, the pipeline can match the pre-APN record to the new parcel via `address_id` without requiring address string matching.

### 1A.8 Scoring Against Existing Fire Behavior Surfaces

The extracted lot polygon is scored against the pre-computed fire behavior surfaces already stored in Layer 7. This is the same scoring process that runs for APN-keyed parcel records (Section 6) applied to the extracted lot geometry.

- Zonal statistics are computed across the full lot polygon extent rather than at a single centroid point, producing fire behavior values that reflect the actual lot area. A full polygon is meaningfully more accurate than a centroid for lots that span slope transitions, fuel model boundaries, or canopy edge zones.
- GeoAI scoring and SHAP attribution run against the enriched lot record exactly as they would for an APN-keyed parcel record. The scoring model receives the same feature set: fire behavior values, structural attributes from Shovels, terrain attributes from LANDFIRE bands.

### 1A.9 Pre-APN Record Storage

The scored pre-APN record is written to the storage layer alongside APN-keyed records, distinguishable by a pre-APN flag.

- **Primary identifier:** Shovels `address_id` (temporary until APN assigned)
- **Additional identifiers:** subdivision name, lot number, block number, county FIPS code, Shovels `permit_number`
- **Stored fields:** full lot boundary geometry, `file_date` from Shovels, fire behavior values from zonal statistics, GeoAI risk score, SHAP attribution, Shovels structural attributes, pre-APN flag, version tag, quality flags consistent with all other stored records
- **APN backfill:** when the assessor assigns a permanent APN and the commercial parcel layer updates to reflect it, the pipeline matches the pre-APN record to the new parcel via the Shovels `address_id` mapping and promotes the record to standard APN-keyed status. The pre-construction risk score is retained as historical context. The score that existed before the foundation was poured is a uniquely valuable longitudinal data point for understanding how a structure's risk profile evolves through construction.

### 1A.10 Raster PDF Exception Queue and Fallback Handling

- **Raster PDF exception queue:** plat PDFs classified as raster documents are written to an exception queue with the county FIPS code, subdivision name, and plat book/page reference. These records are flagged for periodic review. For the defined scope of current filings only, the raster exception queue is expected to be small.
- **County endpoint unavailable:** record written to a retry queue with exponential backoff. After three failed attempts, written to the gap flag log with county FIPS code and timestamp. Shovels permit record is scored using address geocoding as a fallback geometry source with a low positional confidence flag.
- **Lot/block join failure:** if the subdivision name or lot/block reference fields on the Shovels permit record are absent or malformed, the record is flagged for manual review in the exception queue. Address geocoding fallback applies.

> **Version 2.1 refinement:** in every fallback case above that references address geocoding, the geometry source is the Shovels-provided geocode already present on the permit record (see Section 1.5 refinement), not a geocoding step the pipeline performs. The low or moderate positional confidence flag is retained unchanged, with one honest caveat worth preserving in the flag logic: geocode placement is weakest precisely for brand-new addresses in brand-new subdivisions, since the address may not yet exist in geocoder reference data. Vendor-provided coordinates reduce the build burden. They do not upgrade the confidence classification.

- **No existing fire behavior surface for the tile:** if the lot falls outside the current pre-computed tile coverage, it is queued for scoring on the next national computation pass rather than being scored with incomplete data.

---

## Section 2: Defensive Pipeline Architecture

The defensive pipeline architecture is the set of safeguards that prevent failures in any external data source from propagating into the simulation and scoring layers as silent errors. The core principle: every external dependency is treated as independently failable. The pipeline detects failures at the source boundary, contains them to the affected source, and continues operation in a degraded but documented state.

This principle matters because the data dependency risk is identical whether a human runs the workflow manually or the pipeline runs it automatically. A human manually running the workflow hits the same wall when a source goes down or changes its schema. The difference is that the manual workflow produces silent, undocumented errors while the defensive pipeline produces flagged, traceable, version-tagged ones. In an insurance regulatory context, a flagged and documented output is a compliance asset. An undocumented inconsistency is a liability.

### 2.1 Schema Validation

Schema validation runs at every ingestion boundary before any transformation step. It checks that the data returned by an external source matches the expected contract: field names, data types, value ranges, and record counts.

- **LFPS:** validate that the downloaded GeoTIFF contains the expected band count, that the CRS WKT contains the AUTHORITY EPSG 5070 token, that all bands return a NoData count consistent with the AOI's geographic extent.
- **NWS:** validate that the JSON response contains all required forecast fields and that hourly records cover at least the next 24-hour window.
- **RAWS:** validate that temperature and humidity values fall within physically plausible ranges (temperature between -40 and 60 degrees Celsius, relative humidity between 0 and 100 percent).
- **Shovels:** validate that the API response contains the expected fields (`file_date`, address fields, permit type classification, `address_id`) and that the permit type classification is present. Validate that new construction records contain non-null address fields before routing to county recorder retrieval.

> **Version 2.1 refinement:** add lat/long presence and plausibility checks to the Shovels validation contract: `address.latlng` must be present and non-null on permit records routed to fallback geometry, latitude within continental US bounds (24–50), longitude within (-125 to -66). A record missing coordinates routes to the exception queue rather than receiving a null-geometry score.

- **County recorder PDF:** validate that the retrieved file is a valid PDF before passing to the classifier. A corrupted download or HTML error page returned as a PDF is caught here before reaching PyMuPDF.
- **Failure behavior:** schema validation failure raises a named exception that trips the circuit breaker for that source. It does not halt the pipeline. It logs the failure with timestamp, source name, field that failed, and received value.

### 2.2 Circuit Breakers

Each external data source has an independent circuit breaker. When schema validation or a network request fails for a given source, the circuit breaker for that source trips. The pipeline continues running on cached values from the last successful pull for that source. The circuit breaker resets on the next successful pull from that source.

- **Implementation:** a per-source state object tracking current status (open or closed), timestamp of last successful pull, and timestamp of last failure.
- County recorder endpoints operate on a per-county circuit breaker. If a specific county's endpoint fails, the pipeline routes that county's pending plat retrievals to the retry queue and continues processing other counties without interruption.
- LANDFIRE does not have a circuit breaker in the same sense because it is a scheduled annual pull rather than a continuous feed. Failed LANDFIRE pulls are retried with exponential backoff.

### 2.3 Fallback Hierarchy

When a circuit breaker trips, the pipeline falls back through a defined hierarchy rather than halting.

- **NWS weather:** Fallback 1 is the prior hour's cached `.WXS` file. Fallback 2 is NOAA climatological normals expressed as a static `.WXS` file. Output flagged with data quality annotation.
- **RAWS fuel moisture:** Fallback 1 is the prior hour's cached `.FMS` values. Fallback 2 is NFDRS standard moisture table values for the region and season. These are the same static inputs IFTDSS uses for its Auto-97th percentile runs.
- **WindNinja:** prior wind grid files are retained and flagged as stale. Simulation continues rather than reverting to a uniform single wind value.
- **Shovels:** if the bi-monthly pull fails, the pipeline retries on the next scheduled date. No cached fallback applies because permit records are additive rather than replacement. A missed pull results in a one-cycle delay in new permit ingestion, not a data gap in existing records.
- **County recorder endpoint:** after three failed retry attempts, record written to gap flag log. Shovels permit record scored using address geocoding as fallback geometry with low positional confidence flag.

### 2.4 Versioned API Contracts

External APIs change. The versioned API contract pattern prevents silent breakage when they do.

- Every API endpoint URL is pinned to a specific version path. The pipeline does not automatically follow redirects to newer versions.
- When a source releases a new API version, the upgrade is a deliberate engineering decision requiring a test cycle before the pinned version is updated in the pipeline configuration.
- The LFPS v1 to v2 migration is the specific historical case this pattern is designed to handle. A pipeline pinned to v1 and operating without version contracts would have broken silently when v1 was deprecated.

### 2.5 Daily Health Checks

A GitHub Actions workflow runs daily on a timer that hits every external endpoint with a known test query and validates the response against expected output.

- **LFPS health check:** submit a single-band small-AOI test job and validate that the response matches expected schema within a defined time window.
- **NWS health check:** query a fixed reference lat/lon and validate that hourly forecast fields are present in the response.
- **RAWS health check:** query a reference station and validate that temperature and humidity fields are present and within plausible ranges.
- **Shovels health check:** submit a test query and validate that the response contains expected permit record fields in the correct format.
- **County endpoint health check:** on a weekly rather than daily cadence, submit a test plat lookup for a known reference subdivision at each active Tier 1 and Tier 2 endpoint and validate a successful PDF retrieval.
- **Failure notification:** if any health check fails, a notification is sent immediately with the source name, timestamp, and the specific assertion that failed.

---

## Section 3: Refresh-Aware Orchestration Engine

The orchestration engine is the coordinating layer that manages the scheduling relationships between all pipeline components including the pre-APN subsystem. It is described as refresh-aware because it schedules each component at the cadence appropriate to its data source rather than applying a single uniform timer to all components.

### 3.1 Refresh-Aware Dependency Graph

The dependency graph defines which components must complete before others can run, and at what cadence each component executes. Implemented using a DAG (Directed Acyclic Graph) scheduler. Apache Airflow is the recommended implementation for production at national scale. GitHub Actions with scheduled workflows is acceptable for smaller-scale or development deployments.

| Component | Cadence | Trigger Type | Depends On |
|---|---|---|---|
| LFPS acquisition | Annual | New LANDFIRE release | None |
| NWS pull | Hourly | Scheduled timer | None |
| RAWS pull | Hourly | Scheduled timer | None |
| WindNinja | Event-driven | NWS wind change | NWS pull |
| Shovels permit pull | Bi-monthly (1st, 15th) | Scheduled timer | None |
| County recorder retrieval | Bi-monthly | Shovels pull complete | Shovels pull (new construction filter) |
| PDF classifier | Bi-monthly | PDF received | County recorder retrieval |
| Geometry extraction | Bi-monthly | Vector PDF confirmed | PDF classifier |
| Pre-APN scoring | Bi-monthly | Geometry extraction complete | Layer 7 pre-computed surfaces |
| Pre-APN storage write | Bi-monthly | Scoring complete | Pre-APN scoring |
| Simulation (FlamMap/FARSITE) | Annual + on-demand | LFPS release or request | LFPS, NWS, RAWS, WindNinja |
| GIS processing & scoring | Follows simulation | Simulation complete | Simulation layer |
| Pre-computed storage write | Follows scoring | Scoring complete | GIS processing layer |
| Commercial delivery export | Follows storage write | Storage write complete | Storage layer |
| APN backfill check | Bi-monthly | Parcel dataset update detected | Shovels pull, parcel dataset update |

### 3.2 Event-Driven Trigger Logic for WindNinja

After each NWS hourly pull completes, the orchestration engine compares the new wind speed and direction values against the cached values from the prior successful pull. If the delta exceeds the configured threshold (recommended: 10 knots in speed or 30 degrees in direction), the WindNinja subprocess is triggered with the new initial conditions. If the delta is within threshold, WindNinja does not rerun and the prior wind grid files remain in use.

---

## Section 4: National Tiling Strategy

The pipeline cannot submit a single LFPS job covering the entire continental WUI. National coverage is achieved through a checkerboard grid: a pre-defined set of tiles laid over the continental US, each representing a manageable area of interest, processed in parallel batches.

### 4.1 Tile Grid Definition

- **Tile size:** recommended 0.5° × 0.5° bounding box in geographic coordinates, equivalent to approximately 40 × 55 kilometers at mid-latitudes.
- **WUI mask:** the pipeline applies a WUI mask derived from USFS WUI boundary polygons to determine which tiles are in scope. Tiles with no WUI overlap are excluded from the pre-computation batch.
- **Tile index:** a lookup table keyed by tile ID containing geographic bounds, WUI overlap flag, current LANDFIRE version tag, pipeline version tag, last computation timestamp, and data quality flags.
- **Tile ID scheme:** a deterministic alphanumeric ID derived from the tile's southwest corner coordinates (e.g., `T_N290_W980`). Human-readable and geographically inferrable without a lookup.

### 4.2 Parallel Batch Processing

- Tiles are submitted to LFPS in parallel batches. Batch size is constrained by LFPS rate limits and available compute resources.
- Estimated per-tile processing time: 2–5 minutes for LFPS acquisition. FlamMap computation estimated at 5–30 minutes per tile depending on scenario complexity.
- With parallel processing at 50 simultaneous tiles, a full national WUI pass completes in roughly 2–6 hours of wall-clock time, fitting comfortably within the annual LANDFIRE release cycle.

### 4.3 Version Tagging

Every output produced by the pipeline carries a version tag containing: LANDFIRE version string (e.g., `LF2024`), pipeline semantic version number (e.g., `v2.0.0`), and UTC computation timestamp. Version tags travel with every output through all downstream layers into commercial delivery and MCP query responses, providing the complete regulatory audit trail.

---

## Section 5: Fire Behavior Simulation Layer

The simulation layer assembles the inputs required by FlamMap, FARSITE, and BehavePlus and executes the appropriate simulation for each tile and scenario combination. It consists of five distinct subsystems: landscape data acquisition and processing, RAT embedding, co-registration verification, weather file assembly, and simulation execution.

### 5.1 Landscape Data Acquisition

The LFPS pipeline acquires the 11-band GeoTIFF for each tile using the job-based API pattern described in Section 1.1. The production band set:

| Band | Layer Code | Product | Type | RAT Embedded |
|---|---|---|---|---|
| 1 | LF2020_Elev | Elevation | Continuous (m) | No |
| 2 | LF2020_SlpD | Slope | Continuous (deg) | No |
| 3 | LF2020_Asp | Aspect | Continuous (deg) | No |
| 4 | LF2024_FBFM40 | Fire Behavior Fuel Models | Categorical | Yes (46 classes) |
| 5 | LF2024_CC | Canopy Cover | Continuous (%) | No |
| 6 | LF2024_CH | Canopy Height | Continuous (m) | No |
| 7 | LF2024_CBH | Canopy Base Height | Continuous (m) | No |
| 8 | LF2024_CBD | Canopy Bulk Density | Continuous (kg/m³) | No |
| 9 | LF2024_EVT | Existing Vegetation Type | Categorical | Yes (1,069 classes) |
| 10 | LF2024_EVC | Existing Vegetation Cover | Categorical | Yes (293 classes) |
| 11 | LF2024_EVH | Existing Vegetation Height | Categorical | Yes (162 classes) |

### 5.2 RAT Embedding

LFPS multi-band GeoTIFF outputs do not embed categorical attribute tables. The RAT embedding step resolves this permanently.

- Download the versioned LANDFIRE CSV for each categorical band from the LANDFIRE data server.
- **Label column detection:** LANDFIRE CSVs do not share a consistent schema across products. The implementation detects the label column dynamically as the last column before the R (red) column rather than hardcoding a column index.
- Build a GDAL Raster Attribute Table from the CSV and write it into the GeoTIFF in update mode using `gdal.GA_Update`. After embedding, category names travel with the file and ArcGIS Pro renders categorical bands with proper unique value symbology natively.

### 5.3 Co-Registration Verification

Before any simulation run, all bands must be confirmed as co-registered: identical resolution, extent, projection, and datum.

- **Verification method:** compare the NoData pixel mask of each band against band 1 using numpy array equality.
- **Test AOI requirement:** the verification AOI must include geographic features that produce real NoData values. A purely inland AOI may return all-zero NoData footprints, producing a vacuously true result. A test AOI spanning both land and open water confirmed this requirement during development.
- **Failure behavior:** co-registration failure raises a `RuntimeError` and halts. A misaligned landscape file must not proceed to simulation.

### 5.4 Weather File Assembly

- **`.WXS` Weather Stream file:** assembled from the NWS hourly JSON response.
- **`.FMS` Initial Fuel Moisture file:** mandatory text input for a FlamMap run. Populated from RAWS-derived NFDRS equation outputs.
- **WindNinja wind grids:** read by the simulation layer as the wind input for FlamMap and FARSITE, producing terrain-adjusted spatially variable wind fields.

### 5.5 Scenario Library and Auto-97th Percentile

The scenario library is a configuration file maintained by credentialed fire science personnel that defines the approved set of weather and fuel moisture scenarios for pre-computed national coverage.

- **Auto-97th percentile scenario:** the anchor entry. Represents fire weather conditions exceeded only 3 percent of the time historically for a given location. The federal planning standard used by land managers to model near-worst-case operationally realistic fire behavior.

> **Version 2.2 refinement:** the summary above is retained but the underlying mechanics are more specific than a single weather threshold. The percentile is computed on ERC-G (Energy Release Component, Fuel Model G), the NFDRS fire danger index, ranked across approximately 20 years of daily history for the location, not on raw weather values directly. The process runs in two stages: initial dead and live fuel moisture values are averaged across the historical days at or above the 97th percentile ERC-G threshold, then conditioned forward through an automatically selected 1–2 week historical weather window in which most days met that same threshold, so fuels and weather evolve together through the conditioning period rather than being frozen at a single extreme value. The extreme weather stream derives from a national dataset divided into 128 pyromes, regions of similar fire regime characteristics, so the modeled extreme is calibrated to the local fire regime rather than a uniform national standard. RAWS station selection for this process is automatic, using the closest station to the landscape center, with no user override.

- **Historical analog scenarios:** named scenarios derived from historically significant fire events, providing a scenario vocabulary that is immediately interpretable by fire scientists and insurance professionals.
- **Live herbaceous moisture:** specified in the scenario library by region and season based on fire science personnel judgment. Cannot be reliably automated from RAWS observations.

### 5.6 Simulation Execution

- **FlamMap:** landscape-wide static fire behavior surface computation. Up to 22 output raster bands exported as a single multi-band GeoTIFF.
- **FARSITE:** time-stepped fire perimeter modeling. Requires an ignition point shapefile. For pre-computed coverage, standard ignition scenarios from the scenario library. For on-demand runs, fire scientist specifies the ignition point through the MCP interface.
- **BehavePlus:** point-specific scenario calculations. Tabular output. Used primarily in the on-demand interactive lane.

---

## Section 6: GIS Processing and GeoAI Scoring Layer

The GIS processing and scoring layer runs entirely headless: no GUI is opened, no ArcGIS Pro or QGIS desktop session is required. All processing executes via ArcPy or PyQGIS in standalone mode invoked by the orchestration engine.

### 6.1 Output Reclassification

- FlamMap multi-band GeoTIFF outputs are reclassified using USDA-standardized break points that convert continuous values into categorical risk tiers.
- **Flame length break points:** less than 4 feet = low, 4–8 feet = moderate to high, greater than 8 feet = high intensity (indirect attack required above this threshold per USDA standard).
- **Crown fire activity index:** 0 = surface fire, 1 = surface fire with spotting, 2 = passive crown fire (torching), 3 = active crown fire (continuous canopy spread, largely unsuppressable).
- Rate of spread, fireline intensity, and other continuous outputs are reclassified into quantile-based tiers calibrated against the national WUI distribution from the first full national coverage run.

### 6.2 Spatial Join to Parcel Polygons

- The reclassified fire behavior raster is spatially joined to commercial parcel polygons covering the tile extent. Each parcel acquires the fire behavior values for its location, using the zonal statistics pattern.
- **APN acquisition:** the spatial join attaches the Assessor Parcel Number from the parcel polygon to each parcel's fire behavior record. The APN is the universal join key for all downstream attribute enrichment.
- Parcels spanning tile boundaries are handled by the tile index: the orchestration engine checks whether adjacent tiles share the same version tag before finalizing any cross-tile parcel join (see Section 7.2 on temporal inconsistency).

> **Version 2.2 annotation:** this step assumes commercial parcel geometry is already locally accessible at the point of join. The specific acquisition mechanism that makes this true is an unknown internal systems process, addressed in Section 1.6. The join logic itself, and everything downstream of it in Section 6, is unaffected by which mechanism resolves the gap.

### 6.3 Data Currency Model and Source Competition

The data currency model governs how the pipeline selects the best available record for each parcel's structural attributes when multiple sources provide overlapping data. Currency refers to data freshness measured by record timestamp. The model implements source competition: each source competes for each parcel record based on currency, and the most recently updated source providing the most geometrically accurate record wins.

The pipeline operates a four-source currency model reflecting the complete property lifecycle from permit issuance through assessed parcel status.

| Source | Window | Cadence | Persistent ID | Positional Confidence |
|---|---|---|---|---|
| Shovels + plat extraction | Pre-APN, platted subdivisions | Bi-monthly | Shovels `address_id` (temporary) | HIGH (legal survey geometry) |
| NG911 (where a linkage relationship exists) | Pre-APN, outside platted subdivisions | County-specific | NENA GUID | VARIABLE, LOW for large acreage (gate vs. structure issue) |
| Commercial parcel dataset + APN via linkage relationship | Post-APN, full lifecycle | County assessor release cycle | APN (permanent) | HIGH for platted lots, VARIABLE for large rural parcels |
| Pre-APN promoted to APN-keyed | APN assignment event | Follows parcel dataset update | APN (backfilled) | Inherits from original source |

**Currency check:** for each parcel, the pipeline queries the timestamp of the most recent record update from each available source. The source with the most recent timestamp for that specific parcel wins the currency check and provides the structural attributes for that parcel's scoring record. When timestamps are identical or within a defined tolerance window, Shovels plus plat extraction takes precedence over NG911 for platted subdivisions (full polygon geometry versus a point), and the commercial parcel dataset takes precedence over NG911 for geometry once the APN is assigned.

The architecture does not rely on a single authoritative data stream. No single source provides complete, current, and positionally accurate parcel-level structure coverage across the full WUI property universe at all points in time. LANDFIRE does not know a structure exists. A commercial parcel dataset knows it exists but only after the assessor processes it, which can lag months behind permit issuance. Shovels knows a structure is coming before the assessor does but needs the county recorder plat for geometry. NG911 provides the earliest spatial anchor in certain cases but carries variable positional accuracy depending on how the county engineer placed the address point. Each source is authoritative in its own window and insufficient outside it. The architecture treats each source as the best available option for its specific window in the property lifecycle, routes each parcel record to the most current and geometrically accurate source available at that moment, and hands off to the next source as the parcel matures. The result is parcel-level structure coverage that is more complete, more current, and more positionally accurate than any single stream could produce alone. That completeness is what makes the downstream GeoAI risk score defensible at the individual property level, rather than dependent on geographic proxies that cannot distinguish between a wood-shake-roofed structure on a south-facing slope with active gas service and a metal-roofed structure on flat ground two streets away.

### 6.4 Positional Confidence Flagging

Positional confidence is the second quality dimension evaluated alongside data currency. Currency asks how recent is this record. Positional confidence asks does the XY coordinate or polygon boundary actually represent where the structure is located.

- **Plat extraction (HIGH):** lot boundary polygons reconstructed from legally recorded survey documents carry HIGH positional confidence. The boundary coordinates are derived from a survey performed by a licensed surveyor with legal precision requirements. This is the highest positional confidence available from any automated source.
- **Commercial parcel polygon for platted lots (HIGH):** parcel polygon boundaries for platted subdivision lots are derived from the same recorded plats and carry the same HIGH positional confidence.
- **Address geocoding (MODERATE):** street address geocoding places a point at the approximate location of the address along the street centerline. Suitable for urban and suburban parcels where the structure is near the street. Not suitable for rural large-acreage parcels.

> **Version 2.1 refinement:** the geocoded point in this tier is supplied by Shovels on the permit record itself (Enhanced by Shovels), not produced by a pipeline geocoding step. The MODERATE classification and the rural large-acreage exclusion are retained exactly as written.

- **NG911 point for large-acreage parcels (LOW):** county engineers in rural areas have documented placing NG911 address points at property access points rather than structure locations for EMS navigation reasons. On a 100-acre rural parcel the gate and the structure can be separated by a mile or more, crossing multiple fuel model classifications and terrain features. The pipeline flags parcels above a defined acreage threshold (recommended: 10 acres initial threshold) as low positional confidence. The NENA dual-point standard already provides the solution: two distinct GUID-keyed points per property, one at access and one at structure. County-level adoption of this standard is currently inconsistent and is monitored as a data quality improvement opportunity.

### 6.5 Parcel-Permit Attribute Enrichment

A pre-built linkage relationship between a commercial parcel dataset and a permit dataset (such as Shovels) is the mechanism through which structural attributes are attached to each APN-keyed parcel record without custom join engineering at query time.

- Structural attributes retrieved via APN key join through the linkage relationship: construction type, roofing material, exterior cladding material, utility service type (gas versus electric), square footage, permit date.
- Utility service type is particularly significant: active gas service creates fire behavior amplification inside a burning structure that electric-only service does not. This variable class has never previously been available in a wildfire risk score at the individual property level.
- This linkage architecture operates at the scoring layer (Layer 6) as a structural attribute enrichment step and also at the delivery layer (Layer 8) as an output enrichment mechanism. These are two distinct uses of the same architecture at different pipeline stages.

### 6.6 GeoAI Scoring and SHAP Attribution

- **GeoAI scoring model:** a trained machine learning model taking the fully enriched parcel record as input and producing a normalized wildfire risk score. Applies equally to APN-keyed records and pre-APN plat-extracted records.
- **Feature set:** fire behavior outputs (rate of spread, flame length, fireline intensity, crown fire activity index), structural attributes (construction type, roofing material, utility type), terrain attributes (slope, aspect, elevation), vegetation context (EVT, EVC, EVH from Bands 9–11), and historical fire occurrence proximity from MTBS fire perimeter data.
- **SHAP attribution:** SHapley Additive exPlanations analysis runs alongside every scoring inference, quantifying each input variable's contribution to the final score for that specific parcel, using the SHAP library.
- **Regulatory function of SHAP:** the SHAP output is the regulatory audit trail. An insurance regulator reviewing a rate-setting filing can see not just the risk score for a property but which variables drove it, in what proportion, and with what methodology. SHAP attribution travels with every scored record into the storage layer and through downstream delivery to the insurance client.

---

## Section 7: Pre-Computed Surface Storage and Temporal Inconsistency

### 7.1 APN-Indexed and Pre-APN Record Storage

The storage layer holds two classes of scored records: APN-keyed records produced by the main simulation pipeline, and pre-APN records produced by the Section 1A subsystem. Both classes use the same storage schema and carry the same version tags and quality flags. The pre-APN flag distinguishes them.

- **APN-keyed record structure:** tile ID, LANDFIRE version tag, pipeline version tag, computation timestamp, fire behavior values by band, reclassified risk tier, structural attribute values via the parcel-permit linkage relationship, GeoAI risk score, SHAP attribution vector, data quality flags, positional confidence flag.
- **Pre-APN record structure:** Shovels `address_id` (primary identifier, temporary), subdivision name, lot number, block number, county FIPS code, Shovels `permit_number`, `file_date`, full lot boundary geometry, fire behavior values from zonal statistics, GeoAI risk score, SHAP attribution, Shovels structural attributes, pre-APN flag (set to true), version tag, quality flags.
- **APN backfill:** when the assessor assigns a permanent APN and the commercial parcel dataset updates, the pipeline matches the pre-APN record to the new parcel via the Shovels `address_id` mapping and promotes the record to APN-keyed status. The pre-construction risk score is retained as longitudinal history.
- **Storage platform:** any columnar data store capable of high-throughput APN key lookups and bulk export (e.g. Snowflake) is the downstream delivery platform.

### 7.2 Temporal Inconsistency Detection and Handling

A temporal inconsistency occurs when a query spans multiple tiles that were computed under different input versions. The pipeline detects and flags this automatically.

- **Detection:** before stitching any multi-tile query result, the MCP query handler checks the version tags of all tiles in scope against the tile index. If tiles carry different LANDFIRE version tags or different pipeline version tags, a temporal inconsistency is recorded.
- **On-demand interactive lane response:** return results immediately with a data quality annotation identifying which tiles are at the current version and which are pending update.
- **Pre-computed batch lane response:** queue the query until all tiles in scope share the same version tag, then return results.
- **Weather-layer temporal inconsistency:** a lesser form of the same problem. Less severe because weather conditions change continuously and the difference between adjacent hourly pulls is typically small. Severe weather events in progress are the exception, handled the same way through version tagging and quality flags.

### 7.3 Quality Flags

Every stored record carries quality flags that travel through all downstream layers to the end user.

- **LANDFIRE version mismatch flag:** set when the tile was computed under a LANDFIRE version different from the current national release.
- **Weather fallback flag:** set when `.WXS` or `.FMS` inputs were populated from a fallback source rather than live observations.
- **WindNinja stale flag:** set when wind grid files were from a prior event-driven run rather than the current NWS hourly pull.
- **Positional confidence flag:** set when the APN's structural attributes were derived from a coordinate of uncertain positional accuracy. Includes the acreage and the source of the XY used.
- **Temporal inconsistency flag:** set when the query result was assembled from tiles with different version tags.
- **Pre-APN flag:** set on all records produced by the Section 1A subsystem that have not yet been promoted to APN-keyed status.
- **Plat extraction method flag:** for pre-APN records, indicates whether geometry was derived from coordinate table extraction (Method 1) or COGO boundary traversal (Method 2).

### 7.4 Operational Metadata Store (PostgreSQL)

Sections 7.1–7.3 describe what the pipeline delivers: scored parcel records in a columnar store optimized for high-throughput analytical reads. A separate concern is what the pipeline needs to know about itself while it is running. That is operational state, not analytical output, and it belongs in a relational database rather than the columnar store.

- **Why two databases, not one:** a columnar store (e.g. Snowflake) is OLAP-oriented: built for bulk analytical reads across millions of rows, the correct destination for delivered, scored parcel records queried by clients. PostgreSQL is OLTP-oriented: built for frequent small writes and row-level read-modify-write cycles, the correct destination for state that changes constantly during pipeline execution rather than being written once and queried in bulk afterward.
- **Circuit breaker state.** The per-source state object described in Section 2.2 (open/closed status, last successful pull, last failure timestamp) is updated on every failure and every successful pull across all sources, including the per-county circuit breakers for recorder endpoint retrieval. This write pattern is native to a relational database and poorly suited to a columnar store.
- **County endpoint index.** The lookup table described in Section 1A.4 is queried and updated continuously as the orchestration engine works through the priority county scope. Relational structure with FIPS code as a natural key.
- **Retry and exception queues.** Records in the retry queue with exponential backoff (Section 1A.10, 2.3) and the gap flag log are transactional state: written, retried, and either resolved or escalated. This is read-modify-write behavior, not append-only analytical storage.
- **Tile index, with PostGIS.** The tile lookup table described in Section 4.1 is checked before every multi-tile query for temporal inconsistency (Section 7.2) and updated after every computation pass. With PostGIS enabled, tile bounds are stored as true geometry rather than four float columns, enabling direct spatial queries (which tiles intersect a given county, which tiles a parcel spans) rather than bounding-box math performed in application code.
- **Scenario library.** The configuration file described in Section 5.5, approved weather and fuel moisture scenarios, is edited directly by credentialed fire science personnel and benefits from the referential integrity a relational schema provides for free.
- **Cache pointers, not cache payloads.** Cache references described in the fallback hierarchy (Section 2.3) do not need the heavy files themselves stored in PostgreSQL. Weather streams and wind grids belong in object storage. PostgreSQL holds the lightweight pointer: which source, what timestamp, where the file lives, whether it is flagged stale, not the file content itself.
- **No new dependency introduced.** Apache Airflow, already recommended in Section 3.1 as the production orchestration engine, uses PostgreSQL as its own metadata backend by default. The operational store described here is not a new dependency introduced on top of the orchestration engine. It is the same database Airflow already requires, extended to also hold the pipeline's own operational state alongside Airflow's job scheduling metadata.
- **PostGIS scope: selective, not blanket.** PostGIS is enabled selectively, not applied uniformly. It adds real value on the tile index and on staged pre-APN lot boundary geometry (Section 1A.6) before promotion to the columnar store, both of which are inherently spatial. It adds no value on circuit breaker state, the county endpoint index, the scenario library, or SHAP attribution vectors, none of which describe a location. Forcing spatial extension onto non-spatial data adds schema complexity without benefit.

> **Version 2.2 annotation:** whether commercial parcel geometry itself requires a cached or synced representation inside this operational store depends on which Section 1.6 acquisition mechanism is confirmed. If the parcel dataset arrives as a periodic bulk delivery, a synced parcel feature class most plausibly lives in the enterprise geodatabase environment already implied by Section 6.2 rather than in this operational metadata store, which is scoped to pipeline-internal state (circuit breakers, endpoint index, retry queues, tile index, scenario library) rather than externally sourced parcel geometry itself. If the parcel dataset instead arrives as a live feature service or direct linkage-relationship query, no local geometry caching is needed here at all, only the circuit-breaker and schema-validation state described in the Section 1.6 annotation. This determination is deferred to Section 1.6 resolution and does not change the PostGIS scope already defined for the tile index and pre-APN geometry.

---

## Section 8: Data Delivery Layer

### 8.1 Commercial Delivery

A columnar data warehouse platform (e.g. Snowflake) is the commercial delivery mechanism for insurance carriers, reinsurers, mortgage lenders, and other enterprise clients. The enriched, scored, SHAP-attributed parcel records are written to the delivery platform following each national pre-computation pass and following each bi-monthly pre-APN processing cycle.

- **Delivery format:** columnar table keyed by APN or Shovels `address_id` with all scoring fields, quality flags, and SHAP attribution vectors as separate columns. Pre-APN records are delivered alongside APN-keyed records, distinguishable by the pre-APN flag.
- **Native data sharing:** clients query the delivery platform directly without ingesting a copy. Every client is always querying the current version.

### 8.2 ArcGIS Living Atlas and State Jurisdiction Delivery

State emergency management agencies and 911 GIS coordinators operate in GIS-native environments. Fire perimeter polygon outputs are delivered through ArcGIS Online hosted feature layers published to ArcGIS Living Atlas, accessible via REST feature service URL without a data-warehouse account.

---

## Section 9: MCP Interface Layer and Two-Lane Architecture

The MCP server is the outermost layer of the pipeline, exposing its capabilities through the Model Context Protocol. It allows Claude or any MCP-compatible agent to call pipeline functions through natural language without the end user knowing which underlying systems are involved.

### 9.1 MCP Server Architecture

The MCP server exposes a defined set of tools, each corresponding to a specific pipeline capability. The agent reads the tool descriptions at runtime and decides which tools to call based on the user's natural language request, generating the appropriate tool call dynamically for any question within the tool set's scope.

### 9.2 Exposed Tool Definitions

| Tool Name | Description and Parameters |
|---|---|
| `get_parcel_fire_behavior` | Returns pre-computed fire behavior metrics for a specific APN under a named scenario. Parameters: `apn` (string), `weather_scenario` (string). Returns: risk score, SHAP attribution, fire behavior values, quality flags, version tag, pre-APN flag if applicable. |
| `get_portfolio_exposure` | Returns aggregated exposure summary for a list of APNs under a named scenario. Parameters: `apn_list` (array), `weather_scenario` (string), `aggregation_level` (string). Returns: exposure summary with risk tier distribution and quality flag summary. |
| `get_fuelscape_status` | Returns current LANDFIRE fuelscape condition for a parcel with last update date and any calibration corrections applied. Parameters: `apn` (string). |
| `get_pre_apn_pipeline_coverage` | Returns the current count and geographic distribution of pre-APN scored records by county, including records pending APN backfill. Parameters: `state` (string, optional), `county_fips` (string, optional). |
| `run_ondemand_simulation` | Triggers a live fire behavior simulation for a specific AOI with current conditions (on-demand interactive lane). Parameters: `aoi_bounds`, `simulation_type`, `ignition_point` (FARSITE only), `weather_scenario`. Returns: job ID and estimated completion time. |
| `get_simulation_result` | Retrieves the output of a completed on-demand simulation. Parameters: `job_id`. Returns: output GeoTIFF loaded into connected QGIS session via QGIS-MCP plugin, or download URL. |
| `get_risk_score` | Returns the GeoAI risk score with full SHAP attribution for a specific APN or Shovels `address_id`. Parameters: `apn` or `address_id` (string). Returns: normalized risk score, SHAP feature contributions, risk tier, structural attributes, quality flags. |

### 9.3 The On-Demand Interactive Lane

The on-demand interactive lane serves fire scientists who need live simulation results for a specific area under current or user-specified conditions. The lane is triggered when the MCP agent routes a query to the `run_ondemand_simulation` tool.

- The orchestration engine pulls the freshest available inputs for the requested AOI: current RAWS moisture, current NWS weather, WindNinja recalculated if wind conditions warrant it.
- For fire scientists connected to QGIS through the QGIS-MCP plugin, the output GeoTIFF loads directly into QGIS without the scientist touching a file system.
- What previously required 15 or more minutes of manual data entry across IFTDSS, RAWS lookup, NWS retrieval, WindNinja execution, ArcGIS Pro import, and reclassification is compressed to a single natural language request. The scientist's cognitive energy goes entirely toward interpreting results.
- **Human judgment preserved:** the fire scientist specifies the ignition point for FARSITE runs, selects the weather scenario if not defaulting to current conditions, and interprets the results.

### 9.4 The Pre-Computed Batch Lane

The pre-computed batch lane serves insurance underwriters, reinsurance analysts, mortgage lenders, and other commercial clients. Triggered when the MCP agent routes a query to portfolio exposure, parcel fire behavior, or risk score tools.

- No simulation runs at query time. Pre-computed surfaces are queried directly. Query response time is a function of storage retrieval and aggregation speed.
- Pre-APN records are accessible through the batch lane using the Shovels `address_id` if the APN has not yet been assigned. The `get_pre_apn_pipeline_coverage` tool allows clients to assess coverage in their area of interest.
- **Version consistency check:** all APNs in scope are verified to share the same version tag before returning results. Temporal inconsistencies are handled per Section 7.2.

### 9.5 QGIS-MCP Plugin Integration

The QGIS-MCP plugin establishes a direct connection between Claude and QGIS through the Model Context Protocol, creating a socket server within QGIS that receives commands from the MCP server and executes them within the live QGIS session.

- **Setup requirement:** QGIS-MCP plugin installed and enabled. Claude Desktop configuration file includes a server configuration block pointing to the MCP server. Once configured, Claude can directly manage layers, load GeoTIFFs, apply symbology, run processing algorithms, and render maps within the live QGIS session.
- **On-demand simulation delivery:** output GeoTIFF from `get_simulation_result` loads directly into QGIS without the scientist touching a file manager or import dialog.

---

## Section 10: System Boundaries and Human Judgment Requirements

The pipeline automates everything that does not require credentialed fire science judgment. What remains as human responsibility is not a failure of the automation design. It is the correct distribution of labor between a system that scales infinitely and a domain expert whose judgment is irreplaceable precisely because it cannot be scaled without it.

| Human Responsibility | Rationale for Non-Automation |
|---|---|
| Scenario library configuration | Which weather scenarios to pre-compute, what fuel moisture assumptions apply by region and season, which historical analogs to include. Requires credentialed fire science judgment to be regulatorily defensible. |
| Ignition point specification (FARSITE) | The location where a fire starts fundamentally changes every time-stepped output. This is a scientific question that belongs to the analyst specifying the simulation purpose. |
| Fuelscape calibration corrections | LANDFIRE accuracy gaps in recently disturbed areas or vegetation transition zones require a fire scientist to visually inspect suspect areas and manually correct fuel model assignments. |
| Initial system configuration | Schema validation contracts, fallback hierarchy values, circuit breaker thresholds, health check assertions, and county endpoint index initialization must be defined once per source by a human who understands what valid output looks like. |
| QA review of outputs | Final review of scored parcel surfaces before commercial delivery. The pipeline flags anomalies but a human confirms disposition of flagged records. |
| Live herbaceous moisture (scenario library) | Site-specific and seasonally variable. Cannot be reliably automated. Requires scenario-based values set by fire science personnel for each region and season. |
| Priority county scope management | The initial 150–200 county scope and its expansion over time requires human judgment about commercial priority, engineering resource allocation, and endpoint infrastructure maturity. |

The pipeline does not replace the fire scientist. It industrializes their expertise. The scenario library the pipeline pre-computes nationally exists because a credentialed fire scientist approved those scenarios. The fuelscape calibration corrections the pipeline applies exist because a domain expert identified where LANDFIRE drifted from reality. The SHAP attribution the pipeline produces is meaningful because someone who understands fire behavior designed what a valid risk signal looks like. The system carries the procedural burden. The scientist provides the domain judgment that gives the outputs their authority.

---

**Austin Addington Berlin**
Founder, AECE Omnis LLC
[LinkedIn](https://linkedin.com/in/austinberlin) · [GitHub](https://github.com/Austin-AECEomnis) · [aeceomnis.com](https://aeceomnis.com)
